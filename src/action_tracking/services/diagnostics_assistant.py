from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from action_tracking.data.repositories import (
    ActionRepository,
    AnalysisRepository,
    EffectivenessRepository,
)


DEFAULT_TRUSTED_DOMAINS = [
    "plasticstechnology.com",
    "injectionmoldingmagazine.com",
    "azom.com",
    "sciencedirect.com",
    "mdpi.com",
    "matweb.com",
]

TRUSTED_DOMAINS_FILENAME = "diagnostics_trusted_domains.json"

STOPWORDS = {
    "a",
    "an",
    "the",
    "i",
    "you",
    "and",
    "or",
    "of",
    "to",
    "in",
    "na",
    "do",
    "z",
    "za",
    "po",
    "dla",
    "oraz",
    "w",
    "we",
    "jest",
    "są",
    "się",
    "że",
    "jak",
    "przez",
    "bez",
    "with",
    "without",
    "od",
}


@dataclass(frozen=True)
class Source:
    title: str
    url: str
    domain: str
    snippet: str | None = None


@dataclass(frozen=True)
class InternalHit:
    record_type: str
    record_id: str
    title: str
    description: str
    area: str | None
    project_name: str | None
    score: float
    snippet: str
    status: str | None
    created_at: str | None
    closed_at: str | None
    effectiveness_classification: str | None


def load_trusted_domains(data_dir: Path | None = None) -> list[str]:
    """Load trusted domains list from JSON file or return defaults.

    Dev note: to update the allowlist persistently, edit it in Ustawienia Globalne.
    """
    base_dir = data_dir or Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
    path = base_dir / TRUSTED_DOMAINS_FILENAME
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, list):
            cleaned = _normalize_domains(payload)
            if cleaned:
                return cleaned
    return list(DEFAULT_TRUSTED_DOMAINS)


def save_trusted_domains(domains: list[str], data_dir: Path | None = None) -> None:
    base_dir = data_dir or Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / TRUSTED_DOMAINS_FILENAME
    cleaned = _normalize_domains(domains)
    path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")


def build_query_context(inputs: dict[str, Any]) -> dict[str, Any]:
    area = (inputs.get("area") or "").strip()
    defect_type = (inputs.get("defect_type") or "").strip()
    symptom = (inputs.get("symptom") or "").strip()
    project_name = (inputs.get("project_name") or "").strip()
    work_centers = [wc for wc in inputs.get("work_centers") or [] if wc]
    flags = [flag for flag in inputs.get("flags") or [] if flag]
    since_when = inputs.get("since_when")

    query_parts = [area, defect_type, symptom, project_name, " ".join(flags)]
    query_text = " ".join([part for part in query_parts if part]).strip()
    normalized = re.sub(r"\s+", " ", query_text.lower())
    context_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    return {
        "area": area,
        "defect_type": defect_type,
        "symptom": symptom,
        "project_name": project_name,
        "work_centers": work_centers,
        "flags": flags,
        "since_when": since_when.isoformat() if isinstance(since_when, date) else since_when,
        "query_text": query_text,
        "context_hash": context_hash,
        "is_injection": area.lower() == "wtrysk",
    }


def build_search_queries(context: dict[str, Any]) -> list[str]:
    area = context.get("area") or ""
    defect_type = context.get("defect_type") or ""
    symptom = context.get("symptom") or ""

    base_query = " ".join([part for part in [defect_type, symptom, area] if part]).strip()
    queries = [base_query] if base_query else []

    if area.lower() == "wtrysk":
        if defect_type:
            queries.append(f"root cause {defect_type} injection molding")
        else:
            queries.append("injection molding defect root cause")
    if area.lower() == "montaż":
        if defect_type:
            queries.append(f"assembly defect {defect_type} root cause")
        else:
            queries.append("assembly defect root cause")

    if symptom:
        queries.append(f"{defect_type} defect causes {symptom}".strip())

    return [q for q in {q.strip() for q in queries if q.strip()}]


def tavily_search_from_queries(
    queries: list[str],
    allowlist: list[str],
    api_key: str,
    max_results: int = 6,
    timeout_s: int = 12,
) -> list[Source]:
    if not api_key:
        raise ValueError("Missing Tavily API key")
    allowlist_clean = _normalize_domains(allowlist)

    results: list[Source] = []
    for query in queries:
        results.extend(
            _tavily_search_single(
                query=query,
                allowlist=allowlist_clean,
                api_key=api_key,
                max_results=max_results,
                timeout_s=timeout_s,
            )
        )

    return _dedupe_sources(results, allowlist_clean)


def internal_retrieval(
    con: Any,
    context: dict[str, Any],
    limit: int = 6,
) -> list[InternalHit]:
    query_text = context.get("query_text") or ""
    query_terms = _tokenize(query_text)
    if not query_terms:
        return []

    action_repo = ActionRepository(con)
    analysis_repo = AnalysisRepository(con)
    effectiveness_repo = EffectivenessRepository(con)

    actions = [a for a in action_repo.list_actions(status="done") if a.get("title")]
    analyses = [a for a in analysis_repo.list_analyses() if a.get("tool_type")]

    action_ids = [a["id"] for a in actions if a.get("id")]
    effectiveness = effectiveness_repo.get_effectiveness_for_actions(action_ids)

    documents: list[dict[str, Any]] = []

    for action in actions:
        text = " ".join(
            [
                action.get("title") or "",
                action.get("description") or "",
                action.get("category") or "",
                action.get("area") or "",
                action.get("project_name") or "",
            ]
        ).strip()
        documents.append(
            {
                "record_type": "action",
                "record_id": action.get("id"),
                "title": action.get("title") or "—",
                "description": action.get("description") or "",
                "area": action.get("area"),
                "project_name": action.get("project_name"),
                "status": action.get("status"),
                "created_at": action.get("created_at"),
                "closed_at": action.get("closed_at"),
                "effectiveness": (effectiveness.get(action.get("id") or "") or {}).get(
                    "classification"
                ),
                "text": text,
            }
        )

    for analysis in analyses:
        status = analysis.get("status")
        if status not in {"closed", "done"} and not analysis.get("closed_at"):
            continue
        template_text = _flatten_analysis_template(analysis.get("template_json"))
        text = " ".join(
            [
                analysis.get("tool_type") or "",
                analysis.get("area") or "",
                analysis.get("project_name") or "",
                template_text,
            ]
        ).strip()
        documents.append(
            {
                "record_type": "analysis",
                "record_id": analysis.get("id"),
                "title": analysis.get("tool_type") or "Analiza",
                "description": template_text,
                "area": analysis.get("area"),
                "project_name": analysis.get("project_name"),
                "status": status,
                "created_at": analysis.get("created_at"),
                "closed_at": analysis.get("closed_at"),
                "effectiveness": None,
                "text": text,
            }
        )

    if not documents:
        return []

    doc_terms = [_tokenize(doc["text"]) for doc in documents]
    idf = _compute_idf(doc_terms)

    scored: list[InternalHit] = []
    for doc, terms in zip(documents, doc_terms):
        score = _tfidf_score(query_terms, terms, idf)
        if score <= 0:
            continue
        scored.append(
            InternalHit(
                record_type=doc["record_type"],
                record_id=str(doc.get("record_id") or ""),
                title=doc.get("title") or "—",
                description=doc.get("description") or "",
                area=doc.get("area"),
                project_name=doc.get("project_name"),
                score=score,
                snippet=_make_snippet(doc.get("text") or ""),
                status=doc.get("status"),
                created_at=doc.get("created_at"),
                closed_at=doc.get("closed_at"),
                effectiveness_classification=doc.get("effectiveness"),
            )
        )

    scored.sort(key=lambda hit: hit.score, reverse=True)
    return scored[:limit]


def synthesize_answer(
    context: dict[str, Any],
    sources: list[Source],
    internal_hits: list[InternalHit],
) -> dict[str, Any]:
    area = (context.get("area") or "").lower()
    defect_type = context.get("defect_type") or ""
    symptom = context.get("symptom") or ""

    recommendations = _default_recommendations(area, defect_type)
    probable_causes = recommendations["probable_causes"]
    checks = recommendations["checks"]
    corrective = recommendations["corrective_actions"]
    preventive = recommendations["preventive_actions"]
    parameter_hints = recommendations.get("parameter_hints", [])

    summary_parts = [part for part in [defect_type, symptom] if part]
    summary_text = " — ".join(summary_parts) if summary_parts else "Brak szczegółów objawu."

    facts: list[dict[str, str]] = []
    for source in sources[:3]:
        facts.append(
            {
                "text": (
                    f"{source.title} opisuje typowe przyczyny i działania dla {defect_type or 'defektów'}"
                ),
                "url": source.url,
            }
        )

    return {
        "summary_text": _truncate(summary_text, 220),
        "probable_causes": _limit_items(probable_causes, 5),
        "checks": _limit_items(checks, 6),
        "corrective_actions": _limit_items(corrective, 6),
        "preventive_actions": _limit_items(preventive, 6),
        "parameter_hints": _limit_items(parameter_hints, 5) if area == "wtrysk" else [],
        "facts": facts,
        "internal_count": len(internal_hits),
        "safety_note": "To sugestie. Zweryfikuj na linii.",
    }


def serialize_sources(sources: list[Source]) -> list[dict[str, Any]]:
    return [asdict(source) for source in sources]


def serialize_internal_hits(hits: list[InternalHit]) -> list[dict[str, Any]]:
    return [asdict(hit) for hit in hits]


def _normalize_domains(domains: list[str]) -> list[str]:
    cleaned: list[str] = []
    for domain in domains:
        if not domain:
            continue
        value = str(domain).strip().lower()
        value = re.sub(r"^https?://", "", value)
        value = value.split("/")[0]
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned


def _tavily_search_single(
    query: str,
    allowlist: list[str],
    api_key: str,
    max_results: int,
    timeout_s: int,
) -> list[Source]:
    payload = {
        "api_key": api_key,
        "query": query,
        "search_depth": "basic",
        "max_results": max_results,
        "include_domains": allowlist,
    }
    req = Request(
        "https://api.tavily.com/search",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=timeout_s) as response:
        data = json.loads(response.read().decode("utf-8"))

    results: list[Source] = []
    for row in data.get("results", []) or []:
        url = row.get("url") or ""
        title = row.get("title") or url
        snippet = row.get("content") or ""
        domain = _extract_domain(url)
        if allowlist and not _domain_allowed(domain, allowlist):
            continue
        if url:
            results.append(Source(title=title, url=url, domain=domain or "", snippet=snippet))
    return results


def _dedupe_sources(sources: list[Source], allowlist: list[str]) -> list[Source]:
    seen: set[str] = set()
    deduped: list[Source] = []
    for source in sources:
        normalized_url = source.url.split("#")[0].strip()
        if not normalized_url or normalized_url in seen:
            continue
        if allowlist and not _domain_allowed(source.domain, allowlist):
            continue
        seen.add(normalized_url)
        deduped.append(source)
    return deduped


def _domain_allowed(domain: str | None, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    if not domain:
        return False
    domain = domain.lower()
    return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in allowlist)


def _extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        return (parsed.netloc or "").lower()
    except ValueError:
        return ""


def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    tokens = re.split(r"[^\w]+", text.lower())
    return [t for t in tokens if len(t) > 1 and t not in STOPWORDS]


def _compute_idf(doc_terms: list[list[str]]) -> dict[str, float]:
    doc_count = len(doc_terms)
    term_docs: dict[str, int] = {}
    for terms in doc_terms:
        for term in set(terms):
            term_docs[term] = term_docs.get(term, 0) + 1
    return {term: math.log((doc_count + 1) / (count + 1)) + 1 for term, count in term_docs.items()}


def _tfidf_score(query_terms: list[str], doc_terms: list[str], idf: dict[str, float]) -> float:
    if not doc_terms:
        return 0.0
    term_counts: dict[str, int] = {}
    for term in doc_terms:
        term_counts[term] = term_counts.get(term, 0) + 1
    doc_len = len(doc_terms)
    score = 0.0
    for term in query_terms:
        if term not in term_counts:
            continue
        tf = term_counts[term] / doc_len
        score += tf * idf.get(term, 1.0)
    return score


def _make_snippet(text: str, max_len: int = 220) -> str:
    return _truncate(re.sub(r"\s+", " ", text).strip(), max_len)


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _flatten_analysis_template(template_json: str | None) -> str:
    if not template_json:
        return ""
    try:
        payload = json.loads(template_json)
    except json.JSONDecodeError:
        return ""
    if isinstance(payload, dict):
        parts: list[str] = []
        for value in payload.values():
            if isinstance(value, dict):
                parts.extend([str(v) for v in value.values() if v])
            elif value:
                parts.append(str(value))
        return " ".join(parts)
    return ""


def _default_recommendations(area: str, defect_type: str) -> dict[str, list[str]]:
    area = (area or "").lower()
    defect_key = defect_type.lower()

    injection_defaults = {
        "short shot": {
            "probable_causes": [
                "Zbyt niska temperatura tworzywa",
                "Niewystarczające ciśnienie wtrysku",
                "Zbyt mały czas docisku",
            ],
            "checks": [
                "Sprawdź temperaturę cylindra i dyszy",
                "Zweryfikuj odpowietrzenie formy",
                "Sprawdź zużycie kanałów doprowadzających",
            ],
            "corrective_actions": [
                "Zwiększ temperaturę tworzywa i formy",
                "Podnieś ciśnienie i prędkość wtrysku",
                "Wydłuż czas docisku",
            ],
            "preventive_actions": [
                "Ustandaryzuj parametry procesu",
                "Utrzymuj czystość kanałów i odpowietrzeń",
            ],
            "parameter_hints": [
                "Temperatura cylindra +5-15°C",
                "Ciśnienie wtrysku +5-10%",
                "Czas docisku +0.5-1 s",
            ],
        },
        "sink mark": {
            "probable_causes": [
                "Niewystarczający docisk",
                "Zbyt wysoka temperatura formy",
                "Nierównomierne chłodzenie",
            ],
            "checks": [
                "Sprawdź czas i ciśnienie docisku",
                "Zweryfikuj chłodzenie w gnieździe",
                "Oceń grubość ścianki",
            ],
            "corrective_actions": [
                "Zwiększ docisk",
                "Obniż temperaturę formy",
                "Zoptymalizuj chłodzenie",
            ],
            "preventive_actions": [
                "Utrzymuj stabilne chłodzenie",
                "Stosuj odpowiednie parametry pakowania",
            ],
            "parameter_hints": [
                "Docisk +5-15%",
                "Temperatura formy -5-10°C",
            ],
        },
    }

    assembly_defaults = {
        "misfit": {
            "probable_causes": [
                "Tolerancje poza specyfikacją",
                "Błąd w sekwencji montażu",
                "Zużyte narzędzia montażowe",
            ],
            "checks": [
                "Zweryfikuj wymiary komponentów",
                "Sprawdź procedurę montażu",
                "Skontroluj narzędzia i przyrządy",
            ],
            "corrective_actions": [
                "Skoryguj ustawienia narzędzi",
                "Zaktualizuj instrukcję montażu",
                "Wymień zużyte oprzyrządowanie",
            ],
            "preventive_actions": [
                "Wprowadź kontrolę wejściową komponentów",
                "Szkolenia dla operatorów",
            ],
        }
    }

    generic = {
        "probable_causes": [
            "Zmiana parametrów procesu",
            "Niestabilny materiał",
            "Zużycie narzędzia lub formy",
        ],
        "checks": [
            "Zweryfikuj ostatnie zmiany procesu",
            "Sprawdź partię materiału",
            "Skontroluj stan formy/narzędzia",
        ],
        "corrective_actions": [
            "Przywróć sprawdzone parametry",
            "Wykonaj korektę narzędzia",
        ],
        "preventive_actions": [
            "Ustaw limity zmian parametrów",
            "Regularna konserwacja formy",
        ],
        "parameter_hints": [],
    }

    if area == "wtrysk":
        for key, payload in injection_defaults.items():
            if key in defect_key:
                return payload
        return generic

    if area == "montaż":
        for key, payload in assembly_defaults.items():
            if key in defect_key:
                return payload
        return generic

    return generic


def _limit_items(values: list[str], limit: int) -> list[str]:
    return [_truncate(value, 180) for value in values[:limit] if value]
