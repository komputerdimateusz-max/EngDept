import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from action_tracking.services import diagnostics_assistant as da


class DiagnosticsAssistantTests(unittest.TestCase):
    def test_build_query_context_injection(self) -> None:
        context = da.build_query_context(
            {
                "area": "Wtrysk",
                "defect_type": "Short shot",
                "symptom": "nie wypełnia gniazda",
                "project_name": "Proj X",
                "work_centers": ["WC1"],
                "flags": ["rosnący scrap"],
            }
        )
        self.assertTrue(context["is_injection"])
        self.assertIn("Short shot", context["query_text"])
        self.assertEqual(len(context["context_hash"]), 16)

    def test_allowlist_filtering_and_dedup(self) -> None:
        sources = [
            da.Source(title="A", url="https://example.com/a", domain="example.com"),
            da.Source(title="B", url="https://example.com/a", domain="example.com"),
            da.Source(title="C", url="https://other.com/c", domain="other.com"),
            da.Source(title="D", url="https://sub.example.com/d", domain="sub.example.com"),
        ]
        allowlist = ["example.com"]
        deduped = da._dedupe_sources(sources, allowlist)
        urls = [source.url for source in deduped]
        self.assertEqual(len(urls), 2)
        self.assertIn("https://example.com/a", urls)
        self.assertIn("https://sub.example.com/d", urls)

    def test_build_search_queries(self) -> None:
        context = {
            "area": "Montaż",
            "defect_type": "Misfit",
            "symptom": "luz na zatrzasku",
        }
        queries = da.build_search_queries(context)
        self.assertTrue(any("assembly defect" in query for query in queries))
        self.assertTrue(any("Misfit" in query for query in queries))


if __name__ == "__main__":
    unittest.main()
