# -*- coding: utf-8 -*-
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from module_b_lit_scout import StageB


def test_b1_dedup_and_coverage_constraints() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-B1-SCORE-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        (idea_dir / "in" / "idea.txt").write_text("Test idea", encoding="utf-8")
        s = StageB(idea_dir, "BALANCED", offline_fixtures=None)
        papers = [
            {"title": "A", "doi": "10.1/abc", "pmid": "", "openalex_id": "W1", "arxiv_id": "", "year": 2022, "abstract": "context method", "source": "openalex"},
            {"title": "A duplicate", "doi": "https://doi.org/10.1/abc", "pmid": "", "openalex_id": "W2", "arxiv_id": "", "year": 2022, "abstract": "context method", "source": "openalex"},
            {"title": "B drift", "doi": "", "pmid": "", "openalex_id": "W3", "arxiv_id": "", "year": 2021, "abstract": "noise only", "source": "openalex"},
        ]
        ded = s.dedup(papers)
        assert len(ded) == 2
        passed, support, _, _, _, _ = s.apply_relevance_and_score(
            ded,
            primary_token="test",
            keywords_tokens=["test"],
            must_have_tokens=["context"],
            anchor_packs=[["context", "method"]],
            drift_blacklist_raw=[],
            support_tokens=["method"],
        )
        assert len(passed) + len(support) >= 1
