from pathlib import Path
from module_b_lit_scout import StageB


def test_citation_filter_strings(tmp_path: Path) -> None:
    idea = tmp_path / "IDEA-CIT"
    (idea / "in").mkdir(parents=True)
    (idea / "out").mkdir(parents=True)
    stage = StageB(idea, "BALANCED", None)

    seeds = ["https://openalex.org/W1", "https://openalex.org/W2"]
    forward = stage.build_citation_filter_params("forward", seeds, 20)
    backward = stage.build_citation_filter_params("backward", seeds, 20)
    assert forward["filter"] == "cites:W1|W2"
    assert backward["filter"] == "cited_by:W1|W2"
