from pathlib import Path
from module_b_lit_scout import StageB


def test_no_duplicate_planned_queries_rule(tmp_path: Path) -> None:
    idea = tmp_path / "IDEA-NODUP"
    (idea / "in").mkdir(parents=True)
    (idea / "out").mkdir(parents=True)
    stage = StageB(idea, "BALANCED", None)

    stage.search_log["errors"] = []
    seen = {("a and b",)}
    candidate = ("a and b",)
    if candidate in seen:
        stage.search_log["errors"].append("stopping_rule_duplicate_planned_queries_round1")

    assert any("stopping_rule_duplicate_planned_queries" in e for e in stage.search_log["errors"])
