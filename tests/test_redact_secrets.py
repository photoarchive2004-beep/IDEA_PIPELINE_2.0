from pathlib import Path
from module_b_lit_scout import StageB


def test_redact_api_key_from_params(tmp_path: Path) -> None:
    idea = tmp_path / "IDEA-REDACT"
    (idea / "in").mkdir(parents=True)
    (idea / "out").mkdir(parents=True)
    stage = StageB(idea, "BALANCED", None)
    stage.openalex_key = "super_secret_key_1234"

    sanitized = stage.sanitize_params_for_log({"api_key": stage.openalex_key, "search": "abc"})
    assert sanitized["credential"] == "***"
    assert "api_key" not in str(sanitized)
    assert stage.openalex_key not in str(sanitized)
