from module_b_lit_scout import parse_openalex_meta_count


def test_openalex_probe_meta_count_parse() -> None:
    payload = {"meta": {"count": 12345}, "results": []}
    assert parse_openalex_meta_count(payload) == 12345
    assert parse_openalex_meta_count({"meta": {"count": "77"}}) == 77
    assert parse_openalex_meta_count({"meta": {}}) == 0
