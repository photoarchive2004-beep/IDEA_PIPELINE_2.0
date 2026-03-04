# -*- coding: utf-8 -*-
import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

RU_STOP = {
    "и", "или", "но", "для", "это", "как", "что", "при", "без", "над", "под", "между", "если", "чтобы",
    "так", "еще", "только", "уже", "все", "всех", "этап", "идея", "тест", "тесты", "результат", "данные",
}
EN_STOP = {
    "the", "and", "or", "for", "with", "without", "this", "that", "from", "into", "about", "stage", "idea",
    "test", "tests", "result", "results", "data", "method", "approach", "research", "study",
}
CORE_STOPWORDS = {
    # EN
    "and", "or", "the", "a", "an", "of", "for", "in", "on", "to", "from", "with", "without", "by", "via",
    "using", "use", "based", "study", "studies", "result", "results", "method", "methods", "analysis", "analyses",
    "data", "model", "models", "approach", "approaches", "review", "reviews",
    # RU
    "и", "или", "в", "на", "по", "для", "от", "с", "без", "как", "что", "это", "эти", "тот", "та", "те",
    "также", "метод", "методы", "анализ", "данные", "модель", "модели", "обзор",
}
GENERIC_BLACKLIST = {
    "хиты", "и т.п", "и тп", "по чистым", "чистым деревом", "baseline", "overview", "introduction", "введение",
}

GENERIC_TOKEN_BLACKLIST = {
    "model", "analysis", "study", "data", "method", "approach", "results", "result", "review",
    "paper", "research", "system", "effect", "effects", "based", "using", "novel", "case", "cases",
    "метод", "модель", "данные", "результаты", "результат", "обзор", "исследование", "подход",
}

GENERIC_WORDS = GENERIC_TOKEN_BLACKLIST | {
    "models", "methods", "methodology", "paper", "dataset", "datasets", "genomic", "genomics",
    "population", "populations", "algorithm", "framework", "review", "case", "cases", "science",
}

BLACKLIST_SINGLE_WORD_STOP = {
    "data", "model", "study", "analysis", "method", "methods", "approach", "review",
}

DATASET_DOMAIN_HINTS = {
    "gbif.org", "zenodo.org", "figshare.com", "dryad", "datadryad", "pangaea", "kaggle.com",
}

SERVICE_LABELS = {
    "openalex": "OpenAlex",
    "semantic_scholar": "Semantic Scholar",
    "crossref": "Crossref",
}

CYR_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}

GEO_HINTS = {
    "altai", "altaya", "kazakhstan", "siberia", "mongolia", "china", "russia", "balkhash", "ural", "volga",
}

PROBE_MAX = 12
PROBE_TOO_BROAD = 20000
PROBE_BROAD = 3000
PROBE_OK = 50
PROBE_NARROW = 2

SEED_QUERIES_MAX = 8
SEED_QUERIES_MIN_ALIVE = 3
MAX_PER_QUERY = 40
MAX_SEED_ITEMS_TOTAL = 300
AUTO_FIX_ROUNDS = 2
DRIFT_TARGET = 0.30
DRIFT_HARD_STOP = 0.60
PRIMARY_SIGNAL_MIN = 0.20
TOPN_FOR_METRICS = 50
LLM_BUDGET_PER_IDEA = 3
CITATION_CHASE_ENABLE = True
CITATION_TOPK = 20
CITATION_MAX_ADD = 120
CITATION_PER_SEED_CAP = 10
STOPWORD_LIST_EN_RU = sorted(CORE_STOPWORDS | EN_STOP | RU_STOP)

DRIFT_TARGET_DEFAULT = DRIFT_TARGET

GENERIC_TOKEN_BLACKLIST = GENERIC_TOKEN_BLACKLIST | {
    "technology", "application", "applications", "performance", "framework", "evaluation", "problem",
    "objective", "discussion", "conclusion", "findings", "paper", "work", "works", "experiment",
    "experiments", "case", "cases", "review", "reviews", "survey", "surveys",
    "методы", "модели", "подходы", "обсуждение", "вывод", "выводы", "эксперимент", "эксперименты",
}

METHOD_HINTS = {
    "baypass", "lfmm2", "ddrad", "snp", "abba-baba", "hydrorivers", "hydroatlas", "genotype-environment",
    "genotype", "association", "regression", "meta-analysis", "machine-learning", "modeling",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def parse_env(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def normalize_doi(doi: str) -> str:
    x = (doi or "").strip().lower()
    x = x.replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    return x.strip()


def norm_title(title: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-zа-я0-9 ]+", " ", (title or "").lower())).strip()


class StageB:
    def __init__(self, idea_dir: Path, mode: str, offline_fixtures: Optional[Path]):
        self.idea_dir = idea_dir
        self.mode = mode.upper()
        self.offline_fixtures = offline_fixtures

        self.in_dir = idea_dir / "in"
        self.out_dir = idea_dir / "out"
        self.logs_dir = idea_dir / "logs"
        for d in (self.in_dir, self.out_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.run_log = self.out_dir / "runB.log"
        self.search_log_path = self.out_dir / "search_log.json"
        self.search_log_legacy_path = self.out_dir / "search_log_B.json"
        self.prisma_path = self.out_dir / "prisma_lite.md"
        self.prisma_legacy_path = self.out_dir / "prisma_lite_B.md"
        self.checkpoint_path = self.out_dir / "checkpoint.json"
        self.module_log = self.logs_dir / f"moduleB_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.run_id = f"B_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        repo = self.idea_dir.parents[1]
        self.repo_root = repo
        self.secrets = parse_env(repo / "config" / "secrets.env")
        self.mailto = self.secrets.get("OPENALEX_MAILTO", "")
        self.openalex_key = self.secrets.get("OPENALEX_API_KEY", "")
        self.s2_key = self.secrets.get("SEMANTIC_SCHOLAR_API_KEY", "")

        self.session = requests.Session() if requests else None
        self.cache_root = self.repo_root / ".cache" / "stage_b"
        self.cache_ttl_sec = 7 * 24 * 3600
        self.request_caps = {"FOCUSED": 25, "BALANCED": 50, "WIDE": 90}
        self.request_count = 0
        self.degraded_reasons: List[str] = []
        self.search_log: Dict[str, Any] = {
            "run_id": self.run_id,
            "started_at": now_iso(),
            "mode": self.mode,
            "anchor_candidates": [],
            "anchor_top_display": [],
            "anchor_top_search": [],
            "anchor_packs": [],
            "anchor_packs_search": [],
            "support_tokens": [],
            "abbreviation_map": {},
            "queries": [],
            "errors": [],
            "service_status": {
                "openalex": "offline",
                "semantic_scholar": "offline",
                "crossref": "offline",
                "researchrabbit": "offline",
                "europe_pmc": "offline",
            },
            "stats": {},
        }
        self.abbr_full_map: Dict[str, str] = {}
        self.abbr_mentions: Set[str] = set()
        self.query_snippets: List[Dict[str, Any]] = []
        self.dedup_before = 0
        self.dedup_after = 0
        self.dedup_merged_count = 0
        self.geo_terms: Set[str] = set()
        self.llm_info: Dict[str, Any] = {
            "found": False,
            "used": False,
            "schema": "invalid",
            "reason": "response_not_found",
            "path": str((self.in_dir / "llm_response_B_anchors.json").resolve()),
        }
        self.search_log["llm"] = dict(self.llm_info)
        self.search_log["rejected_queries"] = []
        self.search_log["planned_queries"] = []
        self.search_log["token_probe"] = []
        self.search_log["llm_effect"] = {}
        self.search_log["executed_queries"] = []
        self.search_log["rounds"] = []
        self.search_log["token_sanitation"] = {
            "stopwords_removed_count": 0,
            "examples_removed": [],
            "short_removed_count": 0,
            "examples_short": [],
            "punct_removed_count": 0,
            "examples_punct": [],
        }
        self.search_log["sanitation"] = {"stopwords_removed": 0, "examples": []}
        self.llm_budget = LLM_BUDGET_PER_IDEA
        self.llm_budget_path = self.out_dir / "llm_requests_B.json"
        self.llm_budget_state = self.load_llm_budget_state()
        self.llm_prompts_created = 0
        self.llm_prompt_created = False
        self.drift_target = DRIFT_TARGET_DEFAULT
        self.search_log["stats"] = {
            "llm_budget_total": self.llm_budget,
            "llm_budget_used": self.llm_budget_used(),
            "llm_budget_remaining": self.llm_budget_remaining(),
            "llm_prompts_created": 0,
            "llm_used": False,
            "auto_fix_rounds_used": 0,
            "drift_target": self.drift_target,
            "config": {
                "SEED_QUERIES_MAX": SEED_QUERIES_MAX,
                "SEED_QUERIES_MIN_ALIVE": SEED_QUERIES_MIN_ALIVE,
                "MAX_PER_QUERY": MAX_PER_QUERY,
                "MAX_SEED_ITEMS_TOTAL": MAX_SEED_ITEMS_TOTAL,
                "AUTO_FIX_ROUNDS": AUTO_FIX_ROUNDS,
                "DRIFT_TARGET": DRIFT_TARGET,
                "DRIFT_HARD_STOP": DRIFT_HARD_STOP,
                "PRIMARY_SIGNAL_MIN": PRIMARY_SIGNAL_MIN,
                "TOPN_FOR_METRICS": TOPN_FOR_METRICS,
                "LLM_BUDGET_PER_IDEA": LLM_BUDGET_PER_IDEA,
                "CITATION_CHASE_ENABLE": CITATION_CHASE_ENABLE,
                "CITATION_TOPK": CITATION_TOPK,
                "CITATION_MAX_ADD": CITATION_MAX_ADD,
                "CITATION_PER_SEED_CAP": CITATION_PER_SEED_CAP,
                "STOPWORD_LIST_EN_RU_SIZE": len(STOPWORD_LIST_EN_RU),
            },
        }

    def archive_previous_outputs(self) -> None:
        run_dir = self.out_dir / "_runs" / self.run_id
        prev_dir = run_dir / "_prev"
        run_dir.mkdir(parents=True, exist_ok=True)
        move_targets = {
            "corpus.csv", "corpus_all.csv", "corpus_support.csv", "corpus_support_all.csv", "search_log.json", "search_log_B.json", "stageB_summary.txt", "prisma_lite.md", "prisma_lite_B.md", "checkpoint.json", "runB.log", "llm_prompt_B_anchors.txt",
        }
        for item in self.out_dir.iterdir():
            if item.name == "_runs":
                continue
            is_stageb_md = item.is_file() and item.name.endswith(".md") and "B" in item.name
            is_cache = item.name in {"cache_openalex", "cache_semanticscholar"}
            if item.name in move_targets or is_stageb_md or is_cache:
                prev_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(item), str(prev_dir / item.name))
        runs_root = self.out_dir / "_runs"
        run_dirs = sorted([x for x in runs_root.iterdir() if x.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True) if runs_root.exists() else []
        for old in run_dirs[20:]:
            shutil.rmtree(old, ignore_errors=True)

    def log(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        for p in (self.run_log, self.module_log):
            with p.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    def _cache_path(self, source: str, method: str, url: str, params: Dict[str, Any]) -> Path:
        qp = urllib.parse.urlencode(sorted([(str(k), str(v)) for k, v in (params or {}).items()]))
        key = hashlib.sha256(f"{method}|{url}|{qp}".encode("utf-8")).hexdigest()
        return self.cache_root / source / f"{key}.json"

    def _cache_get(self, source: str, method: str, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fp = self._cache_path(source, method, url, params)
        if not fp.exists():
            return None
        age = time.time() - fp.stat().st_mtime
        if age > self.cache_ttl_sec:
            return None
        try:
            return json.loads(read_text(fp))
        except Exception:
            return None

    def _cache_set(self, source: str, method: str, url: str, params: Dict[str, Any], body: Dict[str, Any]) -> None:
        fp = self._cache_path(source, method, url, params)
        fp.parent.mkdir(parents=True, exist_ok=True)
        write_text(fp, json.dumps(body, ensure_ascii=False))

    def _request_budget_exceeded(self) -> bool:
        return self.request_count >= int(self.request_caps.get(self.mode, 50))

    def request_json(self, method: str, url: str, source: str, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None, query_kind: str = "seed") -> Tuple[Dict[str, Any], int, int]:
        if self.offline_fixtures:
            raise RuntimeError("offline")
        params = dict(params or {})
        if method.upper() == "GET":
            cached = self._cache_get(source, method, url, params)
            if cached is not None:
                self.search_log.setdefault("cache", {"hits": 0, "misses": 0})
                self.search_log["cache"]["hits"] += 1
                return cached, 200, 0
            self.search_log.setdefault("cache", {"hits": 0, "misses": 0})
            self.search_log["cache"]["misses"] += 1
        if self._request_budget_exceeded():
            self.degraded_reasons.append("request_cap_reached")
            raise RuntimeError("request_cap_reached")
        if source == "openalex":
            if self.openalex_key:
                params.setdefault("api_key", self.openalex_key)
            else:
                self.degraded_reasons.append("missing_openalex_api_key")

        t0 = time.time()
        headers = {"User-Agent": "IDEA_PIPELINE_2.0-StageB/3.0", "Content-Type": "application/json"}
        if source == "semantic_scholar" and self.s2_key:
            headers["x-api-key"] = self.s2_key

        qtxt = ""
        if isinstance(params, dict):
            qtxt = str(params.get("search") or params.get("query") or params.get("query.title") or "")

        retry_waits = [1, 2, 4]
        status_code = 0
        body_text = ""
        ms = 0
        last_error: Optional[Exception] = None
        for attempt, wait_seconds in enumerate(retry_waits, start=1):
            try:
                t0 = time.time()
                if self.session:
                    resp = self.session.request(method, url, params=params, json=payload, timeout=25, headers=headers)
                    ms = int((time.time() - t0) * 1000)
                    status_code = resp.status_code
                    body_text = resp.text
                    if status_code in (409, 429, 502, 503) and attempt < len(retry_waits):
                        retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
                        delay = int(retry_after) if str(retry_after or "").isdigit() else wait_seconds
                        self.search_log["errors"].append(f"{source}_{query_kind}_retry_http_{status_code}_attempt_{attempt}")
                        time.sleep(max(delay, 1))
                        continue
                else:
                    final_url = url
                    if params:
                        final_url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
                    data = None
                    if payload is not None:
                        data = json.dumps(payload).encode("utf-8")
                    req = urllib.request.Request(final_url, data=data, headers=headers, method=method)
                    with urllib.request.urlopen(req, timeout=25) as r:
                        status_code = int(getattr(r, "status", 200))
                        body_text = r.read().decode("utf-8", errors="ignore")
                    ms = int((time.time() - t0) * 1000)
                break
            except Exception as e:
                last_error = e
                ms = int((time.time() - t0) * 1000)
                if attempt < len(retry_waits):
                    self.search_log["errors"].append(f"{source}_{query_kind}_retry_exception_attempt_{attempt}: {e}")
                    time.sleep(wait_seconds)
                    continue
                self.search_log["queries"].append({
                    "source": source,
                    "query_kind": query_kind,
                    "endpoint_url": url,
                    "params": params or {},
                    "query_text": qtxt,
                    "anchor_pack_used": [],
                    "http_status": 0,
                    "elapsed_ms": ms,
                    "result_total": 0,
                    "result_items": 0,
                    "error": str(e),
                })
                raise

        if last_error and status_code == 0:
            raise last_error

        if status_code >= 400:
            ms = int((time.time() - t0) * 1000)
            self.search_log["queries"].append({
                "source": source,
                "query_kind": query_kind,
                "endpoint_url": url,
                "params": params or {},
                "query_text": qtxt,
                "anchor_pack_used": [],
                "http_status": status_code,
                "elapsed_ms": ms,
                "result_total": 0,
                "result_items": 0,
                "error": f"HTTP {status_code}",
            })
            raise RuntimeError(f"HTTP {status_code}")

        self.search_log["queries"].append({
            "source": source,
            "query_kind": query_kind,
            "endpoint_url": url,
            "params": params or {},
            "query_text": qtxt,
            "anchor_pack_used": [],
            "http_status": status_code,
            "elapsed_ms": ms,
            "result_total": 0,
            "result_items": 0,
        })
        body = json.loads(body_text or "{}")
        self.request_count += 1
        if method.upper() == "GET":
            self._cache_set(source, method, url, params, body)
        return body, status_code, ms

    def decode_openalex_abstract(self, w: Dict[str, Any]) -> str:
        inv = w.get("abstract_inverted_index") or {}
        if not isinstance(inv, dict) or not inv:
            return ""
        max_pos = -1
        for pos_list in inv.values():
            if isinstance(pos_list, list) and pos_list:
                max_pos = max(max_pos, max(pos_list))
        if max_pos < 0:
            return ""
        words = [""] * (max_pos + 1)
        for token, pos_list in inv.items():
            if not isinstance(pos_list, list):
                continue
            for pos in pos_list:
                if isinstance(pos, int) and 0 <= pos <= max_pos:
                    words[pos] = str(token)
        return re.sub(r"\s+", " ", " ".join([w for w in words if w])).strip()

    def input_hash(self, idea_text: str, structured: Dict[str, Any]) -> str:
        blob = json.dumps({"idea": idea_text, "structured": structured}, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def load_checkpoint(self) -> Dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {}
        try:
            return json.loads(read_text(self.checkpoint_path))
        except Exception:
            return {}

    def save_checkpoint(self, input_hash: str, status: str, stats: Dict[str, Any]) -> None:
        payload = {
            "saved_at": now_iso(),
            "mode": self.mode,
            "input_hash": input_hash,
            "status": status,
            "stats": stats,
            "outputs": ["corpus.csv", "corpus_all.csv", "search_log.json", "prisma_lite.md", "stageB_summary.txt"],
        }
        write_text(self.checkpoint_path, json.dumps(payload, ensure_ascii=False, indent=2))

    def ensure_idea_text(self) -> Tuple[bool, str]:
        top = self.idea_dir / "idea.txt"
        in_txt = self.in_dir / "idea.txt"
        if in_txt.exists() and (not top.exists() or in_txt.stat().st_mtime > top.stat().st_mtime):
            top.write_text(read_text(in_txt), encoding="utf-8")
        if not top.exists():
            return False, "Не найден idea.txt"
        return True, read_text(top)

    def load_structured(self) -> Dict[str, Any]:
        p = self.out_dir / "structured_idea.json"
        if not p.exists():
            return {}
        try:
            return json.loads(read_text(p))
        except Exception as e:
            self.search_log["errors"].append(f"structured_parse_error: {e}")
            return {}

    def translit_cyr(self, text: str) -> str:
        out = []
        for ch in text:
            lo = ch.lower()
            if lo in CYR_MAP:
                tr = CYR_MAP[lo]
                out.append(tr.capitalize() if ch.isupper() else tr)
            elif ch.isalnum() or ch in " -":
                out.append(ch)
        return re.sub(r"\s+", " ", "".join(out)).strip(" -")

    def format_human_abbr(self, text: str) -> str:
        def repl(m: re.Match[str]) -> str:
            ab = m.group(0)
            if ab not in self.abbr_mentions:
                return ab
            full = self.abbr_full_map.get(ab)
            return f"{full} ({ab})" if full else ab
        return re.sub(r"\b[A-Z]{2,6}\b", repl, text)

    def extract_abbr(self, text: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for full, abbr in re.findall(r"([A-Za-zА-Яа-я0-9\-\s]{6,120})\(([A-Z0-9]{2,6})\)", text):
            out[abbr] = re.sub(r"\s+", " ", full.strip())
        for abbr, full in re.findall(r"\b([A-Z0-9]{2,6})\s*[—-]\s*([A-Za-zА-Яа-я0-9\-\s]{6,120})", text):
            out[abbr] = re.sub(r"\s+", " ", full.strip())
        return out

    def tokenized(self, s: str) -> List[str]:
        return [t for t in re.split(r"[^A-Za-zА-Яа-я0-9\-]+", s.lower()) if t]

    def is_geography_token(self, token: str) -> bool:
        t = (token or "").strip().lower()
        return t in GEO_HINTS or t in self.geo_terms

    def has_latin_for_seed(self, text: str) -> bool:
        return bool(re.search(r"[A-Za-z]", text or ""))

    def has_seed_chars(self, text: str) -> bool:
        return bool(re.search(r"[A-Za-z0-9\-]", text or ""))

    def normalize_token_list(self, items: Any) -> List[str]:
        if not isinstance(items, list):
            return []
        return self.sanitize_tokens(items, context="normalize_token_list")

    def sanitize_tokens(self, tokens: Any, context: str = "general") -> List[str]:
        if not isinstance(tokens, list):
            return []

        cleaned: List[str] = []
        seen: Set[str] = set()
        san = self.search_log.get("token_sanitation", {})
        for raw in tokens:
            if raw is None:
                continue
            tok = self.normalize_search_anchor(str(raw))
            tok = re.sub(r"\s+", " ", tok).strip()
            if not tok or len(tok) > 60:
                continue
            low = tok.lower()
            if low in CORE_STOPWORDS or low in EN_STOP or low in RU_STOP:
                san["stopwords_removed_count"] = int(san.get("stopwords_removed_count", 0)) + 1
                if len(san.get("examples_removed", [])) < 5 and tok not in san.get("examples_removed", []):
                    san.setdefault("examples_removed", []).append(tok)
                continue
            if re.fullmatch(r"[\W_]+", tok):
                san["punct_removed_count"] = int(san.get("punct_removed_count", 0)) + 1
                if len(san.get("examples_punct", [])) < 5 and tok not in san.get("examples_punct", []):
                    san.setdefault("examples_punct", []).append(tok)
                continue
            if len(tok) < 4 and not (re.search(r"\d", tok) or "-" in tok):
                san["short_removed_count"] = int(san.get("short_removed_count", 0)) + 1
                if len(san.get("examples_short", [])) < 5 and tok not in san.get("examples_short", []):
                    san.setdefault("examples_short", []).append(tok)
                continue
            if not self.has_seed_chars(tok):
                continue
            key = low
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(tok)

        self.search_log["token_sanitation"] = san
        self.search_log["sanitation"] = {
            "stopwords_removed": san.get("stopwords_removed_count", 0),
            "examples": san.get("examples_removed", [])[:3],
            "context": context,
        }
        return cleaned

    def detect_geo_terms(self, idea_text: str, structured: Dict[str, Any]) -> Set[str]:
        src = structured.get("structured_idea", structured) if isinstance(structured, dict) else {}
        text_parts = [idea_text]
        if isinstance(src, dict):
            for val in src.values():
                if isinstance(val, str):
                    text_parts.append(val)
                elif isinstance(val, list):
                    text_parts.extend([str(x) for x in val if isinstance(x, str)])
        blob = "\n".join(text_parts)
        terms = {
            w.lower()
            for w in re.findall(r"\b[A-Z][a-z]{3,19}\b", blob)
            if "-" not in w and not re.search(r"\d", w) and w.lower() not in METHOD_HINTS
        }
        return terms | set(GEO_HINTS)

    def is_geo_like_token(self, token: str) -> bool:
        tok = (token or "").strip()
        if not tok or len(tok) < 4 or len(tok) > 20:
            return False
        if not re.fullmatch(r"[A-Z][a-z]+", tok):
            return False
        low = tok.lower()
        if low in METHOD_HINTS:
            return False
        return low in self.geo_terms

    def token_strength(self, token: str) -> int:
        t = token.strip()
        if len(t) < 4:
            return -99
        score = 0
        if re.search(r"\d", t):
            score += 2
        if re.search(r"[a-z]", t) and re.search(r"[A-Z]", t):
            score += 2
        if "-" in t:
            score += 2
        if len(t) >= 7:
            score += 2
        if t.lower() in GENERIC_WORDS:
            score -= 3
        if self.is_geography_token(t):
            score -= 3
        return score

    def extract_strong_tokens(self, lines: List[str], limit: int = 40) -> List[str]:
        weighted: Dict[str, int] = {}
        for line in lines:
            toks = self.sanitize_tokens(re.findall(r"[A-Za-z0-9\-]{2,}", self.normalize_search_anchor(line)), context="extract_strong_tokens")
            for tok in toks:
                tl = tok.lower()
                sc = self.token_strength(tok)
                if tl not in weighted or sc > weighted[tl]:
                    weighted[tl] = sc
        ranked = sorted(weighted.items(), key=lambda x: (-x[1], -len(x[0]), x[0]))
        return [t for t, _ in ranked[:limit]]

    def is_bad_anchor(self, a: str) -> Optional[str]:
        x = a.strip()
        toks = self.tokenized(x)
        if not x:
            return "empty"
        if x.lower() in GENERIC_BLACKLIST:
            return "generic_blacklist"
        if len(x) < 8 and not re.search(r"\d", x) and "-" not in x:
            return "too_short"
        if len(toks) == 1 and len(toks[0]) <= 4 and not re.search(r"\d", toks[0]) and "-" not in toks[0]:
            return "single_short_word"
        if toks and sum(1 for t in toks if t in RU_STOP or t in EN_STOP) / max(len(toks), 1) > 0.7:
            return "mostly_stop_words"
        if re.search(r"(^|\s)(и т\.?п\.?|etc\.?)(\s|$)", x.lower()):
            return "service_phrase"
        return None

    def score_anchor(self, a: str) -> float:
        s = 1.0
        if "-" in a or re.search(r"\d", a):
            s += 1.2
        if len(a.split()) in (2, 3, 4):
            s += 1.0
        if re.search(r"\b(?:[A-ZА-Я][a-zа-я]+\s+){1,3}[A-ZА-Я][a-zа-я]+\b", a):
            s += 0.9
        if re.search(r"[A-Za-z]+\d+|\d+[A-Za-z]+", a):
            s += 0.8
        return s

    def build_anchors(self, idea_text: str, structured: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, str], Set[str]]:
        pieces: List[str] = []
        src = structured.get("structured_idea", {}) if isinstance(structured.get("structured_idea", {}), dict) else structured
        for k in ("problem", "main_hypothesis", "key_predictions", "decisive_tests", "title"):
            v = src.get(k)
            if isinstance(v, str):
                pieces.append(v)
            elif isinstance(v, list):
                pieces.extend([str(x) for x in v if isinstance(x, str)])
        pieces.append(idea_text)
        blob = "\n".join([p for p in pieces if p])

        ab_map = self.extract_abbr(blob)
        all_abbr = set(re.findall(r"\b[A-Z]{2,6}\b", blob))
        self.search_log["abbreviation_map"] = ab_map
        self.abbr_full_map = dict(ab_map)
        self.abbr_mentions = set(all_abbr)

        candidates: List[str] = []
        candidates += re.findall(r"[«\"]([^\"»]{6,140})[»\"]", blob)
        candidates += re.findall(r"\b[\wА-Яа-я]+-[\wА-Яа-я]+\b", blob)
        candidates += re.findall(r"\b(?:[A-ZА-Я][a-zа-я]+\s+){1,3}[A-ZА-Я][a-zа-я]+\b", blob)

        for line in blob.splitlines():
            clean = re.sub(r"^[\-\d\.\)\s]+", "", line).strip()
            if 12 <= len(clean) <= 120:
                candidates.append(clean)

        scored: Dict[str, float] = {}
        for c in candidates:
            c = re.sub(r"\s+", " ", c.strip(" .,;:"))
            reason = self.is_bad_anchor(c)
            if reason:
                self.search_log["anchor_candidates"].append({"anchor": c, "decision": "excluded", "reason": reason})
                continue
            sc = self.score_anchor(c)
            if c not in scored or sc > scored[c]:
                scored[c] = sc
                self.search_log["anchor_candidates"].append({"anchor": c, "decision": "included", "score": round(sc, 2)})

        top = [k for k, _ in sorted(scored.items(), key=lambda x: (-x[1], x[0]))[:20]]
        if len(top) < 8:
            words = [w for w in re.findall(r"[A-Za-zА-Яа-я0-9\-]{5,}", blob) if self.is_bad_anchor(w) is None]
            for w, c in Counter([w.lower() for w in words]).most_common(20):
                if w not in top:
                    top.append(w)
                if len(top) >= 10:
                    break

        # search anchors: latin/digits/hyphen and transliterated Cyrillic entities
        latin_like = set(re.findall(r"\b(?=[A-Za-z0-9\-]{4,}\b)(?=.*[A-Za-z])[A-Za-z0-9](?:[A-Za-z0-9\-]*[A-Za-z0-9])?\b", blob))
        cyr_entities: Set[str] = set()
        for line in blob.splitlines():
            toks = re.findall(r"\b[А-ЯЁ][а-яё]{3,}\b", line)
            if len(toks) > 1:
                cyr_entities.update(toks[1:])
        ru_common = {"для", "даже", "базовые", "главный", "главные", "глубокие", "внутри", "между", "узким", "стресс", "тест", "модели", "количественно", "нейтральная"}
        translit_terms = {self.translit_cyr(x) for x in cyr_entities if len(x) >= 4 and x.lower() not in ru_common}
        search_top = sorted({x for x in latin_like | translit_terms if len(x) >= 4})[:30]

        self.search_log["anchor_top_display"] = top[:20]
        self.search_log["anchor_top_search"] = search_top[:20]
        return top[:20], search_top[:20], ab_map, all_abbr

    def build_anchor_packs(self, primary_token: str, keywords_tokens: List[str], user_packs: Optional[List[List[str]]] = None) -> List[List[str]]:
        if user_packs:
            cleaned: List[List[str]] = []
            for pack in user_packs:
                terms = [t.lower() for t in self.sanitize_tokens(pack, context="anchor_packs_user") if t]
                terms = [t for t in terms if t and t.lower() not in GENERIC_WORDS and t not in CORE_STOPWORDS]
                if len(terms) >= 2 and not all(self.is_geography_token(t) for t in terms):
                    cleaned.append(terms[:3])
            packs = cleaned[:6]
            self.search_log["anchor_packs"] = packs
            self.search_log["anchor_packs_search"] = packs
            return packs

        candidates = [t.lower() for t in self.sanitize_tokens(keywords_tokens, context="anchor_packs_candidates") if t and t.lower() not in GENERIC_WORDS]
        candidates = [t for t in candidates if t not in CORE_STOPWORDS and t != (primary_token or "").lower()]
        method_vocab = {"lfmm2", "baypass", "ddrad", "snp", "hydrorivers", "hydroatlas", "abba-baba", "genotype", "environment"}
        phenomenon_vocab = {"adaptation", "gene", "flow", "geneflow", "riverscape", "association", "introgression", "selection", "connectivity", "network"}

        method_candidates = [t for t in candidates if t in method_vocab or re.search(r"\d", t) or "-" in t]
        phenomenon_candidates = [t for t in candidates if t in phenomenon_vocab or len(t.split()) >= 2]
        context_candidates = [t for t in candidates if t not in method_candidates and not self.is_geography_token(t)]

        method_token = method_candidates[0] if method_candidates else ""
        phenomenon_token = next((t for t in phenomenon_candidates if t not in {method_token}), "")
        context_token = next((t for t in context_candidates if t not in {method_token, phenomenon_token}), "")

        packs: List[List[str]] = []
        for proto in [
            [primary_token, phenomenon_token],
            [primary_token, context_token],
            [phenomenon_token, context_token],
            [primary_token, method_token],
            [method_token, phenomenon_token],
            [primary_token, context_token, phenomenon_token],
        ]:
            terms = [t.lower() for t in proto if t]
            terms = [t for t in dict.fromkeys(terms) if t.lower() not in GENERIC_WORDS and t not in CORE_STOPWORDS]
            if len(terms) < 2:
                continue
            if all(self.is_geography_token(t) for t in terms):
                continue
            if terms not in packs:
                packs.append(terms)
            if len(packs) >= 6:
                break

        self.search_log["anchor_packs"] = packs
        self.search_log["anchor_packs_search"] = packs
        return packs

    def normalize_search_anchor(self, value: str) -> str:
        val = self.translit_cyr((value or "").strip())
        val = re.sub(r"[^A-Za-z0-9\-\s]", " ", val)
        val = re.sub(r"\s+", " ", val).strip(" -")
        return val

    def keyword_fallback(self, idea_text: str, structured: Dict[str, Any]) -> List[str]:
        src = structured.get("structured_idea", structured) if isinstance(structured, dict) else {}
        for key in ("search_queries", "key_terms"):
            val = src.get(key) if isinstance(src, dict) else None
            if isinstance(val, list):
                out = [re.sub(r"\s+", " ", str(x).strip()) for x in val if isinstance(x, str) and str(x).strip()]
                if out:
                    return out[:20]
        latin_like = re.findall(r"\b(?=[A-Za-z0-9\-]{4,}\b)(?=.*[A-Za-z])[A-Za-z0-9](?:[A-Za-z0-9\-]*[A-Za-z0-9])?\b", idea_text)
        cyr_names = re.findall(r"\b[А-ЯЁ][а-яё]{3,}\b", idea_text)
        out = list(dict.fromkeys(latin_like + [self.translit_cyr(x) for x in cyr_names]))
        return [x for x in out if x and len(x) >= 4][:20]

    def get_keywords_for_search(self, idea_text: str, structured: Dict[str, Any]) -> Tuple[List[str], bool]:
        src = structured.get("structured_idea", structured) if isinstance(structured, dict) else {}
        raw = src.get("keywords_for_search") if isinstance(src, dict) else None
        if not isinstance(raw, list):
            return self.keyword_fallback(idea_text, structured), False
        cleaned: List[str] = []
        for item in raw:
            if not isinstance(item, str):
                continue
            q = re.sub(r"\s+", " ", item.strip())
            if q:
                cleaned.append(q)
        if not cleaned:
            return self.keyword_fallback(idea_text, structured), False
        return cleaned[:20], True

    def extract_keywords_tokens(self, keywords_for_search: List[str]) -> List[str]:
        return self.extract_strong_tokens(keywords_for_search, limit=50)

    def detect_primary_token(self, keywords_for_search: List[str], search_anchors: List[str]) -> str:
        token_pool = self.extract_strong_tokens(keywords_for_search[:5], limit=20)
        if not token_pool:
            token_pool = self.extract_strong_tokens(search_anchors, limit=20)
        for tok in token_pool:
            if tok.lower() in GENERIC_WORDS or self.is_geography_token(tok):
                continue
            return tok.lower()
        return token_pool[0].lower() if token_pool else ""

    def build_must_have_tokens(self, keywords_for_search: List[str]) -> List[str]:
        out: List[str] = []
        for tok in self.extract_strong_tokens(keywords_for_search, limit=30):
            if tok.lower() in GENERIC_WORDS or self.is_geography_token(tok):
                continue
            out.append(tok.lower())
            if len(out) >= 10:
                break
        return out[:10]

    def build_boolean_query(self, terms: List[str]) -> str:
        clean_terms = [x.lower() for x in self.sanitize_tokens(terms, context="query_builder") if x]
        uniq = list(dict.fromkeys(clean_terms))[:3]
        if len(uniq) <= 1:
            return uniq[0] if uniq else ""
        if len(uniq) == 2:
            return f"({uniq[0]} AND {uniq[1]})"
        return f"({uniq[0]} AND ({uniq[1]} OR {uniq[2]}))"

    def normalize_query_text(self, query: str) -> str:
        q = re.sub(r"\s+", " ", (query or "").strip())
        q = re.sub(r"\b(and|or)\b", lambda m: m.group(1).upper(), q, flags=re.IGNORECASE)
        q = re.sub(r"\s+", " ", q)
        return q

    def dedup_planned_queries(self, queries: List[str]) -> List[str]:
        out: List[str] = []
        seen: Set[str] = set()
        removed = 0
        for q in queries:
            nq = self.normalize_query_text(q).lower()
            if not nq:
                continue
            if nq in seen:
                removed += 1
                self.search_log.setdefault("rejected_queries", []).append({"query_text": q, "reason": "duplicate_query"})
                continue
            seen.add(nq)
            out.append(self.normalize_query_text(q))
        self.search_log["removed_duplicate_queries_count"] = removed
        return out

    def is_selective_term(self, token: str, probe_map: Dict[str, Dict[str, Any]]) -> bool:
        t = (token or "").lower().strip()
        if not t or t in CORE_STOPWORDS or t in GENERIC_WORDS:
            return False
        cat = probe_map.get(t, {}).get("category")
        if cat == "TOO_BROAD":
            return False
        if cat in {"OK", "NARROW", "BROAD"}:
            return True
        return self.is_term_like(token)

    def probe_category(self, total: int) -> str:
        if total >= PROBE_TOO_BROAD:
            return "TOO_BROAD"
        if total >= PROBE_BROAD:
            return "BROAD"
        if total >= PROBE_OK:
            return "OK"
        if total >= PROBE_NARROW:
            return "NARROW"
        return "ZERO"

    def is_valid_candidate_token(self, token: str) -> bool:
        tok = self.normalize_search_anchor(token).strip().lower()
        if not tok:
            return False
        if tok in CORE_STOPWORDS or tok in GENERIC_TOKEN_BLACKLIST:
            return False
        if len(tok) < 4 and not (re.search(r"\d", tok) or "-" in tok):
            return False
        return bool(re.search(r"[a-z0-9\-]", tok))

    def gather_candidate_tokens(self, primary_token: str, keywords_tokens: List[str], search_anchors: List[str], llm_tokens: List[str], llm_used: bool) -> List[str]:
        source = llm_tokens if llm_used else (keywords_tokens if keywords_tokens else search_anchors)
        merged = self.sanitize_tokens([primary_token] + source + keywords_tokens + search_anchors, context="candidate_tokens")
        out: List[str] = []
        seen: Set[str] = set()
        for token in merged:
            tok = self.normalize_search_anchor(token).lower()
            if not self.is_valid_candidate_token(tok):
                continue
            if tok in seen:
                continue
            seen.add(tok)
            out.append(tok)
            if len(out) >= 30:
                break
        return out

    def openalex_probe_count(self, token: str) -> Tuple[int, int]:
        params = {"search": token, "per-page": 1}
        if self.mailto:
            params["mailto"] = self.mailto
        endpoint = "https://api.openalex.org/works"
        try:
            obj, _, elapsed = self.request_json("GET", endpoint, "openalex", params=params, query_kind="probe")
            self.search_log["service_status"]["openalex"] = "ok"
            total = int((obj.get("meta") or {}).get("count") or 0)
            return max(total, 0), elapsed
        except Exception as e:
            self.search_log["service_status"]["openalex"] = "degraded"
            self.search_log["errors"].append(f"openalex_probe_error: {e}")
            return 0, 0

    def run_token_probe(self, candidates: List[str], primary_token: str) -> Dict[str, Dict[str, Any]]:
        selected: List[str] = []
        if primary_token and primary_token in candidates:
            selected.append(primary_token)
        strong = sorted(candidates, key=lambda t: self.token_strength(t), reverse=True)
        for tok in strong:
            if tok not in selected and len(selected) < 6:
                selected.append(tok)
        for tok in candidates:
            if tok not in selected and len(selected) < PROBE_MAX:
                selected.append(tok)
        selected = selected[:PROBE_MAX]

        probe_map: Dict[str, Dict[str, Any]] = {}
        for tok in selected:
            total, elapsed = self.openalex_probe_count(tok) if not self.offline_fixtures else (120, 0)
            category = self.probe_category(total)
            decision = "use"
            if category == "TOO_BROAD":
                decision = "avoid_solo"
            elif category == "BROAD":
                decision = "pack_only"
            elif category == "ZERO":
                decision = "exclude"
            probe_map[tok] = {
                "token": tok,
                "result_total": total,
                "total": total,
                "elapsed_ms": elapsed,
                "category": category,
                "decision": decision,
                "geo_like": self.is_geography_token(tok) or self.is_geo_like_token(tok.capitalize()),
            }
        self.search_log["token_probe"] = list(probe_map.values())
        return probe_map

    def preflight_queries(self, queries: List[str], primary_token: str, method_terms: List[str], probe_map: Dict[str, Dict[str, Any]]) -> Tuple[List[str], List[str]]:
        ok: List[str] = []
        bad: List[str] = []
        method_set = {m.lower() for m in method_terms if m}
        ptoken = (primary_token or "").lower()
        for q in queries:
            query = self.normalize_query_text(q)
            terms = [t.lower() for t in self.sanitize_tokens(re.findall(r"[A-Za-z0-9\-]+", query), context="preflight")]
            if not query or not terms:
                bad.append("Пустая поисковая строка")
                continue
            if any(t in CORE_STOPWORDS for t in terms):
                self.search_log["rejected_queries"].append({"query_text": query, "reason": "contains_stopword_term"})
                bad.append(f"Стоп-слово в запросе: {query}")
                continue
            if len(terms) == 1:
                info = probe_map.get(terms[0], {})
                if info.get("category") in {"TOO_BROAD", "BROAD"}:
                    bad.append(f"Слишком общий одиночный токен: {query}")
                    self.search_log["rejected_queries"].append({"query_text": query, "reason": "too_broad_solo"})
                    continue
            if len(terms) == 2 and ptoken and ptoken in terms:
                second = terms[1] if terms[0] == ptoken else terms[0]
                if (second in GENERIC_WORDS) or (not self.is_selective_term(second, probe_map)):
                    bad.append(f"Незначимый второй термин: {query}")
                    self.search_log["rejected_queries"].append({"query_text": query, "reason": "non_significant_second_term", "details": {"primary": ptoken, "term": second}})
                    continue
            geo_terms = [t for t in terms if self.is_geography_token(t) or self.is_geo_like_token(t.capitalize())]
            if len(geo_terms) == len(terms):
                bad.append(f"Geo-only запрос запрещён: {query}")
                self.search_log["rejected_queries"].append({"query_text": query, "reason": "geo_only"})
                continue
            if geo_terms and not any((t == ptoken) or (t in method_set) for t in terms):
                bad.append(f"География без пары primary/method запрещена: {query}")
                self.search_log["rejected_queries"].append({"query_text": query, "reason": "geo_without_pair"})
                continue
            ok.append(query)
        ok = self.dedup_planned_queries(ok)
        return ok[:SEED_QUERIES_MAX], bad

    def classify_query_blocks(self, tokens: List[str]) -> Dict[str, List[str]]:
        primary = []
        method = []
        phenomenon = []
        context = []
        for tok in tokens:
            t = tok.lower()
            if t in CORE_STOPWORDS or t in GENERIC_WORDS:
                continue
            if re.search(r"\d", t) or "-" in t or re.search(r"[a-z][A-Z]|[A-Z]{2,}", tok) or re.search(r"(net|graph|gan|bert|pcr)$", t):
                method.append(t)
            elif self.is_geography_token(t) or self.is_geo_like_token(tok.capitalize()):
                context.append(t)
            elif len(t.split()) >= 2:
                phenomenon.append(t)
            else:
                primary.append(t)
        return {"PRIMARY": primary, "METHOD": method, "PHENOMENON": phenomenon, "CONTEXT": context}

    def build_seed_queries(self, keywords_for_search: List[str], search_anchors: List[str], packs: List[List[str]], primary_token: str, source_terms: List[str], source_mode: str, probe_map: Dict[str, Dict[str, Any]]) -> List[str]:
        queries: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        tokens = self.normalize_token_list(source_terms + keywords_for_search + search_anchors)
        blocks = self.classify_query_blocks(tokens)
        method_terms = [t for t in blocks["METHOD"] if probe_map.get(t, {}).get("category") in {"OK", "NARROW", "BROAD"}]
        phen_terms = [t for t in blocks["PHENOMENON"] if probe_map.get(t, {}).get("category") in {"OK", "NARROW", "BROAD"}]
        context_terms = [t for t in blocks["CONTEXT"] if probe_map.get(t, {}).get("category") not in {"TOO_BROAD", "ZERO"}]

        def add_query(parts: List[str], reason: str) -> None:
            parts = [p for p in self.sanitize_tokens(parts, context="planned_query_parts") if p]
            if len(parts) < 1:
                self.search_log["rejected_queries"].append({"query_text": " ".join(parts), "reason": "empty_after_sanitize"})
                return
            if len(parts) >= 2 and primary_token in [x.lower() for x in parts]:
                second = next((x for x in parts if x.lower() != primary_token.lower()), "")
                if second and not self.is_selective_term(second, probe_map):
                    self.search_log["rejected_queries"].append({"query_text": self.build_boolean_query(parts[:3]), "reason": "non_significant_second_term", "details": {"term": second}})
                    return
            q = self.build_boolean_query(parts[:3])
            nq = self.normalize_query_text(q).lower()
            if q and nq and nq not in seen:
                seen.add(nq)
                queries.append({"query_text": self.normalize_query_text(q), "terms": parts[:3], "reason": reason, "round": 0})

        pcat = probe_map.get(primary_token, {}).get("category", "OK")
        if primary_token and pcat != "ZERO":
            add_query([primary_token], "primary_seed")
        for m in method_terms[:3]:
            add_query([primary_token, m], "primary_method")
        for ph in phen_terms[:2]:
            add_query([primary_token, ph], "primary_phenomenon")
        if (not primary_token) or pcat == "ZERO":
            for m in method_terms[:1]:
                for ph in phen_terms[:1]:
                    add_query([m, ph], "fallback_method_phenomenon")
        for c in context_terms[:1]:
            add_query([primary_token, c], "primary_context")
        for pack in packs[:6]:
            add_query([x.lower() for x in pack], "anchor_pack")

        raw_queries = [q["query_text"] for q in queries]
        good, bad = self.preflight_queries(raw_queries[:SEED_QUERIES_MAX], primary_token, method_terms, probe_map)
        for item in bad:
            self.search_log["errors"].append(f"query_preflight: {item}")
        self.search_log["planner"] = {
            "planned_queries": [q for q in queries if q["query_text"] in good][:SEED_QUERIES_MAX],
            "rejected_queries": self.search_log.get("rejected_queries", []),
            "removed_duplicates": self.search_log.get("removed_duplicate_queries_count", 0),
        }
        self.search_log["planned_queries"] = good[:SEED_QUERIES_MAX]
        self.search_log["planned_queries_detail"] = [q for q in queries if q["query_text"] in good][:SEED_QUERIES_MAX]
        return good[:SEED_QUERIES_MAX]


    def plan_seed_queries(self, keywords_for_search: List[str], search_anchors: List[str], packs: List[List[str]], primary_token: str, source_terms: List[str], source_mode: str, probe_map: Dict[str, Dict[str, Any]]) -> List[str]:
        return self.build_seed_queries(keywords_for_search, search_anchors, packs, primary_token, source_terms, source_mode, probe_map)

    def validate_seed_queries(self, queries: List[str], probe_map: Dict[str, Dict[str, Any]]) -> List[str]:
        alive: List[str] = []
        ok_tokens = [x["token"] for x in self.search_log.get("token_probe", []) if x.get("category") in {"OK", "NARROW"}]
        for query in self.dedup_planned_queries(queries):
            attempts = 0
            current = self.normalize_query_text(query)
            while attempts < 3:
                attempts += 1
                total = 1
                if not self.offline_fixtures:
                    total, _ = self.openalex_probe_count(current)
                if total > 0:
                    terms = [t.lower() for t in self.sanitize_tokens(re.findall(r"[A-Za-z0-9\-]+", current), context="validate_queries")]
                    if total > PROBE_TOO_BROAD and len(terms) == 2 and any(probe_map.get(t, {}).get("category") == "TOO_BROAD" for t in terms):
                        self.search_log["rejected_queries"].append({"query_text": current, "reason": "rejected_too_broad_pair", "details": {"result_total": total}})
                    else:
                        alive.append(current)
                        break
                else:
                    self.search_log["rejected_queries"].append({"query_text": current, "reason": "rejected_zero", "details": {"attempt": attempts}})
                terms = [t.lower() for t in self.sanitize_tokens(re.findall(r"[A-Za-z0-9\-]+", current), context="validate_rebuild")]
                replace_target = next((t for t in terms if probe_map.get(t, {}).get("category") in {"ZERO", "TOO_BROAD"}), "")
                repl = next((t for t in ok_tokens if t not in terms and self.is_selective_term(t, probe_map)), "")
                if replace_target and repl:
                    rebuilt = [repl if t == replace_target else t for t in terms]
                    current = self.build_boolean_query(rebuilt[:3])
                else:
                    break
            if len(alive) >= SEED_QUERIES_MAX:
                break
        alive = self.dedup_planned_queries(alive)[:SEED_QUERIES_MAX]
        self.search_log["planned_queries"] = alive
        self.search_log.setdefault("planner", {})["removed_duplicates"] = self.search_log.get("removed_duplicate_queries_count", 0)
        return alive

    def openalex_search_pack(self, query: str) -> Tuple[List[Dict[str, Any]], int]:
        params = {"search": query, "per-page": MAX_PER_QUERY, "filter": "has_abstract:true"}
        endpoint = "https://api.openalex.org/works"
        try:
            obj, status, elapsed = self.request_json("GET", endpoint, "openalex", params=params, query_kind="seed")
            total = int((obj.get("meta") or {}).get("count") or 0)
            items = obj.get("results", [])
            if total < 5:
                fallback_params = {"search": query, "per-page": MAX_PER_QUERY}
                fallback_obj, _, _ = self.request_json("GET", endpoint, "openalex", params=fallback_params, query_kind="seed")
                fallback_total = int((fallback_obj.get("meta") or {}).get("count") or 0)
                if fallback_total > total:
                    total = fallback_total
                    items = fallback_obj.get("results", [])
            if self.search_log["queries"]:
                self.search_log["queries"][-1].update({
                    "query_text": query,
                    "anchor_pack_used": [query],
                    "result_total": total,
                    "total": total,
                    "result_items": len(items),
                })
            if len(self.query_snippets) < 3:
                self.query_snippets.append({"query_text": query, "result_total": total})
            self.search_log["service_status"]["openalex"] = "ok"
            return items, total
        except Exception:
            if self.search_log["queries"]:
                self.search_log["queries"][-1].update({"query_text": query, "anchor_pack_used": [query]})
            if len(self.query_snippets) < 3:
                self.query_snippets.append({"query_text": query, "result_total": 0})
            raise

    def paper_openalex(self, w: Dict[str, Any], tag: str) -> Dict[str, Any]:
        return {
            "title": w.get("title", ""),
            "year": w.get("publication_year") or "",
            "doi": normalize_doi(w.get("doi", "")),
            "openalex_id": w.get("id", ""),
            "venue": ((w.get("primary_location", {}).get("source") or {}).get("display_name") or ""),
            "source_type": ((w.get("primary_location", {}).get("source") or {}).get("type") or ""),
            "source_display_name": ((w.get("primary_location", {}).get("source") or {}).get("display_name") or ""),
            "authors_short": ", ".join([(a.get("author", {}) or {}).get("display_name", "") for a in (w.get("authorships") or [])[:4]]),
            "cited_by_count": w.get("cited_by_count", 0) or 0,
            "source_tags": {tag},
            "url": w.get("primary_location", {}).get("landing_page_url") or w.get("id", ""),
            "type": w.get("type", ""),
            "concepts": [c.get("display_name", "") for c in (w.get("concepts") or [])[:5]],
            "referenced_works": w.get("referenced_works", [])[:15],
            "related_works": w.get("related_works", [])[:15],
            "abstract": self.decode_openalex_abstract(w),
        }

    def semantic_scholar_search(self, pack: List[str]) -> List[Dict[str, Any]]:
        q = " ".join(pack[:3])
        params = {"query": q, "limit": 25, "fields": "title,year,venue,authors,citationCount,externalIds,url"}
        try:
            obj, _, _ = self.request_json("GET", "https://api.semanticscholar.org/graph/v1/paper/search", "semantic_scholar", params=params, query_kind="expansion")
            self.search_log["service_status"]["semantic_scholar"] = "ok"
            out = []
            for p in obj.get("data", []):
                out.append({
                    "title": p.get("title", ""), "year": p.get("year") or "", "doi": normalize_doi((p.get("externalIds") or {}).get("DOI", "")),
                    "openalex_id": "", "venue": p.get("venue", ""),
                    "authors_short": ", ".join([a.get("name", "") for a in p.get("authors", [])[:4]]),
                    "cited_by_count": p.get("citationCount", 0) or 0,
                    "source_type": "", "source_display_name": p.get("venue", ""),
                    "source_tags": {"semanticscholar_search"}, "url": p.get("url", ""), "type": "",
                    "concepts": [], "referenced_works": [], "related_works": [], "abstract": "",
                })
            return out
        except Exception as e:
            self.search_log["service_status"]["semantic_scholar"] = "degraded"
            if "429" in str(e):
                self.search_log["errors"].append("semantic_search_error: 429 rate limit; fallback to OpenAlex/Crossref")
            else:
                self.search_log["errors"].append(f"semantic_search_error: {e}")
            return []

    def semantic_recommend(self, seeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        positive = [f"DOI:{p['doi']}" for p in seeds if p.get("doi")][:10]
        if not positive:
            return []
        payload = {"positive_paper_ids": positive, "negative_paper_ids": []}
        try:
            obj, _, _ = self.request_json("POST", "https://api.semanticscholar.org/recommendations/v1/papers", "semantic_scholar", payload=payload, query_kind="expansion")
            self.search_log["service_status"]["semantic_scholar"] = "ok"
            out: List[Dict[str, Any]] = []
            for p in obj.get("recommendedPapers", []):
                out.append({
                    "title": p.get("title", ""), "year": p.get("year") or "", "doi": normalize_doi((p.get("externalIds") or {}).get("DOI", "")),
                    "openalex_id": "", "venue": p.get("venue", ""),
                    "authors_short": ", ".join([a.get("name", "") for a in p.get("authors", [])[:4]]),
                    "cited_by_count": p.get("citationCount", 0) or 0,
                    "source_type": "", "source_display_name": p.get("venue", ""),
                    "source_tags": {"semanticscholar_recommendations"}, "url": f"https://www.semanticscholar.org/paper/{p.get('paperId', '')}",
                    "type": "", "concepts": [], "referenced_works": [], "related_works": [], "abstract": "",
                })
            return out
        except Exception as e:
            self.search_log["service_status"]["semantic_scholar"] = "degraded"
            self.search_log["errors"].append(f"semantic_recommendations_error: {e}")
            return []

    def crossref_enrich(self, papers: List[Dict[str, Any]]) -> int:
        count = 0
        for p in papers[:120]:
            if p.get("doi") or not p.get("title"):
                continue
            try:
                params = {"query.title": p["title"], "rows": 1}
                obj, _, _ = self.request_json("GET", "https://api.crossref.org/works", "crossref", params=params, query_kind="metadata")
                items = (obj.get("message") or {}).get("items") or []
                if items:
                    doi = normalize_doi(items[0].get("DOI", ""))
                    if doi:
                        p["doi"] = doi
                        p["source_tags"].add("crossref")
                        count += 1
                self.search_log["service_status"]["crossref"] = "ok"
            except Exception as e:
                self.search_log["service_status"]["crossref"] = "degraded"
                self.search_log["errors"].append(f"crossref_error: {e}")
                break
        return count

    def expand_openalex(self, seeds: List[Dict[str, Any]], anchor_packs: List[List[str]], max_seed: int = 30) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        ids: List[str] = []
        for p in seeds[:max_seed]:
            ids.extend(p.get("referenced_works", [])[:5])
            ids.extend(p.get("related_works", [])[:5])
        for wid in ids[:120]:
            try:
                obj, _, _ = self.request_json("GET", f"https://api.openalex.org/works/{wid.split('/')[-1]}", "openalex", query_kind="expansion")
                cand = self.paper_openalex(obj, "openalex_related")
                text = (cand.get("title", "") + " " + cand.get("venue", "")).lower()
                pack_match = any(sum(1 for a in pack if a.lower() in text) >= 1 for pack in anchor_packs)
                strong = cand.get("cited_by_count", 0) >= 100 and sum(1 for a in self.search_log["anchor_top_search"][:8] if a.lower() in text) >= 2
                if pack_match or strong:
                    out.append(cand)
            except Exception:
                continue
        return out

    def dedup(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.dedup_before = len(papers)

        def first_author_lastname(row: Dict[str, Any]) -> str:
            first = (row.get("authors_short", "").split(",") or [""])[0].strip().lower()
            return re.sub(r"[^a-zа-я0-9]+", "", first.split()[-1] if first else "")

        def quality_score(row: Dict[str, Any]) -> float:
            score = 0.0
            if row.get("abstract"):
                score += 3.0
            if row.get("doi"):
                score += 2.0
            src_type = str(row.get("source_type", "")).lower()
            if src_type == "journal":
                score += 1.0
            score += 0.5 * math.log1p(max(int(row.get("cited_by_count") or 0), 0))
            if row.get("relevance_flag") == "PASS":
                score += 1.0
            return score

        def choose_best(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
            best = max(rows, key=quality_score)
            merged_tags: Set[str] = set()
            max_cit = 0
            for r in rows:
                merged_tags |= set(r.get("source_tags", set()))
                max_cit = max(max_cit, int(r.get("cited_by_count") or 0))
            out = dict(best)
            out["source_tags"] = merged_tags
            out["cited_by_count"] = max_cit
            return out

        groups: List[List[Dict[str, Any]]] = []
        doi_map: Dict[str, List[Dict[str, Any]]] = {}
        no_doi: List[Dict[str, Any]] = []
        for p in papers:
            doi = normalize_doi(str(p.get("doi", "")))
            p["doi"] = doi
            if doi:
                doi_map.setdefault(doi, []).append(p)
            else:
                no_doi.append(p)
        groups.extend(list(doi_map.values()))

        by_t2: Dict[str, List[Dict[str, Any]]] = {}
        for p in no_doi:
            k2 = f"{norm_title(p.get('title',''))}_{p.get('year','')}"
            if k2 == "_":
                continue
            by_t2.setdefault(k2, []).append(p)
        stage2: List[Dict[str, Any]] = [choose_best(v) for v in by_t2.values()]

        by_t3: Dict[str, List[Dict[str, Any]]] = {}
        for p in stage2:
            k3 = f"{norm_title(p.get('title',''))}_{p.get('year','')}_{first_author_lastname(p)}"
            by_t3.setdefault(k3, []).append(p)
        groups.extend(list(by_t3.values()))

        deduped = [choose_best(g) for g in groups if g]
        self.dedup_after = len(deduped)
        self.dedup_merged_count = max(self.dedup_before - self.dedup_after, 0)
        return deduped

    def is_dataset_candidate(self, p: Dict[str, Any], text: str) -> bool:
        if str(p.get("type", "")).lower() == "dataset":
            return True
        if re.search(r"occurrence\s+download", text, flags=re.IGNORECASE):
            return True
        venue_blob = " ".join([str(p.get("venue", "")), str(p.get("source_display_name", "")), str(p.get("title", ""))]).lower()
        if "gbif" in venue_blob:
            return True
        url = str(p.get("url", "")).lower()
        return any(hint in url for hint in DATASET_DOMAIN_HINTS)

    def normalize_text_for_match(self, text: str) -> Dict[str, str]:
        base = (text or "").lower()
        keep_hyphen = re.sub(r"\s+", " ", re.sub(r"[^\w\s\-]", " ", base)).strip()
        hyphen_as_space = re.sub(r"\s+", " ", keep_hyphen.replace("-", " ")).strip()
        return {"with_hyphen": keep_hyphen, "hyphen_as_space": hyphen_as_space}

    def token_match(self, token: str, normalized_texts: Dict[str, str]) -> bool:
        words = [w for w in re.findall(r"[a-z0-9\-]+", token.lower()) if w]
        if not words:
            return False
        variants = [normalized_texts.get("with_hyphen", ""), normalized_texts.get("hyphen_as_space", "")]
        patterns = []
        if len(words) == 1:
            patterns = [r"\b" + re.escape(words[0]) + r"\b"]
            if "-" in words[0]:
                split = [w for w in words[0].split("-") if w]
                if len(split) > 1:
                    patterns.append(r"\b" + r"\s+".join(re.escape(w) for w in split) + r"\b")
        else:
            patterns = [r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b"]
        return any(re.search(pat, txt, flags=re.IGNORECASE) for pat in patterns for txt in variants if txt)

    def sanitize_blacklist(self, raw_list: List[str]) -> Tuple[List[str], List[Dict[str, str]]]:
        sanitized: List[str] = []
        dropped: List[Dict[str, str]] = []
        seen: Set[str] = set()
        for raw in raw_list:
            txt = re.sub(r"\s+", " ", str(raw or "").strip().lower())
            if not txt:
                continue
            tokens = [t for t in re.findall(r"[a-z0-9\-]+", txt) if t]
            is_exception = bool(re.search(r"\d", txt) and "-" in txt)
            if len(txt) < 5 and not is_exception:
                dropped.append({"item": txt, "reason": "too_short"})
                continue
            if len(tokens) == 1 and 5 <= len(tokens[0]) <= 6 and tokens[0] in BLACKLIST_SINGLE_WORD_STOP:
                dropped.append({"item": txt, "reason": "generic_single_word"})
                continue
            if txt in seen:
                continue
            seen.add(txt)
            sanitized.append(txt)
        return sanitized, dropped

    def find_blacklist_match(self, normalized_text: str, blacklist_terms: List[str]) -> str:
        for term in blacklist_terms:
            words = [w for w in re.findall(r"[a-z0-9\-]+", term.lower()) if w]
            if not words:
                continue
            if len(words) == 1:
                pattern = r"\b" + re.escape(words[0]) + r"\b"
            else:
                pattern = r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b"
            if re.search(pattern, normalized_text, flags=re.IGNORECASE):
                return term
        return ""

    def is_term_like(self, token: str) -> bool:
        t = (token or "").strip()
        if not t:
            return False
        return bool(re.search(r"\d", t) or "-" in t or len(t.split()) >= 2 or re.search(r"[a-z][A-Z]|[A-Z]{2,}", t))

    def build_support_tokens(self, keywords_tokens: List[str], primary_token: str, llm_support_tokens: Optional[List[str]] = None) -> List[str]:
        candidate_raw = llm_support_tokens if llm_support_tokens else keywords_tokens
        candidate_raw = self.sanitize_tokens(candidate_raw, context="support_tokens")
        primary_low = (primary_token or "").lower()
        out: List[str] = []
        seen: Set[str] = set()
        for tok in candidate_raw:
            clean = self.normalize_search_anchor(str(tok or "")).lower()
            if not clean or clean == primary_low:
                continue
            if clean in seen:
                continue
            if self.is_geo_like_token(clean.capitalize()) or self.is_geography_token(clean):
                continue
            if clean in GENERIC_TOKEN_BLACKLIST or clean in CORE_STOPWORDS:
                continue
            if llm_support_tokens or self.is_term_like(tok):
                seen.add(clean)
                out.append(clean)
        if not llm_support_tokens and len(out) < 10:
            fallback = [self.normalize_search_anchor(str(t)).lower() for t in keywords_tokens]
            for tok in fallback:
                if tok and tok not in seen and tok != primary_low and tok not in GENERIC_TOKEN_BLACKLIST and tok not in CORE_STOPWORDS:
                    seen.add(tok)
                    out.append(tok)
                if len(out) >= 20:
                    break
        return out[:20]

    def apply_relevance_and_score(self, papers: List[Dict[str, Any]], primary_token: str, keywords_tokens: List[str], must_have_tokens: List[str], anchor_packs: List[List[str]], drift_blacklist_raw: List[str], support_tokens: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], float, Dict[str, int], Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        eval_flags: List[bool] = []
        metrics = {"primary_hit_count": 0, "packs_hit_count": 0, "dataset_low_count": 0, "blacklist_low_count": 0, "support_hit_count": 0}
        pack_examples: List[Dict[str, Any]] = []
        drift_blacklist, drift_blacklist_dropped = self.sanitize_blacklist(drift_blacklist_raw)
        blacklist_matches = 0

        for p in papers:
            title = str(p.get("title", ""))
            abstract = str(p.get("abstract", ""))
            text = f"{title} {abstract}".lower().strip()
            normalized_texts = self.normalize_text_for_match(f"{title} {abstract}")
            normalized_text = normalized_texts.get("with_hyphen", "")
            title_only = title.lower()
            hit_primary = bool(primary_token) and self.token_match(primary_token, normalized_texts)
            hits_kw = sum(1 for t in keywords_tokens if t and self.token_match(t, normalized_texts))
            hits_must = sum(1 for t in must_have_tokens if t and self.token_match(t, normalized_texts))
            matched_support = [t for t in support_tokens if t and self.token_match(t, normalized_texts)]
            hits_support = len(matched_support)
            hits_packs = 0
            for pack in anchor_packs:
                terms = [t.lower() for t in pack if t]
                if len(terms) < 2:
                    continue
                in_text = all(self.token_match(t, normalized_texts) for t in terms)
                in_title = (not abstract.strip()) and all(self.token_match(t, self.normalize_text_for_match(title_only)) for t in terms)
                if in_text or in_title:
                    hits_packs += 1

            dataset_flag = self.is_dataset_candidate(p, text)
            black_term = self.find_blacklist_match(normalized_text, drift_blacklist)
            black_hit = bool(black_term)
            has_abstract = bool(abstract.strip())
            support_rule_hit = (hits_support >= 1) or (hits_kw >= 3 and hits_support >= 1)

            if black_hit and (not hit_primary and hits_packs == 0):
                blacklist_matches += 1

            if dataset_flag:
                relevance_flag = "LOW"
                reason = "low_dataset"
                metrics["dataset_low_count"] += 1
            elif hit_primary or hits_packs >= 1:
                relevance_flag = "PASS_MAIN"
                reason = "pass_primary" if hit_primary else "pass_pack"
            elif black_hit and not hit_primary and hits_packs == 0:
                relevance_flag = "LOW"
                reason = "low_drift_blacklist"
                metrics["blacklist_low_count"] += 1
            elif (not has_abstract) and (not hit_primary) and hits_packs < 1:
                relevance_flag = "LOW"
                reason = "low_missing_text"
            elif support_rule_hit:
                relevance_flag = "PASS_SUPPORT"
                reason = "pass_support"
            else:
                relevance_flag = "LOW"
                reason = "low_no_signal"

            if hit_primary:
                metrics["primary_hit_count"] += 1
            if hits_packs >= 1:
                metrics["packs_hit_count"] += 1
                if len(pack_examples) < 3:
                    pack_examples.append({"title": title, "hits_packs": hits_packs})
            if hits_support >= 1:
                metrics["support_hit_count"] += 1

            score_main = (6 if hit_primary else 0) + (4 * hits_packs) + hits_kw + (2 * hits_must)
            score_support = (3 * hits_support) + (1.5 * hits_kw) + (1.0 * hits_must)
            citation_score = math.log1p(max(int(p.get("cited_by_count") or 0), 0)) * 0.25
            now_year = datetime.now().year
            year = int(p.get("year") or 0) if str(p.get("year") or "").isdigit() else 0
            freshness_score = 0.5 if (year and year >= (now_year - 5)) else 0.0
            score = (score_main + citation_score + freshness_score) if relevance_flag == "PASS_MAIN" else (score_support + (citation_score * 0.5) + freshness_score)

            row = dict(p)
            row.update({
                "tier": "main" if relevance_flag == "PASS_MAIN" else ("support" if relevance_flag == "PASS_SUPPORT" else "low"),
                "score": round(score, 4),
                "score_main": round(score_main + citation_score + freshness_score, 4),
                "score_support": round(score_support + (citation_score * 0.5) + freshness_score, 4),
                "relevance_flag": relevance_flag,
                "reason": reason,
                "reason_detail": black_term if reason == "low_drift_blacklist" else ("matched_tokens: " + "; ".join(matched_support[:4]) if reason == "pass_support" else ""),
                "hits_support": hits_support,
                "hits_kw": hits_kw,
                "hits_must": hits_must,
                "hit_primary": hit_primary,
                "support_reason": "; ".join(matched_support[:4]),
                "score_components": f"main={round(score_main,3)}; support={round(score_support,3)}; primary={int(hit_primary)}; packs={hits_packs}; kw={hits_kw}; must={hits_must}; support_hits={hits_support}; cit={round(citation_score,3)}; fresh={round(freshness_score,3)}",
            })
            rows.append(row)
            drift_pass = hit_primary or hits_packs >= 1 or (hits_kw >= 3 and hits_must >= 1)
            eval_flags.append(drift_pass)

        n = min(50, len(eval_flags))
        pass_top_n = sum(1 for flag in eval_flags[:n] if flag)
        drift_score = 1.0 - (pass_top_n / n) if n else 1.0

        rows.sort(key=lambda x: (-x.get("score_main", 0), -(x.get("cited_by_count", 0) or 0), x.get("title", "")))
        passed_main = [r for r in rows if r.get("relevance_flag") == "PASS_MAIN"]
        passed_support = sorted(
            [r for r in rows if r.get("relevance_flag") == "PASS_SUPPORT"],
            key=lambda x: (-x.get("score_support", 0), -(x.get("cited_by_count", 0) or 0), x.get("title", ""),),
        )
        self.search_log["pack_hit_examples"] = pack_examples
        blacklist_stats = {
            "raw_count": len(drift_blacklist_raw),
            "sanitized_count": len(drift_blacklist),
            "dropped": drift_blacklist_dropped,
            "applied_matches_count": blacklist_matches,
        }
        return passed_main, passed_support, rows, round(drift_score, 4), metrics, blacklist_stats

    def select_auto_must_have_tokens(self, ranked_all: List[Dict[str, Any]], candidate_tokens: List[str], probe_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        top_k = ranked_all[: min(120, len(ranked_all))]
        if not top_k:
            return []
        selected: List[Dict[str, Any]] = []
        for tok in candidate_tokens:
            t = (tok or "").lower().strip()
            if not t:
                continue
            meta = probe_map.get(t, {})
            cat = meta.get("category", "")
            if cat not in {"OK", "NARROW"}:
                continue
            if meta.get("geo_like") or self.is_geography_token(t):
                continue
            freq_hit = 0
            for row in top_k:
                normalized = self.normalize_text_for_match(f"{row.get('title', '')} {row.get('abstract', '')}")
                if self.token_match(t, normalized):
                    freq_hit += 1
            freq = freq_hit / max(len(top_k), 1)
            if 0.10 <= freq <= 0.70:
                selected.append({"token": t, "freq": round(freq, 4), "category": cat})
        selected.sort(key=lambda x: (abs(0.4 - float(x.get("freq", 0))), -len(str(x.get("token", "")))), reverse=False)
        return selected[:6]

    def write_corpus(self, passed_papers: List[Dict[str, Any]], support_papers: List[Dict[str, Any]], all_papers: List[Dict[str, Any]], allow_replace: bool = True) -> None:
        mode_limits = {
            "FOCUSED": {"main": 180, "support": 60, "all": 220, "min_main": 40},
            "BALANCED": {"main": 220, "support": 100, "all": 300, "min_main": 50},
            "WIDE": {"main": 260, "support": 160, "all": 360, "min_main": 50},
        }
        limits = mode_limits.get(self.mode, mode_limits["BALANCED"])
        common_fields = ["rank", "tier", "score", "title", "year", "doi", "openalex_id", "venue", "authors_short", "cited_by_count", "source_tags", "url"]
        extra_fields = ["relevance_flag", "reason", "reason_detail", "hits_support", "support_reason", "score_components"]
        main_rows = passed_papers[: int(limits["main"])]
        support_rows = support_papers[: int(limits["support"])]
        corpus_rows = main_rows + support_rows
        if len(main_rows) < int(limits["min_main"]):
            deficit = int(limits["min_main"]) - len(main_rows)
            corpus_rows = main_rows + support_rows[: max(deficit, 0)]
        targets = [
            ("corpus.csv", corpus_rows, common_fields),
            ("corpus_support.csv", support_rows, common_fields),
            ("corpus_all.csv", all_papers[: int(limits["all"])], common_fields + extra_fields),
            ("corpus_support_all.csv", support_papers, common_fields + extra_fields),
        ]
        for name, rows, fields in targets:
            target = self.out_dir / name
            tmp = self.out_dir / f"{name}.tmp"
            with tmp.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                for idx, p in enumerate(rows, 1):
                    w.writerow({
                        "rank": idx,
                        "tier": p.get("tier", "main" if p in passed_papers else "support"),
                        "score": p.get("score", 0),
                        "title": p.get("title", ""),
                        "year": p.get("year", ""),
                        "doi": p.get("doi", ""),
                        "openalex_id": p.get("openalex_id", ""),
                        "venue": p.get("venue", ""),
                        "authors_short": p.get("authors_short", ""),
                        "cited_by_count": p.get("cited_by_count", 0),
                        "source_tags": ",".join(sorted(list(p.get("source_tags", set())))),
                        "url": p.get("url", ""),
                        "relevance_flag": p.get("relevance_flag", "PASS"),
                        "reason": p.get("reason", ""),
                        "reason_detail": p.get("reason_detail", ""),
                        "score_components": p.get("score_components", ""),
                    })
            shutil.move(str(tmp), str(target))


    def compute_go_metrics(self, ranked_rows: List[Dict[str, Any]], alive_seed_queries: int) -> Dict[str, Any]:
        top = ranked_rows[:TOPN_FOR_METRICS]
        n = len(top) or 1
        primary_hits = sum(1 for r in top if bool(r.get("hit_primary")) or int(r.get("hits_packs", 0)) >= 1)
        pass_hits = sum(1 for r in top if str(r.get("relevance_flag", "")).startswith("PASS_"))
        pass_ratio = pass_hits / n
        primary_signal = primary_hits / n
        drift = 1.0 - pass_ratio
        reasons: List[str] = []
        if alive_seed_queries < SEED_QUERIES_MIN_ALIVE:
            reasons.append("too_few_alive_queries")
        if pass_ratio < (1.0 - self.drift_target):
            reasons.append("drift_high")
        if primary_signal < PRIMARY_SIGNAL_MIN:
            reasons.append("low_primary_signal")
        if drift >= DRIFT_HARD_STOP and "drift_high" not in reasons:
            reasons.append("drift_hard_stop")
        go = len(reasons) == 0
        return {
            "go_nogo": "GO" if go else "NO-GO",
            "go": go,
            "pass_ratio": round(pass_ratio, 4),
            "primary_signal": round(primary_signal, 4),
            "drift": round(drift, 4),
            "alive_seed_queries": alive_seed_queries,
            "reasons": reasons,
        }

    def maybe_add_citation_chasing(self, main_rows: List[Dict[str, Any]], ranked_all: List[Dict[str, Any]],
                                   primary_token: str, keywords_tokens: List[str], must_have_tokens: List[str],
                                   packs: List[List[str]], drift_blacklist: List[str], support_tokens: List[str],
                                   baseline_drift: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], float, bool]:
        if (not CITATION_CHASE_ENABLE) or self.offline_fixtures:
            return main_rows, [], ranked_all, baseline_drift, False
        seeds = sorted(main_rows, key=lambda r: float(r.get("score_main", 0)), reverse=True)[:CITATION_TOPK]
        added: List[Dict[str, Any]] = []
        for seed in seeds:
            if len(added) >= CITATION_MAX_ADD:
                break
            sid = (seed.get("openalex_id") or "").strip()
            if not sid:
                continue
            wid = sid.split("/")[-1]
            for rid in (seed.get("referenced_works") or [])[:CITATION_PER_SEED_CAP]:
                try:
                    obj, _, _ = self.request_json("GET", f"https://api.openalex.org/works/{rid.split('/')[-1]}", "openalex", query_kind="citation")
                    added.append(self.paper_openalex(obj, "openalex_backward"))
                except Exception:
                    continue
            try:
                params = {"filter": f"cites:{wid}", "per-page": CITATION_PER_SEED_CAP}
                if self.mailto:
                    params["mailto"] = self.mailto
                obj, _, _ = self.request_json("GET", "https://api.openalex.org/works", "openalex", params=params, query_kind="citation")
                for w in (obj.get("results") or [])[:CITATION_PER_SEED_CAP]:
                    added.append(self.paper_openalex(w, "openalex_forward"))
            except Exception:
                pass
        added = self.dedup(added)[:CITATION_MAX_ADD]
        if not added:
            return main_rows, [], ranked_all, baseline_drift, False
        passed, support, ranked, drift_after, _, _ = self.apply_relevance_and_score(
            self.dedup(ranked_all + added), primary_token, keywords_tokens, must_have_tokens, packs, drift_blacklist, support_tokens
        )
        if drift_after > self.drift_target and drift_after > baseline_drift:
            self.search_log["citation_addition_rolled_back"] = True
            return main_rows, [], ranked_all, baseline_drift, True
        self.search_log["citation_addition_rolled_back"] = False
        self.search_log["citation_added_count"] = len(added)
        return passed, support, ranked, drift_after, True

    def write_search_strategy(self, stats: Dict[str, Any]) -> None:
        rows = self.search_log.get("executed_queries", [])
        lines = [
            "# Search strategy for Stage B",
            "",
            f"- run_id: {self.run_id}",
            f"- started_at: {self.search_log.get('started_at', '')}",
            f"- finished_at: {self.search_log.get('finished_at', now_iso())}",
            "",
            "## Sources status",
            f"- OpenAlex: {self.search_log.get('service_status', {}).get('openalex', 'offline')}",
            f"- Semantic Scholar: {self.search_log.get('service_status', {}).get('semantic_scholar', 'offline')}",
            f"- Crossref: {self.search_log.get('service_status', {}).get('crossref', 'offline')}",
            "",
            "## Queries",
            "| query_text | result_total | cap_used | items_added |",
            "|---|---:|---:|---:|",
        ]
        for q in rows:
            lines.append(f"| {q.get('query_text','')} | {q.get('result_total',0)} | {q.get('cap_used', MAX_PER_QUERY)} | {q.get('items_added',0)} |")
        lines += [
            "",
            "## Token probe",
            "| token | result_total | category | decision |",
            "|---|---:|---|---|",
        ]
        for t in self.search_log.get("token_probe", []):
            lines.append(f"| {t.get('token','')} | {t.get('total', t.get('result_total',0))} | {t.get('category','')} | {t.get('decision','')} |")
        lines += [
            "",
            "## GO and metrics",
            f"- go_nogo: {stats.get('go_nogo', '')}",
            f"- drift_round0: {stats.get('drift_round0', '')}",
            f"- drift_round1: {stats.get('drift_round1', '')}",
            f"- drift_round2: {stats.get('drift_round2', '')}",
            "",
            "## Final counts",
            f"- seed_count: {stats.get('seed_count', 0)}",
            f"- total_candidates: {stats.get('total_candidates', 0)}",
            f"- dedup_after: {self.search_log.get('stats', {}).get('dedup_after', 0)}",
            f"- main_count: {stats.get('main_count', 0)}",
            f"- support_count: {stats.get('support_count', 0)}",
            f"- low_count: {stats.get('low_count', 0)}",
        ]
        write_text(self.out_dir / "search_strategy_B.md", "\n".join(lines) + "\n")

    def write_prisma(self, stats: Dict[str, Any]) -> None:
        openalex_q = [q for q in self.search_log.get("queries", []) if q.get("source") == "openalex"]
        s2_q = [q for q in self.search_log.get("queries", []) if q.get("source") == "semantic_scholar"]
        unresolved = sorted([ab for ab in self.abbr_mentions if ab not in self.abbr_full_map])
        lines = [
            "# PRISMA-lite для этапа B",
            "",
            f"- Запуск: {self.search_log.get('started_at', '')}",
            f"- Завершение: {self.search_log.get('finished_at', now_iso())}",
            f"- OpenAlex: {self.search_log['service_status']['openalex']}",
            f"- Semantic Scholar: {self.search_log['service_status']['semantic_scholar']}",
            f"- Crossref: {self.search_log['service_status']['crossref']}",
            f"- Seed queries: {stats.get('seed_queries', 0)}",
            f"- Seed count: {stats.get('seed_count', 0)}",
            f"- total_candidates: {stats.get('total_candidates', 0)}",
            f"- main_count: {stats.get('main_count', 0)}",
            f"- support_count: {stats.get('support_count', 0)}",
            f"- low_count: {stats.get('low_count', 0)}",
            f"- drift_score: {stats.get('drift_score', 0)}",
            "- outputs: corpus.csv (object-first main) + corpus_support.csv (support methods/concepts without object)",
            "",
            "## OpenAlex seed queries",
            *[f"- {q.get('query_text','')} → {q.get('result_total', 0)}" for q in openalex_q if q.get("query_kind") == "seed"],
            "",
            "## Semantic Scholar queries",
            *[f"- {q.get('query_text','')} → {q.get('result_total', 0)}" for q in s2_q],
            "",
            "## Token probe",
            *[f"- {i.get('token','')}: total={i.get('total', i.get('result_total',0))}, category={i.get('category','')}, decision={i.get('decision','')}" for i in self.search_log.get('token_probe', [])],
            "",
            "## Query planning",
            *[f"- planned: {q}" for q in self.search_log.get('planned_queries', [])],
            *[f"- rejected: {q.get('query_text','')} ({q.get('reason','')})" for q in self.search_log.get('rejected_queries', [])],
        ]
        if unresolved:
            lines += ["", "## Нераскрытые аббревиатуры", *[f"- {ab}" for ab in unresolved]]
        prisma_text = "\n".join(lines) + "\n"
        write_text(self.prisma_path, prisma_text)
        write_text(self.prisma_legacy_path, prisma_text)
        self.write_search_strategy(stats)

    def load_llm_budget_state(self) -> Dict[str, Any]:
        defaults = {
            "limit": self.llm_budget,
            "used": 0,
            "updated_at": now_iso(),
            "last_stop_reason": "",
            "last_run_id": "",
        }
        if not self.llm_budget_path.exists():
            return defaults
        try:
            raw = json.loads(read_text(self.llm_budget_path))
            if not isinstance(raw, dict):
                return defaults
            budget_total = int(raw.get("limit", raw.get("budget_total", self.llm_budget)) or self.llm_budget)
            used = int(raw.get("used", 0) or 0)
            return {
                "limit": max(budget_total, self.llm_budget),
                "used": max(used, 0),
                "updated_at": str(raw.get("updated_at") or defaults["updated_at"]),
                "last_stop_reason": str(raw.get("last_stop_reason") or ""),
                "last_run_id": str(raw.get("last_run_id") or ""),
            }
        except Exception:
            return defaults

    def llm_budget_used(self) -> int:
        return int(self.llm_budget_state.get("used", 0) or 0)

    def llm_budget_remaining(self) -> int:
        return max(self.llm_budget - self.llm_budget_used(), 0)

    def save_llm_budget_state(self, stop_reason: str, increment_used: bool) -> None:
        used = self.llm_budget_used() + (1 if increment_used else 0)
        self.llm_budget_state = {
            "limit": self.llm_budget,
            "used": max(used, 0),
            "updated_at": now_iso(),
            "last_stop_reason": stop_reason,
            "last_run_id": self.run_id,
        }
        write_text(self.llm_budget_path, json.dumps(self.llm_budget_state, ensure_ascii=False, indent=2) + "\n")

    def write_llm_anchor_prompt(self, anchors: List[str], packs: List[List[str]], reason: str) -> bool:
        prompt_path = self.out_dir / "llm_prompt_B_anchors.txt"
        if self.llm_budget_remaining() <= 0:
            return False
        txt = f"""Этап B остановлен: {reason}.
Нужен ответ для OpenAlex в виде КОРОТКИХ английских токенов/фраз (латиница), а не предложений.
Примеры корректных токенов: Phoxinus, HydroRIVERS, genotype-environment association, BayPass.
Запрещено давать географию одним словом (Altai/Balkhash и т.п.) без пары с объектом, темой или методом. Убери слишком общие слова.
Верни строго JSON в формате ниже, без комментариев и текста вокруг:
{{
  "refined_primary_token": "Phoxinus",
  "refined_keywords_tokens": ["riverscape genomics", "HydroRIVERS", "BayPass"],
  "refined_must_have_tokens": ["Phoxinus", "riverscape"],
  "anchor_packs": [["Phoxinus","BayPass"],["Phoxinus","HydroRIVERS"]],
  "drift_blacklist": ["glaciology", "archaeology", "radiocarbon dating"],
  "support_tokens": ["riverscape genetics", "genotype-environment association"],
  "abbreviation_map": {{"SNP": "single nucleotide polymorphism"}}
}}
Важно: drift_blacklist только на английском и только тематические направления/фразы длиной >= 5 символов.
НЕ добавляй короткие куски вроде ob, ir, alt, gen: они ломают фильтр.
Если сомневаешься в blacklist — лучше не добавляй элемент.
Текущие anchors: {anchors}
Текущие packs: {packs}
        """
        write_text(prompt_path, txt)
        self.llm_prompt_created = True
        self.llm_prompts_created += 1
        return True

    def ensure_llm_response_template(self, force: bool = False) -> Path:
        p = self.in_dir / "llm_response_B_anchors.json"
        if force or not p.exists():
            template = {
                "refined_primary_token": "Phoxinus",
                "refined_keywords_tokens": ["riverscape genomics", "genotype-environment association", "HydroRIVERS", "HydroATLAS", "BayPass", "LFMM2", "ddRAD", "SNP"],
                "refined_must_have_tokens": ["Phoxinus", "riverscape"],
                "anchor_packs": [["Phoxinus", "BayPass"], ["Phoxinus", "HydroRIVERS"], ["Phoxinus", "genotype-environment association"], ["riverscape genomics", "gene flow"]],
                "drift_blacklist": ["glaciology", "archaeology", "radiocarbon dating", "wheat breeding", "water scarcity"],
                "support_tokens": ["riverscape genetics", "genotype-environment association", "gene flow", "landscape genomics"],
                "abbreviation_map": {"SNP": "single nucleotide polymorphism"},
            }
            write_text(p, json.dumps(template, ensure_ascii=False, indent=2) + "\n")
        return p

    def load_llm_anchor_response(self) -> Dict[str, Any]:
        p = self.in_dir / "llm_response_B_anchors.json"
        if not p.exists():
            return {}
        try:
            data = json.loads(read_text(p))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def parse_llm_anchor_response(self, llm: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "schema": "invalid",
            "reason": "invalid_missing_required_keys",
            "primary": "",
            "keywords": [],
            "must_have": [],
            "packs": [],
            "drift_blacklist": [],
            "support_tokens": [],
            "abbreviation_map": {},
        }
        if not isinstance(llm, dict) or not llm:
            return out

        schema = "invalid"
        if isinstance(llm.get("refined_primary_token"), str) or isinstance(llm.get("refined_keywords_tokens"), list):
            schema = "refined"
        elif isinstance(llm.get("cleaned_search_anchors"), list) or isinstance(llm.get("cleaned_anchors"), list):
            schema = "legacy"
        out["schema"] = schema

        if schema == "refined":
            primary = self.normalize_search_anchor(str(llm.get("refined_primary_token") or ""))
            keywords = self.normalize_token_list(llm.get("refined_keywords_tokens", []))
            must_have = self.normalize_token_list(llm.get("refined_must_have_tokens", []))
        else:
            primary = ""
            keywords = self.normalize_token_list(llm.get("cleaned_search_anchors") or llm.get("cleaned_anchors") or [])
            must_have = []

        raw_packs = llm.get("anchor_packs") if isinstance(llm.get("anchor_packs"), list) else []
        packs: List[List[str]] = []
        for pack in raw_packs:
            clean_pack = self.normalize_token_list(pack)
            if len(clean_pack) >= 2:
                packs.append(clean_pack[:3])

        drift_blacklist = self.normalize_token_list(llm.get("drift_blacklist", []))
        support_tokens = self.normalize_token_list(llm.get("support_tokens", []))
        ab_map = llm.get("abbreviation_map") if isinstance(llm.get("abbreviation_map"), dict) else {}
        ab_map_clean = {str(k): str(v).strip() for k, v in ab_map.items() if str(k).strip() and str(v).strip()}

        token_pool = self.normalize_token_list([primary] + keywords + must_have + [x for p in packs for x in p])
        latin_pool = [t for t in token_pool if self.has_latin_for_seed(t)]
        latin_pack_count = sum(1 for p in packs if any(self.has_latin_for_seed(t) for t in p))
        has_primary = bool(primary and self.has_latin_for_seed(primary))

        out.update({
            "primary": primary,
            "keywords": keywords,
            "must_have": must_have,
            "packs": packs,
            "drift_blacklist": [x.lower() for x in drift_blacklist],
            "support_tokens": [x.lower() for x in support_tokens],
            "abbreviation_map": ab_map_clean,
        })

        if len(latin_pool) < 8:
            out["reason"] = "invalid_non_latin_tokens"
            out["schema"] = "invalid"
            return out
        if not has_primary and latin_pack_count < 2:
            out["reason"] = "invalid_missing_primary_or_packs"
            out["schema"] = "invalid"
            return out
        out["reason"] = "validated"
        return out

    def write_summary(self, stats: Dict[str, Any], wait_llm: bool, stop_reason: str = "") -> None:
        sources = f"openalex={self.search_log['service_status']['openalex']}, semanticscholar={self.search_log['service_status']['semantic_scholar']}, crossref={self.search_log['service_status']['crossref']}"
        seed_queries = [q for q in self.search_log.get("queries", []) if q.get("source") == "openalex" and q.get("query_kind") == "seed"]
        unresolved = sorted([ab for ab in self.abbr_mentions if ab not in self.abbr_full_map])
        llm_path = self.llm_info.get("path", str((self.in_dir / "llm_response_B_anchors.json").resolve()))
        lines = [
            f"run_id: {self.run_id}",
            f"sources status: {sources}",
            f"primary_token = {stats.get('primary_token', '')}",
            f"primary_hit_count = {stats.get('primary_hit_count', 0)}",
            f"packs_count = {stats.get('packs_count', 0)}",
            f"packs_hit_count = {stats.get('packs_hit_count', 0)}",
            f"must_have_count = {stats.get('must_have_count', 0)}",
            f"support_tokens_count = {stats.get('support_tokens_count', 0)}",
            f"support_hit_count = {stats.get('support_hit_count', 0)}",
            f"planned_queries_count = {len(self.search_log.get('planned_queries', []))}",
            f"removed_duplicate_queries_count = {self.search_log.get('removed_duplicate_queries_count', 0)}",
            f"stopwords_removed = {self.search_log.get('token_sanitation', {}).get('stopwords_removed_count', 0)}",
            f"dataset_low_count = {stats.get('dataset_low_count', 0)}",
            f"drift_blacklist_raw_count = {stats.get('drift_blacklist_raw_count', 0)}",
            f"drift_blacklist_sanitized_count = {stats.get('drift_blacklist_sanitized_count', 0)}",
            f"drift_blacklist_dropped_count = {stats.get('drift_blacklist_dropped_count', 0)}",
            f"drift_blacklist_low_count = {stats.get('drift_blacklist_low_count', 0)}",
            f"main_count / support_count / low_count = {stats.get('main_count', 0)} / {stats.get('support_count', 0)} / {stats.get('low_count', 0)}",
            f"drift_score = {stats.get('drift_score', 0)}",
            f"drift_target = {self.drift_target}",
            f"drift_round0 = {stats.get('drift_round0', '')}",
            f"drift_round1 = {stats.get('drift_round1', '')}",
            f"drift_round2 = {stats.get('drift_round2', '')}",
            f"go_nogo = {stats.get('go_nogo', '')}",
            f"go_reasons = {', '.join(stats.get('go_reasons', []))}",
            f"pass_ratio = {stats.get('pass_ratio', 0)}",
            f"primary_signal = {stats.get('primary_signal', 0)}",
            f"alive_seed_queries = {stats.get('alive_seed_queries', 0)}",
            f"auto-fix rounds: {stats.get('auto_fix_rounds_used', 0)}; drift before/after = {stats.get('drift_round0', '')} -> {stats.get('drift_score', '')}",
            f"dedup_merged_count = {stats.get('dedup_merged_count', 0)}",
            f"elapsed: {stats.get('elapsed_ms', 0)} ms",
            f"llm_response_found = {'YES' if self.llm_info.get('found') else 'NO'}",
            f"llm_response_used = {'YES' if self.llm_info.get('used') else 'NO'}",
            f"llm_response_path = {llm_path}",
            f"llm_response_reason = {self.llm_info.get('reason', '')}",
            f"llm_response_schema = {self.llm_info.get('schema', 'invalid')}",
            f"llm_budget_total = {self.llm_budget}",
            f"llm_budget_used = {self.llm_budget_used()}",
            f"llm_budget_remaining = {self.llm_budget_remaining()}",
            f"llm_prompt_created = {'YES' if self.llm_prompt_created else 'NO'}",
            f"llm_prompts_created = {self.llm_prompts_created}",
            f"llm_used = {'YES' if self.llm_info.get('used') else 'NO'}",
            f"planned_queries_round0 = {stats.get('planned_queries_round0', 0)}",
            f"planned_queries_round1 = {stats.get('planned_queries_round1', 0)}",
            f"planned_queries_round2 = {stats.get('planned_queries_round2', 0)}",
            f"pass_main_count = {stats.get('main_count', 0)}",
            f"pass_support_count = {stats.get('support_count', 0)}",
            f"must_have_tokens_count = {stats.get('must_have_count', 0)}",
            f"must_have_selected_preview = {', '.join(stats.get('must_have_selected_preview', []))}",
            f"probe_tokens_tested = {len(self.search_log.get('token_probe', []))}",
            f"too_broad_count = {stats.get('probe_counts', {}).get('TOO_BROAD', 0)}",
            f"broad_count = {stats.get('probe_counts', {}).get('BROAD', 0)}",
            f"ok_count = {stats.get('probe_counts', {}).get('OK', 0)}",
            f"narrow_count = {stats.get('probe_counts', {}).get('NARROW', 0)}",
            f"zero_count = {stats.get('probe_counts', {}).get('ZERO', 0)}",
            "support корпус — методические/общие работы без объекта.",
            "Первые 3 OpenAlex seed-запроса:",
        ]
        for q in seed_queries[:3]:
            lines.append(f"- {q.get('query_text','')} → {q.get('result_total', 0)}")
        too_broad = [x.get('token','') for x in self.search_log.get('token_probe', []) if x.get('category') == 'TOO_BROAD'][:3]
        zero = [x.get('token','') for x in self.search_log.get('token_probe', []) if x.get('category') == 'ZERO'][:3]
        lines.append(f"top_too_broad_examples = {', '.join(too_broad)}")
        lines.append(f"top_zero_examples = {', '.join(zero)}")
        stop_examples = self.search_log.get('token_sanitation', {}).get('examples_removed', [])[:3]
        lines.append(f"stopwords_examples = {', '.join(stop_examples)}")
        for ab in unresolved:
            lines.append(f"аббревиатура не раскрыта: {ab}")
            self.log(f"аббревиатура не раскрыта: {ab}")
        if wait_llm:
            lines.append(f"STOP_REASON = {stop_reason}")
            lines.append(f"WAIT_FILE = {llm_path}")
            lines.append(f"LLM budget used: {self.llm_budget_used()} / {self.llm_budget}")
            if stop_reason == "llm_limit_reached_edit_json":
                lines.append("Лимит 3 обращения к ChatGPT исчерпан. Отредактируй in/llm_response_B_anchors.json. Новый prompt не создаётся.")
        write_text(self.out_dir / "stageB_summary.txt", "\n".join(lines) + "\n")

    def round_metrics(self, round_id: int, drift_score: float, planned_queries_count: int, passed: List[Dict[str, Any]], support: List[Dict[str, Any]], ranked: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "round": round_id,
            "drift_score": round(drift_score, 4),
            "planned_queries_count": planned_queries_count,
            "pass_count": len(passed) + len(support),
            "low_count": max(len(ranked) - len(passed) - len(support), 0),
        }

    def stop_for_llm(self, stop_reason: str, reason_text: str, search_anchors: List[str], packs: List[List[str]], stats: Dict[str, Any], t0: float,
                     passed_rows: Optional[List[Dict[str, Any]]] = None, support_rows: Optional[List[Dict[str, Any]]] = None,
                     ranked_all: Optional[List[Dict[str, Any]]] = None) -> int:
        self.write_corpus(passed_rows or [], support_rows or [], ranked_all or [], allow_replace=True)
        response_path = self.ensure_llm_response_template(force=False)
        prompt_path = self.out_dir / "llm_prompt_B_anchors.txt"
        if self.llm_budget_remaining() <= 0:
            stop_reason = "llm_limit_reached_edit_json"
            if prompt_path.exists():
                prompt_path.unlink()
            self.save_llm_budget_state(stop_reason, increment_used=False)
        else:
            created = self.write_llm_anchor_prompt(search_anchors[:20], packs[:6], reason_text)
            self.save_llm_budget_state(stop_reason, increment_used=created)
            if not created:
                stats["stop_reason"] = "internal_error_prompt_missing"
                stats["elapsed_ms"] = int((time.time() - t0) * 1000)
                self.search_log["errors"].append("internal_error_prompt_missing: rc2 requested without llm_prompt_B_anchors.txt")
                self.search_log["stats"] = {**self.search_log.get("stats", {}), **stats}
                self.search_log["finished_at"] = now_iso()
                log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
                write_text(self.search_log_path, log_text)
                write_text(self.search_log_legacy_path, log_text)
                self.write_prisma(stats)
                self.write_summary(stats, wait_llm=True, stop_reason="internal_error_prompt_missing")
                self.save_checkpoint(self.search_log.get("input_hash", ""), "DEGRADED", stats)
                print("Внутренняя ошибка: Stage B должна была создать out/llm_prompt_B_anchors.txt, но файл отсутствует.")
                return 1
        stats["stop_reason"] = stop_reason
        stats["elapsed_ms"] = int((time.time() - t0) * 1000)
        stop_files = [str(response_path)]
        if prompt_path.exists():
            stop_files.insert(0, str(prompt_path))
        self.search_log["stop_files"] = stop_files
        self.search_log["stats"] = {**self.search_log.get("stats", {}), **stats}
        self.search_log["finished_at"] = now_iso()
        log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
        write_text(self.search_log_path, log_text)
        write_text(self.search_log_legacy_path, log_text)
        self.write_prisma(stats)
        self.write_summary(stats, wait_llm=True, stop_reason=stop_reason)
        self.save_checkpoint(self.search_log.get("input_hash", ""), "DEGRADED", stats)
        if stop_reason == "llm_limit_reached_edit_json":
            print("Лимит 3 обращения к ChatGPT исчерпан. Новый prompt не создаётся.")
            print("Отредактируй in/llm_response_B_anchors.json вручную и запусти Stage B снова.")
        else:
            print(f"Этап B остановлен ({stop_reason}) и ждёт файл: {response_path}")
        return 2

    def load_fixture(self) -> List[Dict[str, Any]]:
        fp = self.offline_fixtures / "openalex_seed.json" if self.offline_fixtures else None
        if not fp or not fp.exists():
            return []
        obj = json.loads(read_text(fp))
        self.search_log["service_status"]["openalex"] = "offline"
        return [self.paper_openalex(w, "openalex_seed") for w in obj.get("results", [])]

    def run(self) -> int:
        t0 = time.time()
        self.archive_previous_outputs()
        self.log("Stage B start")
        self.log(f"Secrets: OPENALEX_MAILTO={'***' if self.mailto else '(missing)'}, OPENALEX_API_KEY={'***' if self.openalex_key else '(missing)'}, SEMANTIC_SCHOLAR_API_KEY={'***' if self.s2_key else '(missing)'}")

        ok, idea_text = self.ensure_idea_text()
        if not ok:
            self.search_log["errors"].append(idea_text)
            self.write_corpus([], [], [], allow_replace=False)
            self.write_prisma({})
            self.write_summary({}, wait_llm=False)
            log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
            write_text(self.search_log_path, log_text)
            write_text(self.search_log_legacy_path, log_text)
            self.save_checkpoint("", "DEGRADED", {})
            return 0

        structured = self.load_structured()
        input_hash = self.input_hash(idea_text, structured)
        if self.offline_fixtures:
            fixture_rows = self.load_fixture()
            ded = self.dedup(fixture_rows)
            self.write_corpus(ded, [], ded, allow_replace=True)
            stats = {"seed_queries": 1, "seed_count": len(fixture_rows), "total_candidates": len(ded), "main_count": len(ded), "support_count": 0, "low_count": 0}
            self.write_prisma(stats)
            self.write_summary(stats, wait_llm=False)
            self.search_log["stats"] = stats
            log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
            write_text(self.search_log_path, log_text)
            write_text(self.search_log_legacy_path, log_text)
            self.save_checkpoint(input_hash, "OK", stats)
            return 0
        self.search_log["input_hash"] = input_hash
        cp = self.load_checkpoint()
        if cp.get("input_hash") == input_hash and cp.get("mode") == self.mode:
            required = [self.out_dir / "corpus.csv", self.out_dir / "corpus_all.csv", self.out_dir / "search_log.json", self.out_dir / "prisma_lite.md", self.out_dir / "stageB_summary.txt"]
            if all(x.exists() for x in required):
                self.log("Checkpoint hit: network skipped")
                return 0
        if (not self.openalex_key) and (not self.offline_fixtures):
            self.search_log["service_status"]["openalex"] = "degraded"
            self.search_log["errors"].append("missing_openalex_api_key")
            self.write_corpus([], [], [], allow_replace=True)
            stats = {"main_count": 0, "support_count": 0, "low_count": 0, "seed_queries": 0, "seed_count": 0, "total_candidates": 0}
            self.write_prisma(stats)
            self.write_summary(stats, wait_llm=False)
            log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
            write_text(self.search_log_path, log_text)
            write_text(self.search_log_legacy_path, log_text)
            self.save_checkpoint(input_hash, "DEGRADED", stats)
            return 0
        self.geo_terms = self.detect_geo_terms(idea_text, structured)
        _, search_anchors, ab_map, all_abbr = self.build_anchors(idea_text, structured)
        keywords_for_search, keywords_used = self.get_keywords_for_search(idea_text, structured)
        adjacent_fields = structured.get("adjacent_fields_to_scan", []) if isinstance(structured, dict) else []
        if not isinstance(adjacent_fields, list):
            adjacent_fields = []
        if self.mode == "WIDE":
            extra = [str(x).strip() for x in adjacent_fields if isinstance(x, str) and str(x).strip()]
            keywords_for_search = list(dict.fromkeys(keywords_for_search + extra))[:30]
        elif self.mode == "FOCUSED":
            keywords_for_search = keywords_for_search[:10]
        self.search_log["keywords_for_search_used"] = keywords_used
        self.search_log["keywords_for_search_count"] = len(keywords_for_search)
        self.search_log["keywords_for_search_preview"] = keywords_for_search[:5]

        base_tokens = [x.lower() for x in self.sanitize_tokens(self.extract_keywords_tokens(keywords_for_search), context="keywords_tokens")]
        primary_token = self.detect_primary_token(keywords_for_search, search_anchors)
        must_have_tokens = [x.lower() for x in self.sanitize_tokens(self.build_must_have_tokens(keywords_for_search), context="must_have_tokens")]
        packs = self.build_anchor_packs(primary_token, base_tokens)
        keywords_tokens = base_tokens
        drift_blacklist: List[str] = []
        llm_support_raw: List[str] = []
        support_tokens: List[str] = self.build_support_tokens(base_tokens, primary_token)
        if self.mode == "FOCUSED":
            must_have_tokens = must_have_tokens[:8]
            support_tokens = support_tokens[:8]
            packs = packs[:4]
            self.drift_target = min(self.drift_target, 0.24)
        elif self.mode == "WIDE":
            support_tokens = support_tokens[:20]
            self.drift_target = max(self.drift_target, 0.35)
        self.search_log["stats"]["drift_target"] = self.drift_target
        source_mode = "keywords" if keywords_used else "anchors"
        source_terms = keywords_for_search if keywords_used else search_anchors
        llm_token_source: List[str] = []

        llm_path = self.in_dir / "llm_response_B_anchors.json"
        llm_raw = self.load_llm_anchor_response()
        self.llm_info = {
            "found": llm_path.exists(),
            "used": False,
            "schema": "invalid",
            "reason": "response_not_found" if not llm_path.exists() else "invalid_missing_required_keys",
            "path": str(llm_path.resolve()),
        }
        if llm_path.exists():
            parsed_llm = self.parse_llm_anchor_response(llm_raw)
            self.llm_info["schema"] = parsed_llm.get("schema", "invalid")
            self.llm_info["reason"] = parsed_llm.get("reason", "invalid_missing_required_keys")
            if parsed_llm.get("schema") == "invalid":
                self.llm_info["used"] = False
                self.llm_info["reason"] = parsed_llm.get("reason", "llm_invalid")
                self.search_log["llm"] = dict(self.llm_info)
                stats = {
                    "seed_queries": 0, "seed_count": 0, "total_candidates": 0, "main_count": 0,
                    "support_count": 0, "low_count": 0, "drift_score": 1.0,
                    "stop_reason": "llm_invalid", "primary_token": primary_token, "packs_count": len(packs),
                    "must_have_count": len(must_have_tokens),
                    "support_tokens_count": len(support_tokens),
                    "removed_duplicate_queries_count": self.search_log.get("removed_duplicate_queries_count", 0),
                    "probe_counts": {},
                    "planned_queries_round0": 0,
                    "planned_queries_round1": 0,
                    "planned_queries_round2": 0,
                    "drift_round0": "",
                    "drift_round1": "",
                    "drift_round2": "",
                    "auto_fix_rounds_used": 0,
                }
                return self.stop_for_llm("llm_already_used_need_edit", parsed_llm.get("reason", "llm_invalid"), search_anchors, packs, stats, t0)

            self.llm_info["used"] = True
            self.llm_info["reason"] = "validated_and_applied"
            self.llm_info["schema"] = parsed_llm.get("schema", "invalid")
            primary_token = (parsed_llm.get("primary") or primary_token).lower()
            keywords_tokens = [x.lower() for x in self.sanitize_tokens(parsed_llm.get("keywords", []), context="llm_keywords")] or base_tokens
            must_have_tokens = [x.lower() for x in self.sanitize_tokens(parsed_llm.get("must_have", []), context="llm_must_have")] or must_have_tokens
            packs = self.build_anchor_packs(primary_token, keywords_tokens, parsed_llm.get("packs", []))
            drift_blacklist = [x.lower() for x in self.sanitize_tokens(parsed_llm.get("drift_blacklist", []), context="drift_blacklist")]
            llm_support_raw = [x.lower() for x in self.sanitize_tokens(parsed_llm.get("support_tokens", []), context="llm_support_tokens")]
            support_tokens = self.build_support_tokens(keywords_tokens, primary_token, llm_support_raw)
            if parsed_llm.get("abbreviation_map"):
                self.abbr_full_map.update(parsed_llm.get("abbreviation_map", {}))
            source_mode = "llm"
            source_terms = keywords_tokens
            llm_token_source = [primary_token] + keywords_tokens + must_have_tokens + [x for p in packs for x in p] + support_tokens

        candidate_tokens = self.gather_candidate_tokens(primary_token, keywords_tokens, search_anchors, llm_token_source, bool(self.llm_info.get("used")))
        probe_map = self.run_token_probe(candidate_tokens, primary_token)
        if llm_support_raw:
            support_tokens = [
                t for t in llm_support_raw
                if t != primary_token and probe_map.get(t, {}).get("category") in {"OK", "NARROW", "BROAD"}
                and not (probe_map.get(t, {}).get("geo_like") or self.is_geography_token(t))
            ][:20]
        else:
            auto_support = self.build_support_tokens(keywords_tokens + must_have_tokens, primary_token)
            support_tokens = [
                t for t in auto_support
                if t != primary_token and probe_map.get(t, {}).get("category") in {"OK", "NARROW", "BROAD"}
                and not (probe_map.get(t, {}).get("geo_like") or self.is_geography_token(t))
                and (self.is_term_like(t) or probe_map.get(t, {}).get("category") in {"OK", "NARROW"})
            ][:20]
        probe_stats = Counter([x.get("category", "") for x in self.search_log.get("token_probe", [])])
        self.search_log["llm_effect"] = {
            "from_llm_tokens": len(self.normalize_token_list(llm_token_source)),
            "from_structured_tokens": len(self.normalize_token_list(base_tokens + search_anchors)),
            "filtered_too_broad_or_zero": sum(1 for x in self.search_log.get("token_probe", []) if x.get("category") in {"TOO_BROAD", "ZERO"}),
        }

        self.search_log["llm"] = dict(self.llm_info)
        self.search_log["primary_token"] = primary_token
        self.search_log["packs_count"] = len(packs)
        self.search_log["must_have_tokens"] = must_have_tokens
        self.search_log["support_tokens"] = support_tokens

        seed_rows: List[Dict[str, Any]] = []
        used_queries = 0
        seed_query_list = self.plan_seed_queries(keywords_for_search, search_anchors, packs, primary_token, source_terms, source_mode, probe_map)
        seed_query_list = self.validate_seed_queries(seed_query_list, probe_map)

        if not seed_query_list and not self.offline_fixtures:
            self.search_log["errors"].append("query_preflight: нет валидных seed-запросов")
            stats = {"seed_queries": 0, "seed_count": 0, "total_candidates": 0, "main_count": 0, "support_count": 0, "low_count": 0, "drift_score": 1.0, "primary_token": primary_token, "packs_count": len(packs), "must_have_count": len(must_have_tokens), "support_tokens_count": len(support_tokens), "removed_duplicate_queries_count": self.search_log.get("removed_duplicate_queries_count", 0), "probe_counts": {}, "planned_queries_round0": 0, "planned_queries_round1": 0, "planned_queries_round2": 0, "drift_round0": 1.0, "drift_round1": "", "drift_round2": "", "auto_fix_rounds_used": 0}
            return self.stop_for_llm("llm_invalid", "невалидные поисковые строки", search_anchors, packs, stats, t0)

        if self.offline_fixtures:
            seed_rows = self.load_fixture()
        else:
            for q in seed_query_list:
                used_queries += 1
                try:
                    rows, total = self.openalex_search_pack(q)
                    rows = rows[:MAX_PER_QUERY]
                    self.search_log["executed_queries"].append({"query_text": q, "result_total": total, "total": total, "cap_used": MAX_PER_QUERY, "items_added": len(rows), "round": 0})
                    seed_rows.extend([self.paper_openalex(r, "openalex_seed") for r in rows])
                    if len(seed_rows) >= MAX_SEED_ITEMS_TOTAL:
                        seed_rows = seed_rows[:MAX_SEED_ITEMS_TOTAL]
                        break
                except Exception as e:
                    self.search_log["service_status"]["openalex"] = "degraded"
                    self.search_log["errors"].append(f"openalex_seed_error: {e}")

        if len(seed_rows) >= 30:
            for ab in list(all_abbr):
                if ab in ab_map:
                    search_anchors.append(f"{ab_map[ab]} {ab}")

        need_llm = len(seed_rows) == 0 and not self.offline_fixtures
        semantic_rows: List[Dict[str, Any]] = []
        recommend_rows: List[Dict[str, Any]] = []
        expanded: List[Dict[str, Any]] = []

        if not need_llm and not self.offline_fixtures:
            for p in packs[:2]:
                semantic_rows.extend(self.semantic_scholar_search(p))
            expanded = self.expand_openalex(seed_rows, packs)
        if len(seed_rows) < 20:
            expanded = []

        merged = self.dedup(seed_rows + semantic_rows + recommend_rows + expanded)
        if not self.offline_fixtures:
            self.crossref_enrich(merged)

        passed_rows, support_rows, ranked_all, drift_score, metrics, blacklist_stats = self.apply_relevance_and_score(
            merged, primary_token, keywords_tokens, must_have_tokens, packs, drift_blacklist, support_tokens
        )
        round_history: List[Dict[str, Any]] = []
        round_history.append(self.round_metrics(0, drift_score, len(seed_query_list), passed_rows, support_rows, ranked_all))
        drift_round0 = drift_score

        auto_selected = self.select_auto_must_have_tokens(ranked_all, candidate_tokens, probe_map)
        if auto_selected:
            must_have_tokens = list(dict.fromkeys(([x["token"] for x in auto_selected] + must_have_tokens)))[:10]
        self.search_log["auto_must_have_tokens"] = auto_selected

        auto_fix_rounds_used = 0
        planned_round_counts = {0: len(seed_query_list), 1: 0, 2: 0}
        if (not need_llm) and drift_score > self.drift_target:
            for round_id in (1, 2):
                auto_fix_rounds_used = round_id
                top_queries = [q for q in seed_query_list if any(self.token_match(t, self.normalize_text_for_match(q)) for t in must_have_tokens[:6])]
                if not top_queries:
                    top_queries = seed_query_list[:]
                limit = 6 if round_id == 1 else 5
                seed_query_round = self.dedup_planned_queries(top_queries)[:limit]
                planned_round_counts[round_id] = len(seed_query_round)
                if not seed_query_round:
                    break
                seed_rows_round: List[Dict[str, Any]] = []
                if self.offline_fixtures:
                    seed_rows_round = self.load_fixture()
                else:
                    for q in seed_query_round:
                        try:
                            rows, total = self.openalex_search_pack(q)
                            rows = rows[:MAX_PER_QUERY]
                            self.search_log["executed_queries"].append({"query_text": q, "result_total": total, "total": total, "cap_used": MAX_PER_QUERY, "items_added": len(rows), "round": round_id})
                            seed_rows_round.extend([self.paper_openalex(r, "openalex_seed") for r in rows])
                            if len(seed_rows_round) >= MAX_SEED_ITEMS_TOTAL:
                                seed_rows_round = seed_rows_round[:MAX_SEED_ITEMS_TOTAL]
                                break
                        except Exception as e:
                            self.search_log["errors"].append(f"openalex_seed_error_round{round_id}: {e}")
                merged_round = self.dedup(seed_rows_round)
                passed_round, support_round, ranked_round, drift_round, _, _ = self.apply_relevance_and_score(
                    merged_round, primary_token, keywords_tokens, must_have_tokens, packs, drift_blacklist, support_tokens
                )
                # stronger must-have gating in auto-fix rounds
                filtered_main = [r for r in passed_round if int(r.get("hits_must", 0)) >= 1 or bool(r.get("hit_primary", False))]
                if round_id == 2:
                    filtered_support = [r for r in support_round if int(r.get("hits_must", 0)) >= 2]
                else:
                    filtered_support = [r for r in support_round if int(r.get("hits_must", 0)) >= 1]
                passed_round, support_round = filtered_main, filtered_support
                drift_score = drift_round
                passed_rows, support_rows, ranked_all = passed_round, support_round, ranked_round
                round_history.append(self.round_metrics(round_id, drift_score, len(seed_query_round), passed_rows, support_rows, ranked_all))
                if drift_score <= self.drift_target:
                    break

        self.search_log["rounds"] = round_history
        self.search_log["blacklist"] = blacklist_stats
        self.search_log["primary_hit_count"] = metrics["primary_hit_count"]
        self.search_log["packs_hit_count"] = metrics["packs_hit_count"]
        self.search_log["stats"]["dedup_before"] = self.dedup_before
        self.search_log["stats"]["dedup_after"] = self.dedup_after

        alive_seed_queries = sum(1 for q in self.search_log.get("executed_queries", []) if int(q.get("result_total", 0)) > 0 and int(q.get("round", 0)) == 0)
        if self.offline_fixtures:
            alive_seed_queries = len(seed_query_list)
        go_eval = self.compute_go_metrics(ranked_all, alive_seed_queries)
        go_nogo = go_eval.get("go_nogo", "NO-GO")

        if need_llm:
            stats = {"seed_queries": used_queries, "seed_count": len(seed_rows), "total_candidates": len(ranked_all), "main_count": len(passed_rows), "support_count": len(support_rows), "low_count": max(len(ranked_all) - len(passed_rows) - len(support_rows), 0), "drift_score": round(drift_score, 4), "primary_token": primary_token, "primary_hit_count": metrics["primary_hit_count"], "packs_count": len(packs), "packs_hit_count": metrics["packs_hit_count"], "must_have_count": len(must_have_tokens), "support_tokens_count": len(support_tokens), "support_hit_count": metrics["support_hit_count"], "removed_duplicate_queries_count": self.search_log.get("removed_duplicate_queries_count", 0), "probe_counts": dict(probe_stats), "dataset_low_count": metrics["dataset_low_count"], "drift_blacklist_raw_count": blacklist_stats.get("raw_count", 0), "drift_blacklist_sanitized_count": blacklist_stats.get("sanitized_count", 0), "drift_blacklist_dropped_count": len(blacklist_stats.get("dropped", [])), "drift_blacklist_low_count": metrics["blacklist_low_count"], "dedup_merged_count": self.dedup_merged_count, "planned_queries_round0": planned_round_counts[0], "planned_queries_round1": planned_round_counts[1], "planned_queries_round2": planned_round_counts[2], "drift_round0": round_history[0]["drift_score"] if round_history else drift_round0, "drift_round1": round_history[1]["drift_score"] if len(round_history) > 1 else "", "drift_round2": round_history[2]["drift_score"] if len(round_history) > 2 else "", "auto_fix_rounds_used": auto_fix_rounds_used, "must_have_selected_preview": [x.get("token", "") for x in auto_selected[:3]]}
            return self.stop_for_llm("seed_zero", "seed=0", search_anchors, packs, stats, t0, passed_rows, support_rows, ranked_all)

        if go_eval.get("go"):
            passed_rows, support_rows, ranked_all, drift_score, _ = self.maybe_add_citation_chasing(
                passed_rows, ranked_all, primary_token, keywords_tokens, must_have_tokens, packs, drift_blacklist, support_tokens, drift_score
            )
            go_eval = self.compute_go_metrics(ranked_all, alive_seed_queries)
            go_nogo = go_eval.get("go_nogo", "NO-GO")

        if (not go_eval.get("go")):
            stats = {"seed_queries": used_queries, "seed_count": len(seed_rows), "total_candidates": len(ranked_all), "main_count": len(passed_rows), "support_count": len(support_rows), "low_count": max(len(ranked_all) - len(passed_rows) - len(support_rows), 0), "drift_score": round(drift_score, 4), "primary_token": primary_token, "primary_hit_count": metrics["primary_hit_count"], "packs_count": len(packs), "packs_hit_count": metrics["packs_hit_count"], "must_have_count": len(must_have_tokens), "support_tokens_count": len(support_tokens), "support_hit_count": metrics["support_hit_count"], "removed_duplicate_queries_count": self.search_log.get("removed_duplicate_queries_count", 0), "probe_counts": dict(probe_stats), "dataset_low_count": metrics["dataset_low_count"], "drift_blacklist_raw_count": blacklist_stats.get("raw_count", 0), "drift_blacklist_sanitized_count": blacklist_stats.get("sanitized_count", 0), "drift_blacklist_dropped_count": len(blacklist_stats.get("dropped", [])), "drift_blacklist_low_count": metrics["blacklist_low_count"], "dedup_merged_count": self.dedup_merged_count, "planned_queries_round0": planned_round_counts[0], "planned_queries_round1": planned_round_counts[1], "planned_queries_round2": planned_round_counts[2], "drift_round0": round_history[0]["drift_score"] if round_history else drift_round0, "drift_round1": round_history[1]["drift_score"] if len(round_history) > 1 else "", "drift_round2": round_history[2]["drift_score"] if len(round_history) > 2 else "", "auto_fix_rounds_used": auto_fix_rounds_used, "must_have_selected_preview": [x.get("token", "") for x in auto_selected[:3]], "go_nogo": go_nogo, "go_reasons": go_eval.get("reasons", [])}
            return self.stop_for_llm("no_go", f"no-go: {', '.join(go_eval.get('reasons', []))}", search_anchors, packs, stats, t0, passed_rows, support_rows, ranked_all)

        self.write_corpus(passed_rows, support_rows, ranked_all, allow_replace=True)
        stats = {
            "seed_queries": used_queries,
            "seed_count": len(seed_rows),
            "total_candidates": len(ranked_all),
            "main_count": len(passed_rows),
            "support_count": len(support_rows),
            "low_count": max(len(ranked_all) - len(passed_rows) - len(support_rows), 0),
            "drift_score": round(drift_score, 4),
            "elapsed_ms": int((time.time() - t0) * 1000),
            "primary_token": primary_token,
            "primary_hit_count": metrics["primary_hit_count"],
            "packs_count": len(packs),
            "packs_hit_count": metrics["packs_hit_count"],
            "must_have_count": len(must_have_tokens),
            "support_tokens_count": len(support_tokens),
            "support_hit_count": metrics["support_hit_count"],
            "removed_duplicate_queries_count": self.search_log.get("removed_duplicate_queries_count", 0),
            "probe_counts": dict(probe_stats),
            "dataset_low_count": metrics["dataset_low_count"],
            "drift_blacklist_raw_count": blacklist_stats.get("raw_count", 0),
            "drift_blacklist_sanitized_count": blacklist_stats.get("sanitized_count", 0),
            "drift_blacklist_dropped_count": len(blacklist_stats.get("dropped", [])),
            "drift_blacklist_low_count": metrics["blacklist_low_count"],
            "dedup_merged_count": self.dedup_merged_count,
            "planned_queries_round0": planned_round_counts[0],
            "planned_queries_round1": planned_round_counts[1],
            "planned_queries_round2": planned_round_counts[2],
            "drift_round0": round_history[0]["drift_score"] if round_history else drift_round0,
            "drift_round1": round_history[1]["drift_score"] if len(round_history) > 1 else "",
            "drift_round2": round_history[2]["drift_score"] if len(round_history) > 2 else "",
            "auto_fix_rounds_used": auto_fix_rounds_used,
            "must_have_selected_preview": [x.get("token", "") for x in auto_selected[:3]],
            "llm_budget_total": self.llm_budget,
            "llm_budget_used": self.llm_budget_used(),
            "llm_budget_remaining": self.llm_budget_remaining(),
            "llm_prompts_created": self.llm_prompts_created,
            "llm_used": bool(self.llm_info.get("used")),
            "go_nogo": go_nogo,
            "go_reasons": go_eval.get("reasons", []),
            "pass_ratio": go_eval.get("pass_ratio", 0),
            "primary_signal": go_eval.get("primary_signal", 0),
            "alive_seed_queries": alive_seed_queries,
        }
        self.search_log["stats"] = {**stats, "dedup_before": self.dedup_before, "dedup_after": self.dedup_after, "llm_budget_total": self.llm_budget, "llm_budget_used": self.llm_budget_used(), "llm_budget_remaining": self.llm_budget_remaining(), "llm_prompts_created": self.llm_prompts_created, "llm_used": bool(self.llm_info.get("used"))}
        self.search_log["finished_at"] = now_iso()
        self.write_prisma(stats)
        self.write_summary(stats, wait_llm=False)
        log_text = json.dumps(self.search_log, ensure_ascii=False, indent=2)
        write_text(self.search_log_path, log_text)
        write_text(self.search_log_legacy_path, log_text)
        status = "OK"
        if (not self.openalex_key) or self.degraded_reasons or len(passed_rows) < 10:
            status = "DEGRADED"
        self.save_checkpoint(input_hash, status, stats)
        self.log("Stage B done")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idea", default="")
    ap.add_argument("--idea-dir", default="")
    ap.add_argument("--mode", default="", choices=["", "BALANCED", "FOCUSED", "WIDE"])
    ap.add_argument("--scope", default="", choices=["", "balanced", "focused", "wide"])
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--offline-fixtures", default="")
    args = ap.parse_args()
    idea = args.idea_dir or args.idea
    if not idea:
        raise SystemExit("--idea-dir (или --idea) обязателен")
    mode = args.mode or (args.scope.upper() if args.scope else "BALANCED")
    offline = Path(args.offline_fixtures) if args.offline_fixtures else None
    return StageB(Path(idea), mode, offline).run()


if __name__ == "__main__":
    raise SystemExit(main())
