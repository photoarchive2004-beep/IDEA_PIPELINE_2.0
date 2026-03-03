# -*- coding: utf-8 -*-
import argparse
import csv
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
        self.search_log_path = self.out_dir / "search_log_B.json"
        self.module_log = self.logs_dir / f"moduleB_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.run_id = f"B_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        repo = self.idea_dir.parents[1]
        self.secrets = parse_env(repo / "config" / "secrets.env")
        self.mailto = self.secrets.get("OPENALEX_MAILTO", "")
        self.s2_key = self.secrets.get("SEMANTIC_SCHOLAR_API_KEY", "")

        self.session = requests.Session() if requests else None
        self.search_log: Dict[str, Any] = {
            "run_id": self.run_id,
            "started_at": now_iso(),
            "mode": self.mode,
            "anchor_candidates": [],
            "anchor_top_display": [],
            "anchor_top_search": [],
            "anchor_packs": [],
            "anchor_packs_search": [],
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

    def archive_previous_outputs(self) -> None:
        run_dir = self.out_dir / "_runs" / self.run_id
        prev_dir = run_dir / "_prev"
        run_dir.mkdir(parents=True, exist_ok=True)
        move_targets = {
            "corpus.csv", "corpus_all.csv", "search_log_B.json", "stageB_summary.txt", "prisma_lite_B.md", "runB.log", "llm_prompt_B_anchors.txt",
        }
        for item in self.out_dir.iterdir():
            if item.name == "_runs":
                continue
            is_strategy = item.is_file() and re.fullmatch(r"search_strategy.*\.md", item.name)
            is_cache = item.name in {"cache_openalex", "cache_semanticscholar"}
            if item.name in move_targets or is_strategy or is_cache:
                prev_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(item), str(prev_dir / item.name))

    def log(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        for p in (self.run_log, self.module_log):
            with p.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    def request_json(self, method: str, url: str, source: str, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None, query_kind: str = "seed") -> Tuple[Dict[str, Any], int, int]:
        if self.offline_fixtures:
            raise RuntimeError("offline")
        t0 = time.time()
        headers = {"User-Agent": "IDEA_PIPELINE_2.0-StageB/3.0", "Content-Type": "application/json"}
        if source == "semantic_scholar" and self.s2_key:
            headers["x-api-key"] = self.s2_key

        qtxt = ""
        if isinstance(params, dict):
            qtxt = str(params.get("search") or params.get("query") or params.get("query.title") or "")

        try:
            if self.session:
                resp = self.session.request(method, url, params=params, json=payload, timeout=25, headers=headers)
                ms = int((time.time() - t0) * 1000)
                status_code = resp.status_code
                body_text = resp.text
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
        except Exception as e:
            ms = int((time.time() - t0) * 1000)
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
        if status_code >= 400:
            raise RuntimeError(f"HTTP {status_code}")
        return json.loads(body_text or "{}"), status_code, ms

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
        return token.lower() in GEO_HINTS

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
            for tok in re.findall(r"[A-Za-z0-9\-]{4,}", self.normalize_search_anchor(line)):
                tl = tok.lower()
                if tl in EN_STOP or tl in RU_STOP:
                    continue
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
                terms = [self.normalize_search_anchor(t).lower() for t in pack if isinstance(t, str)]
                terms = [t for t in terms if t and t.lower() not in GENERIC_WORDS]
                if len(terms) >= 2 and not all(self.is_geography_token(t) for t in terms):
                    cleaned.append(terms[:3])
            packs = cleaned[:6]
            self.search_log["anchor_packs"] = packs
            self.search_log["anchor_packs_search"] = packs
            return packs

        candidates = [t for t in keywords_tokens if t and t.lower() not in GENERIC_WORDS]
        method_vocab = {"lfmm2", "baypass", "ddrad", "snp", "hydrorivers", "hydroatlas", "abba-baba", "genotype", "environment"}
        phenomenon_vocab = {"adaptation", "gene", "flow", "geneflow", "riverscape", "association", "introgression", "selection"}
        method_token = next((t for t in candidates if t in method_vocab or re.search(r"\d", t) or "-" in t), "")
        context_token = next((t for t in candidates if t not in {primary_token, method_token} and not self.is_geography_token(t)), "")
        phenomenon_token = next((t for t in candidates if t in phenomenon_vocab and t not in {primary_token, method_token}), "")
        if not phenomenon_token:
            phenomenon_token = next((t for t in candidates if t not in {primary_token, method_token, context_token}), "")

        packs: List[List[str]] = []
        for proto in [
            [primary_token, method_token],
            [primary_token, context_token],
            [primary_token, phenomenon_token],
            [method_token, phenomenon_token],
            [primary_token, method_token, phenomenon_token],
            [primary_token, context_token, phenomenon_token],
        ]:
            terms = [t for t in proto if t]
            terms = [t for t in dict.fromkeys(terms) if t.lower() not in GENERIC_WORDS]
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
        uniq = [self.normalize_search_anchor(x) for x in terms if self.normalize_search_anchor(x)]
        uniq = list(dict.fromkeys(uniq))[:3]
        if len(uniq) <= 1:
            return uniq[0] if uniq else ""
        if len(uniq) == 2:
            return f"({uniq[0]} AND {uniq[1]})"
        return f"({uniq[0]} AND ({uniq[1]} OR {uniq[2]}))"

    def preflight_queries(self, queries: List[str]) -> Tuple[List[str], List[str]]:
        ok: List[str] = []
        bad: List[str] = []
        for q in queries:
            query = re.sub(r"\s+", " ", q.strip())
            terms = [t for t in re.findall(r"[A-Za-z0-9\-]+", query) if len(t) >= 3]
            if not query:
                bad.append("Пустая поисковая строка")
                continue
            if len(query) > 120 or len(terms) > 8:
                bad.append(f"Слишком длинная строка: {query}")
                continue
            qtokens = [x.lower() for x in re.findall(r"[A-Za-z0-9\-]+", query)]
            if len(qtokens) == 1 and qtokens[0] in GEO_HINTS:
                bad.append(f"Geo-only запрос запрещён: {query}")
                continue
            if re.fullmatch(r"[A-Z][a-z]{3,}", query) and not re.search(r"\d", query) and qtokens and qtokens[0] in GEO_HINTS:
                bad.append(f"Похоже на geo-only или одиночный термин: {query}")
                continue
            if " AND " in query and " OR " not in query and query.count("AND") >= 2:
                bad.append(f"Риск нулевой выдачи (много AND без OR): {query}")
                continue
            if any(b in query.lower() for b in GENERIC_BLACKLIST):
                bad.append(f"Мусорная/служебная фраза: {query}")
                continue
            ok.append(query)
        return ok[:8], bad

    def build_seed_queries(self, keywords_for_search: List[str], search_anchors: List[str], packs: List[List[str]]) -> List[str]:
        queries: List[str] = []
        seen: Set[str] = set()

        tokens: List[str] = []
        for kw in keywords_for_search:
            short = re.sub(r"[\"']", "", kw)
            if len(short) > 160:
                short = " ".join(re.findall(r"[A-Za-z0-9\-]{4,}", short)[:3])
            tokens.extend(re.findall(r"[A-Za-z0-9\-]{3,}", short))
        unique_tokens = [t for t in dict.fromkeys(tokens) if len(t) >= 3][:24]
        object_term = unique_tokens[0] if unique_tokens else (search_anchors[0] if search_anchors else "")
        method_terms = [t for t in unique_tokens if re.search(r"\d", t) or "-" in t or t.lower() in {"baypass", "lfmm2", "ddrad", "snp", "abba-baba", "hydrorivers", "hydroatlas"}][:4]
        phenomenon_terms = [t for t in unique_tokens if t.lower() in {"adaptation", "gene", "flow", "genomics", "environment", "association", "riverscape"}][:4]

        def add_query(q: str) -> None:
            query = re.sub(r"\s+", " ", q.strip())
            key = query.lower()
            if not query or key in seen:
                return
            seen.add(key)
            queries.append(query)

        if object_term:
            add_query(self.normalize_search_anchor(object_term))
        for m in method_terms[:3]:
            add_query(self.build_boolean_query([object_term, m]))
        for p in phenomenon_terms[:2]:
            add_query(self.build_boolean_query([object_term, p]))
        for m in method_terms[:2]:
            for p in phenomenon_terms[:2]:
                add_query(self.build_boolean_query([m, p]))

        if not queries:
            for pack in packs:
                add_query(self.build_boolean_query(pack[:3]))
            for a in search_anchors[:6]:
                add_query(self.normalize_search_anchor(a))

        good, bad = self.preflight_queries(queries[:8])
        for item in bad:
            self.search_log["errors"].append(f"query_preflight: {item}")
        return good[:8]

    def openalex_search_pack(self, query: str) -> Tuple[List[Dict[str, Any]], int]:
        params = {"search": query, "per-page": 50}
        if self.mailto:
            params["mailto"] = self.mailto
        endpoint = "https://api.openalex.org/works"
        try:
            obj, status, elapsed = self.request_json("GET", endpoint, "openalex", params=params, query_kind="seed")
            total = int((obj.get("meta") or {}).get("count") or 0)
            items = obj.get("results", [])
            if self.search_log["queries"]:
                self.search_log["queries"][-1].update({
                    "query_text": query,
                    "anchor_pack_used": [query],
                    "result_total": total,
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

    def apply_relevance_and_score(self, papers: List[Dict[str, Any]], primary_token: str, keywords_tokens: List[str], must_have_tokens: List[str], anchor_packs: List[List[str]], drift_blacklist: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], float, Dict[str, int]]:
        rows: List[Dict[str, Any]] = []
        eval_flags: List[bool] = []
        metrics = {"primary_hit_count": 0, "packs_hit_count": 0, "dataset_low_count": 0}

        for p in papers:
            title = str(p.get("title", ""))
            abstract = str(p.get("abstract", ""))
            text = f"{title} {abstract}".lower().strip()
            title_only = title.lower()
            hit_primary = bool(primary_token) and (primary_token in text)
            hits_kw = sum(1 for t in keywords_tokens if t and t in text)
            hits_must = sum(1 for t in must_have_tokens if t and t in text)
            hits_packs = 0
            for pack in anchor_packs:
                terms = [t.lower() for t in pack if t]
                if len(terms) < 2:
                    continue
                in_text = all(t in text for t in terms)
                in_title = (not abstract.strip()) and all(t in title_only for t in terms)
                if in_text or in_title:
                    hits_packs += 1

            dataset_flag = self.is_dataset_candidate(p, text)
            black_hit = any(b.lower() in text for b in drift_blacklist if b)
            has_abstract = bool(abstract.strip())

            if dataset_flag:
                relevance_flag = "LOW"
                reason = "low_dataset"
                metrics["dataset_low_count"] += 1
            elif black_hit and not hit_primary:
                relevance_flag = "LOW"
                reason = "low_drift_blacklist"
            elif (not has_abstract) and (not hit_primary) and hits_packs < 1:
                relevance_flag = "LOW"
                reason = "low_missing_text"
            elif hit_primary:
                relevance_flag = "PASS"
                reason = "pass_primary"
            elif hits_packs >= 1:
                relevance_flag = "PASS"
                reason = "pass_pack"
            elif hits_kw >= 3 and hits_must >= 1:
                relevance_flag = "PASS"
                reason = "pass_keywords"
            else:
                relevance_flag = "LOW"
                reason = "low_no_primary_no_pack_low_kw"

            if hit_primary:
                metrics["primary_hit_count"] += 1
            if hits_packs >= 1:
                metrics["packs_hit_count"] += 1

            relevance_score = (6 if hit_primary else 0) + (4 * hits_packs) + hits_kw + (2 * hits_must)
            citation_score = math.log1p(max(int(p.get("cited_by_count") or 0), 0)) * 0.25
            now_year = datetime.now().year
            year = int(p.get("year") or 0) if str(p.get("year") or "").isdigit() else 0
            freshness_score = 0.5 if (year and year >= (now_year - 5)) else 0.0
            score = relevance_score + citation_score + freshness_score

            row = dict(p)
            row.update({
                "score": round(score, 4),
                "relevance_flag": relevance_flag,
                "reason": reason,
                "score_components": f"rel={round(relevance_score,3)}; primary={int(hit_primary)}; packs={hits_packs}; kw={hits_kw}; must={hits_must}; cit={round(citation_score,3)}; fresh={round(freshness_score,3)}",
            })
            rows.append(row)
            eval_flags.append(relevance_flag == "PASS")

        n = min(50, len(eval_flags))
        pass_top_n = sum(1 for flag in eval_flags[:n] if flag)
        drift_score = 1.0 - (pass_top_n / n) if n else 1.0

        rows.sort(key=lambda x: (-x["score"], -(x.get("cited_by_count", 0) or 0), x.get("title", "")))
        passed = [r for r in rows if r.get("relevance_flag") == "PASS"]
        return passed, rows, round(drift_score, 4), metrics

    def write_corpus(self, passed_papers: List[Dict[str, Any]], all_papers: List[Dict[str, Any]], allow_replace: bool) -> None:
        common_fields = ["rank", "score", "title", "year", "doi", "openalex_id", "venue", "authors_short", "cited_by_count", "source_tags", "url"]
        targets = [
            ("corpus.csv", passed_papers[:300], common_fields),
            ("corpus_all.csv", all_papers, common_fields + ["relevance_flag", "reason", "score_components"]),
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
                        "score_components": p.get("score_components", ""),
                    })
            if allow_replace and len(rows) > 0:
                shutil.move(str(tmp), str(target))
            elif not target.exists():
                shutil.move(str(tmp), str(target))
            elif tmp.exists():
                tmp.unlink()

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
            f"- pass_count: {stats.get('pass_count', 0)}",
            f"- low_count: {stats.get('low_count', 0)}",
            f"- drift_score: {stats.get('drift_score', 0)}",
            "",
            "## OpenAlex seed queries",
            *[f"- {q.get('query_text','')} → {q.get('result_total', 0)}" for q in openalex_q if q.get("query_kind") == "seed"],
            "",
            "## Semantic Scholar queries",
            *[f"- {q.get('query_text','')} → {q.get('result_total', 0)}" for q in s2_q],
        ]
        if unresolved:
            lines += ["", "## Аббревиатуры без расшифровки", *[f"- {ab}" for ab in unresolved]]
        write_text(self.out_dir / "prisma_lite_B.md", "\n".join(lines) + "\n")
        write_text(self.out_dir / "search_strategy.md", "\n".join(lines) + "\n")

    def write_llm_anchor_prompt(self, anchors: List[str], packs: List[List[str]], reason: str) -> None:
        txt = (
            f"Этап B остановлен: {reason}. Уточни фильтр релевантности и ограничения дрейфа. Верни только JSON:\n"
            "{\n"
            "  \"refined_primary_token\": \"...\",\n"
            "  \"refined_must_have_tokens\": [\"...\"],\n"
            "  \"refined_keywords_tokens\": [\"...\"],\n"
            "  \"anchor_packs\": [[\"...\",\"...\"],[\"...\",\"...\",\"...\"]],\n"
            "  \"drift_blacklist\": [\"...\"]\n"
            "}\n"
            "Ограничения: refined_must_have_tokens 5-15, refined_keywords_tokens 10-25 токенов, anchor_packs 4-6 по 2-3 термина, drift_blacklist 10-30.\n"
            f"Текущие anchors: {anchors}\n"
            f"Текущие packs: {packs}\n"
        )
        write_text(self.out_dir / "llm_prompt_B_anchors.txt", txt)

    def ensure_llm_response_template(self) -> Path:
        p = self.in_dir / "llm_response_B_anchors.json"
        if not p.exists():
            template = {
                "refined_primary_token": "Phoxinus",
                "refined_must_have_tokens": ["phoxinus", "cytochrome-b", "riverscape"],
                "refined_keywords_tokens": ["Phoxinus", "Balkhash", "cytochrome-b", "riverscape", "lfmm2"],
                "anchor_packs": [["Phoxinus", "cytochrome-b"], ["Phoxinus", "riverscape"], ["lfmm2", "adaptation"], ["Phoxinus", "lfmm2", "adaptation"]],
                "drift_blacklist": ["water scarcity", "urban water supply"],
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

    def write_summary(self, stats: Dict[str, Any], used_europe: bool, wait_llm: bool, used_user_response: bool, corpus_updated: bool, stop_reason: str = "") -> None:
        sources = f"openalex={self.search_log['service_status']['openalex']}, semanticscholar={self.search_log['service_status']['semantic_scholar']}, crossref={self.search_log['service_status']['crossref']}"
        seed_queries = [q for q in self.search_log.get("queries", []) if q.get("source") == "openalex" and q.get("query_kind") == "seed"]
        unresolved = sorted([ab for ab in self.abbr_mentions if ab not in self.abbr_full_map])
        lines = [
            f"run_id: {self.run_id}",
            f"sources status: {sources}",
            f"primary_token = {stats.get('primary_token', '')}",
            f"primary_hit_count = {stats.get('primary_hit_count', 0)}",
            f"packs_count = {stats.get('packs_count', 0)}",
            f"packs_hit_count = {stats.get('packs_hit_count', 0)}",
            f"must_have_count = {stats.get('must_have_count', 0)}",
            f"dataset_low_count = {stats.get('dataset_low_count', 0)}",
            f"pass_count / low_count = {stats.get('pass_count', 0)} / {stats.get('low_count', 0)}",
            f"drift_score = {stats.get('drift_score', 0)}",
            f"dedup_merged_count = {stats.get('dedup_merged_count', 0)}",
            f"elapsed: {stats.get('elapsed_ms', 0)} ms",
            "Первые 3 OpenAlex seed-запроса:",
        ]
        for q in seed_queries[:3]:
            lines.append(f"- {q.get('query_text','')} → {q.get('result_total', 0)}")
        if unresolved:
            lines.append(f"unresolved_abbreviations_count = {len(unresolved)}")
        if wait_llm:
            lines.append(f"STOP_REASON={stop_reason}; ждёт {self.in_dir / 'llm_response_B_anchors.json'}")
        write_text(self.out_dir / "stageB_summary.txt", "\n".join(lines[:25]) + "\n")

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
        self.log(f"Secrets: OPENALEX_MAILTO={'***' if self.mailto else '(missing)'}, SEMANTIC_SCHOLAR_API_KEY={'***' if self.s2_key else '(missing)'}")

        ok, idea_text = self.ensure_idea_text()
        if not ok:
            self.search_log["errors"].append(idea_text)
            self.write_corpus([], [], allow_replace=False)
            self.write_prisma({})
            self.write_summary({}, used_europe=False, wait_llm=False, used_user_response=False, corpus_updated=False)
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            return 0

        structured = self.load_structured()
        _, search_anchors, ab_map, all_abbr = self.build_anchors(idea_text, structured)
        keywords_for_search, keywords_used = self.get_keywords_for_search(idea_text, structured)
        self.search_log["keywords_for_search_used"] = keywords_used
        self.search_log["keywords_for_search_count"] = len(keywords_for_search)
        self.search_log["keywords_for_search_preview"] = keywords_for_search[:5]

        base_tokens = [x.lower() for x in self.extract_keywords_tokens(keywords_for_search)]
        primary_token = self.detect_primary_token(keywords_for_search, search_anchors)
        must_have_tokens = self.build_must_have_tokens(keywords_for_search)
        packs = self.build_anchor_packs(primary_token, base_tokens)

        llm = self.load_llm_anchor_response()
        used_user_response = False
        refined_primary = ""
        refined_tokens: List[str] = []
        refined_must: List[str] = []
        drift_blacklist: List[str] = []
        if isinstance(llm, dict) and llm:
            if isinstance(llm.get("refined_primary_token"), str):
                refined_primary = self.normalize_search_anchor(llm.get("refined_primary_token", "")).lower()
            if isinstance(llm.get("refined_keywords_tokens"), list):
                refined_tokens = [self.normalize_search_anchor(str(x)).lower() for x in llm.get("refined_keywords_tokens", []) if isinstance(x, str)]
                refined_tokens = [x for x in refined_tokens if x]
            if isinstance(llm.get("refined_must_have_tokens"), list):
                refined_must = [self.normalize_search_anchor(str(x)).lower() for x in llm.get("refined_must_have_tokens", []) if isinstance(x, str)]
                refined_must = [x for x in refined_must if x]
            if isinstance(llm.get("anchor_packs"), list):
                packs = self.build_anchor_packs(refined_primary or primary_token, refined_tokens or base_tokens, llm.get("anchor_packs", []))
            if isinstance(llm.get("drift_blacklist"), list):
                drift_blacklist = [str(x).strip().lower() for x in llm.get("drift_blacklist", []) if isinstance(x, str) and str(x).strip()]
            used_user_response = bool(refined_primary or refined_tokens or refined_must or drift_blacklist)

        primary_token = refined_primary or primary_token
        keywords_tokens = refined_tokens or base_tokens
        must_have_tokens = refined_must or must_have_tokens

        self.search_log["primary_token"] = primary_token
        self.search_log["packs_count"] = len(packs)
        self.search_log["must_have_tokens"] = must_have_tokens

        seed_rows: List[Dict[str, Any]] = []
        used_queries = 0
        seed_query_list = self.build_seed_queries(keywords_for_search, search_anchors, packs)

        if not seed_query_list and not self.offline_fixtures:
            self.search_log["errors"].append("query_preflight: нет валидных seed-запросов")
            self.write_llm_anchor_prompt(search_anchors[:20], packs[:6], "нет валидных seed-запросов после самопроверки")
            response_path = self.ensure_llm_response_template()
            stats = {"seed_queries": 0, "seed_count": 0, "total_candidates": 0, "pass_count": 0, "low_count": 0, "drift_score": 1.0, "elapsed_ms": int((time.time() - t0) * 1000), "stop_reason": "невалидные поисковые строки", "primary_token": primary_token, "packs_count": len(packs), "must_have_count": len(must_have_tokens)}
            self.search_log["stats"] = stats
            self.search_log["finished_at"] = now_iso()
            self.search_log["stop_files"] = [str(self.out_dir / "llm_prompt_B_anchors.txt"), str(self.in_dir / "llm_response_B_anchors.json")]
            self.write_prisma(stats)
            self.write_summary(stats, used_europe=False, wait_llm=True, used_user_response=used_user_response, corpus_updated=False, stop_reason="невалидные поисковые строки")
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            print(f"Этап B остановлен и ждёт файл: {response_path}")
            return 2

        if self.offline_fixtures:
            seed_rows = self.load_fixture()
        else:
            for q in seed_query_list:
                used_queries += 1
                try:
                    rows, _ = self.openalex_search_pack(q)
                    seed_rows.extend([self.paper_openalex(r, "openalex_seed") for r in rows])
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

        passed_rows, ranked_all, drift_score, metrics = self.apply_relevance_and_score(merged, primary_token, keywords_tokens, must_have_tokens, packs, drift_blacklist)
        self.search_log["primary_hit_count"] = metrics["primary_hit_count"]
        self.search_log["packs_hit_count"] = metrics["packs_hit_count"]
        self.search_log["stats"]["dedup_before"] = self.dedup_before
        self.search_log["stats"]["dedup_after"] = self.dedup_after

        drift_stop = drift_score >= 0.40
        if need_llm or drift_stop:
            reason = "seed=0" if need_llm else f"high drift: {round(drift_score * 100,1)}%"
            self.write_llm_anchor_prompt(search_anchors[:20], packs[:6], reason)
            response_path = self.ensure_llm_response_template()
            stats = {
                "seed_queries": used_queries,
                "seed_count": len(seed_rows),
                "total_candidates": len(ranked_all),
                "pass_count": len(passed_rows),
                "low_count": max(len(ranked_all) - len(passed_rows), 0),
                "drift_score": round(drift_score, 4),
                "elapsed_ms": int((time.time() - t0) * 1000),
                "primary_token": primary_token,
                "primary_hit_count": metrics["primary_hit_count"],
                "packs_count": len(packs),
                "packs_hit_count": metrics["packs_hit_count"],
                "must_have_count": len(must_have_tokens),
                "dataset_low_count": metrics["dataset_low_count"],
                "dedup_merged_count": self.dedup_merged_count,
                "stop_reason": reason,
            }
            self.search_log["stop_files"] = [str(self.out_dir / "llm_prompt_B_anchors.txt"), str(self.in_dir / "llm_response_B_anchors.json")]
            self.search_log["stats"] = {**self.search_log.get("stats", {}), **stats, "dedup_before": self.dedup_before, "dedup_after": self.dedup_after}
            self.search_log["finished_at"] = now_iso()
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            self.write_prisma(stats)
            self.write_summary(stats, used_europe=False, wait_llm=True, used_user_response=used_user_response, corpus_updated=False, stop_reason=reason)
            print(f"Этап B остановлен ({reason}) и ждёт файл: {response_path}")
            return 2

        corpus_updated = len(passed_rows) > 0
        self.write_corpus(passed_rows, ranked_all, allow_replace=corpus_updated)
        stats = {
            "seed_queries": used_queries,
            "seed_count": len(seed_rows),
            "total_candidates": len(ranked_all),
            "pass_count": len(passed_rows),
            "low_count": max(len(ranked_all) - len(passed_rows), 0),
            "drift_score": round(drift_score, 4),
            "elapsed_ms": int((time.time() - t0) * 1000),
            "primary_token": primary_token,
            "primary_hit_count": metrics["primary_hit_count"],
            "packs_count": len(packs),
            "packs_hit_count": metrics["packs_hit_count"],
            "must_have_count": len(must_have_tokens),
            "dataset_low_count": metrics["dataset_low_count"],
            "dedup_merged_count": self.dedup_merged_count,
        }
        self.search_log["stats"] = {**stats, "dedup_before": self.dedup_before, "dedup_after": self.dedup_after}
        self.search_log["finished_at"] = now_iso()
        self.write_prisma(stats)
        self.write_summary(stats, used_europe=False, wait_llm=False, used_user_response=used_user_response, corpus_updated=corpus_updated)
        write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
        self.log("Stage B done")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idea", required=True)
    ap.add_argument("--mode", default="BALANCED", choices=["BALANCED", "FOCUSED", "WIDE"])
    ap.add_argument("--offline-fixtures", default="")
    args = ap.parse_args()
    offline = Path(args.offline_fixtures) if args.offline_fixtures else None
    return StageB(Path(args.idea), args.mode, offline).run()


if __name__ == "__main__":
    raise SystemExit(main())
