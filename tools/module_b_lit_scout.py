# -*- coding: utf-8 -*-
import argparse
import csv
import json
import os
import re
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

CYR_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
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

        repo = self.idea_dir.parents[1]
        self.secrets = parse_env(repo / "config" / "secrets.env")
        self.mailto = self.secrets.get("OPENALEX_MAILTO", "")
        self.s2_key = self.secrets.get("SEMANTIC_SCHOLAR_API_KEY", "")

        self.session = requests.Session() if requests else None
        self.search_log: Dict[str, Any] = {
            "started_at": now_iso(),
            "mode": self.mode,
            "anchor_candidates": [],
            "anchor_top_display": [],
            "anchor_top_search": [],
            "anchor_packs": [],
            "abbreviation_map": {},
            "queries": [],
            "errors": [],
            "service_status": {
                "openalex": "offline",
                "semantic_scholar": "offline",
                "crossref": "offline",
                "researchrabbit": "degraded",
                "europe_pmc": "offline",
            },
            "stats": {},
        }
        self.abbr_full_map: Dict[str, str] = {}
        self.abbr_mentions: Set[str] = set()
        self.query_snippets: List[Dict[str, Any]] = []

    def log(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        for p in (self.run_log, self.module_log):
            with p.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    def request_json(self, method: str, url: str, source: str, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], int, int]:
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
            return f"{full} ({ab})" if full else f"аббревиатура не раскрыта: {ab}"
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

    def build_anchor_packs(self, anchors: List[str]) -> List[List[str]]:
        packs: List[List[str]] = []
        idx = 0
        while idx < len(anchors) and len(packs) < 6:
            pack = anchors[idx:idx + 3]
            if len(pack) >= 2:
                packs.append(pack)
            idx += 3
        if len(packs) < 4 and len(anchors) >= 8:
            packs.append([anchors[0], anchors[3]])
            packs.append([anchors[1], anchors[4]])
        packs = packs[:6]
        self.search_log["anchor_packs"] = packs
        return packs

    def pack_query(self, pack: List[str]) -> str:
        return " ".join(pack)

    def openalex_search_pack(self, pack: List[str]) -> Tuple[List[Dict[str, Any]], int]:
        query = self.pack_query(pack)
        params = {"search": query, "per-page": 50}
        if self.mailto:
            params["mailto"] = self.mailto
        endpoint = "https://api.openalex.org/works"
        try:
            obj, status, elapsed = self.request_json("GET", endpoint, "openalex", params=params)
            total = int((obj.get("meta") or {}).get("count") or 0)
            items = obj.get("results", [])
            if self.search_log["queries"]:
                self.search_log["queries"][-1].update({
                    "query_text": query,
                    "anchor_pack_used": pack,
                    "result_total": total,
                    "result_items": len(items),
                })
            if len(self.query_snippets) < 3:
                self.query_snippets.append({"query_text": query, "result_total": total})
            self.search_log["service_status"]["openalex"] = "success"
            return items, total
        except Exception:
            if self.search_log["queries"]:
                self.search_log["queries"][-1].update({"query_text": query, "anchor_pack_used": pack})
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
            "authors_short": ", ".join([(a.get("author", {}) or {}).get("display_name", "") for a in (w.get("authorships") or [])[:4]]),
            "cited_by_count": w.get("cited_by_count", 0) or 0,
            "source_tags": {tag},
            "url": w.get("primary_location", {}).get("landing_page_url") or w.get("id", ""),
            "type": w.get("type", ""),
            "concepts": [c.get("display_name", "") for c in (w.get("concepts") or [])[:5]],
            "referenced_works": w.get("referenced_works", [])[:15],
            "related_works": w.get("related_works", [])[:15],
        }

    def semantic_scholar_search(self, pack: List[str]) -> List[Dict[str, Any]]:
        q = " ".join(pack[:3])
        params = {"query": q, "limit": 25, "fields": "title,year,venue,authors,citationCount,externalIds,url"}
        try:
            obj, _, _ = self.request_json("GET", "https://api.semanticscholar.org/graph/v1/paper/search", "semantic_scholar", params=params)
            self.search_log["service_status"]["semantic_scholar"] = "success"
            out = []
            for p in obj.get("data", []):
                out.append({
                    "title": p.get("title", ""), "year": p.get("year") or "", "doi": normalize_doi((p.get("externalIds") or {}).get("DOI", "")),
                    "openalex_id": "", "venue": p.get("venue", ""),
                    "authors_short": ", ".join([a.get("name", "") for a in p.get("authors", [])[:4]]),
                    "cited_by_count": p.get("citationCount", 0) or 0,
                    "source_tags": {"semanticscholar_search"}, "url": p.get("url", ""), "type": "",
                    "concepts": [], "referenced_works": [], "related_works": [],
                })
            return out
        except Exception as e:
            self.search_log["service_status"]["semantic_scholar"] = "degraded"
            self.search_log["errors"].append(f"semantic_search_error: {e}")
            return []

    def semantic_recommend(self, seeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        positive = [f"DOI:{p['doi']}" for p in seeds if p.get("doi")][:10]
        if not positive:
            return []
        payload = {"positive_paper_ids": positive, "negative_paper_ids": []}
        try:
            obj, _, _ = self.request_json("POST", "https://api.semanticscholar.org/recommendations/v1/papers", "semantic_scholar", payload=payload)
            self.search_log["service_status"]["semantic_scholar"] = "success"
            out: List[Dict[str, Any]] = []
            for p in obj.get("recommendedPapers", []):
                out.append({
                    "title": p.get("title", ""), "year": p.get("year") or "", "doi": normalize_doi((p.get("externalIds") or {}).get("DOI", "")),
                    "openalex_id": "", "venue": p.get("venue", ""),
                    "authors_short": ", ".join([a.get("name", "") for a in p.get("authors", [])[:4]]),
                    "cited_by_count": p.get("citationCount", 0) or 0,
                    "source_tags": {"semanticscholar_recommendations"}, "url": f"https://www.semanticscholar.org/paper/{p.get('paperId', '')}",
                    "type": "", "concepts": [], "referenced_works": [], "related_works": [],
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
                obj, _, _ = self.request_json("GET", "https://api.crossref.org/works", "crossref", params=params)
                items = (obj.get("message") or {}).get("items") or []
                if items:
                    doi = normalize_doi(items[0].get("DOI", ""))
                    if doi:
                        p["doi"] = doi
                        p["source_tags"].add("crossref")
                        count += 1
                self.search_log["service_status"]["crossref"] = "success"
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
                obj, _, _ = self.request_json("GET", f"https://api.openalex.org/works/{wid.split('/')[-1]}", "openalex")
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
        keep: Dict[str, Dict[str, Any]] = {}
        for p in papers:
            key = p.get("doi") or f"{norm_title(p.get('title', ''))}::{p.get('year', '')}"
            if key == "::":
                continue
            old = keep.get(key)
            if not old:
                keep[key] = p
                continue
            old["source_tags"] = set(old.get("source_tags", set())) | set(p.get("source_tags", set()))
            old["cited_by_count"] = max(old.get("cited_by_count", 0), p.get("cited_by_count", 0))
            if old.get("type") == "preprint" and p.get("type") != "preprint":
                keep[key] = p
        return list(keep.values())

    def score(self, papers: List[Dict[str, Any]], anchors: List[str], anchor_packs: List[List[str]]) -> List[Dict[str, Any]]:
        out = []
        for p in papers:
            text = (p.get("title", "") + " " + p.get("venue", "")).lower()
            hits = sum(1 for a in anchors[:12] if a.lower() in text)
            pack_hits = sum(1 for pack in anchor_packs if sum(1 for a in pack if a.lower() in text) >= 2)
            s = hits * 1.6 + pack_hits * 2.5 + (p.get("cited_by_count", 0) or 0) * 0.01
            p["score"] = round(s, 4)
            out.append(p)
        out.sort(key=lambda x: (-x["score"], -(x.get("cited_by_count", 0) or 0), x.get("title", "")))
        return out

    def write_corpus(self, papers: List[Dict[str, Any]]) -> None:
        fields = ["rank", "score", "title", "year", "doi", "openalex_id", "venue", "authors_short", "cited_by_count", "source_tags", "url"]
        for name, rows in (("corpus_all.csv", papers), ("corpus.csv", papers[:300])):
            with (self.out_dir / name).open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
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
                    })

    def write_prisma(self, stats: Dict[str, Any]) -> None:
        lines = [
            "# PRISMA-lite Stage B",
            "",
            f"- Seed queries: {stats.get('seed_queries', 0)}",
            f"- Seed count: {stats.get('seed_count', 0)}",
            f"- Expanded count: {stats.get('expanded_count', 0)}",
            f"- Semantic Scholar count: {stats.get('semanticscholar_count', 0)}",
            f"- Crossref count: {stats.get('crossref_count', 0)}",
            f"- Dedup count: {stats.get('dedup_count', 0)}",
            f"- Final count: {stats.get('final_count', 0)}",
        ]
        write_text(self.out_dir / "prisma_lite_B.md", "\n".join([self.format_human_abbr(x) for x in lines]) + "\n")

    def write_llm_anchor_prompt(self, anchors: List[str], packs: List[List[str]]) -> None:
        txt = (
            "Провал первичного поиска. Верни только JSON:\n"
            "{\n"
            "  \"cleaned_anchors\": [\"...\"],\n"
            "  \"anchor_packs\": [[\"...\",\"...\"],[\"...\",\"...\",\"...\"]],\n"
            "  \"drift_blacklist\": [\"...\"],\n"
            "  \"abbreviation_map\": {\"ABBR\": \"Full expansion\"}\n"
            "}\n"
            "Ограничения: 10-20 cleaned_anchors, 4-6 anchor_packs, только по теме идеи.\n"
            f"Текущие anchors: {anchors}\n"
            f"Текущие packs: {packs}\n"
        )
        write_text(self.out_dir / "llm_prompt_B_anchors.txt", txt)

    def load_llm_anchor_response(self) -> Dict[str, Any]:
        p = self.in_dir / "llm_response_B_anchors.json"
        if not p.exists():
            return {}
        try:
            data = json.loads(read_text(p))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def write_summary(self, stats: Dict[str, Any], used_europe: bool, wait_llm: bool) -> None:
        lines = [
            "Stage B: сбор корпуса литературы завершен." if not wait_llm else "Stage B: требуется дополнительная очистка якорей.",
            f"Идея: {self.idea_dir.name}",
            f"Режим: {self.mode}",
            f"Количество seed запросов: {stats.get('seed_queries', 0)}",
            f"Количество seed работ: {stats.get('seed_count', 0)}",
            f"Количество работ после расширения: {stats.get('expanded_count', 0)}",
            f"Количество работ из Semantic Scholar: {stats.get('semanticscholar_count', 0)}",
            f"Количество нормализованных через Crossref: {stats.get('crossref_count', 0)}",
            f"Количество после дедупликации: {stats.get('dedup_count', 0)}",
            f"Итоговый размер корпуса: {stats.get('final_count', 0)}",
            "Europe PubMed Central (PMC) не использовался: тема не определена как биомедицинская по быстрому признаку." if not used_europe else "Europe PubMed Central (PMC) использован из-за биомедицинского профиля.",
            f"Статус OpenAlex: {self.search_log['service_status']['openalex']}",
            f"Статус Semantic Scholar: {self.search_log['service_status']['semantic_scholar']}",
            f"Статус Crossref: {self.search_log['service_status']['crossref']}",
            f"Файл корпуса: {self.out_dir / 'corpus.csv'}",
            f"Полный корпус: {self.out_dir / 'corpus_all.csv'}",
            f"Диагностика: {self.out_dir / 'search_log_B.json'}",
            f"Текстовый лог: {self.out_dir / 'runB.log'}",
        ]
        if self.query_snippets:
            lines.append("Первые 3 выполненных запроса:")
            for q in self.query_snippets[:3]:
                lines.append(f"- {q['query_text']} -> {q['result_total']}")
        if stats.get("seed_count", 0) == 0:
            lines.append("Seed=0: включён безопасный план Б (якоря от языковой модели), проверь термины поиска.")
        if wait_llm:
            lines += [
                f"Создан prompt: {self.out_dir / 'llm_prompt_B_anchors.txt'}",
                f"Ожидаемый ответ: {self.in_dir / 'llm_response_B_anchors.json'}",
            ]
        write_text(self.out_dir / "stageB_summary.txt", "\n".join([self.format_human_abbr(x) for x in lines[:28]]) + "\n")

    def load_fixture(self) -> List[Dict[str, Any]]:
        fp = self.offline_fixtures / "openalex_seed.json" if self.offline_fixtures else None
        if not fp or not fp.exists():
            return []
        obj = json.loads(read_text(fp))
        self.search_log["service_status"]["openalex"] = "offline"
        return [self.paper_openalex(w, "openalex_seed") for w in obj.get("results", [])]

    def run(self) -> int:
        t0 = time.time()
        self.log("Stage B start")
        self.log(f"Secrets: OPENALEX_MAILTO={'***' if self.mailto else '(missing)'}, SEMANTIC_SCHOLAR_API_KEY={'***' if self.s2_key else '(missing)'}")

        ok, idea_text = self.ensure_idea_text()
        if not ok:
            self.search_log["errors"].append(idea_text)
            self.write_corpus([])
            self.write_prisma({})
            self.write_summary({}, used_europe=False, wait_llm=False)
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            return 0

        structured = self.load_structured()
        display_anchors, search_anchors, ab_map, all_abbr = self.build_anchors(idea_text, structured)
        packs = self.build_anchor_packs(search_anchors)

        llm = self.load_llm_anchor_response()
        if llm.get("cleaned_anchors") and llm.get("anchor_packs"):
            search_anchors = [a for a in llm.get("cleaned_anchors", []) if isinstance(a, str)][:20]
            display_anchors = display_anchors[:20]
            packs = [p for p in llm.get("anchor_packs", []) if isinstance(p, list) and len(p) >= 2][:6]
            ab_map.update({k: v for k, v in (llm.get("abbreviation_map", {}) or {}).items() if isinstance(k, str) and isinstance(v, str)})
            self.search_log["abbreviation_map"] = ab_map
            self.abbr_full_map = dict(ab_map)

        seed_rows: List[Dict[str, Any]] = []
        used_queries = 0

        if self.offline_fixtures:
            seed_rows = self.load_fixture()
        else:
            for p in packs[:6]:
                try:
                    rows, _ = self.openalex_search_pack(p)
                    seed_rows.extend([self.paper_openalex(r, "openalex_seed") for r in rows])
                    used_queries += 1
                except Exception as e:
                    self.search_log["service_status"]["openalex"] = "degraded"
                    self.search_log["errors"].append(f"openalex_seed_error: {e}")
            if len(seed_rows) == 0:
                broad_pack = search_anchors[:10]
                if broad_pack:
                    try:
                        rows, _ = self.openalex_search_pack(broad_pack)
                        seed_rows.extend([self.paper_openalex(r, "openalex_seed") for r in rows])
                        used_queries += 1
                    except Exception as e:
                        self.search_log["errors"].append(f"openalex_fallback_error: {e}")

        # abbreviation policy: only after strong seed and explicit expansion
        if len(seed_rows) >= 30:
            for ab in list(all_abbr):
                if ab in ab_map:
                    search_anchors.append(f"{ab_map[ab]} {ab}")

        need_llm = len(seed_rows) == 0
        if self.offline_fixtures:
            need_llm = False
        filtered_drift_share = 0.0

        semantic_rows: List[Dict[str, Any]] = []
        recommend_rows: List[Dict[str, Any]] = []
        expanded: List[Dict[str, Any]] = []
        crossref_count = 0

        if not need_llm and not self.offline_fixtures:
            for p in packs[:2]:
                semantic_rows.extend(self.semantic_scholar_search(p))
            recommend_rows = self.semantic_recommend(seed_rows)
            expanded = self.expand_openalex(seed_rows, packs)

        if len(seed_rows) < 20:
            expanded = []

        merged = self.dedup(seed_rows + semantic_rows + recommend_rows + expanded)
        crossref_count = self.crossref_enrich(merged) if not self.offline_fixtures else 0
        ranked = self.score(merged, search_anchors, packs)

        # drift estimation: rows with zero anchor hits
        if ranked:
            drifted = 0
            for p in ranked:
                txt = (p.get("title", "") + " " + p.get("venue", "")).lower()
                if sum(1 for a in search_anchors[:10] if a.lower() in txt) == 0:
                    drifted += 1
            filtered_drift_share = drifted / len(ranked)

        if (need_llm or filtered_drift_share > 0.4) and not llm and not self.offline_fixtures:
            self.write_llm_anchor_prompt(search_anchors[:20], packs[:6])
            stats = {
                "seed_queries": min(used_queries, 8),
                "seed_count": len(seed_rows),
                "expanded_count": len(expanded),
                "semanticscholar_count": len(semantic_rows) + len(recommend_rows),
                "crossref_count": crossref_count,
                "dedup_count": len(merged),
                "final_count": len(ranked),
                "elapsed_ms": int((time.time() - t0) * 1000),
            }
            self.search_log["stats"] = stats
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            self.write_corpus(ranked)
            self.write_prisma(stats)
            self.write_summary(stats, used_europe=False, wait_llm=True)
            return 2

        self.write_corpus(ranked)
        stats = {
            "seed_queries": min(used_queries, 8),
            "seed_count": len(seed_rows),
            "expanded_count": len(expanded),
            "semanticscholar_count": len(semantic_rows) + len(recommend_rows),
            "crossref_count": crossref_count,
            "dedup_count": len(merged),
            "final_count": len(ranked),
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
        self.search_log["stats"] = stats
        self.search_log["finished_at"] = now_iso()
        self.write_prisma(stats)
        self.write_summary(stats, used_europe=False, wait_llm=False)
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
