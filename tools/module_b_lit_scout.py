# -*- coding: utf-8 -*-
import argparse
import csv
import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.parse
    import urllib.request


RU_STOPWORDS = {
    "и", "или", "но", "для", "это", "как", "что", "при", "без", "над", "под", "между", "если", "чтобы",
    "экзамен", "объяснение", "ожидаемые", "expected_patterns", "шаг", "этап", "идея", "почему", "как", "проверка",
    "данные", "результат", "гипотеза", "исследование", "метод", "подход", "тест", "вероятно", "может",
}
EN_STOPWORDS = {
    "the", "and", "or", "for", "with", "without", "this", "that", "from", "into", "about", "expected", "patterns",
    "explanation", "exam", "step", "stage", "idea", "result", "results", "method", "approach", "test", "tests",
    "hypothesis", "research", "likely", "possible", "should", "would", "could", "data",
}


class _MiniResp:
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self._body = body

    def json(self):
        return json.loads(self._body or "{}")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _MiniSession:
    def request(self, method, url, timeout=20, headers=None, params=None, json=None, **kwargs):
        headers = headers or {}
        if params:
            qs = urllib.parse.urlencode(params)
            url = url + ("&" if "?" in url else "?") + qs
        data = None
        if json is not None:
            data = __import__("json").dumps(json).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url=url, method=method, headers=headers, data=data)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return _MiniResp(r.getcode(), body)


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def parse_secrets_env(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data


class StageB:
    def __init__(self, idea_dir: Path, mode: str, offline_fixtures: Optional[Path]):
        self.idea_dir = idea_dir
        self.mode = mode.upper()
        self.offline_fixtures = offline_fixtures
        self.in_dir = idea_dir / "in"
        self.out_dir = idea_dir / "out"
        self.logs_dir = idea_dir / "logs"
        self.cache_dir = self.out_dir / "cache_openalex"
        for d in (self.in_dir, self.out_dir, self.logs_dir, self.cache_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.run_log = self.out_dir / "runB.log"
        self.search_log_path = self.out_dir / "search_log_B.json"
        self.trace_path = self.out_dir / "http_trace_B.jsonl"
        self.module_log_path = self.logs_dir / f"moduleB_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        self.debug_http = os.getenv("PIPELINE_DEBUG", "0") == "1"
        self.secrets = parse_secrets_env(self.idea_dir.parents[1] / "config" / "secrets.env")
        self.mailto = self.secrets.get("OPENALEX_MAILTO", "")

        self.session = requests.Session() if requests else _MiniSession()
        self.ua = "IDEA_PIPELINE_2.0-StageB/2.0"

        self.search_log: Dict[str, Any] = {
            "mode": self.mode,
            "started_at": now_iso(),
            "anchor_candidates": [],
            "anchor_top20": [],
            "abbreviation_decisions": [],
            "queries": [],
            "gating": {},
            "timings_ms": {},
            "errors": [],
            "stats": {},
        }

    def log(self, message: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        for p in (self.run_log, self.module_log_path):
            with p.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    def trace_http(self, payload: Dict[str, Any]) -> None:
        if not self.debug_http:
            return
        with self.trace_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def ensure_idea_text(self) -> Tuple[bool, str]:
        idea_top = self.idea_dir / "idea.txt"
        idea_in = self.in_dir / "idea.txt"
        if idea_in.exists() and (not idea_top.exists() or idea_in.stat().st_mtime > idea_top.stat().st_mtime):
            idea_top.write_text(read_text(idea_in), encoding="utf-8")
        if not idea_top.exists():
            return False, "Не найден idea.txt. Заполни in/idea.txt и запусти RUN_B.bat снова."
        return True, read_text(idea_top)

    def load_structured(self) -> Dict[str, Any]:
        p = self.out_dir / "structured_idea.json"
        if p.exists():
            try:
                return json.loads(read_text(p))
            except Exception as e:
                self.search_log["errors"].append(f"structured parse error: {e}")
        return {}

    def _cache_file(self, url: str, params: Dict[str, Any]) -> Path:
        payload = json.dumps({"url": url, "params": params}, sort_keys=True, ensure_ascii=False)
        key = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{key}.json"

    def request_json(self, method: str, url: str, source: str, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        params = params or {}
        headers = {"User-Agent": self.ua}
        if source == "openalex" and method.upper() == "GET":
            cached = self._cache_file(url, params)
            if cached.exists():
                return json.loads(read_text(cached)), {"status": 200, "retries": 0, "cached": True}

        retries = 4
        last_status = -1
        for attempt in range(1, retries + 1):
            t0 = time.time()
            try:
                resp = self.session.request(method.upper(), url, timeout=25, headers=headers, params=params, json=payload)
                ms = int((time.time() - t0) * 1000)
                last_status = getattr(resp, "status_code", -1)
                self.trace_http({"url": url, "status": last_status, "ms": ms, "retries": attempt - 1, "engine": source})
                if last_status in (429, 500, 502, 503, 504):
                    time.sleep(1.2 * attempt)
                    continue
                resp.raise_for_status()
                obj = resp.json()
                if source == "openalex" and method.upper() == "GET":
                    write_text(self._cache_file(url, params), json.dumps(obj, ensure_ascii=False))
                return obj, {"status": last_status, "retries": attempt - 1, "cached": False}
            except Exception as e:
                ms = int((time.time() - t0) * 1000)
                self.trace_http({"url": url, "status": last_status, "ms": ms, "retries": attempt - 1, "engine": source, "error": str(e)})
                if attempt == retries:
                    raise
                time.sleep(1.2 * attempt)
        raise RuntimeError("unreachable")

    def extract_abbrev_map(self, text: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for full, abbr in re.findall(r"([A-Za-zА-Яа-я0-9\-\s]{6,80})\(([A-Z0-9]{2,6})\)", text):
            out[abbr] = full.strip()
        for abbr, full in re.findall(r"\b([A-Z0-9]{2,6})\s*[—-]\s*([A-Za-zА-Яа-я0-9\-\s]{6,80})", text):
            out[abbr] = full.strip()
        return out

    def _tokenize(self, text: str) -> List[str]:
        return [t for t in re.split(r"[^A-Za-zА-Яа-я0-9\-]+", text.lower()) if t]

    def _too_generic(self, phrase: str) -> bool:
        toks = self._tokenize(phrase)
        if not toks:
            return True
        if len(toks) > 5:
            return True
        if len(phrase) > 50:
            return True
        return all(t in RU_STOPWORDS or t in EN_STOPWORDS for t in toks)

    def extract_anchors(self, idea_text: str, structured: Dict[str, Any]) -> Tuple[List[str], Dict[str, str], Set[str]]:
        blob_parts = [idea_text]
        s = structured.get("structured_idea", {}) if isinstance(structured.get("structured_idea", {}), dict) else {}
        for k in ("problem", "main_hypothesis", "key_predictions", "decisive_tests"):
            v = s.get(k)
            if isinstance(v, str):
                blob_parts.append(v)
            elif isinstance(v, list):
                blob_parts.extend([str(x) for x in v if isinstance(x, str)])
        blob = "\n".join(blob_parts)

        scored: Dict[str, float] = {}
        reasons: Dict[str, List[str]] = {}

        def add_anchor(a: str, why: str, score: float) -> None:
            a = re.sub(r"\s+", " ", a.strip())
            if not a or self._too_generic(a):
                return
            scored[a] = max(scored.get(a, 0.0), score)
            reasons.setdefault(a, []).append(why)

        for q in re.findall(r"[\"«]([^\"»]{2,180})[\"»]", blob):
            add_anchor(q, "quoted_short", 2.5)

        for t in re.findall(r"\b[\wА-Яа-я]+-[\wА-Яа-я]+\b", blob):
            add_anchor(t, "hyphen_term", 2.2)

        for t in re.findall(r"\b[A-Za-z][A-Za-z0-9]{2,}\b", blob):
            if any(ch.isdigit() for ch in t) or re.search(r"[a-z][A-Z]|[A-Z]{2,}", t):
                add_anchor(t, "camel_or_digit", 2.1)

        for t in re.findall(r"\b(?:[A-ZА-Я][a-zа-я]+\s+){0,3}[A-ZА-Я][a-zа-я]+\b", blob):
            add_anchor(t, "named_entity_like", 1.8)

        words = re.findall(r"[A-Za-zА-Яа-я0-9\-]{3,}", blob)
        cnt = Counter(w.lower() for w in words)
        for tok, c in cnt.items():
            if c < 2:
                continue
            if tok in RU_STOPWORDS or tok in EN_STOPWORDS:
                continue
            if len(tok) > 2:
                add_anchor(tok, "freq_term", min(2.0, 1 + c / 5.0))

        ranked = sorted(scored.items(), key=lambda x: (-x[1], x[0].lower()))
        anchors = [k for k, _ in ranked[:20]]
        self.search_log["anchor_candidates"] = [{"anchor": a, "score": round(scored[a], 2), "reasons": reasons[a]} for a in anchors]
        self.search_log["anchor_top20"] = self.search_log["anchor_candidates"]
        self.log("TOP-20 anchors: " + "; ".join([f"{a['anchor']} [{','.join(a['reasons'])}]" for a in self.search_log["anchor_top20"][:20]]))

        ab_map = self.extract_abbrev_map(blob)
        all_abbr = set(re.findall(r"\b[A-Z0-9]{2,6}\b", blob))
        return anchors, ab_map, all_abbr

    def build_seed_queries(self, anchors: List[str]) -> List[Dict[str, Any]]:
        packs: List[List[str]] = []
        for i in range(0, min(len(anchors), 18), 3):
            chunk = anchors[i:i + 3]
            if len(chunk) >= 2:
                packs.append(chunk)
        packs = packs[:6]

        out: List[Dict[str, Any]] = []
        for pack in packs:
            strict_terms = [f'"{p}"' if len(p) <= 50 and len(p.split()) <= 5 else p for p in pack]
            out.append({"variant": "strict", "query": " AND ".join(strict_terms), "anchors": pack})
        for pack in packs[:3]:
            out.append({"variant": "loose", "query": " ".join(pack[:2]), "anchors": pack[:2]})
        return out

    def openalex_search(self, query: str, variant: str) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"search": query, "per-page": 50, "page": 1}
        if self.mailto:
            params["mailto"] = self.mailto
        t0 = time.time()
        obj, meta = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
        results = obj.get("results", [])
        self.search_log["queries"].append({
            "query": query,
            "variant": variant,
            "engine": "openalex",
            "results": len(results),
            "http_status": meta.get("status", -1),
            "retries": meta.get("retries", 0),
            "cached": meta.get("cached", False),
        })
        self.search_log["timings_ms"][query] = int((time.time() - t0) * 1000)
        return results

    def _paper_from_openalex(self, w: Dict[str, Any], source: str) -> Dict[str, Any]:
        doi = (w.get("doi") or "").replace("https://doi.org/", "")
        concepts = [c.get("display_name", "") for c in (w.get("concepts") or [])[:5] if c.get("display_name")]
        topics = [t.get("display_name", "") for t in (w.get("topics") or [])[:3] if t.get("display_name")]
        return {
            "title": w.get("title", ""),
            "year": w.get("publication_year", ""),
            "doi": doi,
            "openalex_id": w.get("id", ""),
            "venue": (w.get("host_venue") or {}).get("display_name", "") or (w.get("primary_location", {}).get("source") or {}).get("display_name", ""),
            "authors_short": ", ".join([(a.get("author", {}) or {}).get("display_name", "") for a in (w.get("authorships") or [])[:4]]),
            "cited_by_count": w.get("cited_by_count", 0) or 0,
            "source_tags": {source},
            "url": w.get("primary_location", {}).get("landing_page_url") or w.get("id", ""),
            "concepts": concepts,
            "topics": topics,
            "referenced_works": w.get("referenced_works", [])[:15],
            "related_works": w.get("related_works", [])[:15],
        }

    def apply_abbreviation_policy(self, all_abbr: Set[str], ab_map: Dict[str, str], seed_papers: List[Dict[str, Any]], anchors: List[str]) -> List[str]:
        allowed: List[str] = []
        seed_titles = [p.get("title", "") for p in seed_papers]
        for ab in sorted(all_abbr):
            reason = "blocked_default"
            ok = False
            if ab in ab_map:
                ok = True
                reason = "explicit_expansion"
            else:
                hit = sum(1 for t in seed_titles if re.search(rf"\b{re.escape(ab)}\b", t))
                co = sum(1 for t in seed_titles if re.search(rf"\b{re.escape(ab)}\b", t) and any(a.lower() in t.lower() for a in anchors[:8]))
                if hit >= 2 and co >= 1:
                    ok = True
                    reason = "seed_self_validated"
            self.search_log["abbreviation_decisions"].append({"abbr": ab, "allowed": ok, "reason": reason})
            if ok:
                allowed.append(ab)
        return allowed

    def subject_gate(self, papers: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        cc, tc, vc = Counter(), Counter(), Counter()
        for p in papers:
            for c in p.get("concepts", [])[:3]:
                cc[c] += 1
            for t in p.get("topics", [])[:2]:
                tc[t] += 1
            if p.get("venue"):
                vc[p["venue"]] += 1
        topc = [x for x, _ in cc.most_common(3)]
        topt = [x for x, _ in tc.most_common(3)]
        topv = [x for x, _ in vc.most_common(3)]
        if self.mode == "FOCUSED":
            k = 1
            explore_budget = 0
        elif self.mode == "BALANCED":
            k = 2
            explore_budget = 2
        else:
            k = 3
            explore_budget = 4
        gating = {
            "allowed_concepts": topc[:k],
            "allowed_topics": topt[:k],
            "allowed_venues": topv[:k],
            "explore_budget": explore_budget,
            "why": f"mode={self.mode}; seed-profile top concepts/topics/venues",
        }
        self.search_log["gating"] = gating
        return gating

    def _passes_gate(self, p: Dict[str, Any], gating: Dict[str, List[str]]) -> bool:
        c_ok = not gating.get("allowed_concepts") or any(c in gating["allowed_concepts"] for c in p.get("concepts", []))
        t_ok = not gating.get("allowed_topics") or any(t in gating["allowed_topics"] for t in p.get("topics", []))
        v_ok = not gating.get("allowed_venues") or p.get("venue") in gating.get("allowed_venues", [])
        return c_ok or t_ok or v_ok

    def expand_openalex(self, seed: List[Dict[str, Any]], gating: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        budget = 14 if self.mode == "WIDE" else 10 if self.mode == "BALANCED" else 6
        for p in seed[:budget]:
            wid = p.get("openalex_id", "").split("/")[-1]
            if not wid:
                continue
            queries = [
                {"filter": f"cites:{wid}", "tag": "forward"},
            ]
            for ref in p.get("referenced_works", [])[:5]:
                rid = ref.split("/")[-1]
                if rid:
                    queries.append({"filter": f"openalex:{rid}", "tag": "backward"})
            for rel in p.get("related_works", [])[:5]:
                rid = rel.split("/")[-1]
                if rid:
                    queries.append({"filter": f"openalex:{rid}", "tag": "related"})

            for q in queries[:8]:
                params = {"filter": q["filter"], "per-page": 10}
                if self.mailto:
                    params["mailto"] = self.mailto
                try:
                    obj, _ = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
                    for w in obj.get("results", []):
                        pw = self._paper_from_openalex(w, "citation")
                        if self._passes_gate(pw, gating):
                            out.append(pw)
                except Exception as e:
                    self.search_log["errors"].append(f"expand {q['tag']} {wid}: {e}")
        return out

    def s2_recommend(self, seed: List[Dict[str, Any]], negatives: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        positive = [f"DOI:{p['doi']}" for p in seed if p.get("doi")][:10]
        negative = [f"DOI:{p['doi']}" for p in negatives if p.get("doi")][:10]
        if not positive:
            return []
        payload = {"positive_paper_ids": positive, "negative_paper_ids": negative}
        try:
            obj, _ = self.request_json("POST", "https://api.semanticscholar.org/recommendations/v1/papers", source="s2", payload=payload)
            out = []
            for p in obj.get("recommendedPapers", []):
                out.append({
                    "title": p.get("title", ""),
                    "year": p.get("year", ""),
                    "doi": (p.get("externalIds", {}) or {}).get("DOI", ""),
                    "openalex_id": "",
                    "venue": p.get("venue", ""),
                    "authors_short": ", ".join([a.get("name", "") for a in p.get("authors", [])[:4]]),
                    "cited_by_count": p.get("citationCount", 0) or 0,
                    "source_tags": {"s2_reco"},
                    "url": f"https://www.semanticscholar.org/paper/{p.get('paperId','')}",
                    "concepts": [],
                    "topics": [],
                    "referenced_works": [],
                    "related_works": [],
                })
            return out
        except Exception as e:
            self.search_log["errors"].append(f"s2 recommend failed: {e}")
            return []

    def write_rr_import(self, seeds: List[Dict[str, Any]]) -> None:
        rr_dir = self.out_dir / "researchrabbit"
        rr_dir.mkdir(parents=True, exist_ok=True)
        lines: List[str] = []
        for p in seeds[:50]:
            lines.extend(["TY  - JOUR", f"TI  - {p.get('title','')}"])
            if p.get("year"):
                lines.append(f"PY  - {p.get('year')}")
            if p.get("doi"):
                lines.append(f"DO  - {p.get('doi')}")
            lines.append("ER  -")
        write_text(rr_dir / "RR_IMPORT.ris", "\n".join(lines) + "\n")

    def parse_rr_export(self) -> List[Dict[str, str]]:
        rr_dir = self.out_dir / "researchrabbit"
        for name in ("RR_EXPORT.ris", "RR_EXPORT.bib", "RR_EXPORT.csv"):
            p = rr_dir / name
            if p.exists() and p.suffix.lower() == ".ris":
                out = []
                chunks = [c for c in read_text(p).split("ER  -") if c.strip()]
                for ch in chunks:
                    item = {"title": "", "doi": "", "year": ""}
                    for line in ch.splitlines():
                        if line.startswith("TI  -"):
                            item["title"] = line.split("-", 1)[1].strip()
                        elif line.startswith("DO  -"):
                            item["doi"] = line.split("-", 1)[1].strip()
                        elif line.startswith("PY  -"):
                            item["year"] = line.split("-", 1)[1].strip()
                    out.append(item)
                return out
        return []

    def enrich_rr(self, rr_items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for it in rr_items[:100]:
            doi = it.get("doi", "")
            if not doi:
                continue
            params = {"filter": f"doi:{doi}", "per-page": 1}
            if self.mailto:
                params["mailto"] = self.mailto
            try:
                obj, _ = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
                if obj.get("results"):
                    out.append(self._paper_from_openalex(obj["results"][0], "researchrabbit"))
            except Exception as e:
                self.search_log["errors"].append(f"rr enrich {doi}: {e}")
        return out

    def dedup_merge(self, groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        by_key: Dict[str, Dict[str, Any]] = {}
        for g in groups:
            for p in g:
                key = p.get("doi") or p.get("openalex_id") or p.get("title", "").strip().lower()
                if not key:
                    continue
                if key not in by_key:
                    by_key[key] = p
                else:
                    by_key[key]["source_tags"] = set(by_key[key].get("source_tags", set())) | set(p.get("source_tags", set()))
                    by_key[key]["cited_by_count"] = max(by_key[key].get("cited_by_count", 0), p.get("cited_by_count", 0))
        return list(by_key.values())

    def score(self, papers: List[Dict[str, Any]], anchors: List[str], negatives: List[str], gating: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        ranked = []
        for p in papers:
            text = (p.get("title", "") + " " + p.get("venue", "")).lower()
            hits = sum(1 for a in anchors[:15] if a.lower() in text)
            neg_hits = sum(1 for n in negatives if n.lower() in text)
            gate_bonus = 1.0 if self._passes_gate(p, gating) else -2.0
            source_bonus = 1.2 if "seed" in p.get("source_tags", set()) else 0.7
            score = hits * 2.0 + source_bonus + (p.get("cited_by_count", 0) or 0) * 0.015 + gate_bonus - neg_hits * 1.5
            p["score"] = round(score, 3)
            p["source_tags"] = ",".join(sorted(list(p.get("source_tags", set()))))
            ranked.append(p)
        ranked.sort(key=lambda x: (-x["score"], -(x.get("cited_by_count", 0) or 0)))
        return ranked

    def write_corpus(self, papers: List[Dict[str, Any]]) -> None:
        fields = ["title", "year", "doi", "openalex_id", "venue", "authors_short", "cited_by_count", "source_tags", "score", "url"]
        for name, rows in (("corpus_all.csv", papers), ("corpus.csv", papers[:300])):
            with (self.out_dir / name).open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for p in rows:
                    w.writerow({k: p.get(k, "") for k in fields})

    def write_prisma(self, stats: Dict[str, Any]) -> None:
        txt = (
            "# PRISMA-lite Stage B\n\n"
            f"- Seed queries: {stats.get('seed_queries', 0)}\n"
            f"- Seed found: {stats.get('seed_count', 0)}\n"
            f"- Expansion found: {stats.get('expanded_count', 0)}\n"
            f"- S2 recommendations: {stats.get('s2_count', 0)}\n"
            f"- RR merged: {stats.get('rr_count', 0)}\n"
            f"- Final corpus size: {stats.get('final_count', 0)}\n"
        )
        write_text(self.out_dir / "prisma_lite_B.md", txt)

    def write_wait_files(self, anchors: List[str]) -> None:
        prompt = (
            "Ты помогаешь Stage B научного поиска. Верни ТОЛЬКО валидный JSON:\n"
            "{\n"
            "  \"keywords_en\": [\"...\"],\n"
            "  \"openalex_queries\": [\"...\"],\n"
            "  \"negative_terms\": [\"...\"],\n"
            "  \"abbrev_expansions\": {\"ABBR\": \"full term\"}\n"
            "}\n"
            "Ограничения: openalex_queries максимум 8, короткие (2-6 терминов), без длинных предложений и без длинных цитат.\n"
            f"Текущие якоря Stage B: {', '.join(anchors[:20])}\n"
        )
        write_text(self.out_dir / "llm_prompt_B_keywords.txt", prompt)
        template = {
            "_instruction": "Вставьте сюда ответ ChatGPT (только JSON объект без markdown).",
            "keywords_en": [],
            "openalex_queries": [],
            "negative_terms": [],
            "abbrev_expansions": {},
        }
        write_text(self.in_dir / "llm_response_B.json", json.dumps(template, ensure_ascii=False, indent=2))

    def write_summary(self, stats: Dict[str, Any], wait_mode: bool = False) -> None:
        if wait_mode:
            lines = [
                "Stage B: нужен 1 ручной шаг через ChatGPT.",
                f"Идея: {self.idea_dir.name}",
                f"Seed-статистика: найдено {stats.get('seed_count', 0)} работ (минимум 5).",
                "Что делать:",
                f"1) Откройте: {self.out_dir / 'llm_prompt_B_keywords.txt'}",
                "2) Вставьте prompt в ChatGPT.",
                "3) Скопируйте JSON-ответ без markdown.",
                f"4) Вставьте JSON в: {self.in_dir / 'llm_response_B.json'}",
                "5) Сохраните файл и снова запустите RUN_B.bat.",
                f"Логи: {self.out_dir / 'runB.log'} и {self.out_dir / 'search_log_B.json'}",
            ]
        else:
            lines = [
                "Stage B завершена.",
                f"Идея: {self.idea_dir.name}",
                f"Seed-запросов: {stats.get('seed_queries', 0)}",
                f"Seed-работ: {stats.get('seed_count', 0)}",
                f"Expansion: {stats.get('expanded_count', 0)}",
                f"S2 recommendations: {stats.get('s2_count', 0)}",
                f"ResearchRabbit merged: {stats.get('rr_count', 0)}",
                f"Итоговый корпус: {stats.get('final_count', 0)}",
                f"Файлы: {self.out_dir / 'corpus.csv'}",
                f"Файлы: {self.out_dir / 'corpus_all.csv'}",
                f"Файлы: {self.out_dir / 'prisma_lite_B.md'}",
                f"Логи: {self.out_dir / 'runB.log'}",
                f"Логи: {self.out_dir / 'search_log_B.json'}",
            ]
        write_text(self.out_dir / "stageB_summary.txt", "\n".join(lines) + "\n")

    def load_offline_fixture(self) -> List[Dict[str, Any]]:
        if not self.offline_fixtures:
            return []
        fp = self.offline_fixtures / "openalex_seed.json"
        if not fp.exists():
            return []
        obj = json.loads(read_text(fp))
        return [self._paper_from_openalex(w, "seed") for w in obj.get("results", [])]

    def _load_llm_response(self) -> Dict[str, Any]:
        p = self.in_dir / "llm_response_B.json"
        if not p.exists():
            return {}
        try:
            data = json.loads(read_text(p))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def run(self) -> int:
        t_start = time.time()
        self.log("Stage B started")
        self.log(f"Mode={self.mode}")
        self.log(f"Secrets: OPENALEX_API_KEY={'***' if self.secrets.get('OPENALEX_API_KEY') else '(missing)'}, OPENALEX_MAILTO={'***' if self.mailto else '(missing)'}")

        ok, idea_or_msg = self.ensure_idea_text()
        if not ok:
            self.search_log["errors"].append(idea_or_msg)
            self.write_corpus([])
            self.write_prisma({})
            self.write_summary({"seed_count": 0, "final_count": 0}, wait_mode=False)
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            self.log(idea_or_msg)
            return 0

        structured = self.load_structured()
        anchors, ab_map, all_abbr = self.extract_anchors(idea_or_msg, structured)
        llm_data = self._load_llm_response()
        if llm_data.get("keywords_en"):
            for kw in llm_data.get("keywords_en", [])[:12]:
                if isinstance(kw, str) and kw.strip():
                    anchors.append(kw.strip())

        queries = self.build_seed_queries(anchors)
        if llm_data.get("openalex_queries"):
            priority = [{"variant": "llm_priority", "query": q, "anchors": []} for q in llm_data.get("openalex_queries", []) if isinstance(q, str) and q.strip()][:8]
            queries = priority + queries

        seed_papers: List[Dict[str, Any]] = []
        if self.offline_fixtures:
            seed_papers = self.load_offline_fixture()
        else:
            strict_queries = [q for q in queries if q["variant"] != "loose"][:6]
            loose_queries = [q for q in queries if q["variant"] == "loose"][:3]
            for q in strict_queries:
                try:
                    seed_papers.extend([self._paper_from_openalex(w, "seed") for w in self.openalex_search(q["query"], q["variant"])])
                except Exception as e:
                    self.search_log["errors"].append(f"seed strict query failed: {q['query']} | {e}")
            if len(seed_papers) < 5:
                for q in loose_queries:
                    try:
                        seed_papers.extend([self._paper_from_openalex(w, "seed") for w in self.openalex_search(q["query"], q["variant"])])
                    except Exception as e:
                        self.search_log["errors"].append(f"seed loose query failed: {q['query']} | {e}")

        allowed_abbr = self.apply_abbreviation_policy(all_abbr, ab_map, seed_papers, anchors)
        if (not self.offline_fixtures) and seed_papers and allowed_abbr:
            for ab in allowed_abbr[:3]:
                for anchor in anchors[:5]:
                    q = f"{ab} AND {anchor}"
                    try:
                        seed_papers.extend([self._paper_from_openalex(w, "seed") for w in self.openalex_search(q, "abbr_pair")])
                    except Exception as e:
                        self.search_log["errors"].append(f"abbr query failed: {q} | {e}")

        if len(seed_papers) < 5 and not self.offline_fixtures:
            self.write_wait_files(anchors)
            stats = {"seed_queries": len(self.search_log["queries"]), "seed_count": len(seed_papers), "final_count": 0}
            self.search_log["stats"] = stats
            self.search_log["finished_at"] = now_iso()
            self.write_summary(stats, wait_mode=True)
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            self.log("Seed count <5 after strict+loose, entering WAIT mode")
            return 2

        gating = self.subject_gate(seed_papers)
        negatives = [p for p in seed_papers if not self._passes_gate(p, gating)]
        negative_terms = [x for x in llm_data.get("negative_terms", []) if isinstance(x, str)] if llm_data else []

        expanded: List[Dict[str, Any]] = []
        s2: List[Dict[str, Any]] = []
        rr: List[Dict[str, Any]] = []
        if not self.offline_fixtures:
            expanded = self.expand_openalex(seed_papers, gating)
            s2 = self.s2_recommend(seed_papers, negatives)

        self.write_rr_import(seed_papers)
        rr_items = self.parse_rr_export()
        if rr_items and not self.offline_fixtures:
            rr = self.enrich_rr(rr_items)

        merged = self.dedup_merge([seed_papers, expanded, s2, rr])
        ranked = self.score(merged, anchors, negative_terms, gating)
        self.write_corpus(ranked)

        stats = {
            "seed_queries": len(self.search_log["queries"]),
            "seed_count": len(seed_papers),
            "expanded_count": len(expanded),
            "s2_count": len(s2),
            "rr_count": len(rr),
            "dedup_count": len(merged),
            "final_count": len(ranked),
            "elapsed_ms": int((time.time() - t_start) * 1000),
        }
        self.search_log["stats"] = stats
        self.search_log["finished_at"] = now_iso()
        self.write_prisma(stats)
        self.write_summary(stats, wait_mode=False)
        write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
        self.log("Stage B finished")
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
