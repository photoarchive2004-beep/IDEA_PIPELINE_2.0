# -*- coding: utf-8 -*-
import argparse
import csv
import json
import os
import re
import time
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.request
    import urllib.parse



def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def write_text(path: Path, text: str) -> None:
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




class _MiniResp:
    def __init__(self, status_code:int, body:str):
        self.status_code=status_code
        self._body=body
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
class StageB:
    def __init__(self, idea_dir: Path, mode: str, offline_fixtures: Optional[Path] = None):
        self.idea_dir = idea_dir
        self.mode = mode.upper()
        self.offline_fixtures = offline_fixtures
        self.in_dir = idea_dir / "in"
        self.out_dir = idea_dir / "out"
        self.logs_dir = idea_dir / "logs"
        ensure_dir(self.in_dir)
        ensure_dir(self.out_dir)
        ensure_dir(self.logs_dir)
        self.module_log_path = self.logs_dir / f"moduleB_{now_stamp()}.log"
        self.run_log_path = self.out_dir / "runB.log"
        self.search_log_path = self.out_dir / "search_log_B.json"
        self.http_trace_path = self.out_dir / "http_trace_B.jsonl"
        self.debug_http = os.getenv("PIPELINE_DEBUG", "0") == "1"
        self.search_log: Dict[str, Any] = {
            "mode": self.mode,
            "started_at": datetime.utcnow().isoformat() + "Z",
            "abbreviation_decisions": [],
            "queries": [],
            "gating": {},
            "timings_ms": {},
            "scoring": {
                "formula": "anchor_bonus + source_bonus + cited_bonus - drift_penalty - generic_penalty",
                "weights": {
                    "anchor_bonus": 4.0,
                    "source_bonus": {"seed": 2.0, "citation": 1.4, "related": 1.2, "s2_reco": 1.0, "researchrabbit": 0.8},
                    "cited_by_multiplier": 0.015,
                    "drift_penalty": 2.5,
                    "generic_penalty": 1.5,
                },
            },
            "stats": {},
            "errors": [],
        }
        self.secrets = parse_secrets_env(self.idea_dir.parents[1] / "config" / "secrets.env")
        self.session = requests.Session() if requests else _MiniSession()
        self.ua = "IDEA_PIPELINE_2.0-StageB/1.0"
        self.mailto = self.secrets.get("OPENALEX_MAILTO", "")

    def log(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        with self.module_log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        with self.run_log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def trace_http(self, payload: Dict[str, Any]) -> None:
        if not self.debug_http:
            return
        with self.http_trace_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def request_json(self, method: str, url: str, source: str, **kwargs) -> Dict[str, Any]:
        retries = 3
        timeout = kwargs.pop("timeout", 20)
        headers = kwargs.pop("headers", {})
        headers["User-Agent"] = self.ua
        for attempt in range(1, retries + 1):
            t0 = time.time()
            try:
                r = self.session.request(method, url, timeout=timeout, headers=headers, **kwargs)
                ms = int((time.time() - t0) * 1000)
                self.trace_http({"url": url, "status": r.status_code, "ms": ms, "retries": attempt - 1, "source": source})
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.2 * attempt)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                ms = int((time.time() - t0) * 1000)
                self.trace_http({"url": url, "status": -1, "ms": ms, "retries": attempt - 1, "source": source})
                if attempt == retries:
                    raise
                time.sleep(1.2 * attempt)
        return {}

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
            except Exception:
                self.search_log["errors"].append("structured_idea.json parse error")
        return {}

    def extract_abbrev_map(self, text: str) -> Dict[str, str]:
        m = {}
        for full, abbr in re.findall(r"([A-Za-zА-Яа-я0-9\-\s]{5,})\(([A-Z0-9]{2,6})\)", text):
            m[abbr] = full.strip()
        for abbr, full in re.findall(r"\b([A-Z0-9]{2,6})\s*[—-]\s*([A-Za-zА-Яа-я0-9\-\s]{5,})", text):
            m[abbr] = full.strip()
        return m

    def extract_anchors(self, idea_text: str, structured: Dict[str, Any]) -> List[str]:
        blob_parts = [idea_text]
        for k in ("problem", "main_hypothesis", "key_predictions", "decisive_tests"):
            v = structured.get("structured_idea", {}).get(k)
            if isinstance(v, str):
                blob_parts.append(v)
            elif isinstance(v, list):
                blob_parts.append(json.dumps(v, ensure_ascii=False))
        blob = "\n".join(blob_parts)

        anchors = set()
        for q in re.findall(r"[\"«]([^\"»]{4,120})[\"»]", blob):
            anchors.add(q.strip())
        for t in re.findall(r"\b[\wА-Яа-я]+[-][\wА-Яа-я]+\b", blob):
            if len(t) >= 6:
                anchors.add(t)
        for t in re.findall(r"\b[A-Za-z][A-Za-z0-9]{5,}\b", blob):
            if any(ch.isdigit() for ch in t) or re.search(r"[A-Z].*[A-Z]", t):
                anchors.add(t)
        for t in re.findall(r"\b(?:[A-ZА-Я][\w-]+\s+){1,3}[A-ZА-Я][\w-]+\b", blob):
            if len(t) > 6:
                anchors.add(t)

        ab_map = self.extract_abbrev_map(blob)
        all_abbr = set(re.findall(r"\b[A-Z0-9]{2,6}\b", blob))
        for ab in sorted(all_abbr):
            allow = ab in ab_map
            self.search_log["abbreviation_decisions"].append({"abbr": ab, "allowed": allow, "reason": "expanded" if allow else "blocked"})
            if allow:
                anchors.add(f"{ab} {ab_map[ab]}")

        clean = [a.strip() for a in anchors if len(a.strip()) >= 4]
        clean.sort(key=lambda x: (-len(x), x))
        return clean[:18]

    def build_anchor_packs(self, anchors: List[str]) -> List[List[str]]:
        packs = []
        n = min(6, max(3, len(anchors) // 3 if anchors else 3))
        for i in range(n):
            chunk = anchors[i * 3:(i * 3) + 3]
            if len(chunk) >= 2:
                packs.append(chunk[:3])
        if not packs and len(anchors) >= 2:
            packs.append(anchors[:2])
        return packs[:6]

    def openalex_search(self, pack: List[str]) -> List[Dict[str, Any]]:
        query = " AND ".join([f'"{p}"' for p in pack])
        params = {"search": query, "per-page": 50, "page": 1}
        if self.mailto:
            params["mailto"] = self.mailto
        t0 = time.time()
        obj = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
        self.search_log["queries"].append({"query": query, "engine": "openalex", "results": len(obj.get("results", []))})
        self.search_log["timings_ms"][query] = int((time.time() - t0) * 1000)
        return obj.get("results", [])

    def _paper_from_openalex(self, w: Dict[str, Any], source: str) -> Dict[str, Any]:
        doi = (w.get("doi") or "").replace("https://doi.org/", "")
        authors = ", ".join([(a.get("author", {}) or {}).get("display_name", "") for a in (w.get("authorships") or [])[:4]])
        concepts = [c.get("display_name", "") for c in (w.get("concepts") or [])[:5]]
        return {
            "title": w.get("title", ""),
            "year": w.get("publication_year", ""),
            "doi": doi,
            "openalex_id": w.get("id", ""),
            "venue": (w.get("host_venue") or {}).get("display_name", ""),
            "authors_short": authors,
            "cited_by_count": w.get("cited_by_count", 0) or 0,
            "source_tags": {source},
            "url": w.get("primary_location", {}).get("landing_page_url") or w.get("id", ""),
            "concepts": concepts,
        }

    def subject_gate(self, papers: List[Dict[str, Any]]) -> List[str]:
        concepts = Counter()
        for p in papers:
            for c in p.get("concepts", [])[:3]:
                if c:
                    concepts[c] += 1
        allowed = [name for name, _ in concepts.most_common(2)]
        self.search_log["gating"] = {"allowed_concepts": allowed}
        return allowed

    def expand_openalex(self, seed: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        budget = 20 if self.mode == "WIDE" else 12 if self.mode == "BALANCED" else 8
        for p in seed[:budget]:
            wid = p.get("openalex_id", "").split("/")[-1]
            if not wid:
                continue
            params = {"filter": f"cites:{wid}", "per-page": 10}
            if self.mailto:
                params["mailto"] = self.mailto
            try:
                res = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
                out.extend([self._paper_from_openalex(w, "citation") for w in res.get("results", [])])
            except Exception as e:
                self.search_log["errors"].append(f"expand cites failed {wid}: {e}")
        return out

    def s2_recommend(self, seed: List[Dict[str, Any]], negatives: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        positive = [f"DOI:{p['doi']}" for p in seed if p.get("doi")][:10]
        negative = [f"DOI:{p['doi']}" for p in negatives if p.get("doi")][:10]
        if not positive:
            return []
        payload = {"positive_paper_ids": positive, "negative_paper_ids": negative}
        try:
            obj = self.request_json("POST", "https://api.semanticscholar.org/recommendations/v1/papers", source="s2", json=payload)
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
                })
            return out
        except Exception as e:
            self.search_log["errors"].append(f"s2 recommend failed: {e}")
            return []

    def write_rr_import(self, seeds: List[Dict[str, Any]]) -> None:
        rr_dir = self.out_dir / "researchrabbit"
        ensure_dir(rr_dir)
        ris = []
        for p in seeds[:50]:
            ris.append("TY  - JOUR")
            ris.append(f"TI  - {p.get('title','')}")
            if p.get("year"):
                ris.append(f"PY  - {p.get('year')}")
            if p.get("doi"):
                ris.append(f"DO  - {p.get('doi')}")
            ris.append("ER  -")
        write_text(rr_dir / "RR_IMPORT.ris", "\n".join(ris) + "\n")

    def parse_rr_export(self) -> List[Dict[str, Any]]:
        rr_dir = self.out_dir / "researchrabbit"
        if not rr_dir.exists():
            return []
        export = None
        for name in ("RR_EXPORT.ris", "RR_EXPORT.bib", "RR_EXPORT.csv"):
            p = rr_dir / name
            if p.exists():
                export = p
                break
        if not export:
            return []
        items = []
        txt = read_text(export)
        if export.suffix.lower() == ".ris":
            chunks = [c for c in txt.split("ER  -") if c.strip()]
            for ch in chunks:
                doi = ""
                title = ""
                year = ""
                for line in ch.splitlines():
                    if line.startswith("DO  -"):
                        doi = line.split("-", 1)[1].strip()
                    if line.startswith("TI  -"):
                        title = line.split("-", 1)[1].strip()
                    if line.startswith("PY  -"):
                        year = line.split("-", 1)[1].strip()
                items.append({"doi": doi, "title": title, "year": year})
        return items

    def enrich_rr(self, rr_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for it in rr_items[:100]:
            doi = it.get("doi", "")
            if not doi:
                continue
            params = {"filter": f"doi:{doi}", "per-page": 1}
            if self.mailto:
                params["mailto"] = self.mailto
            try:
                obj = self.request_json("GET", "https://api.openalex.org/works", source="openalex", params=params)
                if obj.get("results"):
                    out.append(self._paper_from_openalex(obj["results"][0], "researchrabbit"))
            except Exception as e:
                self.search_log["errors"].append(f"rr enrich failed for doi {doi}: {e}")
        return out

    def score(self, papers: List[Dict[str, Any]], anchors: List[str], allowed_concepts: List[str]) -> List[Dict[str, Any]]:
        w = self.search_log["scoring"]["weights"]
        rows = []
        for p in papers:
            text = (p.get("title", "") + " " + p.get("venue", "")).lower()
            anchor_hits = sum(1 for a in anchors if a.lower() in text)
            source = next(iter(p.get("source_tags") or {"citation"}))
            source_bonus = w["source_bonus"].get(source, 0.5)
            cited_bonus = (p.get("cited_by_count", 0) or 0) * w["cited_by_multiplier"]
            drift = 0.0
            if allowed_concepts:
                if not any(c in allowed_concepts for c in p.get("concepts", [])):
                    drift = w["drift_penalty"]
            generic_penalty = w["generic_penalty"] if len((p.get("title") or "").split()) < 4 else 0.0
            score = anchor_hits * w["anchor_bonus"] + source_bonus + cited_bonus - drift - generic_penalty
            p2 = dict(p)
            p2["score"] = round(score, 3)
            rows.append(p2)
        rows.sort(key=lambda x: x["score"], reverse=True)
        for i, r in enumerate(rows, 1):
            r["rank"] = i
        return rows

    def dedup_merge(self, batches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        acc: Dict[str, Dict[str, Any]] = {}
        for batch in batches:
            for p in batch:
                key = (p.get("doi") or p.get("openalex_id") or p.get("title") or "").lower()
                if not key:
                    continue
                if key in acc:
                    acc[key]["source_tags"] = set(acc[key].get("source_tags", set())) | set(p.get("source_tags", set()))
                    acc[key]["cited_by_count"] = max(acc[key].get("cited_by_count", 0), p.get("cited_by_count", 0))
                else:
                    acc[key] = p
        for p in acc.values():
            p["source_tags"] = sorted(list(p.get("source_tags", [])))
        return list(acc.values())

    def write_corpus(self, papers: List[Dict[str, Any]]) -> None:
        cols = ["rank", "score", "title", "year", "doi", "openalex_id", "venue", "authors_short", "cited_by_count", "source_tags", "url"]

        def write(path: Path, rows: List[Dict[str, Any]]):
            with path.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                for r in rows:
                    row = {c: r.get(c, "") for c in cols}
                    row["source_tags"] = ",".join(r.get("source_tags", [])) if isinstance(r.get("source_tags"), list) else r.get("source_tags", "")
                    w.writerow(row)

        write(self.out_dir / "corpus_all.csv", papers)
        write(self.out_dir / "corpus.csv", papers[:300])

    def write_prisma(self, stats: Dict[str, Any]) -> None:
        lines = [
            "# PRISMA-lite (Stage B)",
            f"- Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- Mode: {self.mode}",
            f"- Seed queries: {stats.get('seed_queries', 0)}",
            f"- Seed papers: {stats.get('seed_count', 0)}",
            f"- Expanded papers: {stats.get('expanded_count', 0)}",
            f"- S2 recommendations: {stats.get('s2_count', 0)}",
            f"- ResearchRabbit merged: {stats.get('rr_count', 0)}",
            f"- Deduplicated total: {stats.get('dedup_count', 0)}",
            f"- Final ranked: {stats.get('final_count', 0)}",
            "- Sources: OpenAlex Works API, Semantic Scholar Recommendations API, ResearchRabbit export/import bridge",
            "- Strategy: precision-first anchor packs with subject gating and drift penalties",
        ]
        write_text(self.out_dir / "prisma_lite_B.md", "\n".join(lines) + "\n")

    def write_summary(self, stats: Dict[str, Any], degraded: bool, note: str = "") -> None:
        lines = [
            "Stage B (Literature Scout) выполнен.",
            f"Режим: {self.mode}.",
            "1) Проверены входы идеи и синхронизация in/idea.txt -> idea.txt.",
            "2) Если был structured_idea.json из Stage A, использован как главный источник якорей.",
            "3) Извлечены якоря и собраны точные anchor packs (2-4 якоря в запросе).",
            "4) Аббревиатуры проверены по правилам безопасности против шума.",
            "5) Выполнен seed-поиск через OpenAlex с ограничением выдачи.",
            "6) Построен subject-gating по top concepts, чтобы снизить дрейф.",
            "7) Выполнено расширение через citation network OpenAlex.",
            "8) Выполнены рекомендации Semantic Scholar (если доступны DOI/сеть).",
            "9) Сформирован RR_IMPORT.ris для bridge в ResearchRabbit/Zotero.",
            "10) Если присутствовал RR_EXPORT.*, данные объединены обратно в корпус.",
            f"11) Всего после дедупликации: {stats.get('dedup_count', 0)} записей.",
            f"12) В corpus.csv записан top-300 (фактически {min(300, stats.get('final_count', 0))}).",
            "13) Подробный machine-log: out/search_log_B.json.",
            "14) Человекочитаемый лог: out/runB.log.",
            f"15) Лог модуля: {self.module_log_path.name} в ideas/.../logs.",
            "16) Для API-отладки включите PIPELINE_DEBUG=1 (http_trace_B.jsonl).",
            "17) Секреты из config/secrets.env читаются без вывода значений в лог.",
            "18) Следующий шаг: просмотреть top-20 и уточнить якоря/режим.",
            "19) Для строгого режима запустите FOCUSED; для разведки — WIDE.",
            "20) Повторный запуск перезаписывает только файлы Stage B.",
        ]
        if degraded:
            lines.append("21) Режим DEGRADED: сеть/API были недоступны, созданы безопасные выходные файлы-заглушки.")
        if note:
            lines.append(f"22) Примечание: {note}")
        write_text(self.out_dir / "stageB_summary.txt", "\n".join(lines) + "\n")

    def load_offline_fixture(self) -> Dict[str, Any]:
        if not self.offline_fixtures:
            return {}
        fp = self.offline_fixtures / "openalex_seed.json"
        if fp.exists():
            return json.loads(read_text(fp))
        return {}

    def run(self) -> int:
        self.log("Stage B started")
        self.log(f"Mode={self.mode}")
        self.log(f"Secrets: OPENALEX_API_KEY={'***' if self.secrets.get('OPENALEX_API_KEY') else '(missing)'}, OPENALEX_MAILTO={'***' if self.mailto else '(missing)'}")
        ok, idea_or_msg = self.ensure_idea_text()
        if not ok:
            self.log(idea_or_msg)
            self.write_corpus([])
            self.write_prisma({})
            self.write_summary({}, degraded=True, note=idea_or_msg)
            write_text(self.search_log_path, json.dumps(self.search_log, ensure_ascii=False, indent=2))
            return 0

        idea_text = idea_or_msg
        structured = self.load_structured()
        anchors = self.extract_anchors(idea_text, structured)
        packs = self.build_anchor_packs(anchors)
        seed_papers = []
        expanded = []
        s2 = []
        rr = []
        degraded = False

        try:
            if self.offline_fixtures:
                fx = self.load_offline_fixture()
                seed_papers = [self._paper_from_openalex(w, "seed") for w in fx.get("results", [])]
            else:
                for pack in packs[:8]:
                    results = self.openalex_search(pack)
                    seed_papers.extend([self._paper_from_openalex(w, "seed") for w in results])
            allowed = self.subject_gate(seed_papers)
            negatives = [p for p in seed_papers if allowed and not any(c in allowed for c in p.get("concepts", []))]
            if not self.offline_fixtures:
                expanded = self.expand_openalex(seed_papers)
                s2 = self.s2_recommend(seed_papers, negatives)
            self.write_rr_import(seed_papers)
            rr_items = self.parse_rr_export()
            if rr_items and not self.offline_fixtures:
                rr = self.enrich_rr(rr_items)
            merged = self.dedup_merge([seed_papers, expanded, s2, rr])
            ranked = self.score(merged, anchors, allowed)
        except Exception as e:
            degraded = True
            self.search_log["errors"].append(f"DEGRADED: {repr(e)}")
            self.search_log["errors"].append(traceback.format_exc())
            ranked = []

        stats = {
            "seed_queries": len(packs[:8]),
            "seed_count": len(seed_papers),
            "expanded_count": len(expanded),
            "s2_count": len(s2),
            "rr_count": len(rr),
            "dedup_count": len(ranked),
            "final_count": len(ranked),
        }
        self.search_log["stats"] = stats
        self.search_log["finished_at"] = datetime.utcnow().isoformat() + "Z"

        self.write_corpus(ranked)
        self.write_prisma(stats)
        self.write_summary(stats, degraded=degraded)
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
    runner = StageB(Path(args.idea), args.mode, offline)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
