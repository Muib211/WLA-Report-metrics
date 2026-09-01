#!/usr/bin/env python3
"""
refresh_views.py — Re-check views for an existing snapshot, cheaply.

Does NOT re-crawl the Commons category tree (that part doesn't change). Reads
the file list already stored in a snapshot's "sample_pool", re-checks both
Commons-page views and real reuse views on other Wikimedia projects for those
same files, and rewrites "top_viewed" / "sample_total_views" — leaving
"countries" and "total_submissions" untouched.

Usage:
    python scripts/refresh_views.py --year 2024 --data-dir docs/data
    python scripts/refresh_views.py --all --data-dir docs/data
    python scripts/refresh_views.py --all --skip-year 2025 --data-dir docs/data
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

PAGEVIEWS_API = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "commons.wikimedia.org/all-access/all-agents/{title}/monthly/{start}/{end}"
)
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "WikiAfroDemics-WLAReport/1.0 (https://github.com/muib211; muibshefiu@gmail.com)"
HEADERS = {"User-Agent": USER_AGENT}


class Client:
    def __init__(self, session, concurrency=6):
        self.session = session
        self.sem = asyncio.Semaphore(concurrency)

    async def get_json(self, params):
        params = dict(params)
        params.setdefault("format", "json")
        async with self.sem:
            try:
                async with self.session.get(COMMONS_API, params=params, headers=HEADERS, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
        return {}

    async def pageviews(self, file_title: str, year: int) -> int:
        encoded = file_title.replace(" ", "_")
        end = datetime.now(timezone.utc).strftime("%Y%m%d00")
        url = PAGEVIEWS_API.format(title=encoded, start=f"{year}010100", end=end)
        async with self.sem:
            try:
                async with self.session.get(url, headers=HEADERS, timeout=20) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return sum(item.get("views", 0) for item in data.get("items", []))
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
        return 0

    def dbname_to_domain(self, wiki_code):
        if not wiki_code:
            return None
        # Some responses already hand back a full domain (e.g. "sw.wikipedia.org")
        # instead of a short dbname (e.g. "swwiki") — use it directly if so.
        if "." in wiki_code and " " not in wiki_code:
            return wiki_code
        special = {
            "commonswiki": "commons.wikimedia.org", "wikidatawiki": "www.wikidata.org",
            "metawiki": "meta.wikimedia.org", "specieswiki": "species.wikimedia.org",
            "incubatorwiki": "incubator.wikimedia.org", "mediawikiwiki": "www.mediawiki.org",
            "foundationwiki": "foundation.wikimedia.org",
        }
        if wiki_code in special:
            return special[wiki_code]
        suffixes = [
            ("wiktionary", "wiktionary.org"), ("wikibooks", "wikibooks.org"), ("wikinews", "wikinews.org"),
            ("wikiquote", "wikiquote.org"), ("wikisource", "wikisource.org"), ("wikiversity", "wikiversity.org"),
            ("wikivoyage", "wikivoyage.org"), ("wiki", "wikipedia.org"),
        ]
        for suffix, domain in suffixes:
            if wiki_code and wiki_code.endswith(suffix):
                return f"{wiki_code[:-len(suffix)]}.{domain}"
        return None

    async def pageviews_on_page(self, wiki_code, page_title, year):
        domain = self.dbname_to_domain(wiki_code)
        if not domain:
            return 0
        encoded = (page_title or "").replace(" ", "_")
        end = datetime.now(timezone.utc).strftime("%Y%m%d00")
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{domain}/all-access/all-agents/{encoded}/monthly/{year}010100/{end}"
        async with self.sem:
            try:
                async with self.session.get(url, headers=HEADERS, timeout=20) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return sum(item.get("views", 0) for item in data.get("items", []))
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
        return 0

    async def globalusage_detailed_bulk(self, titles: list) -> dict:
        async def fetch_batch(batch):
            data = await self.get_json({"action": "query", "prop": "globalusage", "titles": "|".join(batch), "gulimit": "500", "gunamespace": "0"})
            out = {}
            for page in data.get("query", {}).get("pages", {}).values():
                entries = [{"wiki": g.get("wiki"), "title": g.get("title")} for g in page.get("globalusage", []) if g.get("wiki")]
                if entries:
                    out[page["title"]] = entries
            return out
        batches = [titles[i:i + 50] for i in range(0, len(titles), 50)]
        merged = {}
        for r in await asyncio.gather(*[fetch_batch(b) for b in batches]):
            merged.update(r)
        return merged

    async def thumbnails_bulk(self, titles: list, width: int = 400) -> dict:
        results = {}
        for i in range(0, len(titles), 50):
            batch = titles[i:i + 50]
            data = await self.get_json({"action": "query", "prop": "imageinfo", "iiprop": "url", "iiurlwidth": str(width), "titles": "|".join(batch)})
            for page in data.get("query", {}).get("pages", {}).values():
                info = (page.get("imageinfo") or [{}])[0]
                url = info.get("thumburl") or info.get("url")
                if url:
                    results[page["title"]] = url
        return results


async def refresh_one(path: Path, client: Client):
    snap = json.loads(path.read_text(encoding="utf-8"))
    year = snap["year"]
    pool = snap.get("sample_pool")
    if not pool:
        print(f"[{year}] no sample_pool stored in {path.name} — skipping. Re-run fetch_snapshot.py for this year to enable refreshes.", file=sys.stderr)
        return

    print(f"[{year}] refreshing Commons views for {len(pool)} files...", file=sys.stderr)
    titles = [item["title"] for item in pool]
    commons_views = await asyncio.gather(*[client.pageviews(t, year) for t in titles])

    print(f"[{year}] refreshing reuse views across other Wikimedia projects...", file=sys.stderr)
    usage_map = await client.globalusage_detailed_bulk(titles)

    reuse_pairs = []
    results = []
    for item, cv in zip(pool, commons_views):
        r = {"title": item["title"], "country": item["country"], "commons_views": cv}
        results.append(r)
        for e in usage_map.get(item["title"], []):
            reuse_pairs.append((r, e))

    reuse_view_counts = await asyncio.gather(*[client.pageviews_on_page(e["wiki"], e["title"], year) for _, e in reuse_pairs])
    for (r, _), v in zip(reuse_pairs, reuse_view_counts):
        r["reuse_views"] = r.get("reuse_views", 0) + v

    seen_titles = set()
    deduped = []
    for r in results:
        r["reuse_views"] = r.get("reuse_views", 0)
        r["total_views"] = r["commons_views"] + r["reuse_views"]
        if r["title"] not in seen_titles:
            seen_titles.add(r["title"])
            deduped.append(r)

    ranked = sorted(deduped, key=lambda r: r["total_views"], reverse=True)
    top_ranked = ranked[:20]

    useful_pool = [r for r in deduped if r.get("reuse_views", 0) > 0]
    ranked_useful = sorted(useful_pool, key=lambda r: r["total_views"], reverse=True)
    top_useful = ranked_useful[:20]

    thumb_titles = list({r["title"] for r in top_ranked} | {r["title"] for r in top_useful})
    thumbs = await client.thumbnails_bulk(thumb_titles)
    for r in top_ranked:
        r["thumb"] = thumbs.get(r["title"], "")
    for r in top_useful:
        r["thumb"] = thumbs.get(r["title"], "")

    snap["top_viewed"] = top_ranked
    snap["top_viewed_useful"] = top_useful
    snap["sample_total_views"] = sum(r["total_views"] for r in deduped)
    snap["views_refreshed_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(snap, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[{year}] updated — sample_total_views now {snap['sample_total_views']}", file=sys.stderr)


async def main_async(args):
    data_dir = args.data_dir
    if args.all:
        paths = sorted(data_dir.glob("[0-9][0-9][0-9][0-9].json"))
        if args.skip_year:
            paths = [p for p in paths if p.stem != str(args.skip_year)]
    else:
        paths = [data_dir / f"{args.year}.json"]

    async with aiohttp.ClientSession() as session:
        client = Client(session)
        for p in paths:
            if not p.exists():
                print(f"skipping {p} — not found", file=sys.stderr)
                continue
            await refresh_one(p, client)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int)
    group.add_argument("--all", action="store_true")
    ap.add_argument("--skip-year", type=int)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
