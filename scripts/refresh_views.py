#!/usr/bin/env python3
"""
refresh_views.py — Re-check pageviews for an existing snapshot, cheaply.

Unlike fetch_snapshot.py, this does NOT re-crawl the Commons category tree
(that part is done once and doesn't change). It reads the file list already
stored in a snapshot's "sample_pool", re-checks cumulative pageviews for
those same files (Jan of the contest year through today), and rewrites the
"top_viewed" / "sample_total_views" fields — leaving "countries" and
"total_submissions" untouched.

This is what lets past years' "most viewed" numbers keep growing every
month, instead of freezing the moment the contest year ends.

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

    async def pageviews(self, file_title: str, year: int) -> int:
        encoded = file_title.replace(" ", "_")
        end = datetime.now(timezone.utc).strftime("%Y%m%d00")
        url = PAGEVIEWS_API.format(title=encoded, start=f"{year}010100", end=end)
        async with self.sem:
            for attempt in range(3):
                try:
                    async with self.session.get(url, headers=HEADERS, timeout=20) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return sum(item.get("views", 0) for item in data.get("items", []))
                        if resp.status == 404:
                            return 0
                        await asyncio.sleep(1.0 * (attempt + 1))
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    await asyncio.sleep(1.0 * (attempt + 1))
        return 0

    async def thumbnails_bulk(self, titles: list, width: int = 400) -> dict:
        results = {}
        for i in range(0, len(titles), 50):
            batch = titles[i:i + 50]
            params = {"action": "query", "prop": "imageinfo", "iiprop": "url", "iiurlwidth": str(width), "titles": "|".join(batch), "format": "json"}
            async with self.sem:
                async with self.session.get(COMMONS_API, params=params, headers=HEADERS, timeout=30) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
            for page in data.get("query", {}).get("pages", {}).values():
                info = (page.get("imageinfo") or [{}])[0]
                url = info.get("thumburl") or info.get("url")
                if url:
                    results[page["title"]] = url
        return results


async def refresh_one(path: Path, client: Client):
    snap = json.loads(path.read_text())
    year = snap["year"]
    pool = snap.get("sample_pool")
    if not pool:
        print(f"[{year}] no sample_pool stored in {path.name} (older snapshot format) — skipping. Re-run fetch_snapshot.py for this year to enable refreshes.", file=sys.stderr)
        return

    print(f"[{year}] refreshing views for {len(pool)} files...", file=sys.stderr)
    results = []
    CHUNK = 200
    for i in range(0, len(pool), CHUNK):
        chunk = pool[i:i + CHUNK]
        views = await asyncio.gather(*[client.pageviews(item["title"], year) for item in chunk])
        for item, v in zip(chunk, views):
            results.append({"title": item["title"], "country": item["country"], "views": v})

    ranked = sorted(results, key=lambda r: r["views"], reverse=True)
    top_ranked = ranked[:20]
    thumbs = await client.thumbnails_bulk([r["title"] for r in top_ranked])
    for r in top_ranked:
        r["thumb"] = thumbs.get(r["title"], "")

    snap["top_viewed"] = top_ranked
    snap["sample_total_views"] = sum(r["views"] for r in results)
    snap["views_refreshed_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(snap, indent=2, ensure_ascii=False))
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
    group.add_argument("--year", type=int, help="Refresh a single year")
    group.add_argument("--all", action="store_true", help="Refresh every snapshot found in --data-dir")
    ap.add_argument("--skip-year", type=int, help="With --all, skip this year (e.g. because fetch_snapshot.py already refreshed it in the same run)")
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
