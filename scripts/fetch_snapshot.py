#!/usr/bin/env python3
"""
fetch_snapshot.py — Build a "State of Wiki Loves Africa" snapshot for one year.

Walks the Commons category tree for a given WLA year, counts submissions per
country (recursively, so community-level subcategories are folded into their
parent country), then measures Wikimedia pageviews for a sample of files to
rank the most-viewed submissions.

Pageviews are cumulative from January of the contest year through TODAY —
not capped at the end of that year — so view counts keep growing every time
this is re-run, even for old contest years. Use refresh_views.py to cheaply
re-check views later without re-walking the whole category tree again.

Usage:
    python scripts/fetch_snapshot.py --year 2025
    python scripts/fetch_snapshot.py --year 2025 --sample-cap 40   # faster, for testing
    python scripts/fetch_snapshot.py --year 2025 --full-census     # slow, exact (no sampling)
    python scripts/fetch_snapshot.py --year 2025 --out-dir docs/data

Output:
    <out-dir>/<year>.json
"""

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
PAGEVIEWS_API = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "commons.wikimedia.org/all-access/all-agents/{title}/monthly/{start}/{end}"
)
USER_AGENT = "WikiAfroDemics-WLAReport/1.0 (https://github.com/muib211; muibshefiu@gmail.com)"

SPECIAL_BUCKET_PATTERNS = ["to check", "with unknown country", "without categories", "unidentified"]

HEADERS = {"User-Agent": USER_AGENT}


def clean_country_name(title: str, year: int) -> str:
    name = title.replace("Category:", "")
    for prefix in [
        f"Images from Wiki Loves Africa {year} in ",
        f"Images from Wiki Loves Africa {year} ",
    ]:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    return name.strip()


def is_special_bucket(title: str) -> bool:
    low = title.lower()
    return any(p in low for p in SPECIAL_BUCKET_PATTERNS)


class CommonsClient:
    def __init__(self, session: aiohttp.ClientSession, concurrency: int = 6):
        self.session = session
        self.sem = asyncio.Semaphore(concurrency)

    async def get_json(self, params: dict, base: str = COMMONS_API) -> dict:
        params = dict(params)
        params.setdefault("format", "json")
        async with self.sem:
            for attempt in range(4):
                try:
                    async with self.session.get(base, params=params, headers=HEADERS, timeout=30) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        await asyncio.sleep(1.5 * (attempt + 1))
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    await asyncio.sleep(1.5 * (attempt + 1))
        return {}

    async def category_members(self, cmtitle: str, cmtype: str) -> list:
        members, cont = [], None
        while True:
            params = {"action": "query", "list": "categorymembers", "cmtitle": cmtitle, "cmtype": cmtype, "cmlimit": "500"}
            if cont:
                params["cmcontinue"] = cont
            data = await self.get_json(params)
            members.extend(data.get("query", {}).get("categorymembers", []))
            cont = data.get("continue", {}).get("cmcontinue")
            if not cont:
                break
        return members

    async def walk_files_recursive(self, root_title: str, _seen_cats=None, _depth=0) -> set:
        if _seen_cats is None:
            _seen_cats = set()
        if root_title in _seen_cats or _depth > 6:
            return set()
        _seen_cats.add(root_title)

        files_task = self.category_members(root_title, "file")
        subcats_task = self.category_members(root_title, "subcat")
        files, subcats = await asyncio.gather(files_task, subcats_task)

        file_titles = {f["title"] for f in files}

        if subcats:
            sub_results = await asyncio.gather(
                *[self.walk_files_recursive(s["title"], _seen_cats, _depth + 1) for s in subcats]
            )
            for s in sub_results:
                file_titles |= s

        return file_titles

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

    async def pageviews(self, file_title: str, year: int) -> int:
        """Cumulative views from Jan 1 of `year` through today — grows on every re-check."""
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


async def build_snapshot(year: int, sample_cap, full_census: bool, out_dir: Path):
    root = f"Category:Images from Wiki Loves Africa {year}"
    started = time.time()

    async with aiohttp.ClientSession() as session:
        client = CommonsClient(session)

        print(f"[{year}] discovering subcategories of {root}...", file=sys.stderr)
        top_subcats = await client.category_members(root, "subcat")
        if not top_subcats:
            print(f"[{year}] no subcategories found — category may not exist for this year.", file=sys.stderr)
            return None

        country_branches = [s for s in top_subcats if not is_special_bucket(s["title"])]
        special_branches = [s for s in top_subcats if is_special_bucket(s["title"])]

        print(f"[{year}] {len(country_branches)} country branches, {len(special_branches)} special buckets. Walking file trees...", file=sys.stderr)

        country_files: dict = {}
        for branch in country_branches:
            name = clean_country_name(branch["title"], year)
            files = await client.walk_files_recursive(branch["title"])
            country_files[name] = country_files.get(name, set()) | files
            print(f"  {name}: {len(files)} files", file=sys.stderr)

        pending_files = set()
        for branch in special_branches:
            pending_files |= await client.walk_files_recursive(branch["title"])

        total = sum(len(v) for v in country_files.values())
        countries_sorted = sorted(country_files.items(), key=lambda kv: len(kv[1]), reverse=True)

        pool = []
        for name, files in countries_sorted:
            files_list = sorted(files)
            take = files_list if full_census else files_list[: (sample_cap or 20)]
            for f in take:
                pool.append((f, name))

        print(f"[{year}] measuring pageviews for {len(pool)} files ({'full census' if full_census else f'sampled, cap={sample_cap}'})...", file=sys.stderr)

        results = []
        CHUNK = 200
        for i in range(0, len(pool), CHUNK):
            chunk = pool[i:i + CHUNK]
            views = await asyncio.gather(*[client.pageviews(title, year) for title, _ in chunk])
            for (title, country), v in zip(chunk, views):
                results.append({"title": title, "country": country, "views": v})
            print(f"  ...{min(i + CHUNK, len(pool))}/{len(pool)}", file=sys.stderr)

        ranked = sorted(results, key=lambda r: r["views"], reverse=True)
        top_ranked = ranked[:20]
        thumbs = await client.thumbnails_bulk([r["title"] for r in top_ranked])
        for r in top_ranked:
            r["thumb"] = thumbs.get(r["title"], "")

        sample_total_views = sum(r["views"] for r in results)

        snapshot = {
            "year": year,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_submissions": total,
            "sample_method": "full_census" if full_census else "sampled",
            "sample_size": len(pool),
            "countries": [{"name": name, "count": len(files)} for name, files in countries_sorted],
            "pending_uncategorized": len(pending_files),
            "top_viewed": top_ranked,
            "sample_total_views": sample_total_views,
            # Full pool kept so refresh_views.py can re-check views later without re-crawling categories
            "sample_pool": [{"title": r["title"], "country": r["country"]} for r in results],
            "elapsed_seconds": round(time.time() - started, 1),
        }

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{year}.json"
        out_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[{year}] wrote {out_path} ({total} submissions, {len(countries_sorted)} countries, {sample_total_views} sample views)", file=sys.stderr)
        return snapshot


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--sample-cap", type=int, default=25, help="Files per country to sample for pageviews (ignored with --full-census)")
    ap.add_argument("--full-census", action="store_true", help="Fetch pageviews for every file, not a sample (slow for big years)")
    ap.add_argument("--out-dir", type=Path, default=Path("data"))
    args = ap.parse_args()

    asyncio.run(build_snapshot(args.year, args.sample_cap, args.full_census, args.out_dir))


if __name__ == "__main__":
    main()
