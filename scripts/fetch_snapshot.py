#!/usr/bin/env python3
"""
fetch_snapshot.py — Build a "State of Wiki Loves Africa" snapshot for one year.

Walks the Commons category tree for a given WLA year, counts real (deduplicated)
submissions per country, measures cumulative pageviews (Commons + real reuse on
other Wikimedia projects), fetches contributor usernames and account-age info,
and pulls real jury-selected winners with license and usage data.

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
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
PAGEVIEWS_API = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "commons.wikimedia.org/all-access/all-agents/{title}/monthly/{start}/{end}"
)
USER_AGENT = "WikiAfroDemics-WLAReport/1.0 (https://github.com/muib211; muibshefiu@gmail.com)"

SPECIAL_BUCKET_PATTERNS = ["to check", "with unknown country", "without categories", "unidentified"]
IGNORED_INDEX_PATTERNS = ["by country", "by theme", "by year", "by type"]

HEADERS = {"User-Agent": USER_AGENT}

# A handful of demonym-style branch names Commons uses that a plain word-match
# won't catch (e.g. "Nigerian" isn't literally "Nigeria"). Add more here only
# for this kind of adjective-form naming — everything else (like "... for
# Creatives in Nigeria") is caught automatically by KNOWN_COUNTRIES below.
MANUAL_ALIASES = {
    "Nigerian Communities": "Nigeria",
}

# Every country/territory WLA has seen entries for. Any branch whose cleaned
# name isn't an exact match gets checked against this list (longest name
# first, whole-word match) and auto-merged into whichever real country it
# refers to — so a stray branch merges correctly every year, with no manual
# alias needed.
KNOWN_COUNTRIES = [
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cameroon",
    "Cape Verde", "the Central African Republic", "Central African Republic", "Chad",
    "the Comoros", "Comoros", "the Democratic Republic of the Congo",
    "Democratic Republic of the Congo", "Djibouti", "Egypt", "Equatorial Guinea",
    "Eritrea", "Eswatini", "Ethiopia", "Gabon", "the Gambia", "Gambia", "Ghana",
    "Guinea-Bissau", "Guinea", "Ivory Coast", "Cote d'Ivoire", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
    "Mayotte", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria",
    "the Republic of the Congo", "Republic of the Congo", "Reunion", "Réunion",
    "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone",
    "Somalia", "South Africa", "South Sudan", "Sudan", "Tanzania", "Togo",
    "Tunisia", "Uganda", "Western Sahara", "Zambia", "Zimbabwe", "Haiti",
]
KNOWN_COUNTRIES.sort(key=len, reverse=True)


def canonical_country_name(label: str) -> str:
    """Map any cleaned branch name to the real country it refers to, so
    variant branches for the same country always merge together."""
    if label in MANUAL_ALIASES:
        return MANUAL_ALIASES[label]
    for country in KNOWN_COUNTRIES:
        if re.search(r"\b" + re.escape(country) + r"\b", label, re.IGNORECASE):
            return country
    return label  # unrecognized — kept as its own entry rather than silently dropped


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


def parse_mw_timestamp(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return None


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
                            data = await resp.json()
                            if "error" in data:
                                # MediaWiki often returns HTTP 200 even for a bad request —
                                # the real error is buried in the JSON body. Surface it instead
                                # of silently returning nothing, so a broken query is visible.
                                print(f"  API error for params {params}: {data['error']}", file=sys.stderr)
                                return {}
                            return data
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

    async def uploaders_bulk(self, titles: list) -> dict:
        """Who uploaded each file and when — batched. Timestamp is used to estimate campaign start.

        Note: this reflects the CURRENT file revision's uploader, which can differ from the
        original submitter if a file was later re-uploaded by someone else. An attempt to match
        the project's rev_parent_id=0 (first-revision) definition via prop=revisions failed in
        testing (returned 0 contributors — likely an invalid parameter combination when batching
        multiple titles with rvlimit), so it's reverted here pending a safer, isolated retest."""
        async def fetch_batch(batch):
            data = await self.get_json({"action": "query", "prop": "imageinfo", "iiprop": "user|timestamp", "titles": "|".join(batch)})
            out = {}
            for page in data.get("query", {}).get("pages", {}).values():
                info = page.get("imageinfo") or [{}]
                if info and info[0].get("user"):
                    out[page["title"]] = {"user": info[0]["user"], "timestamp": info[0].get("timestamp")}
            return out
        batches = [titles[i:i + 50] for i in range(0, len(titles), 50)]
        merged = {}
        for r in await asyncio.gather(*[fetch_batch(b) for b in batches]):
            merged.update(r)
        return merged

    async def users_registration_bulk(self, usernames: list) -> dict:
        """When each contributor's account was created — used to flag accounts opened shortly before the campaign."""
        async def fetch_batch(batch):
            data = await self.get_json({"action": "query", "list": "users", "ususers": "|".join(batch), "usprop": "registration"})
            out = {}
            for u in data.get("query", {}).get("users", []):
                if u.get("name"):
                    out[u["name"]] = u.get("registration")
            return out
        batches = [usernames[i:i + 50] for i in range(0, len(usernames), 50)]
        merged = {}
        for r in await asyncio.gather(*[fetch_batch(b) for b in batches]):
            merged.update(r)
        return merged

    async def license_bulk(self, titles: list) -> dict:
        """Each file's license, straight from Commons' own metadata."""
        async def fetch_batch(batch):
            data = await self.get_json({
                "action": "query", "prop": "imageinfo", "iiprop": "extmetadata",
                "iiextmetadatafilter": "LicenseShortName", "titles": "|".join(batch),
            })
            out = {}
            for page in data.get("query", {}).get("pages", {}).values():
                info = page.get("imageinfo") or [{}]
                if info:
                    meta = (info[0].get("extmetadata") or {}).get("LicenseShortName", {})
                    if meta.get("value"):
                        out[page["title"]] = meta["value"]
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

    async def globalusage_bulk(self, titles: list) -> dict:
        async def fetch_batch(batch):
            data = await self.get_json({"action": "query", "prop": "globalusage", "titles": "|".join(batch), "gulimit": "500", "gunamespace": "0"})
            out = {}
            for page in data.get("query", {}).get("pages", {}).values():
                wikis = [g.get("wiki") for g in page.get("globalusage", []) if g.get("wiki")]
                if wikis:
                    out[page["title"]] = wikis
            return out
        batches = [titles[i:i + 50] for i in range(0, len(titles), 50)]
        merged = {}
        for r in await asyncio.gather(*[fetch_batch(b) for b in batches]):
            merged.update(r)
        return merged

    async def globalusage_detailed_bulk(self, titles: list) -> dict:
        """Same as globalusage_bulk but keeps each usage's specific page title, so it can be linked directly."""
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

    def dbname_to_domain(self, wiki_code):
        """Converts any wiki dbname (e.g. 'hawiki') to its real domain — works for any language, not just English."""
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
        """Real views of a specific page on a specific wiki — measures actual reuse, not just Commons visits."""
        domain = self.dbname_to_domain(wiki_code)
        if not domain:
            print(f"  warning: unrecognized wiki code '{wiki_code}' — reuse views for this page couldn't be counted", file=sys.stderr)
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

    async def pageviews(self, file_title: str, year: int) -> int:
        """Cumulative Commons-page views from Jan 1 of `year` through today."""
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

        # Cross-check figure, matching the exact definition used in this project's own
        # verified Quarry queries (cl_to = root category, page_namespace = 6, direct
        # membership only — not the recursive per-country walk this script otherwise
        # uses). Kept as a separate field rather than replacing the country-based total,
        # since only the country walk carries per-country attribution.
        root_direct_files = await client.category_members(root, "file")
        root_direct_file_count = len(root_direct_files)
        print(f"[{year}] {root_direct_file_count} files directly in the flat root category (cross-check figure)", file=sys.stderr)

        before_count = len(top_subcats)
        top_subcats = [s for s in top_subcats if not any(p in s["title"].lower() for p in IGNORED_INDEX_PATTERNS)]
        if len(top_subcats) < before_count:
            print(f"[{year}] ignored {before_count - len(top_subcats)} navigation/index categor(ies), e.g. '... by country'", file=sys.stderr)

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

        # Auto-merge variant branch names into the real country they refer to.
        merged_country_files: dict = {}
        for name, files in country_files.items():
            canonical = canonical_country_name(name)
            if canonical != name:
                print(f"  merged '{name}' into '{canonical}'", file=sys.stderr)
            merged_country_files[canonical] = merged_country_files.get(canonical, set()) | files
        country_files = merged_country_files

        all_unique_files = set().union(*country_files.values()) if country_files else set()
        overlap_with_pending = all_unique_files & pending_files
        if overlap_with_pending:
            print(f"[{year}] {len(overlap_with_pending)} files are tagged with a country AND still in the pending bucket (real unsorted count is smaller)", file=sys.stderr)
        total = len(all_unique_files)

        countries_sorted = sorted(
            ((name, files) for name, files in country_files.items() if len(files) > 0),
            key=lambda kv: len(kv[1]), reverse=True
        )

        print(f"[{year}] fetching contributor usernames and upload timestamps for {len(all_unique_files)} files...", file=sys.stderr)
        upload_info = await client.uploaders_bulk(sorted(all_unique_files))
        contributors = sorted({v["user"] for v in upload_info.values() if v.get("user")})

        # Estimate campaign start AND end from real upload timestamps (skip a few
        # of the very earliest/latest in case of stray outlier uploads), so this
        # works automatically for any year without hardcoded dates.
        timestamps = sorted(t for t in (parse_mw_timestamp(v.get("timestamp")) for v in upload_info.values()) if t)
        campaign_start = None
        campaign_end = None
        new_account_count = 0
        new_account_percentage = 0
        if timestamps:
            skip = min(5, len(timestamps) - 1)
            campaign_start = timestamps[skip]
            campaign_end = timestamps[-(skip + 1)]
            if campaign_end < campaign_start:
                campaign_end = campaign_start
            cutoff_start = campaign_start - timedelta(days=30)
            print(f"[{year}] estimated campaign window: {campaign_start.date()} to {campaign_end.date()} (from upload data)", file=sys.stderr)

            print(f"[{year}] checking account registration dates for {len(contributors)} contributors...", file=sys.stderr)
            registrations = await client.users_registration_bulk(contributors)
            flagged = []
            for user, reg in registrations.items():
                reg_dt = parse_mw_timestamp(reg)
                if reg_dt and cutoff_start <= reg_dt <= campaign_end:
                    flagged.append(user)
            new_account_count = len(flagged)
            new_account_percentage = round(new_account_count / len(contributors) * 100, 2) if contributors else 0
            print(f"[{year}] {new_account_count} contributors ({new_account_percentage}%) opened their account within ~1 month before or during the campaign", file=sys.stderr)

        print(f"[{year}] fetching cross-wiki usage for {len(all_unique_files)} files...", file=sys.stderr)
        usage_map_detailed = await client.globalusage_detailed_bulk(sorted(all_unique_files))
        wiki_counts: dict = {}
        total_usage_entries = 0
        files_used_count = 0
        for title, entries in usage_map_detailed.items():
            if entries:
                files_used_count += 1
                for e in entries:
                    wiki_counts[e["wiki"]] = wiki_counts.get(e["wiki"], 0) + 1
                    total_usage_entries += 1
        usage_by_wiki = [
            {"wiki": w, "count": c, "percentage": round(c / total_usage_entries * 100, 2) if total_usage_entries else 0}
            for w, c in sorted(wiki_counts.items(), key=lambda kv: kv[1], reverse=True)
        ]
        files_used_percentage = round(files_used_count / total * 100, 2) if total else 0
        print(f"[{year}] {len(contributors)} contributors, {files_used_count} files used across {len(usage_by_wiki)} wikis ({total_usage_entries} total usages)", file=sys.stderr)

        # Real jury-selected winners, from Commons' own winners category.
        winners_category_candidates = [
            f"Category:Wiki Loves Africa {year} winning pictures",
            f"Category:Wiki Loves Africa {year} Winners",
            f"Category:Wiki Loves Africa {year} winners",
        ]
        winner_titles = []
        winners_category_used = None
        for cat in winners_category_candidates:
            members = await client.category_members(cat, "file")
            if members:
                winner_titles = [m["title"] for m in members]
                winners_category_used = cat
                break

        file_to_country = {}
        for name, files in country_files.items():
            for f in files:
                file_to_country[f] = name

        winners = []
        if winner_titles:
            print(f"[{year}] found {len(winner_titles)} winning pictures in {winners_category_used}, fetching details...", file=sys.stderr)
            w_uploaders = await client.uploaders_bulk(winner_titles)
            w_usage = await client.globalusage_detailed_bulk(winner_titles)
            w_thumbs = await client.thumbnails_bulk(winner_titles, width=500)
            w_licenses = await client.license_bulk(winner_titles)
            for t in winner_titles:
                usage_entries = w_usage.get(t, [])
                core_usage = [e for e in usage_entries if e.get("wiki") != "metawiki"]
                winners.append({
                    "title": t,
                    "uploader": (w_uploaders.get(t) or {}).get("user"),
                    "country": file_to_country.get(t),
                    "year": year,
                    "license": w_licenses.get(t, "Unknown"),
                    "thumb": w_thumbs.get(t, ""),
                    "usage": usage_entries,
                    "core_usage_count": len(core_usage),
                })
        else:
            print(f"[{year}] no winners category found under any known naming pattern — skipping winners.", file=sys.stderr)

        # Build the ranking pool for "top viewed" — full census means every file.
        pool = []
        for name, files in countries_sorted:
            files_list = sorted(files)
            take = files_list if full_census else files_list[: (sample_cap or 20)]
            for f in take:
                pool.append((f, name))

        print(f"[{year}] measuring Commons-page views for {len(pool)} files ({'full census' if full_census else f'sampled, cap={sample_cap}'})...", file=sys.stderr)

        results = []
        CHUNK = 200
        for i in range(0, len(pool), CHUNK):
            chunk = pool[i:i + CHUNK]
            views = await asyncio.gather(*[client.pageviews(title, year) for title, _ in chunk])
            for (title, country), v in zip(chunk, views):
                results.append({"title": title, "country": country, "views": v})
            print(f"  ...{min(i + CHUNK, len(pool))}/{len(pool)}", file=sys.stderr)

        seen_titles = set()
        deduped_results = []
        for r in results:
            if r["title"] not in seen_titles:
                seen_titles.add(r["title"])
                deduped_results.append(r)
        if len(deduped_results) < len(results):
            print(f"[{year}] removed {len(results) - len(deduped_results)} duplicate file(s) before ranking", file=sys.stderr)

        reuse_pairs = []
        for r in deduped_results:
            for e in usage_map_detailed.get(r["title"], []):
                reuse_pairs.append((r, e))
        print(f"[{year}] measuring real reuse views across {len(reuse_pairs)} usage(s) on other Wikimedia pages...", file=sys.stderr)
        reuse_view_counts = await asyncio.gather(*[client.pageviews_on_page(e["wiki"], e["title"], year) for _, e in reuse_pairs])
        for (r, _), v in zip(reuse_pairs, reuse_view_counts):
            r["reuse_views"] = r.get("reuse_views", 0) + v

        for r in deduped_results:
            r["commons_views"] = r["views"]
            r["reuse_views"] = r.get("reuse_views", 0)
            r["total_views"] = r["commons_views"] + r["reuse_views"]

        ranked = sorted(deduped_results, key=lambda r: r["total_views"], reverse=True)
        top_ranked = ranked[:20]

        # A genuinely independent ranking, not a filter of the "Most Seen" list —
        # only files with real reuse are even in the running, so a highly-viewed
        # but never-reused file can't crowd out a less-viewed but genuinely used one.
        useful_pool = [r for r in deduped_results if r.get("reuse_views", 0) > 0]
        ranked_useful = sorted(useful_pool, key=lambda r: r["total_views"], reverse=True)
        top_useful = ranked_useful[:20]

        thumb_titles = list({r["title"] for r in top_ranked} | {r["title"] for r in top_useful})
        thumbs = await client.thumbnails_bulk(thumb_titles)
        for r in top_ranked:
            r["thumb"] = thumbs.get(r["title"], "")
        for r in top_useful:
            r["thumb"] = thumbs.get(r["title"], "")
        sample_total_views = sum(r["total_views"] for r in deduped_results)

        snapshot = {
            "year": year,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_submissions": total,
            "root_category_direct_file_count": root_direct_file_count,
            "sample_method": "full_census" if full_census else "sampled",
            "sample_size": len(pool),
            "countries": [{"name": name, "count": len(files)} for name, files in countries_sorted],
            "pending_uncategorized": len(pending_files - all_unique_files),
            "top_viewed": top_ranked,
            "top_viewed_useful": top_useful,
            "sample_total_views": sample_total_views,
            "contributors_count": len(contributors),
            "contributors": contributors,
            "campaign_start_estimate": campaign_start.isoformat() if campaign_start else None,
            "new_account_before_campaign_count": new_account_count,
            "new_account_before_campaign_percentage": new_account_percentage,
            "usage": {
                "total_usage_entries": total_usage_entries,
                "files_used_count": files_used_count,
                "files_used_percentage": files_used_percentage,
                "by_wiki": usage_by_wiki,
            },
            "winners": winners,
            "winners_source_category": winners_category_used,
            "sample_pool": [{"title": r["title"], "country": r["country"]} for r in results],
            "elapsed_seconds": round(time.time() - started, 1),
        }

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{year}.json"
        out_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[{year}] wrote {out_path} ({total} submissions, {len(countries_sorted)} participating countries, {len(contributors)} contributors, {len(winners)} winners, {sample_total_views} total views)", file=sys.stderr)
        return snapshot


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--sample-cap", type=int, default=25)
    ap.add_argument("--full-census", action="store_true")
    ap.add_argument("--out-dir", type=Path, default=Path("data"))
    args = ap.parse_args()

    asyncio.run(build_snapshot(args.year, args.sample_cap, args.full_census, args.out_dir))


if __name__ == "__main__":
    main()