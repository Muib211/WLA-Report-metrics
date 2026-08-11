#!/usr/bin/env python3
"""
build_trends.py — Aggregate every data/<year>.json snapshot into data/trends.json.

Run this after fetch_snapshot.py / refresh_views.py have produced or updated
one or more yearly files.

Usage:
    python scripts/build_trends.py
    python scripts/build_trends.py --data-dir docs/data
"""

import argparse
import json
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args()

    years = []
    all_contributors = set()
    grand_wiki_counts = {}
    grand_total_usage_entries = 0

    for path in sorted(args.data_dir.glob("[0-9][0-9][0-9][0-9].json")):
        snap = json.loads(path.read_text(encoding="utf-8"))
        top_country = snap["countries"][0]["name"] if snap.get("countries") else None
        usage = snap.get("usage", {})

        years.append({
            "year": snap["year"],
            "total_submissions": snap["total_submissions"],
            "countries_count": len(snap.get("countries", [])),
            "top_country": top_country,
            "top_country_count": snap["countries"][0]["count"] if snap.get("countries") else 0,
            "sample_method": snap.get("sample_method"),
            "sample_total_views": snap.get("sample_total_views", 0),
            "top_viewed_max_views": snap["top_viewed"][0]["views"] if snap.get("top_viewed") else 0,
            "contributors_count": snap.get("contributors_count", 0),
            "usage_total": usage.get("total_usage_entries", 0),
            "files_used_percentage": usage.get("files_used_percentage", 0),
            "generated_at": snap.get("generated_at"),
            "views_refreshed_at": snap.get("views_refreshed_at"),
        })

        # True dedup: same person may contribute in multiple years, so union
        # their usernames rather than summing per-year counts.
        all_contributors.update(snap.get("contributors", []))

        # Usage entries belong to files that belong to exactly one year each,
        # so summing per-wiki counts across years is safe (no double count).
        for entry in usage.get("by_wiki", []):
            grand_wiki_counts[entry["wiki"]] = grand_wiki_counts.get(entry["wiki"], 0) + entry["count"]
            grand_total_usage_entries += entry["count"]

    years.sort(key=lambda y: y["year"])
    grand_total_sample_views = sum(y["sample_total_views"] for y in years)

    grand_usage_by_wiki = [
        {"wiki": w, "count": c, "percentage": round(c / grand_total_usage_entries * 100, 2) if grand_total_usage_entries else 0}
        for w, c in sorted(grand_wiki_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    out = {
        "years": years,
        "grand_total_sample_views": grand_total_sample_views,
        "grand_total_unique_contributors": len(all_contributors),
        "grand_total_usage_entries": grand_total_usage_entries,
        "grand_usage_by_wiki": grand_usage_by_wiki,
        "note": (
            "sample_total_views / grand_total_sample_views are sums over sampled (or, for full-census "
            "years, all) files - check each year's sample_method. contributors_count and "
            "grand_total_unique_contributors are exact (every sorted file's uploader is checked, not "
            "sampled). Usage figures come from Commons' GlobalUsage data - exact, not estimated - but "
            "only cover files that have already been sorted into a country category."
        ),
    }
    out_path = args.data_dir / "trends.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out_path} with {len(years)} year(s), {len(all_contributors)} unique contributors, {grand_total_usage_entries} total usages")


if __name__ == "__main__":
    main()
