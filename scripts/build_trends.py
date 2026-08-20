#!/usr/bin/env python3
"""
build_trends.py — Aggregate every data/<year>.json snapshot into data/trends.json.

Adds cross-year analysis that individual year snapshots can't compute alone:
cumulative country/usage totals, contributor retention (how many years each
contributor has been active), and new-vs-returning contributor counts per year.

Usage:
    python scripts/build_trends.py
    python scripts/build_trends.py --data-dir docs/data
"""

import argparse
import json
from collections import Counter
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args()

    snapshots = []
    for path in sorted(args.data_dir.glob("[0-9][0-9][0-9][0-9].json")):
        snapshots.append(json.loads(path.read_text(encoding="utf-8")))
    snapshots.sort(key=lambda s: s["year"])

    years = []
    all_contributors = set()
    grand_wiki_counts = {}
    grand_total_usage_entries = 0
    grand_country_counts = {}
    years_active = {}  # username -> count of years they contributed
    seen_before = set()  # running set of everyone who's contributed in a prior year

    for snap in snapshots:
        usage = snap.get("usage", {})
        contributors_this_year = set(snap.get("contributors", []))

        new_this_year = contributors_this_year - seen_before
        returning_this_year = contributors_this_year & seen_before
        total_this_year = len(contributors_this_year)

        years.append({
            "year": snap["year"],
            "total_submissions": snap["total_submissions"],
            "countries_count": len(snap.get("countries", [])),
            "top_country": snap["countries"][0]["name"] if snap.get("countries") else None,
            "top_country_count": snap["countries"][0]["count"] if snap.get("countries") else 0,
            "sample_method": snap.get("sample_method"),
            "sample_total_views": snap.get("sample_total_views", 0),
            "top_viewed_max_views": snap["top_viewed"][0].get("total_views", snap["top_viewed"][0].get("views", 0)) if snap.get("top_viewed") else 0,
            "contributors_count": snap.get("contributors_count", 0),
            "usage_total": usage.get("total_usage_entries", 0),
            "files_used_percentage": usage.get("files_used_percentage", 0),
            "new_account_before_campaign_count": snap.get("new_account_before_campaign_count", 0),
            "new_account_before_campaign_percentage": snap.get("new_account_before_campaign_percentage", 0),
            "new_contributors_count": len(new_this_year),
            "new_contributors_percentage": round(len(new_this_year) / total_this_year * 100, 1) if total_this_year else 0,
            "returning_contributors_count": len(returning_this_year),
            "returning_contributors_percentage": round(len(returning_this_year) / total_this_year * 100, 1) if total_this_year else 0,
            "winners_count": len(snap.get("winners", [])),
            "generated_at": snap.get("generated_at"),
            "views_refreshed_at": snap.get("views_refreshed_at"),
        })

        for c in contributors_this_year:
            years_active[c] = years_active.get(c, 0) + 1
        seen_before |= contributors_this_year
        all_contributors.update(contributors_this_year)

        for entry in usage.get("by_wiki", []):
            grand_wiki_counts[entry["wiki"]] = grand_wiki_counts.get(entry["wiki"], 0) + entry["count"]
            grand_total_usage_entries += entry["count"]

        for c in snap.get("countries", []):
            grand_country_counts[c["name"]] = grand_country_counts.get(c["name"], 0) + c["count"]

    grand_total_sample_views = sum(y["sample_total_views"] for y in years)

    grand_usage_by_wiki = [
        {"wiki": w, "count": c, "percentage": round(c / grand_total_usage_entries * 100, 2) if grand_total_usage_entries else 0}
        for w, c in sorted(grand_wiki_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    grand_countries = [
        {"name": name, "count": count}
        for name, count in sorted(grand_country_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    # Retention: how many contributors have been active for at least N years.
    total_contributors = len(years_active)
    max_years = max(years_active.values()) if years_active else 0
    retention = []
    for k in range(1, max_years + 1):
        count_at_least_k = sum(1 for v in years_active.values() if v >= k)
        retention.append({
            "min_years": k,
            "count": count_at_least_k,
            "percentage": round(count_at_least_k / total_contributors * 100, 1) if total_contributors else 0,
        })

    out = {
        "years": years,
        "grand_total_sample_views": grand_total_sample_views,
        "grand_total_unique_contributors": len(all_contributors),
        "grand_total_usage_entries": grand_total_usage_entries,
        "grand_usage_by_wiki": grand_usage_by_wiki,
        "grand_countries": grand_countries,
        "contributor_retention": retention,
        "note": (
            "sample_total_views figures use full totals for full-census years and sums over a sample "
            "otherwise — check each year's sample_method. Views combine Commons-page visits with real "
            "reuse pageviews on other Wikimedia projects. contributors_count and "
            "grand_total_unique_contributors are exact. Usage figures come from Commons' GlobalUsage "
            "data. Retention and new-vs-returning figures only reflect years with a snapshot generated "
            "so far."
        ),
    }
    out_path = args.data_dir / "trends.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out_path} with {len(years)} year(s), {len(all_contributors)} unique contributors, {grand_total_usage_entries} total usages")


if __name__ == "__main__":
    main()