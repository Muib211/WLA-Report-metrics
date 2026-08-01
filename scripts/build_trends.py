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
    for path in sorted(args.data_dir.glob("[0-9][0-9][0-9][0-9].json")):
        snap = json.loads(path.read_text(encoding="utf-8"))
        top_country = snap["countries"][0]["name"] if snap.get("countries") else None
        years.append({
            "year": snap["year"],
            "total_submissions": snap["total_submissions"],
            "countries_count": len(snap.get("countries", [])),
            "top_country": top_country,
            "top_country_count": snap["countries"][0]["count"] if snap.get("countries") else 0,
            "sample_method": snap.get("sample_method"),
            "sample_total_views": snap.get("sample_total_views", 0),
            "top_viewed_max_views": snap["top_viewed"][0]["views"] if snap.get("top_viewed") else 0,
            "generated_at": snap.get("generated_at"),
            "views_refreshed_at": snap.get("views_refreshed_at"),
        })

    years.sort(key=lambda y: y["year"])
    grand_total_sample_views = sum(y["sample_total_views"] for y in years)

    out = {
        "years": years,
        "grand_total_sample_views": grand_total_sample_views,
        "note": "sample_total_views and grand_total_sample_views are sums over sampled files only, not a full census of every submission.",
    }
    out_path = args.data_dir / "trends.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out_path} with {len(years)} year(s), grand_total_sample_views={grand_total_sample_views}")


if __name__ == "__main__":
    main()
