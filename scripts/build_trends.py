#!/usr/bin/env python3
"""
build_trends.py — Aggregate every data/<year>.json snapshot into data/trends.json.

Run this after fetch_snapshot.py has produced/updated one or more yearly files.

Usage:
    python scripts/build_trends.py
    python scripts/build_trends.py --data-dir data
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
        snap = json.loads(path.read_text())
        top_country = snap["countries"][0]["name"] if snap.get("countries") else None
        years.append({
            "year": snap["year"],
            "total_submissions": snap["total_submissions"],
            "countries_count": len(snap.get("countries", [])),
            "top_country": top_country,
            "top_country_count": snap["countries"][0]["count"] if snap.get("countries") else 0,
            "sample_method": snap.get("sample_method"),
            "top_viewed_max_views": snap["top_viewed"][0]["views"] if snap.get("top_viewed") else 0,
            "generated_at": snap.get("generated_at"),
        })

    years.sort(key=lambda y: y["year"])
    out = {"years": years}
    out_path = args.data_dir / "trends.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"wrote {out_path} with {len(years)} year(s)")


if __name__ == "__main__":
    main()
