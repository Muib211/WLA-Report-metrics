# State of Wiki Loves Africa — snapshot pipeline

Turns the Wiki Loves Africa category tree on Commons into yearly JSON
snapshots (submissions per country, most-viewed images), and a static
dashboard that reads them — no live API calls on page load, so it's fast
and works offline once built.

## How it fits together

```
scripts/fetch_snapshot.py   → crawls one year's category tree, writes data/<year>.json
scripts/build_trends.py     → combines every data/<year>.json into data/trends.json
site/index.html             → static dashboard, reads ./data/*.json
.github/workflows/…         → runs fetch_snapshot + build_trends on a schedule, commits the result
```

## One-time setup

1. Create a new GitHub repo (or add this into an existing one, e.g. next to
   your other Wiki In Africa dashboards) and push these files.
2. Enable GitHub Pages for the repo, serving from the branch root (or move
   `site/index.html` to wherever your Pages config expects it — e.g. copy it
   to the repo root, or set Pages to serve from `/site`). The dashboard
   expects `data/` to be a sibling folder of `index.html`, so if you move the
   HTML file, move (or symlink) `data/` alongside it too.
3. In the repo's Settings → Actions → General, make sure "Read and write
   permissions" is enabled for the `GITHUB_TOKEN`, so the workflow can commit
   snapshot updates back to the repo.

## Backfilling past years (2014–2025)

Run this locally first — it's the slowest part, and you'll want to watch it
for errors on the first run since it hasn't been tested against the live API
yet:

```bash
pip install -r requirements.txt

# Quick sanity check on a recent, well-structured year, sampled (fast):
python scripts/fetch_snapshot.py --year 2025 --sample-cap 15

# Once that looks right, backfill the rest:
for y in $(seq 2014 2025); do
  python scripts/fetch_snapshot.py --year $y --sample-cap 25
done

python scripts/build_trends.py
git add data/ && git commit -m "Backfill WLA snapshots 2014-2025" && git push
```

Or trigger it from GitHub instead of locally: go to Actions → "Update WLA
snapshot" → "Run workflow", enter the year, and run it once per year you want
to backfill.

Early years (2014–2016) may use a different category naming pattern than
2025's — if `fetch_snapshot.py` reports "no subcategories found" for an old
year, check the category name on Commons
(`Category:Images from Wiki Loves Africa <year>`) and adjust the `root`
pattern in `fetch_snapshot.py` if needed.

## Ongoing updates

The scheduled workflow runs monthly and refreshes **only the current year**
(cheap — a few hundred to a few thousand files, not the full decade). Past
years' snapshots stay as committed unless you re-run them manually.

## Sampling vs. full census

By default, pageview ranking is based on a sample (`--sample-cap 25` files
per country) rather than every file, because checking pageviews on ~24,000
files one-by-one is slow even server-side. Pass `--full-census` for an exact
ranking — expect this to take significantly longer (potentially 30–60+
minutes for a busy year) and be mindful of Wikimedia's API rate-limit
etiquette if you do.

## Known gaps / next steps

- Country-level submission counts are a recursive file count under each
  country's category branch (handles nested "community" subcategories like
  `... in Nigeria > ... in WUGN Abuja Network`), not just the top-level
  category's own file count.
- No de-duplication across years if a file is recategorized after the fact.
- No theme/tag breakdown yet (e.g. what fraction of "Farm to Plate" entries
  are markets vs. cooking vs. fields) — would need a text/category
  classification pass on file descriptions or categories.
- No gender-of-contributor breakdown — would need to cross-reference
  uploader usernames against self-declared gender on-wiki, which is a much
  more sensitive and manual process.
