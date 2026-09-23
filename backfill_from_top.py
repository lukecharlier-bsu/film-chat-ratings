"""
backfill_from_top.py

Diffs consecutive top.json snapshots in git history to find when each
(username, film) rating first appeared. Uses the commit date as the
approximate "rated around this time" date for diary entries that have
no date from CSV or RSS.

Merges results into data/rss_history.json without overwriting entries
that already have real dates.

Run once:  python backfill_from_top.py
Then:      python generate.py && git add data/ && git commit -m "Backfill diary dates from git history" && git push
"""

import json
import subprocess
from pathlib import Path
import csv as csv_module
import re

DATA_DIR         = Path("data")
RATINGS_DIR      = Path("ratings data")
RSS_HISTORY_FILE = DATA_DIR / "rss_history.json"

# All commits that contain top.json, oldest first
COMMITS = [
    ("d2a7fb3", "2026-04-15"),
    ("1e74bbf", "2026-04-15"),
    ("fb8848d", "2026-04-16"),
    ("36db3c4", "2026-04-16"),
    ("535c7d1", "2026-04-19"),
    ("52a8a16", "2026-04-20"),
    ("bb67e6e", "2026-04-22"),
    ("20d5ca6", "2026-04-27"),
    ("897edf1", "2026-05-03"),
    ("5b40fa3", "2026-05-04"),
    ("4a3f6a4", "2026-05-04"),
    ("419c7bc", "2026-05-22"),
    ("dec0af1", "2026-05-22"),
    ("986a252", "2026-06-05"),
    ("1007eb8", "2026-07-08"),
    ("3ee68b8", "2026-07-30"),
    ("0cc75d9", "2026-09-22"),
    ("81a0bda", "2026-09-23"),
]


def username_from_folder(folder: Path) -> str:
    match = re.match(r"letterboxd-(.+?)-\d{4}-\d{2}-\d{2}", folder.name)
    return match.group(1) if match else folder.name


def load_csv_keys() -> dict:
    """Returns {username: set of (name_lower, year)} from current CSV exports."""
    keys = {}
    for folder in sorted(RATINGS_DIR.iterdir()):
        if not folder.is_dir():
            continue
        csv_path = folder / "ratings.csv"
        if not csv_path.exists():
            continue
        username = username_from_folder(folder)
        keys[username] = set()
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv_module.DictReader(f):
                name = row.get("Name", "").strip()
                year_str = row.get("Year", "").strip()
                year = int(year_str) if year_str.isdigit() else None
                if name:
                    keys[username].add((name.lower().strip(), year))
    return keys


def get_top_from_commit(commit: str) -> dict:
    """Returns {(username, name_lower, year): rating} from a top.json snapshot."""
    result = subprocess.run(
        ["git", "show", f"{commit}:data/top.json"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return {}

    entries = json.loads(result.stdout)
    ratings = {}
    for e in entries:
        name = e.get("movie_name", "")
        year = e.get("year")
        uri  = e.get("letterboxd_uri", "")
        breakdown = e.get("breakdown", "")
        for part in breakdown.split(", "):
            if ":" not in part:
                continue
            username, rating_str = part.split(":", 1)
            try:
                rating = float(rating_str)
            except ValueError:
                continue
            ratings[(username, name.lower().strip(), year)] = {
                "name": name, "year": year, "rating": rating, "uri": uri
            }
    return ratings


def main():
    DATA_DIR.mkdir(exist_ok=True)

    print("Loading current CSV keys...")
    csv_keys = load_csv_keys()

    print("Loading existing RSS history...")
    rss_history: dict = {}
    if RSS_HISTORY_FILE.exists():
        rss_history = json.loads(RSS_HISTORY_FILE.read_text(encoding="utf-8"))

    # Build existing rss_history lookup for fast dedup
    history_keys: dict = {}
    for username, entries in rss_history.items():
        history_keys[username] = {(e["name"].lower().strip(), e["year"]): e for e in entries}

    prev_ratings = {}
    total_added = 0

    for commit, date in COMMITS:
        print(f"Processing {commit} ({date})...")
        curr_ratings = get_top_from_commit(commit)

        # Find ratings that are NEW in this commit vs the previous one
        new_ratings = {k: v for k, v in curr_ratings.items() if k not in prev_ratings}
        print(f"  {len(new_ratings)} new ratings appeared")

        for (username, name_lower, year), info in new_ratings.items():
            # Skip if already covered by current CSV exports (any year variant)
            if username in csv_keys and any(
                k[0] == name_lower for k in csv_keys[username]
            ):
                continue

            # Skip if already in rss_history with a real (non-backfill) date
            existing = history_keys.get(username, {}).get((name_lower, year))
            if existing and existing.get("date") and existing.get("source") != "backfill":
                continue

            entry = {
                "name":     info["name"],
                "year":     year,
                "date":     date,
                "rating":   info["rating"],
                "uri":      info["uri"],
                "reviewed": False,
                "source":   "backfill",
            }

            if username not in history_keys:
                history_keys[username] = {}

            history_keys[username][(name_lower, year)] = entry
            total_added += 1

        prev_ratings = curr_ratings

    # Write back
    for username, key_map in history_keys.items():
        rss_history[username] = list(key_map.values())

    RSS_HISTORY_FILE.write_text(json.dumps(rss_history, indent=2), encoding="utf-8")
    print(f"\nDone. Added/updated {total_added} entries in {RSS_HISTORY_FILE}")
    print("Now run:")
    print("  python generate.py && git add data/ && git commit -m 'Backfill diary dates from git history' && git push")


if __name__ == "__main__":
    main()
