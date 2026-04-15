"""
generate.py — builds the static JSON files that the site reads.

Run locally:        python generate.py
Run by Actions:     same command, but automatically every day

Outputs:
    data/top.json           top 25 films by average rating
    data/controversial.json top 25 most divisive films
    data/members.json       list of members with rating counts
    data/meta.json          last-updated timestamp
"""

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import feedparser

# ── Config ────────────────────────────────────────────────────────────────

RATINGS_DIR = Path("ratings data")   # where the Letterboxd export folders live
DATA_DIR    = Path("data")           # where we write the JSON output
TOP_LIMIT   = 25


# ── Step 1: Load CSVs ─────────────────────────────────────────────────────

def username_from_folder(folder: Path) -> str:
    """Extract username from folder like 'letterboxd-paityne-2026-04-15-...'"""
    match = re.match(r"letterboxd-(.+?)-\d{4}-\d{2}-\d{2}", folder.name)
    return match.group(1) if match else folder.name


def load_csvs() -> tuple[dict, list[str]]:
    """
    Reads every ratings.csv found under RATINGS_DIR.

    Returns:
        movies  — dict keyed by Letterboxd URI:
                  { uri: { "name": str, "year": int|None, "ratings": { username: float } } }
        users   — list of usernames found
    """
    movies: dict = {}
    users:  list = []

    for folder in sorted(RATINGS_DIR.iterdir()):
        if not folder.is_dir():
            continue
        csv_path = folder / "ratings.csv"
        if not csv_path.exists():
            continue

        username = username_from_folder(folder)
        users.append(username)

        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uri        = row.get("Letterboxd URI", "").strip()
                name       = row.get("Name", "").strip()
                rating_str = row.get("Rating", "").strip()
                if not uri or not name or not rating_str:
                    continue
                try:
                    rating = float(rating_str)
                except ValueError:
                    continue

                year_str = row.get("Year", "").strip()
                year = int(year_str) if year_str.isdigit() else None

                if uri not in movies:
                    movies[uri] = {"name": name, "year": year, "ratings": {}}
                movies[uri]["ratings"][username] = rating

    return movies, users


# ── Step 2: Poll RSS feeds ────────────────────────────────────────────────

def parse_stars(title: str) -> float | None:
    """Fallback star parser for titles like 'Film Name, 2023 - ★★★½'"""
    match = re.search(r"[★½]+$", title.strip())
    if not match:
        return None
    s = match.group(0)
    return s.count("★") + (0.5 if "½" in s else 0)


def poll_rss(users: list[str], movies: dict):
    """
    Fetches each user's RSS feed and merges any new/updated ratings into movies.
    Modifies movies in place.
    """
    for username in users:
        rss_url = f"https://letterboxd.com/{username}/rss/"
        print(f"  Polling {username}...")
        try:
            feed = feedparser.parse(rss_url)
        except Exception as e:
            print(f"    Failed: {e}")
            continue

        for entry in feed.entries:
            # Try structured field first, then fall back to star parsing
            rating = getattr(entry, "letterboxd_memberrating", None)
            if rating is not None:
                try:
                    rating = float(rating)
                except (ValueError, TypeError):
                    rating = None
            if rating is None:
                rating = parse_stars(entry.get("title", ""))
            if rating is None:
                continue

            movie_name = getattr(entry, "letterboxd_filmtitle", None) \
                         or entry.get("title", "").split(",")[0].strip()
            year_str = getattr(entry, "letterboxd_filmyear", None)
            try:
                year = int(year_str) if year_str else None
            except (ValueError, TypeError):
                year = None

            uri = entry.get("link", "").strip()
            if not uri or not movie_name:
                continue

            if uri not in movies:
                movies[uri] = {"name": movie_name, "year": year, "ratings": {}}
            # RSS is more recent than the CSV export, so it wins
            movies[uri]["ratings"][username] = rating


# ── Step 3: Compute outputs ───────────────────────────────────────────────

def build_row(uri: str, info: dict) -> dict:
    """Turns a movies entry into the dict shape the frontend expects."""
    ratings = info["ratings"]
    avg = round(sum(ratings.values()) / len(ratings), 2)
    breakdown = ", ".join(f"{u}:{v}" for u, v in sorted(ratings.items()))
    return {
        "movie_name":     info["name"],
        "year":           info["year"],
        "letterboxd_uri": uri,
        "avg_rating":     avg,
        "rater_count":    len(ratings),
        "breakdown":      breakdown,
    }


def compute_top(movies: dict) -> list[dict]:
    # Output ALL films with at least 1 rating — the frontend handles filtering by min raters.
    rows = [build_row(uri, info) for uri, info in movies.items()]
    rows.sort(key=lambda r: (r["avg_rating"], r["rater_count"]), reverse=True)
    return rows


def compute_controversial(movies: dict) -> list[dict]:
    # Only films with 2+ raters can have variance — no point including solo ratings here.
    results = []
    for uri, info in movies.items():
        if len(info["ratings"]) < 2:
            continue
        values = list(info["ratings"].values())
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        row = build_row(uri, info)
        row["variance"] = round(variance, 4)
        results.append(row)
    results.sort(key=lambda r: r["variance"], reverse=True)
    return results


def compute_members(users: list[str], movies: dict) -> list[dict]:
    counts = {u: 0 for u in users}
    for info in movies.values():
        for username in info["ratings"]:
            if username in counts:
                counts[username] += 1
    return [
        {
            "username":     u,
            "rating_count": counts.get(u, 0),
            "profile_url":  f"https://letterboxd.com/{u}/",
        }
        for u in sorted(users)
    ]


# ── Step 4: Write JSON files ──────────────────────────────────────────────

def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"  Wrote {path} ({len(data)} items)")


# ── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    DATA_DIR.mkdir(exist_ok=True)

    print("Loading CSVs...")
    movies, users = load_csvs()
    print(f"  {len(users)} members, {len(movies)} unique films from CSVs")

    print("Polling RSS feeds...")
    poll_rss(users, movies)
    print(f"  {len(movies)} unique films after RSS merge")

    print("Writing data files...")
    write_json(DATA_DIR / "top.json",           compute_top(movies))
    write_json(DATA_DIR / "controversial.json",  compute_controversial(movies))
    write_json(DATA_DIR / "members.json",        compute_members(users, movies))
    write_json(DATA_DIR / "meta.json",           {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    })

    print("Done.")
