"""
generate.py — the only Python script in this project.

It does three things in order:
  1. Reads every ratings.csv from the "ratings data/" folder
  2. Fetches each user's Letterboxd RSS feed to pick up recent ratings
  3. Crunches the combined data and writes it out as JSON files in "data/"

The website (index.html) just reads those JSON files directly —
there's no server, no database, nothing else running.

Run it manually:   python generate.py
GitHub Actions runs it automatically every day at 8am UTC.
"""

# csv — Python's built-in library for reading .csv files.
# Each row becomes a dictionary keyed by the header row.
import csv

# time — used to add short delays between web requests so we don't
# hammer Letterboxd's servers too fast.
import time

# urllib.request — Python's built-in HTTP library.
# Used to fetch Letterboxd film pages to scrape global average ratings.
# We use this instead of the requests library to avoid adding a dependency.
import urllib.request
import urllib.error

# json — Python's built-in library for writing JSON files.
# json.dumps() converts a Python list/dict into a JSON string.
import json

# re — Python's built-in regular expression library.
# Used to extract the username from folder names and parse star ratings.
import re

# datetime — for generating the "last updated" timestamp.
# timezone.utc ensures the time is always in UTC, not your local timezone.
from datetime import datetime, timezone

# Path — a modern, cross-platform way to work with file paths.
# Path("ratings data") / "file.csv" is cleaner than string concatenation.
from pathlib import Path

# feedparser — third-party library (installed via pip) that downloads
# and parses RSS feeds. Handles all the XML complexity for us.
import feedparser


# ── Config ────────────────────────────────────────────────────────────────

# These are the two folders we care about, defined as Path objects.
# Path("ratings data") means "a folder called 'ratings data' in the same
# directory as this script."
RATINGS_DIR   = Path("ratings data")      # where the Letterboxd export folders live
DATA_DIR      = Path("data")             # where we write the output JSON files
LB_CACHE_FILE = DATA_DIR / "lb_ratings.json"  # cache of global LB averages

# How many top films (by group avg) to fetch LB global ratings for.
# We don't fetch all 2000+ films — just the most relevant ones.
# Fetching takes ~0.4s per film, so 300 films ≈ 2 minutes on first run.
# Subsequent runs are instant because of the cache.
LB_FETCH_LIMIT = 300


# ── Step 1: Load CSVs ─────────────────────────────────────────────────────

def username_from_folder(folder: Path) -> str:
    """
    Extracts the Letterboxd username from an export folder name.

    Letterboxd names export folders like: letterboxd-paityne-2026-04-15-19-54-utc
    We want just "paityne".

    re.match() checks if the pattern matches at the START of the string.
    r"..." is a raw string — backslashes are treated literally (needed for regex).

    Pattern breakdown:
      letterboxd-        matches the literal text "letterboxd-"
      (.+?)              captures one or more characters, non-greedy (stops early)
      -\d{4}-\d{2}-\d{2} matches a date like -2026-04-15

    match.group(1) returns the first capture group — the username.
    If the folder name doesn't match (e.g. a manually named folder),
    we just return the whole folder name as a fallback.
    """
    match = re.match(r"letterboxd-(.+?)-\d{4}-\d{2}-\d{2}", folder.name)
    return match.group(1) if match else folder.name


def load_csvs() -> tuple[dict, list[str], dict]:
    """
    Scans RATINGS_DIR for all export subfolders and reads their ratings.csv files.

    Returns three things (as a tuple):
      movies — a dict of every film ever rated, keyed by (title, year).
                Each entry contains the film's name, year, URI, and a dict
                of { username: rating } for everyone who rated it.
      users  — a list of all usernames found (one per export folder).
      latest — a dict of { username: most_recent_rating } from the CSV.
                This gets overwritten later by RSS data if available.
    """
    movies: dict = {}   # will hold all films
    users:  list = []   # will hold all usernames
    latest: dict = {}   # will hold the most recent rating per user

    # sorted() makes the order consistent across runs.
    # .iterdir() yields everything inside the folder — files and subfolders.
    for folder in sorted(RATINGS_DIR.iterdir()):

        # Skip anything that isn't a subfolder (e.g. a stray .DS_Store file)
        if not folder.is_dir():
            continue

        # Each export subfolder should contain a ratings.csv.
        # The / operator on Path objects joins paths (like os.path.join).
        csv_path = folder / "ratings.csv"
        if not csv_path.exists():
            continue

        username = username_from_folder(folder)
        users.append(username)

        # open() the file, then csv.DictReader turns each row into a dict.
        # newline="" is required by the csv module on all platforms.
        # encoding="utf-8" handles special characters in film titles.
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):

                # .get(key, default) safely reads a value — returns the default
                # if the key doesn't exist instead of crashing.
                # .strip() removes leading/trailing whitespace.
                uri        = row.get("Letterboxd URI", "").strip()
                name       = row.get("Name", "").strip()
                rating_str = row.get("Rating", "").strip()

                # Skip rows missing any essential field.
                # Some rows are watches without ratings — no rating_str.
                if not uri or not name or not rating_str:
                    continue

                # Convert the rating string to a float.
                # float("3.5") → 3.5, float("4") → 4.0
                # If it fails (unexpected value), skip this row.
                try:
                    rating = float(rating_str)
                except ValueError:
                    continue

                year_str = row.get("Year", "").strip()
                # .isdigit() returns True only for pure digit strings like "2021".
                # This guards against empty strings or weird values.
                year = int(year_str) if year_str.isdigit() else None

                # "or None" converts empty string "" → None.
                # "" is falsy in Python, so "" or None evaluates to None.
                date = row.get("Date", "").strip() or None

                # We use (lowercase title, year) as the unique key for each film.
                # This is better than using the URI because the CSV uses short
                # boxd.it links while RSS uses full film page URLs — same film,
                # different URIs. Keying by title+year prevents duplicates.
                key = (name.lower().strip(), year)

                # If we haven't seen this film before, create a new entry.
                # Otherwise we just add/update this user's rating below.
                if key not in movies:
                    movies[key] = {"name": name, "year": year, "uri": uri, "ratings": {}}

                # Store this user's rating for this film.
                # If they rated it before (e.g. re-watched), this overwrites.
                movies[key]["ratings"][username] = rating

                # Letterboxd CSV exports are ordered newest-first.
                # So the FIRST row we see for each user is their most recent rating.
                # We save it as a fallback — RSS will override this later if available.
                if username not in latest:
                    latest[username] = {
                        "name":   name,
                        "rating": rating,
                        "date":   date,
                        "uri":    uri,
                        "source": "csv"   # tag so we know this came from the CSV
                    }

    return movies, users, latest


# ── Step 2: Poll RSS feeds ────────────────────────────────────────────────

def parse_stars(title: str) -> float | None:
    """
    Fallback: parses a star rating out of an RSS entry title string.
    Letterboxd titles in RSS look like: "Oppenheimer, 2023 - ★★★★½"

    This is only used when the structured <letterboxd:memberRating> XML
    field is missing from an entry.

    re.search() scans the whole string for a match (unlike re.match
    which only checks from the start).

    Pattern: [★½]+$ means "one or more star/half characters at the end"
    $ anchors to the end of the string.

    Returns a float like 3.5, or None if no stars found.
    float | None is a type hint — means "returns either a float or None".
    """
    match = re.search(r"[★½]+$", title.strip())
    if not match:
        return None
    s = match.group(0)   # the matched star string, e.g. "★★★½"
    return s.count("★") + (0.5 if "½" in s else 0)
    # "½" in s checks if the half-star character is anywhere in the string.
    # 0.5 if "½" in s else 0 is a ternary — adds 0.5 if there's a half star.


def poll_rss(users: list[str], movies: dict, latest: dict):
    """
    Fetches every user's Letterboxd RSS feed and merges new ratings into movies.

    Letterboxd exposes a public RSS feed for every user at:
      https://letterboxd.com/[username]/rss/

    The feed contains their ~50 most recent diary entries.
    feedparser downloads and parses the XML for us.

    This function modifies movies and latest IN PLACE — it doesn't return
    anything, it just updates the dicts that were passed in.
    """
    for username in users:
        rss_url = f"https://letterboxd.com/{username}/rss/"
        print(f"  Polling {username}...")

        # try/except means: attempt the code in try, and if ANY error
        # occurs jump to except instead of crashing the whole script.
        # This way if one user's feed fails, we still process the others.
        try:
            # feedparser.parse() downloads the RSS URL and parses the XML.
            # feed.entries is a list of items (one per diary entry).
            feed = feedparser.parse(rss_url)
        except Exception as e:
            print(f"    Failed: {e}")
            continue   # skip to the next user

        for entry in feed.entries:

            # getattr(object, name, default) safely gets an attribute.
            # It's like entry.letterboxd_memberrating but returns None
            # instead of crashing if the attribute doesn't exist.
            # feedparser maps <letterboxd:memberRating> to this attribute.
            rating = getattr(entry, "letterboxd_memberrating", None)

            if rating is not None:
                try:
                    rating = float(rating)
                except (ValueError, TypeError):
                    # TypeError handles unexpected types, ValueError handles
                    # strings that can't be parsed as floats.
                    rating = None

            # If the structured field was missing, try parsing the title string.
            if rating is None:
                rating = parse_stars(entry.get("title", ""))

            # If we still have no rating, this is just a watch log — skip it.
            if rating is None:
                continue

            # Get film title — prefer the structured field, fall back to
            # splitting the title string on the comma before the year.
            movie_name = getattr(entry, "letterboxd_filmtitle", None) \
                         or entry.get("title", "").split(",")[0].strip()

            # Get the release year from the structured XML field.
            year_str = getattr(entry, "letterboxd_filmyear", None)
            try:
                year = int(year_str) if year_str else None
            except (ValueError, TypeError):
                year = None

            # The film's page URL — used as a fallback URI if we don't
            # already have one from the CSV.
            uri = entry.get("link", "").strip()

            if not movie_name:
                continue

            # Same key strategy as in load_csvs — (lowercase title, year).
            key = (movie_name.lower().strip(), year)

            if key not in movies:
                # Film not seen in any CSV — add it fresh from RSS.
                movies[key] = {"name": movie_name, "year": year, "uri": uri, "ratings": {}}
            elif not movies[key]["uri"] and uri:
                # We have the film but no URI yet — fill it in.
                movies[key]["uri"] = uri

            # Update this user's rating. RSS is more recent than the CSV,
            # so it always wins if the same film appears in both.
            movies[key]["ratings"][username] = rating

            # RSS entries are ordered newest-first.
            # The FIRST entry we process per user is their most recent rating.
            # We override the CSV latest because RSS is more up to date.
            # latest[username].get("source") == "csv" means: only override
            # if we haven't already set it from a previous RSS entry.
            if username not in latest or latest[username].get("source") == "csv":
                latest[username] = {
                    "name":   movie_name,
                    "rating": rating,
                    "uri":    uri,
                    "source": "rss"
                }


# ── Step 2b: Fetch global Letterboxd ratings ─────────────────────────────

def load_lb_cache() -> dict:
    """
    Loads the cached global LB ratings from disk.

    The cache is a dict keyed by film URI:
      { "https://boxd.it/xxxx": { "avg": 3.89, "fetched": "2026-04-17" }, ... }

    Returns an empty dict if the cache file doesn't exist yet.
    """
    if LB_CACHE_FILE.exists():
        return json.loads(LB_CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def film_page_url(uri: str) -> str:
    """
    Converts any Letterboxd URI into a canonical film page URL.

    RSS diary entry URLs look like: letterboxd.com/paityne/film/get-out/
    The actual film page is:        letterboxd.com/film/get-out/

    We strip the username to get the canonical URL.
    boxd.it short links pass through unchanged — urllib will follow
    the redirect to the film page automatically when we fetch them.
    """
    match = re.match(r"(https://letterboxd\.com)/[^/]+(/film/[^/]+/?)", uri)
    if match:
        return match.group(1) + match.group(2)
    return uri


def fetch_lb_rating(url: str) -> float | None:
    """
    Fetches a Letterboxd film page and scrapes the global average rating.

    Letterboxd embeds structured data (JSON-LD) in every film page:
      <script type="application/ld+json">
        { ..., "ratingValue": 3.89, ... }
      </script>

    We use a regex to pull out the ratingValue number.
    Returns a float like 3.89, or None if the fetch/parse fails.
    """
    try:
        # Build a request with a browser-like User-Agent header.
        # Some sites reject requests that look like bots.
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        )
        # timeout=10 means give up after 10 seconds if the server doesn't respond.
        with urllib.request.urlopen(req, timeout=10) as resp:
            # resp.read() downloads the full HTML as bytes.
            # .decode() converts bytes to a string. errors="ignore" skips bad characters.
            html = resp.read().decode("utf-8", errors="ignore")

        # Search the entire HTML for "ratingValue": 3.89
        match = re.search(r'"ratingValue"\s*:\s*([\d.]+)', html)
        if match:
            return float(match.group(1))
        return None

    except Exception:
        # If anything goes wrong (network error, 404, timeout, parse error),
        # just return None instead of crashing the whole script.
        return None


def fetch_all_lb_ratings(movies: dict) -> dict:
    """
    Fetches global Letterboxd average ratings for the top LB_FETCH_LIMIT films
    (by group average rating), using a cache to avoid re-fetching.

    Returns a dict: { uri: float } mapping each film's URI to its LB global avg.
    Also saves the updated cache to disk.
    """
    cache = load_lb_cache()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build a sorted list of (uri, group_avg) for all films with a URI.
    # We only want to fetch for the most relevant films, not all 2000+.
    films_by_avg = sorted(
        [(info["uri"], sum(info["ratings"].values()) / len(info["ratings"]))
         for info in movies.values()
         if info["uri"] and info["ratings"]],
        key=lambda x: x[1],   # sort by group average rating
        reverse=True           # highest first
    )

    # Take only the top LB_FETCH_LIMIT films.
    top_uris = [uri for uri, _ in films_by_avg]

    results = {}
    fetched_count = 0

    for uri in top_uris:
        # Check if we have a fresh cache entry (fetched today).
        # We only re-fetch if it's a new film or the cache is from a previous day.
        if uri in cache and cache[uri].get("fetched") == today:
            # Cache hit — use it directly, no network request needed.
            if cache[uri].get("avg") is not None:
                results[uri] = cache[uri]["avg"]
            continue

        # Cache miss — need to fetch from Letterboxd.
        page_url = film_page_url(uri)
        avg = fetch_lb_rating(page_url)

        # Store in cache regardless of success (even None, so we don't retry today).
        cache[uri] = {"avg": avg, "fetched": today}

        if avg is not None:
            results[uri] = avg
            fetched_count += 1

        # Be polite — wait 0.4 seconds between requests.
        # Without this, Letterboxd might rate-limit or block us.
        time.sleep(0.4)

        # Save after every fetch so Ctrl+C doesn't lose progress.
        LB_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    print(f"  Fetched {fetched_count} new LB ratings ({len(results)} total with cache)")

    return results


# ── Step 3: Compute outputs ───────────────────────────────────────────────

def build_row(info: dict, lb_ratings: dict | None = None) -> dict:
    """
    Converts a single film entry from the movies dict into the flat dict
    shape that the frontend (index.html) expects.

    info looks like:
      { "name": "Parasite", "year": 2019, "uri": "https://...", "ratings": {"luke": 5.0, "paityne": 4.5} }

    lb_ratings is the dict of global LB averages { uri: float }.
    If provided, the row will include "lb_avg" for films we have data for.

    Returns a flat dict like:
      { "movie_name": "Parasite", "year": 2019, "avg_rating": 4.75, "lb_avg": 4.22, ... }
    """
    ratings = info["ratings"]
    avg = round(sum(ratings.values()) / len(ratings), 2)
    breakdown = ", ".join(f"{u}:{v}" for u, v in sorted(ratings.items()))

    # Look up the LB global average for this film if we have it.
    lb_avg = None
    if lb_ratings and info["uri"]:
        raw = lb_ratings.get(info["uri"])
        lb_avg = round(raw, 2) if raw is not None else None

    return {
        "movie_name":     info["name"],
        "year":           info["year"],
        "letterboxd_uri": info["uri"],
        "avg_rating":     avg,
        "lb_avg":         lb_avg,   # global LB average, or null if unavailable
        "rater_count":    len(ratings),
        "breakdown":      breakdown,
    }


def compute_top(movies: dict, lb_ratings: dict) -> list[dict]:
    """
    Builds the full sorted list of films by average rating.
    Returns ALL films — the frontend slices it to 100 and applies filters.
    """
    rows = [build_row(info, lb_ratings) for info in movies.values()]
    rows.sort(key=lambda r: (r["avg_rating"], r["rater_count"]), reverse=True)
    return rows


def compute_controversial(movies: dict, lb_ratings: dict) -> list[dict]:
    """
    Finds films where people DISAGREE the most — high variance in ratings.

    Variance = average of squared differences from the mean.
    Example: ratings [1, 5] → mean=3, variance = ((1-3)² + (5-3)²) / 2 = 4
    Example: ratings [3, 3] → mean=3, variance = 0 (no disagreement)

    Only films with 2+ raters can have any variance, so we skip solo ratings.
    """
    results = []
    for info in movies.values():

        # Can't have variance with only one rater — skip.
        if len(info["ratings"]) < 2:
            continue

        values = list(info["ratings"].values())  # just the numeric ratings

        # Step 1: compute the mean (average).
        mean = sum(values) / len(values)

        # Step 2: for each value, square its distance from the mean, then average those.
        # ** is Python's power operator: (v - mean) ** 2 = (v - mean) squared.
        variance = sum((v - mean) ** 2 for v in values) / len(values)

        row = build_row(info, lb_ratings)
        row["variance"] = round(variance, 4)
        results.append(row)

    # Sort by variance descending — most controversial first.
    results.sort(key=lambda r: r["variance"], reverse=True)
    return results


def compute_deviations(movies: dict, lb_ratings: dict) -> list[dict]:
    """
    Finds films where the group's average rating deviates most from
    Letterboxd's global average rating.

    A high positive deviation means the group loves it more than the world.
    A high negative deviation means the group is more critical than the world.

    Only includes films where:
      - We have a LB global average (lb_avg is not None)
      - At least 2 group members have rated it

    Sorted by absolute deviation — biggest disagreement with the world first.
    """
    results = []
    for info in movies.values():
        if len(info["ratings"]) < 2:
            continue

        uri = info["uri"]
        if not uri or uri not in lb_ratings:
            continue

        lb_avg = lb_ratings[uri]
        if lb_avg is None:
            continue

        group_avg = sum(info["ratings"].values()) / len(info["ratings"])

        # deviation = how much the group differs from the world.
        # Positive: group rates higher. Negative: group rates lower.
        deviation = round(group_avg - lb_avg, 2)

        row = build_row(info, lb_ratings)
        row["deviation"] = deviation
        results.append(row)

    # Sort by absolute deviation — |+2.0| and |-2.0| are equally interesting.
    # abs() gives the absolute value: abs(-2.0) = 2.0
    results.sort(key=lambda r: abs(r["deviation"]), reverse=True)
    return results


def compute_members(users: list[str], movies: dict, latest: dict) -> list[dict]:
    """
    Builds the members list for the Members tab.

    Counts how many films each user has rated by scanning all movies,
    then combines that with their latest rating info.
    """
    # Start everyone at 0.
    counts = {u: 0 for u in users}

    # Go through every film and increment the count for each user who rated it.
    for info in movies.values():
        for username in info["ratings"]:
            if username in counts:   # ignore RSS-only users not in our CSV list
                counts[username] += 1

    # Build the output list, sorted alphabetically by username.
    return [
        {
            "username":      u,
            "rating_count":  counts.get(u, 0),
            "profile_url":   f"https://letterboxd.com/{u}/",
            # latest.get(u) returns the latest rating dict, or None if missing.
            "latest":        latest.get(u),
        }
        for u in sorted(users)
    ]


# ── Step 4: Write JSON files ──────────────────────────────────────────────

def write_json(path: Path, data):
    """
    Writes a Python list or dict to a JSON file at the given path.

    json.dumps(data, indent=2) converts Python → JSON string with 2-space indenting.
    path.write_text(...) writes the string to disk.
    encoding="utf-8" handles special characters in film titles.
    """
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"  Wrote {path} ({len(data)} items)")


# ── Main ──────────────────────────────────────────────────────────────────
# This block only runs when you execute the script directly: python generate.py
# It does NOT run when another file imports from this one.

if __name__ == "__main__":

    # Create the data/ folder if it doesn't already exist.
    # exist_ok=True means don't error if it's already there.
    DATA_DIR.mkdir(exist_ok=True)

    # ── Step 1: Load all the CSV export files ──
    print("Loading CSVs...")
    movies, users, latest = load_csvs()
    print(f"  {len(users)} members, {len(movies)} unique films from CSVs")

    # ── Step 2: Fetch RSS feeds to add recent ratings ──
    print("Polling RSS feeds...")
    poll_rss(users, movies, latest)
    print(f"  {len(movies)} unique films after RSS merge")

    # ── Step 2b: Fetch global Letterboxd ratings ──
    # Only fetches the top LB_FETCH_LIMIT films and caches the rest.
    print("Fetching Letterboxd global ratings...")
    lb_ratings = fetch_all_lb_ratings(movies)

    # ── Step 3 + 4: Compute and write the JSON files ──
    print("Writing data files...")
    write_json(DATA_DIR / "top.json",            compute_top(movies, lb_ratings))
    write_json(DATA_DIR / "controversial.json",   compute_controversial(movies, lb_ratings))
    write_json(DATA_DIR / "deviations.json",      compute_deviations(movies, lb_ratings))
    write_json(DATA_DIR / "members.json",         compute_members(users, movies, latest))
    write_json(DATA_DIR / "meta.json", {
        # datetime.now(timezone.utc) gets the current time in UTC.
        # .strftime() formats it as a readable string like "2026-04-17 08:00 UTC".
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    })

    print("Done.")
