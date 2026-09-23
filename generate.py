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
import urllib.parse

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
LB_CACHE_FILE      = DATA_DIR / "lb_ratings.json"    # cache of global LB averages
TMDB_CACHE_FILE    = DATA_DIR / "tmdb_cache.json"    # cache of TMDB metadata
RSS_HISTORY_FILE   = DATA_DIR / "rss_history.json"   # accumulated RSS diary entries across all runs
TMDB_POSTER_BASE = "https://image.tmdb.org/t/p/w92"

# Read TMDB API key from local file (never commit this file to git).
# Falls back to empty string — TMDB fetch is skipped if no key is found.
_tmdb_key_file = Path("tmdb api key.txt")
TMDB_API_KEY = _tmdb_key_file.read_text(encoding="utf-8").strip() if _tmdb_key_file.exists() else ""


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


def load_csvs() -> tuple[dict, list[str], dict, dict]:
    """
    Scans RATINGS_DIR for all export subfolders and reads their ratings.csv files.
    Also reads reviews.csv (if present) to flag which films each user has reviewed.

    Returns four things:
      movies  — dict of every film, keyed by (title, year)
      users   — list of all usernames
      latest  — dict of { username: most_recent_rating }
      diary   — dict of { username: [{"name", "year", "date", "rating", "uri", "reviewed"}, ...] }
    """
    movies: dict = {}
    users:  list = []
    latest: dict = {}
    diary:  dict = {}

    for folder in sorted(RATINGS_DIR.iterdir()):
        if not folder.is_dir():
            continue

        csv_path = folder / "ratings.csv"
        if not csv_path.exists():
            continue

        username = username_from_folder(folder)
        users.append(username)
        diary[username] = []

        # Load URIs of reviewed films from reviews.csv so we can flag them
        reviewed_uris: set = set()
        reviews_path = folder / "reviews.csv"
        if reviews_path.exists():
            with open(reviews_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    uri = row.get("Letterboxd URI", "").strip()
                    if uri:
                        reviewed_uris.add(uri)

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
                date = row.get("Date", "").strip() or None

                key = (name.lower().strip(), year)

                if key not in movies:
                    movies[key] = {"name": name, "year": year, "uri": uri, "ratings": {}}

                movies[key]["ratings"][username] = rating

                if username not in latest:
                    latest[username] = {
                        "name":   name,
                        "rating": rating,
                        "date":   date,
                        "uri":    uri,
                        "source": "csv"
                    }

                diary[username].append({
                    "name":     name,
                    "year":     year,
                    "date":     date,
                    "rating":   rating,
                    "uri":      uri,
                    "reviewed": uri in reviewed_uris,
                })

    return movies, users, latest, diary


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


def poll_rss(users: list[str], movies: dict, latest: dict) -> dict:
    """
    Fetches every user's Letterboxd RSS feed and merges new ratings into movies.

    Letterboxd exposes a public RSS feed for every user at:
      https://letterboxd.com/[username]/rss/

    The feed contains their ~50 most recent diary entries.
    feedparser downloads and parses the XML for us.

    This function modifies movies and latest IN PLACE — it doesn't return
    anything, it just updates the dicts that were passed in.
    """
    rss_coverage = {}  # { username: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"} }
    rss_diary    = {}  # { username: [diary entries from RSS] }

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

            # Track RSS date range per user + collect diary entry
            published = getattr(entry, "published_parsed", None)
            date_str = None
            if published:
                try:
                    date_str = datetime(*published[:3]).strftime("%Y-%m-%d")
                    if username not in rss_coverage:
                        rss_coverage[username] = {"from": date_str, "to": date_str}
                    else:
                        if date_str < rss_coverage[username]["from"]:
                            rss_coverage[username]["from"] = date_str
                        if date_str > rss_coverage[username]["to"]:
                            rss_coverage[username]["to"] = date_str
                except Exception:
                    pass

            rss_diary.setdefault(username, []).append({
                "name":     movie_name,
                "year":     year,
                "date":     date_str,
                "rating":   rating,
                "uri":      uri,
                "reviewed": False,  # RSS doesn't tell us if they wrote a review
            })

    return rss_coverage, rss_diary


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
    total = len(top_uris)

    for i, uri in enumerate(top_uris, 1):
        # Check if we have a fresh cache entry (fetched today).
        # We only re-fetch if it's a new film or the cache is from a previous day.
        fetched_on = cache.get(uri, {}).get("fetched")
        cache_age = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(fetched_on, "%Y-%m-%d")).days if fetched_on else 999
        if uri in cache and cache_age < 14:
            # Cache hit — use it directly, no network request needed.
            if cache[uri].get("avg") is not None:
                results[uri] = cache[uri]["avg"]
            continue

        # Cache miss — need to fetch from Letterboxd.
        page_url = film_page_url(uri)
        avg = fetch_lb_rating(page_url)
        print(f"  ({i}/{total}) {page_url} → {avg}", flush=True)

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


# ── Step 2c: Fetch TMDB metadata (year, genres, poster) ──────────────────

def load_tmdb_cache() -> dict:
    if TMDB_CACHE_FILE.exists():
        return json.loads(TMDB_CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def fetch_tmdb_genres() -> dict:
    """Fetches the TMDB genre ID → name mapping once. Returns {} on failure."""
    if not TMDB_API_KEY:
        return {}
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={TMDB_API_KEY}&language=en-US"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return {g["id"]: g["name"] for g in data.get("genres", [])}
    except Exception:
        return {}


def fetch_tmdb_data(title: str, year: int | None, genres_map: dict) -> dict | None:
    """
    Searches TMDB for a film by title (+year) and returns:
      { "tmdb_year": int, "genres": [str, ...], "poster": str | None }

    Picks the best match from the top 5 results — prefers exact title match,
    then closest release year to what Letterboxd reported.
    If no results with the year constraint, retries without it.
    """
    if not TMDB_API_KEY:
        return None

    query = urllib.parse.quote(title)
    year_param = f"&year={year}" if year else ""
    url = (f"https://api.themoviedb.org/3/search/movie"
           f"?api_key={TMDB_API_KEY}&query={query}{year_param}&language=en-US&page=1")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

    results = data.get("results", [])
    if not results:
        # Nothing found with year constraint — retry without it
        if year:
            return fetch_tmdb_data(title, None, genres_map)
        return None

    # Pick best match: exact title first, then closest year
    title_lower = title.lower()
    best = None
    best_year_diff = 999

    for r in results[:5]:
        r_title = r.get("title", "").lower()
        r_year_str = r.get("release_date", "")[:4]
        r_year = int(r_year_str) if r_year_str.isdigit() else None
        year_diff = abs((r_year or 0) - (year or 0)) if year and r_year else 999

        if r_title == title_lower:
            if best is None or year_diff < best_year_diff:
                best = r
                best_year_diff = year_diff
        elif best is None:
            best = r
            best_year_diff = year_diff

    if not best:
        best = results[0]

    r_year_str = best.get("release_date", "")[:4]
    tmdb_year = int(r_year_str) if r_year_str.isdigit() else year
    genres = [genres_map[gid] for gid in best.get("genre_ids", []) if gid in genres_map]
    poster_path = best.get("poster_path")
    poster = (TMDB_POSTER_BASE + poster_path) if poster_path else None

    return {"tmdb_year": tmdb_year, "genres": genres, "poster": poster}


def fetch_all_tmdb_data(movies: dict) -> None:
    """
    Fetches TMDB metadata for every film and stores it IN PLACE in each info dict.
    Uses a 30-day cache — genres and posters rarely change.
    Saves the cache after every fetch so Ctrl+C is safe.
    """
    if not TMDB_API_KEY:
        print("  No TMDB API key found — skipping.")
        return

    cache = load_tmdb_cache()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    genres_map = fetch_tmdb_genres()
    print(f"  Loaded {len(genres_map)} genres from TMDB")

    total = len(movies)
    fetched_count = 0

    for i, (key, info) in enumerate(movies.items(), 1):
        name, year = key
        cache_key = f"{name}|{year}"

        fetched_on = cache.get(cache_key, {}).get("fetched")
        cache_age = (
            (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(fetched_on, "%Y-%m-%d")).days
            if fetched_on else 999
        )

        if cache_key in cache and cache_age < 30:
            entry = cache[cache_key]
            info["tmdb_year"] = entry.get("tmdb_year")
            info["genres"]    = entry.get("genres", [])
            info["poster"]    = entry.get("poster")
            continue

        # Cache miss — fetch from TMDB
        result = fetch_tmdb_data(info["name"], info["year"], genres_map)
        if result:
            info["tmdb_year"] = result["tmdb_year"]
            info["genres"]    = result["genres"]
            info["poster"]    = result["poster"]
            fetched_count += 1
        else:
            info["tmdb_year"] = info["year"]
            info["genres"]    = []
            info["poster"]    = None

        cache[cache_key] = {
            "tmdb_year": info["tmdb_year"],
            "genres":    info["genres"],
            "poster":    info["poster"],
            "fetched":   today,
        }

        TMDB_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        print(f"  ({i}/{total}) {info['name']} ({year}) → {info['genres']}", flush=True)
        time.sleep(0.1)

    print(f"  Fetched {fetched_count} new TMDB entries ({total} total with cache)")


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
        "year":           info.get("tmdb_year") or info["year"],  # TMDB year is more accurate
        "letterboxd_uri": info["uri"],
        "avg_rating":     avg,
        "lb_avg":         lb_avg,
        "rater_count":    len(ratings),
        "breakdown":      breakdown,
        "genres":         info.get("genres", []),
        "poster":         info.get("poster"),
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


def compute_diary(diary: dict, movies: dict) -> list[dict]:
    """
    Builds a flat list of all diary entries across all users, enriched with
    TMDB poster/genres pulled from the movies dict.
    Sorted by date descending (newest first).
    """
    # Build a lookup by name only — year can differ between CSV/RSS/TMDB
    tmdb_lookup = {}
    for (name_lower, year), info in movies.items():
        tmdb_lookup[name_lower] = {
            "poster":    info.get("poster"),
            "genres":    info.get("genres", []),
            "tmdb_year": info.get("tmdb_year") or year,
        }

    entries = []
    for username, user_entries in diary.items():
        for e in user_entries:
            tmdb = tmdb_lookup.get(e["name"].lower().strip(), {})
            approx = e.get("source") == "backfill"
            entries.append({
                "username": username,
                "name":     e["name"],
                "year":     tmdb.get("tmdb_year") or e["year"],
                "date":     e["date"],
                "rating":   e["rating"],
                "uri":      e["uri"],
                "reviewed": e["reviewed"],
                "poster":   tmdb.get("poster"),
                "genres":   tmdb.get("genres", []),
                "approx":   approx,
            })

    # Sort chronologically (newest first). For entries on the same date, real dates
    # sort before approximate dates. Undated entries go last.
    # Backfill entries appear inline in the timeline at their approximate date,
    # rather than being exiled to the bottom — they fill the gap when nothing better exists.
    from functools import cmp_to_key
    def cmp(a, b):
        da = a.get("date") or ""
        db = b.get("date") or ""
        if da != db:
            # No date sorts last; otherwise newer first
            if not da: return 1
            if not db: return -1
            return -1 if da > db else 1
        # Same date: real before approx
        aa, ab = a.get("approx", False), b.get("approx", False)
        if aa != ab:
            return 1 if aa else -1
        return 0
    entries.sort(key=cmp_to_key(cmp))
    return entries


def compute_members(users: list[str], movies: dict, latest: dict, rss_coverage: dict) -> list[dict]:
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
            "rss_coverage":  rss_coverage.get(u),
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
    movies, users, latest, diary = load_csvs()
    print(f"  {len(users)} members, {len(movies)} unique films from CSVs")

    # ── Step 2: Fetch RSS feeds to add recent ratings ──
    print("Polling RSS feeds...")
    rss_coverage, rss_diary = poll_rss(users, movies, latest)
    print(f"  {len(movies)} unique films after RSS merge")

    # ── Accumulate RSS history ──
    # Load previously saved RSS entries, merge in today's pull, save back.
    # This means every run adds to the history — we never lose old RSS data.
    rss_history: dict = {}
    if RSS_HISTORY_FILE.exists():
        rss_history = json.loads(RSS_HISTORY_FILE.read_text(encoding="utf-8"))

    # One-time migration: tag entries whose dates match known backfill commit dates.
    # These were written by backfill_from_top.py before source tagging was added.
    _COMMIT_DATES = {
        "2026-04-15", "2026-04-16", "2026-04-19", "2026-04-20", "2026-04-22",
        "2026-04-27", "2026-05-03", "2026-05-04", "2026-05-22", "2026-06-05",
        "2026-07-08", "2026-07-30", "2026-09-22", "2026-09-23",
    }
    _migration_count = 0
    for _entries in rss_history.values():
        for _e in _entries:
            if _e.get("source") is None and _e.get("date") in _COMMIT_DATES:
                _e["source"] = "backfill"
                _migration_count += 1
    if _migration_count:
        print(f"  Tagged {_migration_count} legacy entries as backfill")

    for username, rss_entries in rss_diary.items():
        existing = {(e["name"].lower().strip(), e["year"]): e for e in rss_history.get(username, [])}
        for e in rss_entries:
            key = (e["name"].lower().strip(), e["year"])
            # Tag live RSS entries so they're distinguishable from backfill
            e_tagged = {**e, "source": "rss"}
            existing_e = existing.get(key)
            # Real RSS entries always replace backfill entries; otherwise keep the more recent date
            should_replace = (
                key not in existing
                or (e["date"] and e["date"] > (existing_e.get("date") or ""))
                or (e["date"] and existing_e.get("source") == "backfill")
            )
            if should_replace:
                existing[key] = e_tagged
        rss_history[username] = list(existing.values())

    RSS_HISTORY_FILE.write_text(json.dumps(rss_history, indent=2), encoding="utf-8")
    total_rss = sum(len(v) for v in rss_history.values())
    print(f"  RSS history: {total_rss} total entries across all users")

    # Merge full RSS history into both the diary AND the movies dict.
    # Match by name only (ignore year) to handle CSV/TMDB year mismatches.
    # Date priority: CSV real date > RSS real date > backfill approximate date.
    for username, rss_entries in rss_history.items():
        # Build diary lookup by name_lower → entry (ignore year for matching)
        diary_by_name = {}
        for entry in diary.get(username, []):
            diary_by_name[entry["name"].lower().strip()] = entry

        for e in rss_entries:
            name_lower = e["name"].lower().strip()
            key = (name_lower, e["year"])
            is_backfill = e.get("source") == "backfill"

            # Update movies dict so group ratings are included
            if key not in movies:
                movies[key] = {"name": e["name"], "year": e["year"], "uri": e.get("uri", ""), "ratings": {}}
            movies[key]["ratings"][username] = e["rating"]
            if not movies[key]["uri"] and e.get("uri"):
                movies[key]["uri"] = e["uri"]

            if name_lower in diary_by_name:
                existing = diary_by_name[name_lower]
                existing["rating"] = e["rating"]
                existing_is_backfill = existing.get("source") == "backfill"
                if not is_backfill and e.get("date"):
                    # Real RSS date: overwrite backfill approximate date or fill missing date
                    if not existing.get("date") or existing_is_backfill:
                        existing["date"] = e["date"]
                        existing["source"] = "rss"
                elif is_backfill and not existing.get("date"):
                    # Backfill only fills in when there's truly no date
                    existing["date"] = e["date"]
            else:
                # Film not in CSV — add from rss_history with whatever date it has.
                # For real RSS entries this is an accurate date; for backfill entries
                # it is an approximate commit date, but still better than no date.
                diary.setdefault(username, []).append(e)
                diary_by_name[name_lower] = e

    # ── Deduplicate movies dict ──
    # Same film can end up under two keys if CSV year and rss_history year differ.
    # Group by lowercase name, merge any entries that share a name.
    by_name: dict = {}
    for key, info in movies.items():
        name_lower = key[0]
        if name_lower not in by_name:
            by_name[name_lower] = key
        else:
            # Prefer the most recent year (wide release > festival year)
            canonical_key = by_name[name_lower]
            canon_year = canonical_key[1] or 0
            this_year  = key[1] or 0
            if this_year > canon_year:
                # Swap — make this key the canonical one
                movies[key]["ratings"].update({
                    u: r for u, r in movies[canonical_key]["ratings"].items()
                    if u not in movies[key]["ratings"]
                })
                if not movies[key]["uri"]:
                    movies[key]["uri"] = movies[canonical_key]["uri"]
                by_name[name_lower] = key
            else:
                # Keep existing canonical, merge ratings in
                for username, rating in info["ratings"].items():
                    if username not in movies[canonical_key]["ratings"]:
                        movies[canonical_key]["ratings"][username] = rating
                if not movies[canonical_key]["uri"] and info["uri"]:
                    movies[canonical_key]["uri"] = info["uri"]

    # Remove the duplicate keys
    keys_to_delete = [k for k in movies if k[0] in by_name and k != by_name[k[0]]]
    for k in keys_to_delete:
        del movies[k]
    if keys_to_delete:
        print(f"  Deduplicated {len(keys_to_delete)} duplicate film entries")

    # ── Step 2b: Fetch TMDB metadata (year, genres, poster) ──
    print("Fetching TMDB metadata...")
    fetch_all_tmdb_data(movies)

    # ── Step 2c: Fetch global Letterboxd ratings ──
    print("Fetching Letterboxd global ratings...")
    lb_ratings = fetch_all_lb_ratings(movies)

    # ── Step 3 + 4: Compute and write the JSON files ──
    print("Writing data files...")
    write_json(DATA_DIR / "top.json",            compute_top(movies, lb_ratings))
    write_json(DATA_DIR / "controversial.json",   compute_controversial(movies, lb_ratings))
    write_json(DATA_DIR / "deviations.json",      compute_deviations(movies, lb_ratings))
    write_json(DATA_DIR / "members.json",         compute_members(users, movies, latest, rss_coverage))
    write_json(DATA_DIR / "diary.json",           compute_diary(diary, movies))
    write_json(DATA_DIR / "meta.json", {
        # datetime.now(timezone.utc) gets the current time in UTC.
        # .strftime() formats it as a readable string like "2026-04-17 08:00 UTC".
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    })

    print("Done.")
