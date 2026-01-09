import os
import json
import time
from typing import Any, Dict, Optional

import requests


def fetch_open_leaderboard_page(
    year: int,
    division: int,
    scaled: int,
    page: int,
    base_url: str = "https://c3po.crossfit.com/api/leaderboards/v2",
) -> Dict[str, Any]:
    """
    Fetch a single page of the CrossFit Open leaderboard.

    Mirrors the helper you prototyped in test.ipynb, but kept in a script.
    """
    url = f"{base_url}/competitions/open/{year}/leaderboards"

    params = {
        "division": division,
        "scaled": scaled,
        "page": page,
        "region": 0,
        "view": 0,
        "sort": 0,
    }

    r = requests.get(url, params=params)
    data = r.json()

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for page {page}: {data!r}")

    # Basic sanity checks (can be relaxed if needed)
    assert int(data["competition"]["year"]) == year
    assert int(data["competition"]["division"]) == division
    assert int(data["competition"]["scaled"]) == scaled

    return data


def get_total_pages_from_response(data: Dict[str, Any]) -> Optional[int]:
    """
    Try to infer total number of pages from the API response.

    This depends on how the API encodes pagination metadata.
    Adjust if you find a different field in the JSON.
    """

    pagination = data.get("pagination") or {}
    if "totalPages" in pagination:
        return int(pagination["totalPages"])

    # If we can't infer, return None and let the caller decide
    return None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

NONEXISTENT_CACHE_FILE = "nonexistent_divisions.json"

def _cache_key(year: int, division: int, scaled: int) -> str:
    return f"{year}-{division}-{scaled}"

def _load_nonexistent_cache() -> set[str]:
    if not os.path.exists(NONEXISTENT_CACHE_FILE):
        return set()
    try:
        with open(NONEXISTENT_CACHE_FILE, "r", encoding="utf-8") as f:
            items = json.load(f)
        if isinstance(items, list):
            return set(str(x) for x in items)
    except Exception:
        # If cache is corrupted, ignore it rather than crashing.
        return set()
    return set()


def _save_nonexistent_cache(cache: set[str]) -> None:
    with open(NONEXISTENT_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(cache), f, ensure_ascii=False, indent=2)


def page_filename(
    output_dir: str, year: int, division: int, scaled: int, page: int
) -> str:
    """
    Build a deterministic JSON filename for a given page & parameters.
    """
    return os.path.join(
        output_dir,
        f"open_{year}_division{division}_scaled{scaled}_page{page}.json",
    )


def download_all_pages(
    year: int = 2025,
    division: int = 1,
    scaled: int = 0,
    output_dir: str = "data",
    rate_limit_seconds: float = 0.2,
) -> None:
    """
    Download all leaderboard pages for the given parameters.

    - Checks if a JSON file for each page already exists; if so, skips.
    - Automatically determines total pages from the first response (if possible).
    - If the API reports zero pages for a given (year, division, scaled)
      combination, no file is saved and that combination is cached so
      subsequent runs skip API calls entirely.
    """
    # Check cache for previously-known nonexistent combinations
    cache = _load_nonexistent_cache()
    key = _cache_key(year, division, scaled)
    if key in cache:
        print(
            f"Skipping {year}/division{division}/scaled{scaled}: "
            "previously detected as having zero pages."
        )
        return

    ensure_dir(output_dir)

    # Fetch first page (or reuse if it already exists)
    first_path = page_filename(output_dir, year, division, scaled, 1)

    first_data = None
    if os.path.exists(first_path):
        print(f"Page 1 already exists at {first_path}, loading from disk...")
        with open(first_path, "r", encoding="utf-8") as f:
            first_data = json.load(f)
    else:
        print("Fetching page 1 from API...")
        first_data = fetch_open_leaderboard_page(year, division, scaled, 1)

    total_pages = get_total_pages_from_response(first_data)
    if total_pages is None:
        raise RuntimeError(
            "Could not determine total pages from API response. "
            "Inspect the JSON to find the pagination metadata and update "
            "get_total_pages_from_response()."
        )

    if total_pages == 0:
        print(
            f"API reports zero pages for {year}/division{division}/scaled{scaled}. "
            "No files will be saved; caching this result."
        )
        cache.add(key)
        _save_nonexistent_cache(cache)
        return

    # Now that we know there are pages, save page 1 if it's not already cached on disk
    if not os.path.exists(first_path):
        with open(first_path, "w", encoding="utf-8") as f:
            json.dump(first_data, f, ensure_ascii=False, indent=2)

    print(f"Total pages reported by API: {total_pages}")

    # Start from page 2, since page 1 handled above
    for page in range(2, total_pages + 1):
        path = page_filename(output_dir, year, division, scaled, page)

        if os.path.exists(path):
            print(f"Skipping page {page}, file already exists: {path}")
            continue

        print(f"Fetching page {page} from API...")
        data = fetch_open_leaderboard_page(year, division, scaled, page)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Be polite to the API
        time.sleep(rate_limit_seconds)

    print("Download complete.")


if __name__ == "__main__":
    # Basic example; adjust or replace with argparse if you want CLI args.
    root_dir = os.path.expanduser('~/data/crossfit_games_leaderboards')

    download_all_pages(
        year=2025,
        division=1,
        scaled=0,
        output_dir=root_dir,
    )


