import os
import json
import time
import random
import argparse
from typing import Any, Dict, Optional

import requests
from tqdm import tqdm


def fetch_open_leaderboard_page(
    year: int,
    division: int,
    scaled: int,
    page: int,
    base_url: str = "https://c3po.crossfit.com/api/leaderboards/v2",
    max_retries: int = 5,
    initial_wait: float = 10.0,
    max_wait: float = 300.0,  # 5 minutes max wait
) -> Dict[str, Any]:
    """
    Fetch a single page of the CrossFit Open leaderboard with exponential backoff retry.

    Mirrors the helper you prototyped in test.ipynb, but kept in a script.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_wait: Initial wait time in seconds before first retry
        max_wait: Maximum wait time in seconds (caps exponential growth)
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

    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, params=params)
            
            # Success case
            if r.status_code == 200:
                data = r.json()
                # Basic sanity checks (can be relaxed if needed)
                assert int(data["competition"]["year"]) == year
                assert int(data["competition"]["division"]) == division
                assert int(data["competition"]["scaled"]) == scaled
                return data
            
            # Handle rate limiting and server errors
            if r.status_code in (403, 429, 500, 502, 503, 504):
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    wait_time = min(initial_wait * (2 ** attempt), max_wait)
                    # Add jitter (random 0-20% of wait time) to avoid thundering herd
                    jitter = wait_time * 0.2 * random.random()
                    total_wait = wait_time + jitter
                    
                    print(
                        f"HTTP {r.status_code} for page {page} (attempt {attempt + 1}/{max_retries + 1}). "
                        f"Waiting {total_wait:.1f}s before retry..."
                    )
                    time.sleep(total_wait)
                    continue
                else:
                    # Final attempt failed
                    raise RuntimeError(
                        f"HTTP {r.status_code} after {max_retries + 1} attempts for page {page}: {r.text[:200]}"
                    )
            else:
                # Other HTTP errors (4xx, etc.) - don't retry
                raise RuntimeError(f"HTTP {r.status_code} for page {page}: {r.text[:200]}")
                
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < max_retries:
                # Exponential backoff with jitter for network errors
                wait_time = min(initial_wait * (2 ** attempt), max_wait)
                jitter = wait_time * 0.2 * random.random()
                total_wait = wait_time + jitter
                
                print(
                    f"Network error for page {page} (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Waiting {total_wait:.1f}s before retry..."
                )
                time.sleep(total_wait)
                continue
            else:
                raise RuntimeError(
                    f"Network error after {max_retries + 1} attempts for page {page}: {e}"
                ) from e
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError(f"Failed to fetch page {page} after {max_retries + 1} attempts")


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
    
    Structure: comp=open/year=2025/division=1/scaled=0/page=1.json
    """
    path = os.path.join(
        output_dir,
        "comp=open",
        f"year={year}",
        f"division={division}",
        f"scaled={scaled}",
        f"page={page}.json",
    )
    return path


def download_all_pages(
    year: int = 2025,
    division: int = 1,
    scaled: int = 0,
    output_dir: str = "data",
    rate_limit_min_seconds: float = 2.0,
    rate_limit_max_seconds: float = 5.0,
    progress_bar: Optional[tqdm] = None,
) -> None:
    """
    Download all leaderboard pages for the given parameters.

    - Checks if a JSON file for each page already exists; if so, skips.
    - Automatically determines total pages from the first response (if possible).
    - If the API reports zero pages for a given (year, division, scaled)
      combination, no file is saved and that combination is cached so
      subsequent runs skip API calls entirely.
    
    Args:
        rate_limit_min_seconds: Minimum wait time in seconds (for uniform distribution)
        rate_limit_max_seconds: Maximum wait time in seconds (for uniform distribution)
        progress_bar: Optional tqdm progress bar for page-level progress
    """
    # Check cache for previously-known nonexistent combinations
    cache = _load_nonexistent_cache()
    key = _cache_key(year, division, scaled)
    if key in cache:
        if progress_bar is not None:
            progress_bar.set_description(
                f"year={year} div={division} scaled={scaled} [SKIPPED: zero pages]"
            )
            progress_bar.update(0)
        return

    ensure_dir(output_dir)

    # Fetch first page (or reuse if it already exists)
    first_path = page_filename(output_dir, year, division, scaled, 1)

    first_data = None
    if os.path.exists(first_path):
        if progress_bar is not None:
            progress_bar.set_description(
                f"year={year} div={division} scaled={scaled} [Loading page 1 from disk...]"
            )
        with open(first_path, "r", encoding="utf-8") as f:
            first_data = json.load(f)
    else:
        if progress_bar is not None:
            progress_bar.set_description(
                f"year={year} div={division} scaled={scaled} [Fetching page 1...]"
            )
        first_data = fetch_open_leaderboard_page(year, division, scaled, 1)

    total_pages = get_total_pages_from_response(first_data)
    if total_pages is None:
        raise RuntimeError(
            "Could not determine total pages from API response. "
            "Inspect the JSON to find the pagination metadata and update "
            "get_total_pages_from_response()."
        )

    if total_pages == 0:
        if progress_bar is not None:
            progress_bar.set_description(
                f"year={year} div={division} scaled={scaled} [Zero pages - caching]"
            )
            progress_bar.update(0)
        cache.add(key)
        _save_nonexistent_cache(cache)
        return

    # Now that we know there are pages, save page 1 if it's not already cached on disk
    if not os.path.exists(first_path):
        # Ensure the directory exists before writing
        os.makedirs(os.path.dirname(first_path), exist_ok=True)
        with open(first_path, "w", encoding="utf-8") as f:
            json.dump(first_data, f, ensure_ascii=False, indent=2)

    # Initialize or update progress bar
    if progress_bar is not None:
        progress_bar.reset(total=total_pages)
        progress_bar.set_description(
            f"year={year} div={division} scaled={scaled} [Pages: {total_pages}]"
        )
        progress_bar.update(1)  # Page 1 is done

    # Start from page 2, since page 1 handled above
    for page in range(2, total_pages + 1):
        path = page_filename(output_dir, year, division, scaled, page)

        if os.path.exists(path):
            if progress_bar is not None:
                progress_bar.update(1)
            continue

        if progress_bar is not None:
            progress_bar.set_description(
                f"year={year} div={division} scaled={scaled} [Page {page}/{total_pages}]"
            )

        data = fetch_open_leaderboard_page(year, division, scaled, page)
        # Ensure the directory exists before writing
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        if progress_bar is not None:
            progress_bar.update(1)

        # Be polite to the API with randomized wait time
        wait_time = random.uniform(rate_limit_min_seconds, rate_limit_max_seconds)
        time.sleep(wait_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download CrossFit Open leaderboard data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download years 2020-2025, divisions 1-10
  python fetch_leaderboard.py --year-start 2020 --year-end 2025 --division-start 1 --division-end 10
  
  # Download single year and division
  python fetch_leaderboard.py --year-start 2025 --year-end 2025 --division-start 1 --division-end 1
  
  # Download with custom output directory
  python fetch_leaderboard.py --year-start 2024 --year-end 2025 --division-start 1 --division-end 5 --output-dir ./my_data
  
  # Download only scaled option 0 (non-scaled)
  python fetch_leaderboard.py --year-start 2025 --year-end 2025 --scaled-start 0 --scaled-end 0
        """
    )
    
    parser.add_argument(
        '--year-start',
        type=int,
        default=2011,
        help='Start year (inclusive). Default: 2011'
    )
    parser.add_argument(
        '--year-end',
        type=int,
        default=2025,
        help='End year (inclusive). Default: 2025'
    )
    parser.add_argument(
        '--division-start',
        type=int,
        default=1,
        help='Start division (inclusive). Default: 1'
    )
    parser.add_argument(
        '--division-end',
        type=int,
        default=50,
        help='End division (inclusive). Default: 50'
    )
    parser.add_argument(
        '--scaled-start',
        type=int,
        default=0,
        help='Start scaled option (inclusive). Default: 0'
    )
    parser.add_argument(
        '--scaled-end',
        type=int,
        default=2,
        help='End scaled option (inclusive). Default: 2'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=os.path.expanduser('~/data/crossfit_games_leaderboards'),
        help='Output directory for downloaded JSON files. Default: ~/data/crossfit_games_leaderboards'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.year_start > args.year_end:
        parser.error("--year-start must be <= --year-end")
    if args.division_start > args.division_end:
        parser.error("--division-start must be <= --division-end")
    if args.scaled_start > args.scaled_end:
        parser.error("--scaled-start must be <= --scaled-end")
    if args.division_start < 1:
        parser.error("--division-start must be >= 1")
    if args.scaled_start < 0:
        parser.error("--scaled-start must be >= 0")
    if args.year_start < 2011:
        print(f"Warning: Year {args.year_start} is before 2011 (first Open year). Continuing anyway...")
    
    years = range(args.year_start, args.year_end + 1)
    divisions = range(args.division_start, args.division_end + 1)
    scaled_options = range(args.scaled_start, args.scaled_end + 1)
    
    # Calculate total combinations for outer progress bar
    total_combinations = len(list(years)) * len(list(divisions)) * len(list(scaled_options))
    
    print(f"Downloading leaderboard data:")
    print(f"  Years: {args.year_start} to {args.year_end} ({len(list(years))} years)")
    print(f"  Divisions: {args.division_start} to {args.division_end} ({len(list(divisions))} divisions)")
    print(f"  Scaled options: {args.scaled_start} to {args.scaled_end} ({len(list(scaled_options))} options)")
    print(f"  Total combinations: {total_combinations}")
    print(f"  Output directory: {args.output_dir}")
    print()
    
    # Outer progress bar for year/division/scaled combinations
    outer_bar = tqdm(
        total=total_combinations,
        desc="Processing combinations",
        position=0,
        leave=True,
    )
    
    for year in years:
        for division in divisions:
            for scaled in scaled_options:
                # Create inner progress bar for pages (total will be set later)
                page_bar = tqdm(
                    desc=f"year={year} div={division} scaled={scaled}",
                    position=1,
                    leave=False,
                    total=None,  # Will be set after we know total_pages
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} pages",
                )
                
                try:
                    download_all_pages(
                        year=year,
                        division=division,
                        scaled=scaled,
                        output_dir=args.output_dir,
                        progress_bar=page_bar,
                    )
                finally:
                    page_bar.close()
                
                # Update outer progress bar
                outer_bar.update(1)
    
    outer_bar.close()
    print("All downloads complete!")


