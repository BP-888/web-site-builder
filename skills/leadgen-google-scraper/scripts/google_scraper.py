#!/usr/bin/env python3
"""Scrape Google Maps businesses via the Apify actor compass/crawler-google-places.

Starts one actor run per suburb through the Apify REST API, polls each run to
completion, normalises the results to a fixed 18-column schema and writes a
single CSV per script run into PIPELINE_RAW_DIR, alongside a matching
.run.json manifest.

The synchronous run-and-get-items endpoint is deliberately not used: it times
out on runs of any real size.

Requires: Python 3.9+ and requests. No other third-party dependencies.

Credentials are read from the environment only. Never hard-code a token.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("ERROR: the 'requests' package is required. Install it with: pip install requests")

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

ACTOR_ID = "compass~crawler-google-places"
API_BASE = "https://api.apify.com/v2"
SOURCE = "google_maps"
SYDNEY_TZ = "Australia/Sydney"

# The fixed output contract. Order matters; do not reorder or extend without
# agreeing the change downstream first.
COLUMNS = [
    "segment",
    "search_term",
    "suburb_searched",
    "firm_name",
    "website",
    "domain",
    "phone",
    "address",
    "state",
    "postcode",
    "google_category",
    "rating",
    "reviews_count",
    "place_id",
    "maps_url",
    "scraped_at",
    "source",
    "run_id",
]

VALID_SEGMENTS = ["accounting", "law", "healthcare", "finance", "aged-care"]
VALID_MODES = ["pilot", "batch"]

# Script fallbacks only. The authoritative values live in the Notion SOP and
# should be passed in explicitly on every real run.
DEFAULT_MAX_PLACES_PER_SEARCH = 20
DEFAULT_RUN_CAP_USD = 5.0
PILOT_MAX_PLACES = 20

POLL_INTERVAL_SECONDS = 10
POLL_TIMEOUT_SECONDS = 3600
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT", "TIMING-OUT"}

HERE = os.path.dirname(os.path.abspath(__file__))
SKILL_ROOT = os.path.dirname(HERE)
DEFAULT_EXCLUSIONS = os.path.join(SKILL_ROOT, "config", "exclusions.txt")
SEGMENTS_DIR = os.path.join(SKILL_ROOT, "config", "segments")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def fail(message):
    """Exit with a clear, single-line error on stderr."""
    sys.exit("ERROR: {}".format(message))


def now_sydney():
    """Return (iso8601_string, tz_label). Falls back to UTC if tzdata is absent."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(SYDNEY_TZ)).isoformat(), SYDNEY_TZ
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat(), "UTC (Australia/Sydney unavailable)"


def slugify(value):
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    return slug or "unknown"


def derive_domain(website):
    """Lowercase the website, strip the scheme, any www. prefix and the path."""
    if not website:
        return ""
    host = str(website).strip().lower()
    host = re.sub(r"^[a-z][a-z0-9+.\-]*://", "", host)
    host = host.split("/")[0].split("?")[0].split("#")[0]
    host = host.split("@")[-1]   # drop any embedded credentials
    host = host.split(":")[0]    # drop any port
    if host.startswith("www."):
        host = host[4:]
    return host


def pick(item, *keys):
    """Return the first non-empty value among the given keys.

    The actor's output field names are matched tolerantly because the output
    schema was not verifiable at build time.
    """
    for key in keys:
        value = item.get(key)
        if value not in (None, "", [], {}):
            return value
    return ""


def as_text(value):
    if value in (None, "", [], {}):
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(as_text(v) for v in value if v not in (None, ""))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value).strip()


def load_exclusions(path):
    """Load exclusion terms, one per line. Blank lines and # comments ignored."""
    if not os.path.exists(path):
        fail("exclusions file not found: {}".format(path))
    terms = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                terms.append(line.lower())
    return terms


def load_segment_terms(segment):
    """Load the fallback search terms for a segment from config/segments/."""
    path = os.path.join(SEGMENTS_DIR, "{}.json".format(segment))
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return [t for t in data.get("search_terms", []) if t]


def parse_suburb(raw):
    """Parse 'Suburb,STATE' into (suburb, state)."""
    if "," not in raw:
        fail("suburb '{}' must be given as 'Suburb,STATE' (for example 'Parramatta,NSW')".format(raw))
    suburb, state = raw.split(",", 1)
    suburb, state = suburb.strip(), state.strip().upper()
    if not suburb or not state:
        fail("suburb '{}' must be given as 'Suburb,STATE' (for example 'Parramatta,NSW')".format(raw))
    return suburb, state


# --------------------------------------------------------------------------
# Apify REST API
# --------------------------------------------------------------------------

def build_actor_input(search_terms, suburb, state, max_places):
    """Build the actor input JSON.

    Every paid add-on is switched off. maxTotalChargeUsd is NOT part of this
    payload: it is a platform-level parameter passed on the request query
    string by start_run().
    """
    return {
        "searchStringsArray": list(search_terms),
        "locationQuery": "{} {} Australia".format(suburb, state),
        "countryCode": "au",
        "language": "en",
        "skipClosedPlaces": True,
        "maxCrawledPlacesPerSearch": int(max_places),
        # Paid add-ons: all off.
        "reviews": 0,
        "images": 0,
        "contacts": False,
        "social": False,
        "scrapePlaceDetailPage": False,
    }


def start_run(token, actor_input, cap_usd, timeout=60):
    """Start an actor run. The spend cap rides on the query string."""
    url = "{}/acts/{}/runs".format(API_BASE, ACTOR_ID)
    params = {"token": token, "maxTotalChargeUsd": cap_usd}
    response = requests.post(url, params=params, json=actor_input, timeout=timeout)
    if response.status_code >= 400:
        fail("Apify refused the run ({}): {}".format(response.status_code, response.text[:500]))
    return response.json().get("data", {})


def poll_run(token, run_id, timeout=POLL_TIMEOUT_SECONDS):
    """Poll a run until it reaches a terminal status. Returns the run object."""
    url = "{}/actor-runs/{}".format(API_BASE, run_id)
    deadline = time.time() + timeout
    while True:
        response = requests.get(url, params={"token": token}, timeout=60)
        if response.status_code >= 400:
            fail("could not read run {} ({}): {}".format(run_id, response.status_code, response.text[:300]))
        run = response.json().get("data", {})
        status = run.get("status")
        if status in TERMINAL_STATUSES:
            return run
        if time.time() > deadline:
            fail("run {} did not finish within {}s (last status: {})".format(run_id, timeout, status))
        print("    ... {} [{}]".format(run_id, status), flush=True)
        time.sleep(POLL_INTERVAL_SECONDS)


def fetch_items(token, dataset_id, timeout=300):
    """Fetch every item from a run's default dataset."""
    url = "{}/datasets/{}/items".format(API_BASE, dataset_id)
    response = requests.get(
        url,
        params={"token": token, "format": "json", "clean": "true"},
        timeout=timeout,
    )
    if response.status_code >= 400:
        fail("could not read dataset {} ({}): {}".format(dataset_id, response.status_code, response.text[:300]))
    items = response.json()
    return items if isinstance(items, list) else []


def run_cost(run):
    """The cost Apify reported for a run, in USD."""
    for key in ("usageTotalUsd", "costUsd"):
        value = run.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

def normalise_item(item, segment, suburb, state, fallback_term, scraped_at, run_id):
    """Map one actor result onto the fixed 18-column schema."""
    website = as_text(pick(item, "website", "webSite", "url_website"))
    return {
        "segment": segment,
        "search_term": as_text(pick(item, "searchString", "searchTerm", "query")) or fallback_term,
        "suburb_searched": "{} {}".format(suburb, state),
        "firm_name": as_text(pick(item, "title", "name", "placeName")),
        "website": website,
        "domain": derive_domain(website),
        "phone": as_text(pick(item, "phone", "phoneUnformatted", "internationalPhoneNumber")),
        "address": as_text(pick(item, "address", "street", "formattedAddress")),
        "state": as_text(pick(item, "state", "administrativeArea")) or state,
        "postcode": as_text(pick(item, "postalCode", "postcode", "zip")),
        "google_category": as_text(pick(item, "categoryName", "category", "categories")),
        "rating": as_text(pick(item, "totalScore", "rating", "score")),
        "reviews_count": as_text(pick(item, "reviewsCount", "reviewCount", "userRatingCount")),
        "place_id": as_text(pick(item, "placeId", "place_id", "fid", "cid")),
        "maps_url": as_text(pick(item, "url", "mapsUrl", "googleMapsUrl", "placeUrl")),
        "scraped_at": scraped_at,
        "source": SOURCE,
        "run_id": run_id,
    }


def is_excluded(row, exclusions):
    """True if the firm name or category matches an exclusion term."""
    haystack = "{} {}".format(row["firm_name"], row["google_category"]).lower()
    for term in exclusions:
        if term in haystack:
            return term
    return None


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def resolve_output_paths(raw_dir, segment, suburbs):
    """Build the CSV and manifest paths. Never overwrites an existing file."""
    if len(suburbs) == 1:
        location = slugify(suburbs[0][0])
    else:
        location = "multi"
    try:
        stamp = datetime.now(ZoneInfo(SYDNEY_TZ))
    except Exception:
        stamp = datetime.now()
    base = "{}_{}_{}_{}".format(
        slugify(segment), location, stamp.strftime("%Y-%m-%d"), stamp.strftime("%H%M")
    )
    csv_path = os.path.join(raw_dir, base + ".csv")
    manifest_path = os.path.join(raw_dir, base + ".run.json")
    for path in (csv_path, manifest_path):
        if os.path.exists(path):
            fail("refusing to overwrite an existing file: {}".format(path))
    return csv_path, manifest_path


def write_csv(path, rows):
    # 'x' mode fails rather than overwriting, even under a race.
    with open(path, "x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_manifest(path, payload):
    with open(path, "x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps businesses via Apify into a single CSV."
    )
    parser.add_argument("--segment", required=True, choices=VALID_SEGMENTS)
    parser.add_argument("--mode", required=True, choices=VALID_MODES)
    parser.add_argument(
        "--search-term", action="append", dest="search_terms", default=[],
        help="Repeatable. Falls back to config/segments/<segment>.json if omitted.",
    )
    parser.add_argument(
        "--suburb", action="append", dest="suburbs", default=[], required=True,
        help="Repeatable. Format: 'Suburb,STATE' (for example 'Parramatta,NSW').",
    )
    parser.add_argument("--max-places-per-search", type=int, default=DEFAULT_MAX_PLACES_PER_SEARCH)
    parser.add_argument("--run-cap-usd", type=float, default=DEFAULT_RUN_CAP_USD)
    parser.add_argument("--sop-version", default="", help="SOP version recorded in the run log.")
    parser.add_argument("--exclusions", default=DEFAULT_EXCLUSIONS)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the actor input and exit without calling Apify or spending anything.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    # --- validate inputs -------------------------------------------------
    if args.max_places_per_search < 1:
        fail("--max-places-per-search must be 1 or more")
    if args.run_cap_usd <= 0:
        fail("--run-cap-usd must be greater than 0")

    suburbs = [parse_suburb(s) for s in args.suburbs]
    search_terms = args.search_terms or load_segment_terms(args.segment)
    if not search_terms:
        fail(
            "no search terms given and no fallback found at "
            "config/segments/{}.json".format(args.segment)
        )

    max_places = args.max_places_per_search
    if args.mode == "pilot":
        # A pilot is deliberately one term against one suburb.
        search_terms = search_terms[:1]
        suburbs = suburbs[:1]
        max_places = min(max_places, PILOT_MAX_PLACES)

    exclusions = load_exclusions(args.exclusions)

    if args.dry_run:
        for suburb, state in suburbs:
            print(json.dumps(build_actor_input(search_terms, suburb, state, max_places), indent=2))
        print("\nDry run: nothing was sent to Apify and nothing was spent.")
        print("Spend cap that would be sent on the query string: "
              "maxTotalChargeUsd={}".format(args.run_cap_usd))
        return 0

    # --- environment -----------------------------------------------------
    token = os.environ.get("APIFY_TOKEN", "").strip()
    raw_dir = os.environ.get("PIPELINE_RAW_DIR", "").strip()
    if not token:
        fail("APIFY_TOKEN is not set in the environment. Export it and try again.")
    if not raw_dir:
        fail("PIPELINE_RAW_DIR is not set in the environment. Export it and try again.")
    if not os.path.isdir(raw_dir):
        fail("PIPELINE_RAW_DIR is not a directory: {}".format(raw_dir))

    scraped_at, tz_label = now_sydney()
    csv_path, manifest_path = resolve_output_paths(raw_dir, args.segment, suburbs)

    # --- run -------------------------------------------------------------
    rows = []
    seen = {}
    dropped = {"excluded": 0, "duplicate": 0, "no_name": 0, "no_identifier": 0}
    excluded_by_term = {}
    runs = []
    spent = 0.0
    fetched = 0

    for suburb, state in suburbs:
        remaining = round(args.run_cap_usd - spent, 4)
        if remaining <= 0:
            print("Spend cap of ${} reached; skipping {} {}.".format(args.run_cap_usd, suburb, state))
            break

        actor_input = build_actor_input(search_terms, suburb, state, max_places)
        print("Starting run for {} {} (cap ${} remaining)...".format(suburb, state, remaining))

        started = start_run(token, actor_input, remaining)
        run_id = started.get("id")
        if not run_id:
            fail("Apify did not return a run id for {} {}".format(suburb, state))

        run = poll_run(token, run_id)
        status = run.get("status")
        cost = run_cost(run)
        spent = round(spent + cost, 4)

        items = []
        dataset_id = run.get("defaultDatasetId")
        if status == "SUCCEEDED" and dataset_id:
            items = fetch_items(token, dataset_id)
        elif status != "SUCCEEDED":
            print("  WARNING: run {} finished as {}; keeping whatever it produced.".format(run_id, status))
            if dataset_id:
                items = fetch_items(token, dataset_id)

        fetched += len(items)
        runs.append({
            "run_id": run_id,
            "suburb": suburb,
            "state": state,
            "status": status,
            "actor_input": actor_input,
            "max_total_charge_usd_sent": remaining,
            "cost_usd_reported": cost,
            "items_fetched": len(items),
        })
        print("  {} finished as {}: {} items, ${} reported.".format(run_id, status, len(items), cost))

        for item in items:
            row = normalise_item(
                item, args.segment, suburb, state, search_terms[0], scraped_at, run_id
            )
            if not row["firm_name"]:
                dropped["no_name"] += 1
                continue
            term = is_excluded(row, exclusions)
            if term:
                dropped["excluded"] += 1
                excluded_by_term[term] = excluded_by_term.get(term, 0) + 1
                continue
            key = row["domain"] or ("place:" + row["place_id"] if row["place_id"] else "")
            if not key:
                # No domain and no place_id: nothing to dedupe on.
                dropped["no_identifier"] += 1
                continue
            if key in seen:
                dropped["duplicate"] += 1
                continue
            seen[key] = True
            rows.append(row)

    # --- write -----------------------------------------------------------
    write_csv(csv_path, rows)
    write_manifest(manifest_path, {
        "segment": args.segment,
        "mode": args.mode,
        "sop_version": args.sop_version,
        "search_terms": search_terms,
        "suburbs": ["{} {}".format(s, st) for s, st in suburbs],
        "max_places_per_search": max_places,
        "run_cap_usd": args.run_cap_usd,
        "scraped_at": scraped_at,
        "timezone": tz_label,
        "source": SOURCE,
        "runs": runs,
        "rows_fetched": fetched,
        "rows_written": len(rows),
        "rows_dropped": dropped,
        "rows_dropped_by_exclusion_term": excluded_by_term,
        "total_cost_usd_reported": spent,
        "output_csv": csv_path,
    })

    # --- report ----------------------------------------------------------
    print("\nRows fetched:  {}".format(fetched))
    print("Rows written:  {}".format(len(rows)))
    print("Rows dropped:  {} (excluded {}, duplicates {}, no name {}, no identifier {})".format(
        sum(dropped.values()), dropped["excluded"], dropped["duplicate"],
        dropped["no_name"], dropped["no_identifier"]))
    if excluded_by_term:
        for term, count in sorted(excluded_by_term.items(), key=lambda kv: -kv[1]):
            print("  excluded by '{}': {}".format(term, count))
    print("Cost reported by Apify: ${}".format(spent))
    print("Output:   {}".format(csv_path))
    print("Manifest: {}".format(manifest_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
