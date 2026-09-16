---
name: leadgen-google-scraper
description: >-
  Scrape Google Maps business listings into a raw CSV for the outbound pipeline,
  via the Apify actor compass/crawler-google-places. Use when asked to scrape
  Google Maps, find firms on Google, run the Google scraper, or pull businesses
  of a given segment in a given suburb (for example "pull accounting businesses
  in Parramatta"). Collects firm name, website, phone and address only. It does
  not enrich and it does not contact anyone.
---

# leadgen-google-scraper

Pulls Google Maps business listings for a segment across one or more Australian
suburbs and writes a single raw CSV per run into the hand-off folder
(`PIPELINE_RAW_DIR`) for the enrichment stage to pick up.

## Step 0 — read the SOP first (do this before anything else)

Fetch the Notion page titled exactly **`leadgen-google-scraper`**, in
**Ermos HQ > 03 SOPs**.

- Proceed **only** if the page's **Status is `Approved`**.
- If Notion is unreachable, or the page is missing, or its Status is anything
  other than `Approved`: **stop** and tell the user which of those happened. Do
  not run the scraper.
- Note the SOP's **version** and pass it to the script via `--sop-version` so it
  is recorded in the run log.

The SOP is the source of truth for the operating values: search terms,
thresholds, spend caps and exclusion rules. Read them from the SOP and pass them
to the script as arguments. The script's built-in defaults and the checked-in
`config/` files are a fallback only, and must be kept in step with the SOP — if
they disagree with the SOP, the SOP wins and the fallback should be corrected.

## Inputs

| Input | Type | Notes |
|---|---|---|
| `segment` | one of `accounting`, `law`, `healthcare`, `finance`, `aged-care` | Required. |
| `search_terms` | list of strings | From the SOP. Falls back to `config/segments/<segment>.json`. |
| `suburbs` | list, each with a state | Required. Each entry is a suburb plus its state, e.g. `Parramatta, NSW`. |
| `max_places_per_search` | integer | Per the SOP. |
| `run_cap_usd` | number | Per the SOP. Hard ceiling on what the run may spend. |
| `mode` | `pilot` or `batch` | Required. See below. |

## Procedure

1. **Validate the inputs.** Confirm `segment` is one of the five permitted
   values, every suburb carries a state, and `mode` is `pilot` or `batch`. If
   anything is missing or malformed, ask the user rather than guessing.
2. **Pilot first — always.** Run a pilot of **one search term against one
   suburb, 20 places**, unless the user has explicitly said the pilot has
   already passed. Do not begin a batch off your own judgement.
3. **Run the script.**
   ```bash
   python3 scripts/google_scraper.py \
     --segment <segment> \
     --mode pilot \
     --suburb "<Suburb>,<STATE>" \
     --sop-version "<version from the SOP>"
   ```
   Add `--search-term` (repeatable), `--max-places-per-search` and
   `--run-cap-usd` to pass the SOP's values. `--dry-run` prints the actor input
   and exits without calling Apify or spending anything.
4. **Report back**, in this order:
   - rows written,
   - rows dropped **and why** (excluded, duplicate, unusable),
   - the cost Apify reported for the run,
   - the full output path.
5. **Stop.** Do not continue to a batch, and do not hand the file onward, until
   the user has reviewed the pilot and said to proceed.

## Limits

These are structural and hold regardless of what the SOP says:

- **Never enrich.** This skill collects what Google Maps lists and nothing more.
  Enrichment is a separate department and a separate skill.
- **Never email or contact anyone.** This skill produces a file; it does not
  reach out.
- **Never write outside `PIPELINE_RAW_DIR`.**
- **One output file per run**, plus its `.run.json` manifest. Never overwrite an
  existing file.
- **Never raise `run_cap_usd`** because a run hit the cap. Report that it capped
  and stop. Only raise it when the user supplies the new number themselves.
- **Every paid Apify add-on stays off** (contacts, social, reviews, images,
  place detail pages). Turning one on changes the cost profile and needs the
  user's agreement first.

## Environment

| Variable | Purpose |
|---|---|
| `APIFY_TOKEN` | Apify API token. Read from the environment only — never written to any file, and never echoed into the run log. |
| `PIPELINE_RAW_DIR` | Destination folder for the CSV and its manifest. |

The script exits with a clear message if either is missing.

## Output

One CSV per run, named `<segment>_<suburb-or-multi>_<YYYY-MM-DD>_<HHMM>.csv`,
with a fixed 18-column schema in this order:

`segment`, `search_term`, `suburb_searched`, `firm_name`, `website`, `domain`,
`phone`, `address`, `state`, `postcode`, `google_category`, `rating`,
`reviews_count`, `place_id`, `maps_url`, `scraped_at`, `source`, `run_id`

`scraped_at` is ISO 8601 in Sydney time; `source` is always `google_maps`.
Rows are de-duplicated within the run on `domain`, falling back to `place_id`
where a firm has no website.

A matching `.run.json` records the actor input, run ids, row counts, the SOP
version and the cost Apify reported.

## Files

- `scripts/google_scraper.py` — the runner.
- `config/segments/<segment>.json` — fallback search terms per segment.
- `config/exclusions.txt` — fallback exclusion terms, one per line, matched
  case-insensitively against the firm name and the Google category.
