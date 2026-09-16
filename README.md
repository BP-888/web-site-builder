# b2b-outbound-engine

A Claude Code plugin holding the skills that run the B2B outbound pipeline, from
raw lead collection through to outreach.

## Skill naming

Every skill name carries its department prefix:

| Prefix | Department |
|---|---|
| `hq-` | Coordination and reporting |
| `leadgen-` | Finding and collecting raw leads |
| `enrichment-` | Filling out and verifying lead records |
| `outreach-` | Contacting leads |

## Skills

### `leadgen-google-scraper`

Scrapes Google Maps business listings into a raw CSV for the pipeline, via the
Apify actor `compass/crawler-google-places`. Triggers on requests such as
"scrape Google Maps", "find firms on Google", "run the Google scraper", or
"pull accounting businesses in Parramatta".

Writes one CSV per run to `PIPELINE_RAW_DIR` on a fixed 18-column schema, plus a
`.run.json` manifest recording the actor input, run ids, row counts, SOP version
and the cost Apify reported. It collects only what Google Maps lists — it does
not enrich and it does not contact anyone.

Operating values (search terms, thresholds, spend caps, exclusion rules) live in
the Notion SOP, which the skill reads before every run and which must be marked
`Approved`. The files under `config/` are fallbacks and must be kept in step
with the SOP.

## Conventions

- **Secrets come from the environment**, never from a file in this repository.
  `leadgen-google-scraper` reads `APIFY_TOKEN` and `PIPELINE_RAW_DIR`.
- **Scripts are Python standard library plus `requests`.**
- **Australian English** throughout.
- Skills that spend money carry an explicit run cap and always pilot before a
  batch.

## Layout

```
.claude-plugin/plugin.json
skills/
└── leadgen-google-scraper/
    ├── SKILL.md
    ├── scripts/google_scraper.py
    └── config/
        ├── exclusions.txt
        └── segments/accounting.json
```
