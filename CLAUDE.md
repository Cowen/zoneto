# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-05-21 -->

## Purpose

Zoneto is a development application intelligence platform for Toronto. It provides development professionals with structured data on comparable planning applications, outcome patterns, and expected timelines — using ML models to rank and prioritize where the data supports it, and presenting raw data where it doesn't.

**Target user:** Development firms doing site acquisition due diligence.

## Quick Start

```bash
uv sync && just test && just lint
just sync        # fetch all sources -> data/
just status      # always run before analyzing model results
just aic         # scrape AIC (prerequisite for survival model in enrich)
just olt         # scrape Ontario Land Tribunal decisions
just enrich      # enrich raw parquet with spatial + outcome labels
just train && just score
just serve       # FastAPI on port 8000
just pipeline    # enrich -> train -> score in sequence
just regression  # CI-safe regression tests (synthetic data)
just regression-integration  # real enriched data — not CI-safe
```

**COA freshness caveat:** The `coa` source always shows data concentrated in 2022 — this is the complete CKAN picture (city only publishes closed CSVs for 2022–2023). A 2022-heavy distribution is not a sign of stale data.

## Architecture

```
src/zoneto/
  cli.py             Typer CLI
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/           Source protocol, CKAN/AIC/OLT scrapers, registry
  analytics/         Enrichment, feature extraction, training, scoring
  api/               FastAPI app, routes, comps, narrator, site context
static/
  index.html         Frontend: address search, /evaluate, comps
Dockerfile
```

## Registry (`sources/registry.py`)

| Key | Dataset | Mode | year_start | year_column |
|---|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 | default |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 | default |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2018 | default |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |
| `aic_applications` | AIC ArcGIS FeatureServer | AICSource | — | — |

`dev_applications` is marked retired in CKAN but still actively updated. `aic_applications` (`COTGEO_IBMS_AIC_POINT`) is the live alternative.

## Invariants

- Python ≥ 3.13.
- Column names normalized to snake_case; duplicates get `_2`/`_3` suffixes.
- Date columns parsed to `pl.Date` best-effort; unrecognizable formats stay String.
- `year` derived from `year_column` only if Date-parsed; otherwise 0.
- `fetch()` keeps `year == 0` or `year >= year_start`.
- Storage is full-replace per source (rmtree + rewrite). Uses native polars Parquet writer — pyarrow creates flat files, not Hive dirs.
- Tests mock all HTTP via `pytest-httpx`; no network in CI.
- CKAN base URL: `https://ckan0.cf.opendata.inter.prod-toronto.ca`


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:7510c1e2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
