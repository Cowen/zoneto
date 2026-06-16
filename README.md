# zoneto

Toronto development application intelligence platform.

Zoneto helps development professionals make informed decisions by providing structured intelligence on comparable planning applications, outcome patterns, and expected timelines. It uses ML models to rank and prioritize where the data supports it, and presents raw data where it doesn't.

**Target user:** Development firms doing site acquisition due diligence.

## Quick start

```bash
uv sync                    # install deps
just pipeline              # enrich → train → score
just serve                 # start API at http://localhost:8000
```

Open `http://localhost:8000/` in your browser to query comparable applications.

## API endpoints

```
GET  /health                                      # liveness check
GET  /ready                                       # readiness check (models + data loaded)
GET  /comps?ward=10&type=OZ&lat=43.65&lon=-79.38  # comparable applications
POST /score                                       # ML predictions (production-ready only)
POST /score?explain=true                          # predictions + SHAP explanations
```

### Example: comparable applications

```bash
curl "http://localhost:8000/comps?type=OZ&ward=10&lat=43.6532&lon=-79.3832&radius_m=500&years=5"
```

### Example: score an application

```bash
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"source": "dev_applications", "features": {"application_type": "OZ", "ward_number": "10"}}'
```

## Pipeline commands

```bash
just sync          # fetch all CKAN data sources
just aic           # scrape AIC for decision milestone dates
just aic-full      # fetch full AIC application records (replaces CKAN dev_applications)
just olt           # scrape OLT decisions
just enrich        # enrich raw data with spatial features and outcome labels
just train         # train ML models
just score         # batch inference
just summary       # score distributions
just pipeline      # enrich → train → score in one step
```

## Docker

```bash
just docker-build  # build Docker image
just docker-run    # start serving layer on port 8000
```

The Docker image includes only the serving layer — enriched Parquet files, models, and `static/`. Run the pipeline separately before building the image.

## Dev tasks

```bash
just test          # run pytest
just lint          # ruff check + ty check
just fmt           # ruff format
just regression    # performance regression tests (synthetic data, CI-safe)
```

## Data sources

All primary data comes from the City of Toronto CKAN open data portal
(`https://ckan0.cf.opendata.inter.prod-toronto.ca`) and the AIC ArcGIS FeatureServer.

| Source | Description |
|---|---|
| `dev_applications` | Development applications (CKAN, 2000–) |
| `aic_applications` | Live AIC application records (ArcGIS, replaces retired CKAN dataset) |
| `coa` | Committee of Adjustment applications |
| `permits_cleared` | Building permits (cleared) |
| `permits_active` | Building permits (active) |

Reference data: zoning boundaries, heritage register, secondary plans, ward profiles, MTSA boundaries, OLT decisions.

## ML models

`dev_days_to_decision` (survival) is the only predictive model — it is the sole model
that ever cleared the production quality bar. Five structured classifier/regressor
models were **deleted**: each failed the bar because of underlying training-data
limitations, not tunable modelling choices.

| Model | Type | Metric | Status |
|---|---|---|---|
| `dev_days_to_decision` | Survival (GradientBoostingSurvivalAnalysis) | C-index ≥ 0.65 | Production |
| `dev_applications_appealed` | Classifier | AUC 0.559 (< 0.65) | Deleted (survivorship-biased labels) |
| `coa_days_to_approval` | Regressor | R² −0.27 | Deleted (no signal in structured COA fields) |
| `dev_applications_approved` | Classifier | 97.3% class imbalance | Deleted (dataset frozen) |
| `coa_approved` | Classifier | AUC 0.535 @ 94% base rate | Deleted (needs text, not structured) |
| `permit_issuance_days` | Regressor | R² 0.039 | Deleted (queue depth not in open data) |

`desc_tfidf` (TF-IDF → SVD) remains as a feature extractor for the survival model.
