# Sources — Zoneto

## Source Protocol (`base.py`)

Any data source must satisfy this `@runtime_checkable` protocol:
- `name: str`
- `fetch() -> pl.DataFrame` — must contain at least `year` (Int32) and `source_name` (String)

## CKANConfig (`models.py`)

| Field | Type | Default |
|---|---|---|
| `dataset_id` | `str` | required |
| `access_mode` | `Literal["datastore", "bulk_csv"]` | required |
| `year_start` | `int` | 2015 |
| `year_column` | `str` | `"application_date"` — parsed to `pl.Date` for year extraction |
