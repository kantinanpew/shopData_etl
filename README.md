# ShopData ETL

Prefect pipeline that reads the raw customer/order/exchange-rate views out of
a SQLite source database, cleans and standardizes them, converts order
amounts to USD, and loads the result into an analytics database
(`dim_customers` + `fct_orders`) for downstream CLV reporting.

```
shopdata.db (raw views)              analytics.db (star schema)
┌─────────────────────┐              ┌─────────────────────┐
│ vw_raw_customers    │─ extract ──▶ │                     │
│ vw_raw_orders       │─ extract ──▶ │  dim_customers      │
│ vw_exchange_rates   │─ extract ──▶ │  fct_orders         │
└─────────────────────┘   transform  └─────────────────────┘
                           & load
```

## Requirements

- Python 3.12+
- A source SQLite database exposing `vw_raw_customers`, `vw_raw_orders`,
  `vw_exchange_rates`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Place the source database at `data/shopdata.db` before running anything.

## Usage

Run the pipeline:

```bash
python pipeline.py
```

Prefect 3 spins up a temporary local server on first run, so there is nothing
to configure. To watch the run in the Prefect UI instead:

```bash
prefect server start
```

Run against a different source/target path, e.g. a bigger or alternate
dataset, by calling the flow directly instead of the CLI entrypoint:

```bash
python3 -c "
from pathlib import Path
from pipeline import shopdata_etl
shopdata_etl(source_db=Path('data/shopdata_big.db'), target_db=Path('data/analytics.db'))
"
```

Run the test suite:

```bash
pytest -v
```

Query either database directly:

```bash
sqlite3 -header -column data/shopdata.db  < exploration.sql
sqlite3 -header -column data/analytics.db < clv_report.sql
```

## Project structure

```
pipeline.py        # cleaning functions + Prefect tasks/flow (see below)
clv_report.sql      # customer lifetime value report against analytics.db
exploration.sql     # ad-hoc queries against the raw source db
tests/test_pipeline.py
data/               # gitignored, holds the sqlite files at runtime
```

## Pipeline

`pipeline.py` keeps the cleaning rules as plain functions (DataFrame in,
DataFrame out) at the top of the file, separate from the Prefect tasks below
them, which only add logging, column checks and retries. This lets the tests
exercise the logic with plain DataFrames, without touching SQLite or Prefect.

**Flow:** `extract_view` (x3, one per source view) -> `transform_customers` /
`transform_orders` -> `load_to_sqlite`.

Loads use `if_exists="replace"`, so re-running the flow is idempotent and
never doubles up rows. If the SQLite write fails, `load_to_sqlite` falls back
to `data/clean_customers.csv` / `data/clean_orders.csv`.

## Data model

**`dim_customers`** — one row per `customer_id`

| column        | notes                                                |
| ------------- | ---------------------------------------------------- |
| `customer_id` | primary key                                          |
| `full_name`   | trimmed                                              |
| `email`       | null/blank source values become `unknown@domain.com` |
| `phone`       | digits only, `NULL` if nothing is left               |
| `signup_date` | `YYYY-MM-DD`, `NULL` if unparseable                  |

**`fct_orders`** — one row per valid order, indexed on `customer_id`

| column            | notes                                                                  |
| ----------------- | ---------------------------------------------------------------------- |
| `order_id`        |                                                                        |
| `customer_id`     | may reference a `customer_id` not present in `dim_customers` (orphans) |
| `order_date`      |                                                                        |
| `total_amount`    | original currency, always > 0                                          |
| `currency`        | trimmed, upper-cased, blank/null becomes `USD`                         |
| `status`          | not filtered on — carried through as-is for downstream use             |
| `usd_amount`      | `total_amount x fx_rate_used`, rounded to 2dp                          |
| `fx_rate_used`    | 1.0 for USD or when no rate was found                                  |
| `fx_rate_assumed` | 1 when a non-USD order had no matching rate and was treated as USD     |

## Transformation rules

| Table     | Rule                                                                         |
| --------- | ---------------------------------------------------------------------------- |
| customers | keep the row with the latest `signup_date` per `customer_id`                 |
| customers | phone stripped to digits only; empty result -> `NULL`                        |
| customers | null or blank email -> `unknown@domain.com`; names trimmed                   |
| orders    | drop rows where `total_amount` is null, zero or negative                     |
| orders    | currency trimmed and upper-cased; null or blank -> `USD`                     |
| orders    | `usd_amount = total_amount x rate`, rate matched on `(currency, order_date)` |

Note on phones: a phone that is `NULL` in the source stays `NULL`, and a
phone that strips down to an empty string also becomes `NULL` — both end up
as the same `NULL` case in `dim_customers`, so downstream code only has one
case to handle.

## Known data issues

These are properties of the source data the pipeline handles deliberately,
not bugs — flagged here so consumers of `analytics.db` know what to expect.

| Issue                                                                         | Handling                                                                                | Consequence                                                                                                                                       |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Duplicate customer rows (same `customer_id`, different `signup_date`/email)   | Keep the row with the latest `signup_date`; ties broken on source order                 | none — one row per customer in `dim_customers`                                                                                                    |
| Missing email/phone                                                           | Email defaults to `unknown@domain.com`; phone becomes `NULL`                            | downstream code should treat `unknown@domain.com` as "no real contact"                                                                            |
| Inconsistent phone formats (spaces, dashes, `Ext 444`, `1-800-555-DINO`, ...) | Stripped to digits only, per spec                                                       | a few values (e.g. `Ext 444` -> `444`) are not real numbers after stripping — worth raising with the business before dialing                      |
| Orders with `total_amount` <= 0                                               | Dropped, regardless of `status`                                                         | some are tagged `SYSTEM_ERROR`, but not all — filtering on amount catches the ones filtering on status would miss                                 |
| Missing currency/`order_date`                                                 | Blank currency -> `USD`; null `order_date` kept as-is                                   | —                                                                                                                                                 |
| Orphan orders (`customer_id` not present in `dim_customers`)                  | Kept in `fct_orders` rather than dropped                                                | invisible to `clv_report.sql` (it joins through `dim_customers`), so `SUM(usd_amount)` over `fct_orders` will not tie out to the CLV report total |
| Exchange rate coverage gaps                                                   | Orders outside the rate window are treated as `USD` and flagged via `fx_rate_assumed=1` | can distort totals for large non-USD orders that fall outside the window — see benchmark below for how big an effect this had at scale            |
| Non-completed orders (`CANCELLED`, `PENDING`) with positive amounts           | Kept; `status` is carried into `fct_orders`                                             | downstream BI should filter on `status` if it wants completed orders only                                                                         |

## Testing

```bash
pytest -v
```

22 unit tests cover the pure cleaning functions (`standardize_phone`,
`deduplicate_customers`, `clean_customers`, `filter_valid_orders`,
`convert_to_usd`, `clean_orders`) against dummy DataFrames — no database
required.

## Scale benchmark

Also ran the pipeline against `data/shopdata_big.db`, a larger synthetic
dataset on the same schema, to confirm the logic holds up past the ~12-row
sample used during development:

|           | raw       | after cleaning                                     |
| --------- | --------- | -------------------------------------------------- |
| customers | 1,064,589 | 1,043,827 (20,762 duplicate signups removed)       |
| orders    | 5,172,316 | 5,120,364 (51,952 dropped for `total_amount` <= 0) |

Same shape of issues as the sample dataset, just scaled up: 76,876 orders had
no matching exchange rate and were assumed USD, and 25,291 orders are
orphaned (see table above). Total `usd_amount` across `fct_orders` is
$354,913,254.67. Unlike the sample dataset, no single order's fx gap was
large enough to distort the top of the CLV ranking.

## Limitations / roadmap

- Forward-fill exchange rates and alert on gaps instead of silently assuming USD
- Add an unknown-customer row to `dim_customers` so orphan orders reconcile
- Move the cleaning rules to a separate module once there are more than a handful
- Swap pandas for chunked SQL reads if the source ever outgrows memory
