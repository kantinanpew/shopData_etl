# ShopData ETL

Reads the raw views in
`shopdata.db`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Needs Python 3.12+

## Running it

```bash
python pipeline.py
pytest -v
```

Prefect 3 starts a temporary local server on the first run, so there is
nothing to configure. To watch the run in the UI instead:

```bash
prefect server start
```

## Data exploration

What I found:

**Duplicate customers.** Customers 1 and 2 each appear twice with different
signup dates and emails. Keeping the row with the latest `signup_date`, as the
spec says. Where two rows share the same `signup_date` I break the tie on the
last row in source order, since there is nothing better to sort on.

**Missing contact info.** 2 null emails, 2 null phones. Customer 8 has neither.
Null and blank emails become `unknown@domain.com`.

**Phone formats are all over the place.** 8 of 12 rows carry spaces, dashes,
brackets or letters, e.g. `Ext 444` and `1-800-555-DINO`. Stripping to digits
fixes most of them, but those two collapse to `444` and `1800555`, which are
not real numbers. The spec only asks for the strip, so that is what the
pipeline does, but this is worth raising with the business before anyone dials
these.

**Bad amounts.** 3 orders with `total_amount` at or below zero. Two are tagged
SYSTEM_ERROR, but one is a 0.0 marked COMPLETED, so filtering on status alone
would miss it. Filtering on the amount is the right call here.

**Missing currency and date.** 2 orders with null currency, 1 with null
`order_date`. Blank currency is treated as USD per the spec.

**Orphan orders.** Orders 106 and 118 point at `customer_id` 99, which does not
exist in the customers view. I keep them in `fct_orders` rather than dropping
revenue on the floor, but they are invisible to the CLV report because that
joins through `dim_customers`. Consequence to be aware of: `SUM(usd_amount)`
over `fct_orders` will not tie out to the CLV report total. If reconciliation
matters more than completeness, the fix is either an unknown-customer row in
`dim_customers` or dropping orphans at load time.

**Exchange rate gap.** Rates only cover 2023-05-01 to 2023-05-05, but orders
run to 2023-05-14. 5 non-USD orders fall outside that window and get treated as
USD per the spec. This is not harmless: order 115 is 25,000 JPY and becomes
$25,000, which puts that customer at the top of the CLV ranking on what is
really a ~$180 order. The pipeline follows the spec but flags every affected
row so the number is at least traceable. In a real pipeline I would forward
fill the last known rate and alert when the gap exceeds a threshold.

**Non-completed orders.** One CANCELLED and one PENDING order with positive
amounts. The spec only filters on amount, so they stay, and `status` is carried
into `fct_orders` so BI can filter downstream if they want completed orders
only.

The SQL files run against the two databases:

```bash
sqlite3 -header -column data/shopdata.db  < exploration.sql
sqlite3 -header -column data/analytics.db < clv_report.sql
```

If writing `analytics.db` fails, the load task falls back to
`data/clean_customers.csv` and `data/clean_orders.csv`.

## How it is put together

All the cleaning rules are plain functions (DataFrame in, DataFrame out) at the
top of `pipeline.py`. The Prefect tasks below them only add logging, column
checks and retries. Keeping them apart means the tests can pass dummy
DataFrames straight into the logic without touching SQLite.

Flow: `extract_view` (x3) -> `transform_customers` / `transform_orders` -> `load_to_sqlite`.

| Table     | Rules applied                                                      |
| --------- | ------------------------------------------------------------------ |
| customers | keep the row with the latest signup_date per customer_id           |
| customers | phone stripped to non-digits removed, empty result becomes NULL    |
| customers | null or blank email becomes `unknown@domain.com`, names trimmed    |
| orders    | drop rows where total_amount is null, zero or negative             |
| orders    | currency trimmed and upper-cased, null or blank becomes USD        |
| orders    | usd_amount = total_amount x rate matched on (currency, order_date) |

Note on phones: a phone that is null in the source stays null, and a phone that
strips down to an empty string also becomes null. Both end up as NULL in
`dim_customers`, so downstream code only has one case to handle.

`fct_orders` carries two extra columns: `fx_rate_used` and `fx_rate_assumed`
(1 when a non-USD order had no matching rate and was treated as USD).

Load uses `if_exists="replace"`, so re-running the flow is safe and will not
double up rows.

## What I would do differently with more time

- Forward fill exchange rates and alert on gaps instead of silently assuming USD
- Add an unknown-customer row to `dim_customers` so orphan orders reconcile
- Move the cleaning rules to a separate module once there are more than a handful
- Swap pandas for chunked SQL if the source ever outgrows memory
