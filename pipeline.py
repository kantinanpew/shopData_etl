import re
import sqlite3
from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger, task

DEFAULT_SOURCE_DB = Path("data/shopdata.db")
DEFAULT_TARGET_DB = Path("data/analytics.db")

UNKNOWN_EMAIL = "unknown@domain.com"
BASE_CURRENCY = "USD"

CUSTOMER_COLUMNS = ["customer_id", "full_name",
                    "email", "phone", "signup_date"]
ORDER_COLUMNS = ["order_id", "customer_id",
                 "order_date", "total_amount", "currency", "status"]


def standardize_phone(phone):
    """Keep digits only. '+1 (555) 123-4567' -> '15551234567'. None if nothing left"""
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits or None


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per customer_id, keeping the most recent signup_date"""

    out = df.copy()
    out["_signup_dt"] = pd.to_datetime(out["signup_date"], errors="coerce")
    out = (
        out.sort_values(["customer_id", "_signup_dt"], na_position="first")
        .drop_duplicates(subset="customer_id", keep="last")
        .drop(columns="_signup_dt")
        .reset_index(drop=True)
        # drop nat on top and keep the last row which is recent date

    )
    return out


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = deduplicate_customers(df)
    out["phone"] = out["phone"].map(standardize_phone)

    email = out["email"].astype("string").str.strip()
    out["email"] = email.where(email.notna() & (
        email != ""), UNKNOWN_EMAIL).astype(object)

    out["full_name"] = out["full_name"].astype(
        "string").str.strip().astype(object)
    out["signup_date"] = pd.to_datetime(
        out["signup_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    return out[CUSTOMER_COLUMNS].sort_values("customer_id").reset_index(drop=True)


def filter_valid_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Drop orders with a NULL, zero or negative total_amount (system errors)"""
    amount = pd.to_numeric(df["total_amount"], errors="coerce")
    return df.loc[amount > 0].copy().reset_index(drop=True)


def convert_to_usd(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    out = orders.copy()
    out["currency"] = out["currency"].astype("string").str.strip().str.upper()
    out["currency"] = out["currency"].where(
        out["currency"].notna() & (out["currency"] != ""), BASE_CURRENCY
    ).astype(object)

    r = rates.copy()
    r["currency"] = r["currency"].astype("string").str.strip().str.upper()
    r["date"] = r["date"].astype(str)

    # keep the lastest rate
    r = r.drop_duplicates(subset=["currency", "date"], keep="last")
    r = r.rename(columns={"date": "order_date", "rate_to_usd": "_rate"})

    merged = out.merge(r[["currency", "order_date", "_rate"]], on=[
                       "currency", "order_date"], how="left")
    is_usd = merged["currency"] == BASE_CURRENCY
    has_rate = merged["_rate"].notna()

    merged["fx_rate_used"] = 1.0
    merged.loc[has_rate & ~is_usd, "fx_rate_used"] = merged.loc[has_rate &
                                                                ~is_usd, "_rate"].astype(float)
    # assume that no-rate match are USD
    merged["fx_rate_assumed"] = (~is_usd & ~has_rate).astype(int)

    merged["total_amount"] = pd.to_numeric(
        merged["total_amount"], errors="coerce")
    merged["usd_amount"] = (merged["total_amount"] *
                            merged["fx_rate_used"]).round(2)

    return merged.drop(columns="_rate")


def clean_orders(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid amounts then convert what is left to USD"""
    out = filter_valid_orders(orders)
    out = convert_to_usd(out, rates)
    cols = ORDER_COLUMNS + ["usd_amount", "fx_rate_used", "fx_rate_assumed"]
    return out[cols].sort_values("order_id").reset_index(drop=True)


# prefect tasks
@task(name="extract_view", retries=2, retry_delay_seconds=2)
def extract_view(db_path: Path, view: str) -> pd.DataFrame:

    logger = get_run_logger()
    if not Path(db_path).exists:
        raise FileNotFoundError(f"Source database not found: {db_path}")
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(f"SELECT * FROM {view}", conn)
    except sqlite3.Error as exc:
        logger.error("Failed to read view %s from %s: %s", view, db_path, exc)
        raise
    logger.info("Extracted %d rows from %s", len(df), view)
    return df


@task(name="transform_customers")
def transform_customers(raw: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    missing = set(CUSTOMER_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"Customer data missing columns: {sorted(missing)}")
    cleaned = clean_customers(raw)

    logger.info(
        "Customers: %d raw -> %d unique (%d duplicates removed)",
        len(raw), len(cleaned), len(raw) - len(cleaned),
    )
    logger.info("Customers: %d emails filled with %s",
                int((cleaned["email"] == UNKNOWN_EMAIL).sum()), UNKNOWN_EMAIL)
    logger.info("Customers: %d phones still NULL after standardization",
                int(cleaned["phone"].isna().sum()))
    return cleaned


@task(name="transform_orders")
def transform_orders(raw: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Clean the order view, convert to USD and log what was dropped or assumed"""
    logger = get_run_logger()
    missing = set(ORDER_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"Order data missing columns: {sorted(missing)}")

    cleaned = clean_orders(raw, rates)

    dropped = len(raw) - len(cleaned)
    logger.info("Orders: %d raw -> %d valid (%d dropped for total_amount <= 0)",
                len(raw), len(cleaned), dropped)
    assumed = int(cleaned["fx_rate_assumed"].sum())
    if assumed:
        logger.warning(
            "Orders: %d non-USD orders had no exchange rate and were treated as USD", assumed)
    logger.info("Orders: total USD value = %.2f", cleaned["usd_amount"].sum())
    return cleaned


@task(name="load_to_sqlite")
def load_to_sqlite(customers: pd.DataFrame, orders: pd.DataFrame, target_db: Path) -> str:
    """Write dim_customers and fct_orders to the target database.
    Falls back to CSV if the SQLite write fails, as allowed by the spec"""
    logger = get_run_logger()
    target_db = Path(target_db)
    target_db.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sqlite3.connect(target_db) as conn:
            customers.to_sql("dim_customers", conn,
                             if_exists="replace", index=False)
            orders.to_sql("fct_orders", conn, if_exists="replace", index=False)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fct_orders_customer ON fct_orders(customer_id)")
        logger.info("Loaded %d customers and %d orders into %s",
                    len(customers), len(orders), target_db)
        return f"sqlite:{target_db}"
    except Exception as exc:
        logger.error("SQLite load failed (%s), falling back to CSV", exc)
        cust_csv = Path("data/clean_customers.csv")
        ord_csv = Path("data/clean_orders.csv")
        customers.to_csv(cust_csv, index=False)
        orders.to_csv(ord_csv, index=False)
        logger.warning("Wrote fallback CSVs: %s, %s", cust_csv, ord_csv)
        return f"csv:{cust_csv},{ord_csv}"


# flow
@flow(name="shopdata-etl")
def shopdata_etl(source_db: Path = DEFAULT_SOURCE_DB, target_db: Path = DEFAULT_TARGET_DB) -> str:
    """Extract the raw views, clean them, and load the result into analytics.db."""
    logger = get_run_logger()
    logger.info("Starting ETL: %s -> %s", source_db, target_db)

    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    rates = extract_view(source_db, "vw_exchange_rates")

    dim_customers = transform_customers(raw_customers)
    fct_orders = transform_orders(raw_orders, rates)

    destination = load_to_sqlite(dim_customers, fct_orders, target_db)
    logger.info("ETL complete, data written to %s", destination)
    return destination


if __name__ == "__main__":
    shopdata_etl()
