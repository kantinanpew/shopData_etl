import re
import sqlite3
from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger, task

DEFUALT_SOURCE_DB = Path("data/shopdata.db")
DEFUALT_TARGET_DB = Path("data/analytics.db")

UNKNOWN_EMAIL = "unknown@domain.com"
BASE_CURRENCY = "USD"

CUSTOMER_COLUMNS = ["customer_id", "full_name",
                    "email", "phone", "signup_date"]
ORDER_COLUMNS = ["order_id", "customer_id",
                 "order_date", "total_amount", "currency", "status"]


def standardize_phone(phone):
    """Keep digits only. '+1 (555) 123-4567' -> '15551234567'. None if nothing left."""
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits or None


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per customer_id, keeping the most recent signup_date."""

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

    email = out["email"].astype("string").str.stripe()
    out["email"] = email.where(email.notna() & (
        email != ""), UNKNOWN_EMAIL).astype(object)

    out["full_name"] = out["full_name"].astype(
        "string").str.strip().astype(object)
    out["signup_date"] = pd.to_datetime(
        out["signup_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    return out[CUSTOMER_COLUMNS].sort_values("customer_id").reset_index(drop=True)


def filter_valid_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Drop orders with a NULL, zero or negative total_amount (system errors)."""
    amount = pd.to_numeric(df["total_amount"], errors="coerce")
    return df.loc[amount > 0].copy().reset_index(drop=True)
