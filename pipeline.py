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
