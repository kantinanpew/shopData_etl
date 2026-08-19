import pandas as pd
import pytest

from pipeline import (
    UNKNOWN_EMAIL,
    clean_customers,
    deduplicate_customers,
    standardize_phone,
    clean_orders,
    convert_to_usd,
    filter_valid_orders
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+1 (555) 123-4567", "15551234567"),
        ("555-987-6543", "5559876543"),
        ("(555) 333 4444", "5553334444"),
        ("+44 20 7123 1234", "442071231234"),
        ("1234567890", "1234567890"),
        ("1-800-555-DINO", "1800555"),  # letters stripe
        ("Ext 444", "444"),
        (None, None),
        (float("nan"), None),
        ("", None),
        ("abc", None),  # no values
    ],
)
def test_standardize_phone(raw, expected):
    assert standardize_phone(raw) == expected


def _customers():
    """Small fixture covering duplicates, a null email and an empty email"""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 1, 3],
            "full_name": ["Alice Smith", "Bob Jones", "Alice Smith", "Carl "],
            "email": ["alice@old.com", None, "alice@new.com", ""],
            "phone": ["+1 (555) 123-4567", "555-987-6543", "15551234567", None],
            "signup_date": ["2023-01-15", "2023-02-20", "2023-06-01", "2023-03-05"],
        }
    )


def test_deduplicate_keeps_most_recent_signup():
    out = deduplicate_customers(_customers())
    assert len(out) == 3
    alice = out.loc[out.customer_id == 1].iloc[0]
    assert alice.signup_date == "2023-06-01"
    assert alice.email == "alice@new.com"


def test_deduplicate_valid_date_beats_invalid_date():
    df = pd.DataFrame(
        {
            "customer_id": [1, 1],
            "full_name": ["A", "A"],
            "email": ["good@x.com", "bad@x.com"],
            "phone": [None, None],
            "signup_date": ["2023-01-01", "not-a-date"],
        }
    )
    out = deduplicate_customers(df)
    assert len(out) == 1
    assert out.iloc[0].email == "good@x.com"


def test_clean_customers_fills_missing_email_and_standardizes_phone():
    out = clean_customers(_customers()).set_index("customer_id")

    assert out.loc[2, "email"] == UNKNOWN_EMAIL      # unknown
    assert out.loc[3, "email"] == UNKNOWN_EMAIL      # unknown
    assert out.loc[1, "email"] == "alice@new.com"    # keep this
    assert out.loc[1, "phone"] == "15551234567"
    assert pd.isna(out.loc[3, "phone"])

    assert out.loc[3, "full_name"] == "Carl"


def test_filter_valid_orders_drops_zero_negative_and_null():
    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [1, 1, 1, 1, 1],
            "order_date": ["2023-05-01"] * 5,
            "total_amount": [100.0, 0.0, -50.0, None, 0.01],
            "currency": ["USD"] * 5,
            "status": ["COMPLETED"] * 5,
        }
    )
    out = filter_valid_orders(df)
    assert out.order_id.tolist() == [1, 5]
