import pandas as pd
import pytest

from pipeline import (
    UNKNOWN_EMAIL,
    clean_customers,
    deduplicate_customers,
    standardize_phone
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+1 (555) 123-4567", "15551234567"),
        ("555-987-6543", "5559876543"),
        ("(555) 333 4444", "5553334444"),
        ("+44 20 7123 1234", "442071231234"),
        ("1234567890", "1234567890"),
        ("1-800-555-DINO", "1800555"),
        ("Ext 444", "444"),
        (None, None),
        (float("nan"), None),
        ("", None),
        ("abc", None),
    ],
)
def test_standardize_phone(raw, expected):
    assert standardize_phone(raw) == expected
