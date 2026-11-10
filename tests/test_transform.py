from decimal import Decimal

import pandas as pd
import pytest

from pipeline.transform import SchemaError, make_id, standardize


def orders(**overrides):
    row = {"Order_ID": "EM-1", "Client": " ahmed  ali ", "Item": "tomato seeds", "Qty": "10",
           "Unit Price": "150.005", "Location": "cairo", "Date": "2026-10-12 10:00"}
    row.update(overrides)
    return pd.DataFrame([row])


def test_columns_are_mapped_and_text_is_cleaned():
    clean = standardize(orders(), "email").clean
    row = clean.iloc[0]
    assert row.source_id == "EM-1"
    assert row.customer_name == "Ahmed Ali"
    assert row["product"] == "Tomato Seeds"
    assert row.city == "Cairo"


def test_money_is_exact_decimal():
    row = standardize(orders(), "email").clean.iloc[0]
    assert row.unit_price == Decimal("150.01")
    assert row.total_amount == Decimal("1500.10")


def test_dates_are_converted_from_cairo_to_utc():
    row = standardize(orders(), "email").clean.iloc[0]
    assert str(row.order_date) == "2026-10-12 07:00:00"


@pytest.mark.parametrize("override, reason", [
    ({"Qty": "abc"}, "invalid_quantity"),
    ({"Qty": "0"}, "non_positive_quantity"),
    ({"Qty": "2.5"}, "non_integer_quantity"),
    ({"Unit Price": "-5"}, "negative_price"),
    ({"Item": None}, "missing_product"),
    ({"Order_ID": None}, "missing_source_id"),
    ({"Date": "not a date"}, "invalid_order_date"),
    ({"Date": "2099-01-01"}, "future_order_date"),
])
def test_bad_rows_are_rejected_with_a_reason(override, reason):
    result = standardize(orders(**override), "email")
    assert result.clean.empty
    assert reason in result.rejected.iloc[0]["reason"]


def test_total_mismatch_is_rejected():
    df = orders(**{"Unit Price": "150"})
    df["total"] = "999"
    result = standardize(df, "email")
    assert "total_mismatch" in result.rejected.iloc[0]["reason"]


def test_duplicates_in_one_batch_keep_the_latest():
    df = pd.concat([orders(Qty="1", updated_at="2026-10-12 10:00"),
                    orders(Qty="7", updated_at="2026-10-12 11:00")])
    result = standardize(df, "database")
    assert len(result.clean) == 1
    assert result.duplicates == 1
    assert result.clean.iloc[0].quantity == 7


def test_record_id_is_stable_and_depends_on_source():
    assert make_id("email", "EM-1") == make_id("email", "EM-1")
    assert make_id("email", "EM-1") != make_id("database", "EM-1")


def test_same_business_data_gives_same_hash():
    a = standardize(orders(), "email").clean.iloc[0].row_hash
    b = standardize(orders(Client="AHMED ALI"), "email").clean.iloc[0].row_hash
    c = standardize(orders(Qty="11"), "email").clean.iloc[0].row_hash
    assert a == b
    assert a != c


def test_missing_required_column_raises():
    with pytest.raises(SchemaError):
        standardize(orders().drop(columns=["Qty"]), "email")


def test_conflicting_columns_are_merged():
    df = orders()
    df["amount"] = None
    clean = standardize(df, "email").clean
    assert clean.iloc[0].quantity == 10
