import json
from pathlib import Path

from app.services.aws_client import OfferTarget
from app.services.transform import offer_to_rows

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _service_target() -> OfferTarget:
    return OfferTarget(
        service_code="AmazonS3",
        region_code="us-east-1",
        offer_type="service",
        version="20260501000000",
        offer_url="https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/20260501000000/us-east-1/index.json",
    )


def _sp_target() -> OfferTarget:
    return OfferTarget(
        service_code="AWSComputeSavingsPlan",
        region_code="us-east-1",
        offer_type="savings_plan",
        version="20260501000000",
        offer_url="https://pricing.us-east-1.amazonaws.com/savingsPlan/v1.0/aws/AWSComputeSavingsPlan/20260501000000/us-east-1/index.json",
    )


def test_service_offer_yields_one_row_per_price_dimension():
    offer = _load("service_offer_sample.json")
    rows = list(
        offer_to_rows(
            _service_target(),
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
            include_reserved=True,
        )
    )

    # 2 OnDemand priceDimensions for SKU-A + 1 for SKU-B + 1 Reserved for SKU-A = 4
    assert len(rows) == 4

    by_rate = {r["rate_code"]: r for r in rows}

    # Tiered OnDemand row for SKU-A
    a_tier1 = by_rate["SKU-A.JRTCKXETXF.6YS6EN2CT7"]
    assert a_tier1["term_type"] == "OnDemand"
    assert a_tier1["price_per_unit"] == "0.0230000000"
    assert a_tier1["currency"] == "USD"
    assert a_tier1["unit"] == "GB-Mo"
    assert a_tier1["starting_range"] == "0"
    assert a_tier1["ending_range"] == "51200"
    assert a_tier1["effective_date"] == "2025-04-01"
    assert a_tier1["service_code"] == "AmazonS3"
    assert a_tier1["region_code"] == "us-east-1"
    assert a_tier1["service_name"] == "Amazon Simple Storage Service"

    # 'Inf' endRange is normalized to None so BIGNUMERIC parsing won't blow up.
    a_tier2 = by_rate["SKU-A.JRTCKXETXF.7YS6EN2CT7"]
    assert a_tier2["ending_range"] is None

    # Scientific-notation price string is preserved verbatim so BQ BIGNUMERIC keeps full precision.
    b = by_rate["SKU-B.JRTCKXETXF.RATE1"]
    assert b["price_per_unit"] == "1.0E-4"

    # Reserved row carries termAttributes as a real dict (NOT a pre-serialized string —
    # otherwise BQ stores a JSON string scalar and JSON_VALUE / JSON_KEYS break).
    res = by_rate["SKU-A.HU7G6KETJZ.6YS6EN2CT7"]
    assert res["term_type"] == "Reserved"
    assert isinstance(res["term_attributes"], dict)
    assert res["term_attributes"]["LeaseContractLength"] == "1yr"
    assert res["term_attributes"]["PurchaseOption"] == "No Upfront"

    # attributes JSON column has the full per-service bag, as a dict.
    assert isinstance(a_tier1["attributes"], dict)
    assert a_tier1["attributes"]["storageClass"] == "General Purpose"
    assert a_tier1["attributes"]["volumeType"] == "Standard"


def test_service_offer_can_skip_reserved():
    offer = _load("service_offer_sample.json")
    rows = list(
        offer_to_rows(
            _service_target(),
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
            include_reserved=False,
        )
    )
    assert all(r["term_type"] == "OnDemand" for r in rows)
    assert len(rows) == 3  # the 3 OnDemand priceDimensions only


def test_service_offer_row_has_required_history_columns():
    """All REQUIRED columns from aws_pricing_history.json must be populated."""
    offer = _load("service_offer_sample.json")
    rows = list(
        offer_to_rows(
            _service_target(),
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
        )
    )
    for row in rows:
        for col in (
            "ingestion_date",
            "service_code",
            "region_code",
            "version",
            "offer_type",
            "sku",
            "rate_code",
            "ingested_at",
        ):
            assert row.get(col), f"missing required column {col} in row {row}"


def test_savings_plan_offer_yields_one_row_per_rate():
    offer = _load("savings_plan_sample.json")
    rows = list(
        offer_to_rows(
            _sp_target(),
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
        )
    )
    assert len(rows) == 2

    rate_a = next(r for r in rows if r["rate_code"] == "SP-SKU-1.RATE-A")
    assert rate_a["offer_type"] == "savings_plan"
    assert rate_a["term_type"] == "SavingsPlan"
    assert rate_a["price_per_unit"] == "0.0660000000"
    assert rate_a["currency"] == "USD"
    assert rate_a["unit"] == "Hrs"
    assert isinstance(rate_a["term_attributes"], dict)
    assert rate_a["term_attributes"]["leaseContractLength"] == {"duration": 1, "unit": "year"}


def test_pricing_with_no_usd_falls_back_to_first_currency():
    """Rows with non-USD pricing keep the original currency rather than dropping the price."""
    target = _service_target()
    offer = {
        "version": "v1",
        "publicationDate": "2026-05-01T00:00:00Z",
        "products": {
            "SKU": {"sku": "SKU", "productFamily": "X", "attributes": {"servicename": "X"}}
        },
        "terms": {
            "OnDemand": {
                "SKU": {
                    "SKU.TERM": {
                        "offerTermCode": "TERM",
                        "sku": "SKU",
                        "priceDimensions": {
                            "SKU.TERM.RATE": {
                                "rateCode": "SKU.TERM.RATE",
                                "unit": "Hrs",
                                "pricePerUnit": {"CNY": "0.5000000000"},
                            }
                        },
                        "termAttributes": {},
                    }
                }
            }
        },
    }
    rows = list(
        offer_to_rows(
            target,
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
        )
    )
    assert len(rows) == 1
    assert rows[0]["currency"] == "CNY"
    assert rows[0]["price_per_unit"] == "0.5000000000"
    # raw map is preserved end-to-end for downstream consumers, as a real dict so
    # BigQuery stores it as a JSON object (not a JSON string scalar).
    assert rows[0]["price_per_unit_raw"] == {"CNY": "0.5000000000"}


def test_json_columns_are_dicts_not_strings():
    """Regression: BigQuery JSON columns must receive dicts, not pre-serialized
    JSON strings.

    If the loader emits `"attributes": "{\"foo\":\"bar\"}"`, BigQuery stores it
    as a JSON string scalar — and `JSON_VALUE(attributes, '$.foo')` returns NULL
    while `JSON_KEYS(attributes)` returns nothing. Tests that go through
    `json.loads(row[col])` accidentally pass either way, so guard the invariant
    directly here.
    """
    offer = _load("service_offer_sample.json")
    rows = list(
        offer_to_rows(
            _service_target(),
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
        )
    )
    populated = [r for r in rows if r["attributes"] is not None]
    assert populated, "fixture should produce at least one row with attributes"
    for r in populated:
        assert isinstance(r["attributes"], dict), (
            f"attributes must be a dict for BQ JSON column, got {type(r['attributes']).__name__}"
        )
    # Same invariant for the other two JSON columns where populated.
    for r in rows:
        if r["term_attributes"] is not None:
            assert isinstance(r["term_attributes"], dict)
        if r["price_per_unit_raw"] is not None:
            assert isinstance(r["price_per_unit_raw"], dict)


def test_attributes_is_null_when_product_has_none():
    """If products[sku].attributes is missing, the attributes column should be NULL, not '{}'."""
    target = _service_target()
    offer = {
        "version": "v1",
        "products": {"SKU": {"sku": "SKU"}},
        "terms": {
            "OnDemand": {
                "SKU": {
                    "SKU.T": {
                        "offerTermCode": "T",
                        "priceDimensions": {
                            "SKU.T.R": {
                                "rateCode": "SKU.T.R",
                                "unit": "Hrs",
                                "pricePerUnit": {"USD": "0.1"},
                            }
                        },
                    }
                }
            }
        },
    }
    rows = list(
        offer_to_rows(
            target,
            offer,
            ingestion_date_str="2026-05-01",
            ingested_at_str="2026-05-01T00:00:00+00:00",
        )
    )
    assert rows[0]["attributes"] is None
