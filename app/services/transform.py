"""Flatten AWS Price List offer JSON into one NDJSON row per priceDimension.

The shape diverges across the two AWS catalogs:

- /offers/v1.0/aws/<service>/<version>/<region>/index.json
  `products` is a map(sku -> product); `terms` is map(term_type -> sku -> term_id -> term).
  Each term has a `priceDimensions` map keyed by rate code. We produce one row per
  priceDimension.

- /savingsPlan/v1.0/aws/<plan>/<version>/<region>/index.json
  `products` is an array (each with sku + attributes); `terms.savingsPlan` is an
  array of plans, each with a `rates` array. We produce one row per rate.

`pricePerUnit` values come in as strings from AWS (e.g. "0.0960000000") and are kept
as strings in the NDJSON output so BigQuery can land them into BIGNUMERIC without
the IEEE-754 precision loss that a Python float round-trip would introduce.

JSON-typed columns (`attributes`, `term_attributes`, `price_per_unit_raw`) are
emitted as nested dicts — **NOT** as pre-serialized JSON strings. When BigQuery
loads NDJSON into a JSON column, a string value lands as a JSON string scalar
(so `JSON_VALUE(col, '$.foo')` returns NULL); a dict / list value lands as a
real JSON object / array, which is what query consumers expect.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from app.services.aws_client import OfferTarget

logger = logging.getLogger(__name__)


_USD = "USD"


def _json_or_none(value):
    """Pass dicts/lists through verbatim for BigQuery JSON columns.

    Returns None for empty / missing values so the column ends up NULL rather
    than `{}` (which would be a non-null empty-object JSON value — harder to
    distinguish in queries from a populated row).

    Do NOT pre-serialize via json.dumps here: when BigQuery loads NDJSON into
    a column declared as type JSON, a string value becomes a JSON string scalar
    (queries like `JSON_VALUE(col, '$.foo')` then return NULL), but a nested
    dict becomes a real JSON object that JSON_VALUE / JSON_KEYS can traverse.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)) and not value:
        return None
    return value


def _coerce_range(value) -> str | None:
    """beginRange/endRange may be 'Inf' or missing — return None in those cases so
    BIGNUMERIC parsing doesn't blow up at LOAD time."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("inf", "infinity"):
        return None
    return s


def _pick_currency(price_per_unit: dict | None) -> tuple[str | None, str | None]:
    """Return (price_string, currency_code) preferring USD if present."""
    if not price_per_unit:
        return None, None
    if _USD in price_per_unit:
        return price_per_unit[_USD], _USD
    # Take the first arbitrary currency. Multi-currency rows are rare in practice.
    currency, value = next(iter(price_per_unit.items()))
    return value, currency


def _service_name_from_attrs(attributes: dict | None) -> str | None:
    if not attributes:
        return None
    return (
        attributes.get("servicename")
        or attributes.get("serviceName")
        or attributes.get("ServiceName")
    )


def _base_row(
    target: OfferTarget,
    offer: dict,
    sku: str,
    product_family: str | None,
    attributes: dict | None,
    ingestion_date_str: str,
    ingested_at_str: str,
) -> dict:
    return {
        "ingestion_date": ingestion_date_str,
        "service_code": target.service_code,
        "service_name": _service_name_from_attrs(attributes),
        "region_code": target.region_code,
        "version": target.version or offer.get("version") or "",
        "publication_date": offer.get("publicationDate"),
        "offer_type": target.offer_type,
        "sku": sku,
        "product_family": product_family,
        "attributes": _json_or_none(attributes),
        "source_url": target.offer_url,
        "ingested_at": ingested_at_str,
    }


def _flatten_service_offer(
    target: OfferTarget,
    offer: dict,
    ingestion_date_str: str,
    ingested_at_str: str,
    *,
    include_reserved: bool,
) -> Iterator[dict]:
    products: dict = offer.get("products") or {}
    terms: dict = offer.get("terms") or {}

    term_buckets = ["OnDemand"]
    if include_reserved:
        term_buckets.append("Reserved")

    for term_type in term_buckets:
        sku_terms = terms.get(term_type) or {}
        for sku, term_id_map in sku_terms.items():
            product = products.get(sku) or {}
            product_family = product.get("productFamily")
            attributes = product.get("attributes")
            for term_id, term in (term_id_map or {}).items():
                offer_term_code = term.get("offerTermCode")
                term_effective_date = term.get("effectiveDate")
                term_attributes = term.get("termAttributes")
                price_dimensions = term.get("priceDimensions") or {}
                for _rate_id, pd in price_dimensions.items():
                    price_per_unit_raw = pd.get("pricePerUnit")
                    price, currency = _pick_currency(price_per_unit_raw)
                    row = _base_row(
                        target=target,
                        offer=offer,
                        sku=sku,
                        product_family=product_family,
                        attributes=attributes,
                        ingestion_date_str=ingestion_date_str,
                        ingested_at_str=ingested_at_str,
                    )
                    row.update(
                        {
                            "rate_code": pd.get("rateCode") or term_id,
                            "offer_term_code": offer_term_code,
                            "term_type": term_type,
                            "price_per_unit": price,
                            "currency": currency,
                            "unit": pd.get("unit"),
                            "starting_range": _coerce_range(pd.get("beginRange")),
                            "ending_range": _coerce_range(pd.get("endRange")),
                            "effective_date": (
                                term_effective_date[:10] if term_effective_date else None
                            ),
                            "description": pd.get("description"),
                            "term_attributes": _json_or_none(term_attributes),
                            "price_per_unit_raw": _json_or_none(price_per_unit_raw),
                        }
                    )
                    yield row


def _products_index(products) -> dict:
    """Normalize savings-plan `products` (list or dict) into sku -> product dict."""
    if isinstance(products, dict):
        return products
    if isinstance(products, list):
        return {p.get("sku"): p for p in products if p.get("sku")}
    return {}


def _flatten_savings_plan_offer(
    target: OfferTarget,
    offer: dict,
    ingestion_date_str: str,
    ingested_at_str: str,
) -> Iterator[dict]:
    products = _products_index(offer.get("products"))
    sp_terms = (offer.get("terms") or {}).get("savingsPlan") or []
    for plan in sp_terms:
        sku = plan.get("sku")
        if not sku:
            continue
        product = products.get(sku) or {}
        product_family = product.get("productFamily")
        attributes = product.get("attributes")
        lease = plan.get("leaseContractLength") or {}
        term_attributes = {
            "leaseContractLength": lease,
            "description": plan.get("description"),
            "effectiveDate": plan.get("effectiveDate"),
        }
        for rate in plan.get("rates") or []:
            discounted_rate = rate.get("discountedRate") or {}
            price = discounted_rate.get("price")
            currency = _USD  # AWS savings plans publish in USD
            row = _base_row(
                target=target,
                offer=offer,
                sku=sku,
                product_family=product_family,
                attributes=attributes,
                ingestion_date_str=ingestion_date_str,
                ingested_at_str=ingested_at_str,
            )
            row.update(
                {
                    "rate_code": rate.get("rateCode"),
                    "offer_term_code": None,
                    "term_type": "SavingsPlan",
                    "price_per_unit": price,
                    "currency": currency,
                    "unit": discounted_rate.get("unit"),
                    "starting_range": None,
                    "ending_range": None,
                    "effective_date": (plan.get("effectiveDate") or "")[:10] or None,
                    "description": rate.get("description") or plan.get("description"),
                    "term_attributes": _json_or_none(term_attributes),
                    "price_per_unit_raw": _json_or_none({_USD: price} if price is not None else None),
                }
            )
            yield row


def offer_to_rows(
    target: OfferTarget,
    offer: dict,
    *,
    ingestion_date_str: str,
    ingested_at_str: str,
    include_reserved: bool = True,
) -> Iterator[dict]:
    """Dispatcher: flatten a single AWS offer JSON document into NDJSON rows."""
    if target.offer_type == "savings_plan":
        yield from _flatten_savings_plan_offer(
            target, offer, ingestion_date_str, ingested_at_str
        )
    else:
        yield from _flatten_service_offer(
            target, offer, ingestion_date_str, ingested_at_str, include_reserved=include_reserved
        )
