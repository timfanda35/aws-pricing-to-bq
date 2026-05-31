from unittest.mock import patch

import pytest
import responses

from app.services import aws_client
from app.services.aws_client import OfferTarget


def _master(*service_codes: str) -> dict:
    return {
        "formatVersion": "v1.0",
        "publicationDate": "2026-05-01T00:00:00Z",
        "offers": {
            svc: {
                "offerCode": svc,
                "currentVersionUrl": f"/offers/v1.0/aws/{svc}/current/index.json",
                "currentRegionIndexUrl": f"/offers/v1.0/aws/{svc}/current/region_index.json",
            }
            for svc in service_codes
        },
    }


def _region_index(service_code: str, regions: dict[str, str], offer_type: str = "offers") -> dict:
    """Build a region_index response matching AWS's actual on-the-wire shape.

    Per-service offers use a dict-keyed `regions` with `currentVersionUrl`.
    Savings plans use a list-shaped `regions` with `versionUrl` (different field name!).
    """
    if offer_type == "offers":
        out: dict = {"formatVersion": "v1.0", "regions": {}}
        for region, version in regions.items():
            out["regions"][region] = {
                "regionCode": region,
                "currentVersionUrl": f"/offers/v1.0/aws/{service_code}/{version}/{region}/index.json",
            }
        return out
    # savings-plan shape: list of {regionCode, versionUrl}
    return {
        "formatVersion": "v1.0",
        "regions": [
            {
                "regionCode": region,
                "versionUrl": f"/savingsPlan/v1.0/aws/{service_code}/{version}/{region}/index.json",
            }
            for region, version in regions.items()
        ],
    }


@responses.activate
def test_discover_targets_walks_master_and_region_indices(settings):
    settings.aws_include_savings_plans = False
    base = settings.aws_pricing_base_url

    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/index.json",
        json=_master("AmazonEC2", "AmazonS3"),
    )
    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/AmazonEC2/current/region_index.json",
        json=_region_index("AmazonEC2", {"us-east-1": "20260116160258", "eu-west-1": "20260116160258"}),
    )
    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/AmazonS3/current/region_index.json",
        json=_region_index("AmazonS3", {"us-east-1": "20260101000000"}),
    )

    targets = aws_client.discover_targets(settings, include_savings_plans=False)

    by_key = {(t.service_code, t.region_code): t for t in targets}
    assert ("AmazonEC2", "us-east-1") in by_key
    assert ("AmazonEC2", "eu-west-1") in by_key
    assert ("AmazonS3", "us-east-1") in by_key
    assert by_key[("AmazonEC2", "us-east-1")].version == "20260116160258"
    assert by_key[("AmazonS3", "us-east-1")].version == "20260101000000"
    assert all(t.offer_type == "service" for t in targets)
    # offer_url is absolute
    assert by_key[("AmazonEC2", "us-east-1")].offer_url.startswith(base)


@responses.activate
def test_discover_targets_applies_service_filter(settings):
    settings.aws_include_savings_plans = False
    base = settings.aws_pricing_base_url

    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/index.json",
        json=_master("AmazonEC2", "AmazonS3", "AmazonRDS"),
    )
    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/AmazonEC2/current/region_index.json",
        json=_region_index("AmazonEC2", {"us-east-1": "v1"}),
    )

    targets = aws_client.discover_targets(
        settings, service_filter={"AmazonEC2"}, include_savings_plans=False
    )

    assert {t.service_code for t in targets} == {"AmazonEC2"}
    # Only the filtered service's region_index was hit.
    region_index_calls = [c for c in responses.calls if "region_index.json" in c.request.url]
    assert len(region_index_calls) == 1


@responses.activate
def test_discover_targets_includes_savings_plans(settings):
    base = settings.aws_pricing_base_url

    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/index.json",
        json=_master(),  # no services
    )
    for plan in aws_client.SAVINGS_PLAN_OFFERS:
        responses.add(
            responses.GET,
            f"{base}/savingsPlan/v1.0/aws/{plan}/current/region_index.json",
            json=_region_index(plan, {"us-east-1": "20260101000000"}, offer_type="savingsPlan"),
        )

    targets = aws_client.discover_targets(settings, include_savings_plans=True)

    assert {t.service_code for t in targets} == set(aws_client.SAVINGS_PLAN_OFFERS)
    assert all(t.offer_type == "savings_plan" for t in targets)
    # Version is extracted from the savingsPlan URL pattern.
    assert all(t.version == "20260101000000" for t in targets)


@responses.activate
def test_discover_targets_skips_failed_region_index(settings):
    """A single bad region_index must not block discovery of the others."""
    settings.aws_include_savings_plans = False
    base = settings.aws_pricing_base_url
    # Allow tenacity retries before final 500 — fill the queue and assert it gives up cleanly.
    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/index.json",
        json=_master("AmazonEC2", "AmazonS3"),
    )
    for _ in range(settings.aws_max_retries):
        responses.add(
            responses.GET,
            f"{base}/offers/v1.0/aws/AmazonEC2/current/region_index.json",
            status=500,
        )
    responses.add(
        responses.GET,
        f"{base}/offers/v1.0/aws/AmazonS3/current/region_index.json",
        json=_region_index("AmazonS3", {"us-east-1": "v1"}),
    )

    with patch("time.sleep"):
        targets = aws_client.discover_targets(settings, include_savings_plans=False)

    # Only the healthy service yields targets.
    assert {t.service_code for t in targets} == {"AmazonS3"}


@responses.activate
def test_fetch_master_index_retries_on_429(settings):
    base = settings.aws_pricing_base_url
    responses.add(
        responses.GET, f"{base}/offers/v1.0/aws/index.json", status=429, headers={"Retry-After": "0"}
    )
    responses.add(responses.GET, f"{base}/offers/v1.0/aws/index.json", json=_master())

    with patch("time.sleep"):
        result = aws_client.fetch_master_index(settings)

    assert "offers" in result
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_master_index_gives_up_after_max_retries(settings):
    base = settings.aws_pricing_base_url
    for _ in range(settings.aws_max_retries):
        responses.add(responses.GET, f"{base}/offers/v1.0/aws/index.json", status=503)

    with patch("time.sleep"):
        with pytest.raises(aws_client.RetryableHTTPError):
            aws_client.fetch_master_index(settings)


def test_diff_targets_splits_known_and_changed():
    t1 = OfferTarget("AmazonEC2", "us-east-1", "service", "v1", "url1")
    t2 = OfferTarget("AmazonEC2", "us-east-1", "service", "v2", "url2")
    t3 = OfferTarget("AmazonS3", "us-east-1", "service", "v9", "url3")

    known = {("AmazonEC2", "us-east-1", "service", "v1")}
    changed, skipped = aws_client.diff_targets([t1, t2, t3], known)

    assert skipped == 1  # t1 was already loaded
    # Both t2 (different version of same service+region) and t3 (new) are changed.
    assert {t.service_code for t in changed} == {"AmazonEC2", "AmazonS3"}
    assert any(t.version == "v2" for t in changed)
    assert any(t.version == "v9" for t in changed)


def test_region_index_to_targets_handles_dict_shape(settings):
    """Per-service offers expose `regions` as a dict keyed by region_code."""
    region_index = {
        "regions": {
            "us-east-1": {
                "regionCode": "us-east-1",
                "currentVersionUrl": "/offers/v1.0/aws/AmazonS3/20260528222723/us-east-1/index.json",
            }
        }
    }
    targets = aws_client._region_index_to_targets(region_index, "AmazonS3", "service", settings)
    assert len(targets) == 1
    assert targets[0].region_code == "us-east-1"
    assert targets[0].version == "20260528222723"


def test_region_index_to_targets_handles_list_shape(settings):
    """Savings-plan offers expose `regions` as a list of {regionCode, versionUrl}.

    The field name also differs ('versionUrl' instead of 'currentVersionUrl'). This is
    the real format observed at /savingsPlan/v1.0/aws/AWSComputeSavingsPlan/current/region_index.json.
    """
    region_index = {
        "regions": [
            {
                "regionCode": "us-east-1",
                "versionUrl": "/savingsPlan/v1.0/aws/AWSComputeSavingsPlan/20260529053702/us-east-1/index.json",
            },
            {
                "regionCode": "us-west-2-den-1",
                "versionUrl": "/savingsPlan/v1.0/aws/AWSComputeSavingsPlan/20260529053702/us-west-2-den-1/index.json",
            },
        ]
    }
    targets = aws_client._region_index_to_targets(
        region_index, "AWSComputeSavingsPlan", "savings_plan", settings
    )
    assert {t.region_code for t in targets} == {"us-east-1", "us-west-2-den-1"}
    assert all(t.version == "20260529053702" for t in targets)
    assert all(t.offer_type == "savings_plan" for t in targets)


def test_extract_version_from_offer_url():
    url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/20260116160258/us-east-1/index.json"
    assert aws_client._extract_version_from_offer_url(url) == "20260116160258"

    # Savings plan URL has the same shape after the offer code.
    sp = "https://pricing.us-east-1.amazonaws.com/savingsPlan/v1.0/aws/AWSComputeSavingsPlan/20260101000000/us-east-1/index.json"
    assert aws_client._extract_version_from_offer_url(sp) == "20260101000000"


@responses.activate
def test_download_offer_to_file_streams_to_disk(settings, tmp_path):
    """Body is written via streamed iter_content, not buffered into a Python dict."""
    base = settings.aws_pricing_base_url
    body = b'{"version":"v1","products":{"X":{"sku":"X"}},"terms":{}}'
    url = f"{base}/offers/v1.0/aws/AmazonS3/v1/us-east-1/index.json"
    responses.add(responses.GET, url, body=body, status=200)

    target = aws_client.OfferTarget(
        service_code="AmazonS3",
        region_code="us-east-1",
        offer_type="service",
        version="v1",
        offer_url=url,
    )
    dest = tmp_path / "offer.json"
    n = aws_client.download_offer_to_file(target, str(dest), settings)

    assert n == len(body)
    assert dest.read_bytes() == body


@responses.activate
def test_download_offer_to_file_retries_on_5xx(settings, tmp_path):
    base = settings.aws_pricing_base_url
    url = f"{base}/offers/v1.0/aws/AmazonS3/v1/us-east-1/index.json"
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, body=b'{"version":"v1"}', status=200)

    target = aws_client.OfferTarget(
        service_code="AmazonS3",
        region_code="us-east-1",
        offer_type="service",
        version="v1",
        offer_url=url,
    )
    dest = tmp_path / "offer.json"
    with patch("time.sleep"):
        n = aws_client.download_offer_to_file(target, str(dest), settings)

    assert n > 0
    assert len(responses.calls) == 2


def test_proxy_settings_applied_to_session(settings):
    settings.http_proxy = "http://proxy.example.com:8080"
    settings.https_proxy = "https://proxy.example.com:8080"
    session = aws_client.make_session(settings)
    assert session.proxies.get("http") == "http://proxy.example.com:8080"
    assert session.proxies.get("https") == "https://proxy.example.com:8080"


def test_no_proxy_applied_to_session(settings):
    settings.https_proxy = "https://proxy.example.com:8080"
    settings.no_proxy = "169.254.169.254,metadata.google.internal"
    session = aws_client.make_session(settings)
    assert session.proxies.get("no") == "169.254.169.254,metadata.google.internal"
