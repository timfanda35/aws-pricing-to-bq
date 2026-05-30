"""HTTP client + target discovery for the AWS Price List Bulk API.

AWS publishes pricing under two URL trees, both anonymous HTTPS:

- /offers/v1.0/aws/index.json                          (master service index)
  -> /offers/v1.0/aws/<service>/current/region_index.json
  -> /offers/v1.0/aws/<service>/<version>/<region>/index.json   (the actual offer file)

- /savingsPlan/v1.0/aws/<plan>/current/region_index.json
  -> /savingsPlan/v1.0/aws/<plan>/<version>/<region>/index.json

The three documented savings-plan offers are hardcoded — they are not
exposed through the master service index in a uniform way.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

MASTER_INDEX_PATH = "/offers/v1.0/aws/index.json"

# Documented AWS Savings Plan offer codes. These live at a different URL prefix
# from the per-service offers (see module docstring).
SAVINGS_PLAN_OFFERS: tuple[str, ...] = (
    "AWSComputeSavingsPlan",
    "AWSDatabaseSavingsPlans",
    "AWSMachineLearningSavingsPlans",
)


@dataclass(frozen=True)
class OfferTarget:
    """One (service, region) offer file we may need to download.

    `version` is the AWS-supplied timestamp embedded in the URL — comparing
    it to the value already stored in `aws_pricing_versions` is what lets
    us skip unchanged files entirely.
    """

    service_code: str  # "AmazonEC2" or "AWSComputeSavingsPlan"
    region_code: str  # "us-east-1" or "aws-global"
    offer_type: str  # "service" | "savings_plan"
    version: str
    offer_url: str  # absolute URL to the offer JSON file


class RetryableHTTPError(Exception):
    """Raised for 429/5xx responses so tenacity will retry them."""

    def __init__(self, status_code: int, retry_after: float | None = None):
        super().__init__(f"retryable HTTP {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after


def _is_retryable_status(status: int) -> bool:
    return status == 429 or 500 <= status < 600


def _honor_retry_after(state: RetryCallState) -> float:
    """If the last exception carried a Retry-After hint, honor it; else exponential backoff."""
    exc = state.outcome.exception() if state.outcome else None
    if isinstance(exc, RetryableHTTPError) and exc.retry_after is not None:
        return max(0.0, float(exc.retry_after))
    return wait_exponential_jitter(initial=1, max=30, jitter=1)(state)


def make_session(settings: Settings) -> requests.Session:
    """Build a requests.Session honoring proxy settings."""
    session = requests.Session()
    proxies = {
        k: v
        for k, v in {"http": settings.http_proxy, "https": settings.https_proxy}.items()
        if v
    }
    if proxies:
        session.proxies.update(proxies)
    if settings.no_proxy:
        session.proxies["no"] = settings.no_proxy
    return session


def _build_get(settings: Settings, session: requests.Session):
    """Build a retry-wrapped GET that returns parsed JSON."""

    @retry(
        retry=retry_if_exception_type(
            (RetryableHTTPError, requests.ConnectionError, requests.Timeout)
        ),
        stop=stop_after_attempt(max(1, settings.aws_max_retries)),
        wait=_honor_retry_after,
        reraise=True,
    )
    def _get(url: str) -> dict:
        resp = session.get(url, timeout=settings.aws_request_timeout_s)
        if _is_retryable_status(resp.status_code):
            retry_after = resp.headers.get("Retry-After")
            ra_seconds: float | None = None
            if retry_after:
                try:
                    ra_seconds = float(retry_after)
                except ValueError:
                    ra_seconds = None
            raise RetryableHTTPError(resp.status_code, ra_seconds)
        resp.raise_for_status()
        return resp.json()

    return _get


def _abs_url(settings: Settings, path_or_url: str) -> str:
    """Resolve a path against the configured base URL. Pass-through if already absolute."""
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    base = settings.aws_pricing_base_url.rstrip("/")
    path = path_or_url if path_or_url.startswith("/") else "/" + path_or_url
    return base + path


def _extract_version_from_offer_url(url: str) -> str:
    """The offer URL embeds the version: .../<service>/<version>/<region>/index.json.

    Returns the third-from-last path segment.
    """
    parts = url.rstrip("/").split("/")
    if len(parts) < 3:
        return ""
    return parts[-3]


def fetch_master_index(settings: Settings, session: requests.Session | None = None) -> dict:
    """Fetch the master AWS offer index."""
    own = session is None
    session = session or make_session(settings)
    try:
        get = _build_get(settings, session)
        url = _abs_url(settings, MASTER_INDEX_PATH)
        logger.info("aws.fetch.master_index url=%s", url)
        return get(url)
    finally:
        if own:
            session.close()


def _fetch_region_index(
    settings: Settings, session: requests.Session, region_index_path: str
) -> dict:
    get = _build_get(settings, session)
    return get(_abs_url(settings, region_index_path))


def _iter_region_entries(regions):
    """Yield (region_code, region_meta_dict) regardless of the catalog's shape.

    AWS publishes the per-service offer region_index as a dict keyed by region:
      {"regions": {"us-east-1": {"regionCode": ..., "currentVersionUrl": ...}}}

    But the savings-plan region_index is a list of entries:
      {"regions": [{"regionCode": "us-east-1", "versionUrl": ...}, ...]}

    This helper normalizes both into the same iteration shape.
    """
    if isinstance(regions, dict):
        for region_code, region_meta in regions.items():
            yield region_code, (region_meta or {})
    elif isinstance(regions, list):
        for entry in regions:
            entry = entry or {}
            region_code = entry.get("regionCode")
            if region_code:
                yield region_code, entry


def _region_index_to_targets(
    region_index: dict,
    service_code: str,
    offer_type: str,
    settings: Settings,
) -> list[OfferTarget]:
    out: list[OfferTarget] = []
    regions = region_index.get("regions") or []
    for region_code, region_meta in _iter_region_entries(regions):
        # Per-service offers expose `currentVersionUrl`; savings plans use `versionUrl`.
        current_version_url = region_meta.get("currentVersionUrl") or region_meta.get(
            "versionUrl"
        )
        if not current_version_url:
            continue
        absolute = _abs_url(settings, current_version_url)
        version = _extract_version_from_offer_url(absolute)
        out.append(
            OfferTarget(
                service_code=service_code,
                region_code=region_code,
                offer_type=offer_type,
                version=version,
                offer_url=absolute,
            )
        )
    return out


def discover_targets(
    settings: Settings | None = None,
    *,
    service_filter: set[str] | None = None,
    include_savings_plans: bool = True,
    session: requests.Session | None = None,
) -> list[OfferTarget]:
    """Walk master + region indices and return every (service, region) offer target.

    Region-index fetches run in a ThreadPoolExecutor with `aws_max_workers` workers.
    """
    settings = settings or get_settings()
    own = session is None
    session = session or make_session(settings)

    try:
        master = fetch_master_index(settings, session=session)

        # ---- Service offers ----
        offers: dict = master.get("offers") or {}
        if service_filter:
            offers = {k: v for k, v in offers.items() if k in service_filter}
        logger.info(
            "aws.discover.master offers=%d filter=%s",
            len(offers),
            sorted(service_filter) if service_filter else "(all)",
        )

        service_jobs: list[tuple[str, str, str]] = []  # (service_code, region_index_path, offer_type)
        for service_code, meta in offers.items():
            region_index_path = meta.get("currentRegionIndexUrl")
            if not region_index_path:
                logger.warning(
                    "aws.discover.skip service=%s reason=missing currentRegionIndexUrl",
                    service_code,
                )
                continue
            service_jobs.append((service_code, region_index_path, "service"))

        # ---- Savings plan offers ----
        if include_savings_plans:
            for plan_code in SAVINGS_PLAN_OFFERS:
                path = f"/savingsPlan/v1.0/aws/{plan_code}/current/region_index.json"
                service_jobs.append((plan_code, path, "savings_plan"))

        targets: list[OfferTarget] = []
        with ThreadPoolExecutor(max_workers=settings.aws_max_workers) as pool:
            futures = {
                pool.submit(_fetch_region_index, settings, session, path): (
                    service_code,
                    offer_type,
                )
                for (service_code, path, offer_type) in service_jobs
            }
            for fut in as_completed(futures):
                service_code, offer_type = futures[fut]
                try:
                    region_index = fut.result()
                except Exception as exc:
                    # A single bad region index shouldn't tank the whole discovery — log + skip.
                    logger.exception(
                        "aws.discover.region_index_failed service=%s offer_type=%s err=%s",
                        service_code,
                        offer_type,
                        exc,
                    )
                    continue
                targets.extend(
                    _region_index_to_targets(region_index, service_code, offer_type, settings)
                )

        logger.info(
            "aws.discover.complete targets=%d service_jobs=%d", len(targets), len(service_jobs)
        )
        return targets
    finally:
        if own:
            session.close()


def diff_targets(
    targets: Iterable[OfferTarget], known: set[tuple[str, str, str, str]]
) -> tuple[list[OfferTarget], int]:
    """Split targets into (changed, skipped_count).

    `known` is a set of (service_code, region_code, offer_type, version) tuples
    pulled from the aws_pricing_versions table.
    """
    changed: list[OfferTarget] = []
    skipped = 0
    for t in targets:
        key = (t.service_code, t.region_code, t.offer_type, t.version)
        if key in known:
            skipped += 1
        else:
            changed.append(t)
    return changed, skipped


def fetch_offer_json(
    target: OfferTarget,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> dict:
    """Download a single offer file."""
    settings = settings or get_settings()
    own = session is None
    session = session or make_session(settings)
    try:
        get = _build_get(settings, session)
        return get(target.offer_url)
    finally:
        if own:
            session.close()
