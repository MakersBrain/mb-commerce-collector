"""BigCommerce public Storefront GraphQL connector."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Protocol, cast
from urllib.parse import urljoin, urlparse

import httpx
from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    DocumentRef,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .base import (
    BrowserBackendName,
    BrowserRequirement,
    CollectionRequest,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    RefreshMode,
    SnapshotField,
    result_limit_diagnostic,
)
from .budget import (
    BudgetExhausted,
    ConnectorBudget,
    RequestBudgetProtocol,
    RequestPriority,
    budget_diagnostic,
)

TOKEN_PATTERN = re.compile(
    r"(?:storefront_api_token|storefront_token|local_token|storefrontApiToken)"
    r'\\?["\']?\s*[:=]\s*\\?["\']([A-Za-z0-9._-]{40,})'
)

CATALOGUE_QUERY = """
query Catalogue($after: String, $first: Int!) {
  site { products(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      entityId name path sku description brand { name }
      availabilityV2 { status description }
      defaultImage { urlOriginal }
      images(first: 12) { edges { node { urlOriginal } } }
      prices { price { value currencyCode } retailPrice { value } }
      categories { edges { node { name path } } }
      customFields { edges { node { name value } } }
      variants(first: 30) { edges { node {
        entityId sku defaultImage { urlOriginal }
        prices { price { value currencyCode } }
        inventory { isInStock aggregated { availableToSell } }
        options { edges { node { displayName values { edges { node { label } } } } } }
      } } }
    } }
  } }
}
"""


class BigCommerceTransport(Protocol):
    async def document(self, url: str, *, rendered: bool = False) -> str: ...

    async def request_json(
        self,
        url: str,
        *,
        headers: dict[str, str],
        body: dict[str, JsonValue],
        browser_context_url: str | None = None,
    ) -> JsonValue: ...


class BigCommerceOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    token_page: str | None = None
    page_size: int = Field(default=50, ge=1, le=50)
    page_limit: int = Field(default=200, ge=1)
    allow_rendered_token_fallback: bool = True
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None


class BigCommerceConnector(CommerceConnector):
    name = "bigcommerce"
    platform = "bigcommerce"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_incremental_cursor=False,
        supports_category_filter=False,
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
        browser_backends=frozenset({BrowserBackendName.CAMOUFOX, BrowserBackendName.CDP_EXTENSION_PROXY}),
    )

    def __init__(
        self,
        transport: BigCommerceTransport,
        options: BigCommerceOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or BigCommerceOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        origin = self._origin(request.base_url)
        after, sequence = self._resume(checkpoint)
        token_page = self.options.token_page or request.base_url
        try:
            token, rendered = await self._discover_token(origin, token_page)
        except BudgetExhausted as error:
            page = self._failed_page(
                sequence, after, DiagnosticCode.REQUEST_BUDGET_EXHAUSTED,
                str(error), error.url, retryable=True,
            )
            yield page.model_copy(update={"diagnostics": (budget_diagnostic(error.priority, error.url),)})
            return
        if token is None:
            yield self._failed_page(
                0,
                None,
                DiagnosticCode.PARSER_UNSUPPORTED,
                "no origin-scoped BigCommerce storefront token found on public pages",
                token_page,
                retryable=False,
            )
            return

        endpoint = f"{origin}/graphql"
        emitted = 0
        for _ in range(self.options.page_limit):
            if request.cancelled():
                return
            remaining_before = (
                None if request.result_limit is None else request.result_limit - emitted
            )
            page_size = (
                self.options.page_size
                if remaining_before is None
                else min(self.options.page_size, remaining_before)
            )
            try:
                self._budget.require(
                    RequestPriority.DISCOVERY,
                    endpoint,
                    browser=rendered,
                )
                payload = await self.transport.request_json(
                    endpoint,
                    headers={
                        "authorization": f"Bearer {token}",
                        "content-type": "application/json",
                        "origin": origin,
                        "referer": token_page,
                    },
                    body={
                        "query": CATALOGUE_QUERY,
                        "variables": {"after": after, "first": page_size},
                    },
                    browser_context_url=token_page if rendered else None,
                )
            except BudgetExhausted as error:
                page = self._failed_page(
                    sequence, after, DiagnosticCode.REQUEST_BUDGET_EXHAUSTED,
                    str(error), error.url, retryable=True,
                )
                yield page.model_copy(
                    update={"diagnostics": (budget_diagnostic(error.priority, error.url),)}
                )
                return
            except (httpx.HTTPError, RuntimeError) as error:
                yield self._failed_page(
                    sequence,
                    after,
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"BigCommerce GraphQL page failed: {type(error).__name__}",
                    endpoint,
                    retryable=True,
                )
                return
            if not isinstance(payload, dict) or payload.get("errors"):
                yield self._failed_page(
                    sequence,
                    after,
                    DiagnosticCode.SCHEMA_CHANGED,
                    "BigCommerce GraphQL returned an error response",
                    endpoint,
                    retryable=True,
                )
                return
            payload_data: dict[str, Any] = payload
            data_value = payload_data.get("data")
            data = data_value if isinstance(data_value, dict) else {}
            site_value = data.get("site")
            site = site_value if isinstance(site_value, dict) else {}
            connection_value = site.get("products")
            connection: dict[str, Any] = (
                connection_value if isinstance(connection_value, dict) else {}
            )
            nodes = _edges(connection)
            remaining = None if request.result_limit is None else request.result_limit - emitted
            selected = nodes if remaining is None else nodes[: max(remaining, 0)]
            observed_at = self._clock()
            snapshots = tuple(
                snapshot
                for node in selected
                if (snapshot := self._normalize(node, request.source_id, origin, observed_at)) is not None
            )
            emitted += len(selected)
            page_info_value = connection.get("pageInfo")
            page_info = page_info_value if isinstance(page_info_value, dict) else {}
            has_next = page_info.get("hasNextPage") is True
            end_cursor = page_info.get("endCursor")
            limited = (
                request.result_limit is not None
                and emitted >= request.result_limit
                and has_next
            )
            terminal = limited or not has_next
            resume_after: JsonValue | None = None
            if not terminal or limited:
                if not isinstance(end_cursor, str) or not end_cursor:
                    yield self._failed_page(
                        sequence,
                        after,
                        DiagnosticCode.SCHEMA_CHANGED,
                        "BigCommerce pagination omitted its next cursor",
                        endpoint,
                        retryable=False,
                    )
                    return
                resume_after = {"after": end_cursor, "sequence": sequence + 1}
            diagnostics = (
                (result_limit_diagnostic(request.result_limit, endpoint),)
                if limited and request.result_limit is not None
                else ()
            )
            yield EntityPage(
                page_id=_page_id(sequence, after),
                sequence=sequence,
                items=snapshots,
                resume_after=resume_after,
                terminal=terminal,
                enumeration_intact=not limited,
                discovered=len(nodes),
                diagnostics=diagnostics,
            )
            if terminal:
                return
            # The non-terminal branch above proves this before constructing a
            # resumable page.
            assert isinstance(end_cursor, str)
            after = end_cursor
            sequence += 1

        yield self._failed_page(
            sequence,
            after,
            DiagnosticCode.ENUMERATION_INCOMPLETE,
            f"BigCommerce page limit {self.options.page_limit} reached",
            endpoint,
            retryable=False,
        )

    async def _discover_token(self, origin: str, token_page: str) -> tuple[str | None, bool]:
        pages = tuple(dict.fromkeys((token_page, origin)))
        for page in pages:
            for rendered in (False, True):
                if rendered and not self.options.allow_rendered_token_fallback:
                    continue
                try:
                    self._budget.require(
                        RequestPriority.DISCOVERY,
                        page,
                        browser=rendered,
                    )
                    document = await self.transport.document(page, rendered=rendered)
                except BudgetExhausted:
                    raise
                except (httpx.HTTPError, RuntimeError):
                    if rendered:
                        continue
                    continue
                for candidate in dict.fromkeys(TOKEN_PATTERN.findall(document)):
                    if _token_allows_origin(candidate, origin):
                        return candidate, rendered
                # Rendering the same successfully fetched document is useful
                # only when no valid token was present in its initial HTML.
                if not rendered and not self.options.allow_rendered_token_fallback:
                    break
        return None, False

    def _normalize(
        self, product: dict[str, Any], source_id: str, origin: str, observed_at: datetime
    ) -> CommerceProductSnapshot | None:
        path = _text(product.get("path"))
        title = _text(product.get("name"))
        external_id = _text(product.get("entityId")) or path
        if not path or not title or not external_id:
            return None
        product_url = urljoin(f"{origin}/", path)
        evidence = Evidence(method="api", source_url=product_url, observed_at=observed_at)
        images = tuple(
            MediaRef(url=url)
            for node in _edges(product.get("images") or {})
            if (url := _text(node.get("urlOriginal")))
        )
        default_image = _text((product.get("defaultImage") or {}).get("urlOriginal"))
        attributes: dict[str, str] = {
            name: value
            for node in _edges(product.get("customFields") or {})
            if (name := _text(node.get("name"))) and (value := _text(node.get("value")))
        }
        documents = tuple(
            DocumentRef(
                url=urljoin(product_url, value), title=name, observed_at=observed_at, evidence=(evidence,)
            )
            for name, value in attributes.items()
            if ".pdf" in value.casefold()
        )
        product_price = (product.get("prices") or {}).get("price") or {}
        retail = (product.get("prices") or {}).get("retailPrice") or {}
        raw_variants = _edges(product.get("variants") or {})
        variants = tuple(
            self._variant(node, product, product_price, evidence, observed_at, default_image)
            for node in raw_variants
        )
        if not variants:
            offer = _offers(
                product_price,
                retail,
                evidence,
                observed_at,
                _availability(product),
                self.options.vat_status,
            )
            if offer:
                variants = (
                    CommerceVariant(
                        external_id=f"product:{external_id}",
                        is_default=True,
                        sku=_text(product.get("sku")),
                        image=MediaRef(url=default_image) if default_image else None,
                        offers=offer,
                        stock=StockState(
                            availability=_availability(product),
                            observed_at=observed_at,
                            evidence=(evidence,),
                        ),
                        published_attributes=cast(dict[str, JsonValue], dict(attributes)),
                        platform_extensions={"legacy_raw_variant": None},
                    ),
                )
        categories = tuple(
            CategoryRef(name=name)
            for node in _edges(product.get("categories") or {})
            if (name := _text(node.get("name")))
        )
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=external_id,
            canonical_url=product_url,
            title=title,
            description=_text(product.get("description")),
            vendor=_text((product.get("brand") or {}).get("name")),
            observed_at=observed_at,
            categories=categories,
            images=images or ((MediaRef(url=default_image),) if default_image else ()),
            documents=documents,
            variants=variants,
            published_attributes={
                **attributes,
                "supplier_reference": _text(product.get("sku")),
            },
            platform_extensions={
                "legacy_raw_product": (
                    {key: value for key, value in product.items() if key != "variants"}
                    if raw_variants
                    else product
                )
            },
        )

    def _variant(
        self,
        variant: dict[str, Any],
        product: dict[str, Any],
        parent_price: dict[str, Any],
        evidence: Evidence,
        observed_at: datetime,
        default_image: str | None,
    ) -> CommerceVariant:
        options: dict[str, str] = {
            name: ", ".join(labels)
            for node in _edges(variant.get("options") or {})
            if (name := _text(node.get("displayName")))
            and (labels := [_text(value.get("label")) for value in _edges(node.get("values") or {})])
        }
        options = {key: value for key, value in options.items() if value}
        price = dict((variant.get("prices") or {}).get("price") or {})
        if price.get("value") is None:
            price["value"] = parent_price.get("value")
        if not price.get("currencyCode"):
            price["currencyCode"] = parent_price.get("currencyCode")
        inventory = variant.get("inventory") or {}
        quantity = (inventory.get("aggregated") or {}).get("availableToSell")
        exact_quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) else None
        availability = (
            Availability.IN_STOCK
            if inventory.get("isInStock") is True
            else Availability.OUT_OF_STOCK
            if inventory.get("isInStock") is False
            else _availability(product)
        )
        image = _text((variant.get("defaultImage") or {}).get("urlOriginal")) or default_image
        return CommerceVariant(
            external_id=_text(variant.get("entityId")) or _text(variant.get("sku")) or "default",
            title=", ".join(options.values()) or None,
            sku=_text(variant.get("sku")) or _text(product.get("sku")),
            image=MediaRef(url=image) if image else None,
            options=options,
            offers=_offers(
                price,
                {},
                evidence,
                observed_at,
                availability,
                self.options.vat_status,
            ),
            stock=StockState(
                availability=availability,
                quantity=max(exact_quantity, 0) if exact_quantity is not None else None,
                quantity_kind=(
                    StockQuantityKind.EXACT
                    if exact_quantity is not None
                    else StockQuantityKind.UNKNOWN
                ),
                observed_at=observed_at,
                evidence=(evidence,),
            ),
            published_attributes=cast(dict[str, JsonValue], dict(options)),
            platform_extensions={"legacy_raw_variant": variant},
        )

    def _validate_request(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None) -> None:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("BigCommerce connector does not support the requested contract")
        if checkpoint is not None and (
            checkpoint.connector != self.name
            or checkpoint.connector_version != self.version
            or checkpoint.source_id != request.source_id
        ):
            raise ValueError("checkpoint does not belong to this connector")

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str | None, int]:
        if checkpoint is None:
            return None, 0
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("BigCommerce checkpoint cursor must be an object")
        after, sequence = cursor.get("after"), cursor.get("sequence")
        if after is not None and not isinstance(after, str):
            raise ValueError("BigCommerce checkpoint cursor is invalid")
        if not isinstance(sequence, int) or sequence < 0:
            raise ValueError("BigCommerce checkpoint cursor is invalid")
        return after, sequence

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("BigCommerce base_url must be absolute HTTP(S)")
        return f"{parsed.scheme}://{parsed.netloc}"

    @staticmethod
    def _failed_page(
        sequence: int,
        after: str | None,
        code: DiagnosticCode,
        message: str,
        url: str,
        *,
        retryable: bool,
    ) -> EntityPage[CommerceProductSnapshot]:
        resume: JsonValue = {"after": after, "sequence": sequence}
        return EntityPage(
            page_id=_page_id(sequence, after),
            sequence=sequence,
            items=(),
            resume_after=resume,
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(
                Diagnostic(
                    code=code,
                    severity=DiagnosticSeverity.ERROR,
                    message=message,
                    retryable=retryable,
                    affects_completeness=True,
                    url=url,
                ),
            ),
        )


def _edges(connection: Any) -> list[dict[str, Any]]:
    return [
        edge.get("node") or {}
        for edge in (connection or {}).get("edges") or []
        if isinstance(edge, dict) and isinstance(edge.get("node"), dict)
    ]


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _availability(product: dict[str, Any]) -> Availability:
    return {
        "Available": Availability.IN_STOCK,
        "Preorder": Availability.PREORDER,
        "Unavailable": Availability.OUT_OF_STOCK,
    }.get(_text((product.get("availabilityV2") or {}).get("status")), Availability.UNKNOWN)


def _offers(
    price: dict[str, Any],
    retail: dict[str, Any],
    evidence: Evidence,
    observed_at: datetime,
    availability: Availability,
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None,
) -> tuple[CommerceOffer, ...]:
    amount = _decimal(price.get("value"))
    currency = _text(price.get("currencyCode")).upper()
    if amount is None or not re.fullmatch(r"[A-Z]{3}", currency):
        return ()
    retail_amount = _decimal(retail.get("value"))
    sale = retail_amount is not None and retail_amount > amount
    current = CommerceOffer(
        price=Money(amount=amount, currency=currency),
        role="sale" if sale else "regular",
        observed_at=observed_at,
        evidence=(evidence,),
        availability=availability,
        availability_evidence=(evidence,),
        vat_status=vat_status or "unknown",
    )
    if not sale or retail_amount is None:
        return (current,)
    return (
        current,
        CommerceOffer(
            price=Money(amount=retail_amount, currency=currency),
            role="regular",
            observed_at=observed_at,
            evidence=(evidence,),
            availability=availability,
            availability_evidence=(evidence,),
            vat_status=vat_status or "unknown",
        ),
    )


def _decimal(value: Any) -> Decimal | None:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount.is_finite() and amount >= 0 else None


def _token_allows_origin(token: str, origin: str) -> bool:
    parts = token.split(".")
    if len(parts) < 2:
        return False
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    if not isinstance(claims, dict):
        return False
    allowed = claims.get("cors")
    if not allowed:
        return True
    return isinstance(allowed, list) and any(_text(item).rstrip("/") == origin for item in allowed)


def _page_id(sequence: int, after: str | None) -> str:
    digest = hashlib.sha256((after or "first").encode()).hexdigest()[:12]
    return f"graphql:{sequence}:{digest}"
