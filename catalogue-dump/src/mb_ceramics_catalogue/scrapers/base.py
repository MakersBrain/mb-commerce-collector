"""Fetching, scope policy and the Scraper contract every supplier implements."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json as json_lib
import logging
import random
import re
import time
import urllib.robotparser
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urljoin, urlparse

import httpx
from mb_commerce_scraper.proxy import (
    BrowserSubrequestAuthorization,
    BrowserSubrequestAuthorizer,
    BrowserSubrequestOutcome,
    ProxyBudgetExhausted,
)

from mb_ceramics_catalogue.observability import metrics
from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.transports.browser import (
    BrowserEvaluationResult,
    BrowserFetchResponse,
    BrowserJobContext,
    BrowserNetworkAccounting,
    BrowserSession,
    BrowserUnavailable,
    TransportBlocked,
)

if TYPE_CHECKING:
    from mb_ceramics_catalogue.proxy import ProxyLease

from . import domain
from . import record as record_module
from .activity import ACTIVITY, CURRENT_SOURCE
from .cache import CachedResponse, ResponseCache

LOGGER = logging.getLogger("catalogue-dump.scrapers")

USER_AGENT = "AtelieraCatalogueResearch/1.0 (+catalogue import; contact operator before production use)"
BROWSER_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"


# Compatibility export for legacy scrapers and callers.  The exception is
# transport-owned so low-level backends never need to import this module.
Blocked = TransportBlocked


#: Block pages that arrive with a 200. Matched against the document title only,
#: because the phrases themselves are ordinary enough to appear in the prose of a
#: real page — a shop may well sell a book about Cloudflare — while a *title* of
#: "403 Forbidden" is never a product.
BLOCK_TITLES = (
    "403 forbidden",
    "401 unauthorized",
    "access denied",
    "attention required",
    "just a moment",
    "checking your browser",
    "you have been blocked",
    "access to this site has been limited",
    "request unsuccessful",
    "are you a robot",
    "pardon our interruption",
)

TITLE_TAG = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)

#: A block page is small. Above this a title match is far more likely to be a
#: real page about the subject than a refusal wearing a 200.
BLOCK_MAX_BYTES = 32_768


def looks_like_a_block(body: str, content_type: str = "") -> str | None:
    """Name the refusal if this 200 is really a block page, else None.

    theceramicshop.com serves an entire "403 Forbidden" document with HTTP 200.
    Nothing keyed on the status code notices, so the fetcher returned it as a
    successful page, `pagecrawl` read a page with no links as a page with no
    links, and the source reported success having discovered nothing — with
    `truncated` false, which is the one combination that invites the loader to
    retire a live catalogue.

    The status code is not a reliable statement of what a server did, so the
    body has to be read. Two conditions together, because either alone is
    wrong: the document has to be small, and its *title* has to be a refusal.
    """
    if content_type and "html" not in content_type.lower():
        return None
    if len(body) > BLOCK_MAX_BYTES:
        return None
    match = TITLE_TAG.search(body)
    if not match:
        return None
    title = " ".join(match.group(1).split()).casefold()
    for phrase in BLOCK_TITLES:
        if phrase in title:
            return title[:120]
    return None


class NotCached(Blocked):
    """Raised in replay mode when a request was never recorded.

    A subclass of Blocked so a replay gap is handled exactly like any other
    reason a page could not be read: the source records it and carries on
    instead of the run dying on the first missing entry.
    """


#: Platforms whose shops are separate hostnames in front of one shared edge, by
#: the `platform` of the scraper that reads them. That edge meters by client
#: address across every shop on it, so politeness has to be counted there too —
#: both inside a process (`HostLimiter.join_group`) and across the fleet, where
#: `ops.leases` claims a slot under this name as well as under the hostname.
#:
#: Only Shopify is listed, because only Shopify has been observed doing it: on
#: 2026-08-12 five storefronts on five unrelated domains refused their first
#: request with 429, seconds after two others had been throttled part-way
#: through. Anything added here should have the same kind of evidence behind it.
SHARED_EDGES: dict[str, str] = {"shopify": "edge:shopify"}


class HostLimiter:
    """Cap how many requests are in flight per host, and slow down when told to.

    By default there is **no gap between requests**: a host that answers is
    asked again immediately, and the only limit is how many requests may be in
    flight at once. Waiting a fixed time between requests spends real minutes on
    every source to protect a host that never showed any sign of strain, and an
    API that returns a hundred products per call is not helped by it.

    What replaces the wait is the response itself. Slots start low, climb one at
    a time while the host keeps answering, and **halve on any error** — the
    usual additive-increase / multiplicative-decrease shape. A host that errors
    also earns a real gap between requests, doubling with each further failure,
    and if it published a `Crawl-delay` that figure is adopted instead. Both are
    released once the host has fully recovered, so a stumble costs a slowdown
    rather than the rest of the run.

    Three things set a gap:

    * `--delay`, divided by the live slot count (0 by default: no gap);
    * a `delay` configured on a source, which is a hard floor from the first
      request and is never divided — an operator asking for a slow rate gets it;
    * the backoff a host earns by failing, or the `Crawl-delay` it published.

    Any gap is jittered, since an exact metronome is both easy to fingerprint
    and needlessly bursty against a shop's cache.
    """

    #: Consecutive good responses before one slot is handed back. Small, because
    #: many sources are a handful of API calls in total and would otherwise
    #: finish before ever earning a second slot.
    RECOVERY = 3

    #: First gap a host earns by failing, and the ceiling that doubling stops at.
    BACKOFF_START = 0.5
    BACKOFF_MAX = 8.0

    #: Fraction of a published rate limit below which we start pacing. Well
    #: under half: the point is to glide down to the limit rather than discover
    #: it, and a shop that meters per minute gives no warning once it is spent.
    HEADROOM = 0.35

    def __init__(self, delay: float, concurrency: int, *, start: int | None = None) -> None:
        self.delay = delay
        self.maximum = max(1, concurrency)
        self.initial = max(1, min(start if start is not None else 2, self.maximum))
        self.floors: dict[str, float] = {}
        self.backoff: dict[str, float] = {}
        self.published: dict[str, float] = {}
        #: Gap derived from a host's own rate-limit headers, not from a failure.
        self.metered: dict[str, float] = {}
        self.slots: dict[str, int] = {}
        self.streaks: dict[str, int] = {}
        self.gates: dict[str, _Gate] = {}
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.last: dict[str, float] = {}
        #: Hosts that answer from one shared edge, mapped to the name of that
        #: edge. See `join_group`.
        self.groups: dict[str, str] = {}

    # -- pacing -----------------------------------------------------------

    def join_group(self, url: str, group: str) -> None:
        """Declare that this host's pacing is shared with the rest of `group`.

        Some hosts are only nominally separate. Nineteen of these shops are
        Shopify storefronts on custom domains, and every one of them answers
        from the same edge, which meters by client address across all of them:
        on 2026-08-12 five shops returned 429 to their *first* request, seconds
        after two unrelated shops had been throttled mid-pagination. Pacing each
        hostname on its own cannot see that, because from the hostname's point
        of view nothing had gone wrong yet.

        So the gap, the backoff and the metered pace are held against the group
        rather than the host, and one member being refused immediately slows
        every other member in this process. Concurrency slots stay per host —
        the edge's limit is on the rate, and a shop that is answering well is
        not made slower by a sibling that is not.
        """
        self.groups[urlparse(url).netloc if "//" in url else url] = group

    def _key(self, host: str) -> str:
        """The name this host is paced under: its group, or itself."""
        return self.groups.get(host, host)

    def set_delay(self, url: str, delay: float) -> None:
        """Set a hard minimum gap for one host; the strictest request wins."""
        key = self._key(urlparse(url).netloc)
        self.floors[key] = max(self.floors.get(key, 0.0), float(delay))

    def remember_crawl_delay(self, url: str, delay: float | None) -> None:
        """Keep a published Crawl-delay for the day the host starts refusing."""
        if delay and delay > 0:
            self.published.setdefault(self._key(urlparse(url).netloc), float(delay))

    def spacing(self, host: str) -> float:
        """The gap before this host's next request; zero means send it now."""
        key = self._key(host)
        return max(
            self.floors.get(key, 0.0),
            self.backoff.get(key, 0.0),
            self.metered.get(key, 0.0),
            self.delay / self.slots.get(host, self.initial),
        )

    def _jittered(self, host: str) -> float:
        gap = self.spacing(host)
        if gap <= 0:
            return 0.0
        # A floor is a minimum, so jitter around one may only ever add time.
        key = self._key(host)
        floored = key in self.floors or key in self.backoff or key in self.metered
        low, high = (1.0, 1.35) if floored else (0.7, 1.3)
        return gap * random.uniform(low, high)

    def gate(self, url: str) -> _Gate:
        host = urlparse(url).netloc
        if host not in self.gates:
            self.slots.setdefault(host, self.initial)
            self.gates[host] = _Gate(self.slots[host])
        return self.gates[host]

    async def wait(self, url: str) -> None:
        host = urlparse(url).netloc
        gap = self._jittered(host)
        if gap <= 0:
            # Nothing to space out, so nothing to serialise on either: taking
            # the per-host lock here would queue every slot behind one another.
            return
        # Under the group's name, so that a gap earned on a shared edge is a gap
        # between *all* of that edge's requests rather than one per shop.
        key = self._key(host)
        async with self.locks[key]:
            loop = asyncio.get_running_loop()
            elapsed = loop.time() - self.last.get(key, 0.0)
            if elapsed < gap:
                await asyncio.sleep(gap - elapsed)
            self.last[key] = loop.time()

    # -- adaptation -------------------------------------------------------

    def record_success(self, url: str) -> None:
        host = urlparse(url).netloc
        key = self._key(host)
        if self.slots.get(host, self.initial) >= self.maximum and key not in self.backoff:
            return
        self.streaks[host] = self.streaks.get(host, 0) + 1
        if self.streaks[host] < self.RECOVERY:
            return
        self.streaks[host] = 0
        self.slots[host] = min(self.maximum, self.slots.get(host, self.initial) + 1)
        if host in self.gates:
            self.gates[host].resize(self.slots[host])
        if self.slots[host] >= self.maximum and self.backoff.pop(key, None):
            # Back to full speed: the host has answered every request since it
            # regained its last slot, so the gap it earned by failing is spent.
            LOGGER.debug("host=%s recovered; backoff released", host)
        LOGGER.debug("host=%s slots=%d (recovered)", host, self.slots[host])

    def record_failure(self, url: str, reason: Any = None) -> None:
        """Halve the slots and start leaving a gap, since this host is unhappy."""
        host = urlparse(url).netloc
        key = self._key(host)
        self.streaks[host] = 0
        current = self.slots.get(host, self.initial)
        self.slots[host] = max(1, current // 2)
        if host in self.gates:
            self.gates[host].resize(self.slots[host])

        if (published := self.published.pop(key, None)) is not None:
            # The host asked for a pace in robots.txt and has now shown it meant
            # it, so take that figure rather than a guessed one.
            self.set_delay(url, published)
            metrics.host_backoff(key, published)
            LOGGER.warning(
                "host=%s failed (%s); falling back to its published Crawl-delay of %.1fs",
                host, reason, published,
            )
            return

        self.backoff[key] = min(
            self.BACKOFF_MAX, max(self.BACKOFF_START, self.backoff.get(key, 0.0) * 2),
        )
        metrics.host_backoff(key, self.backoff[key])
        LOGGER.warning(
            "host=%s failed (%s); slots %d -> %d, waiting %.1fs between requests%s",
            host, reason, current, self.slots[host], self.backoff[key],
            f" across {key}" if key != host else "",
        )

    # -- what the host says about itself ----------------------------------

    def observe_headers(self, url: str, headers: Any) -> None:
        """Take a pace from the host's own rate-limit accounting.

        Backing off after an error is reactive: the 429 has already been spent,
        and on a shop that counts requests per minute the run has already lost
        the window. Most storefronts that meter say so on every response, and
        the useful moment to slow down is while there is still budget left
        rather than once it is gone.

        Two families cover what these shops actually send. `X-RateLimit-*` is a
        budget and a window, and Shopify's `X-Shopify-Shop-Api-Call-Limit` is
        the same idea written `used/total`. Either way the interesting figure is
        how much is left, and the response to nearly-empty is a gap, not a stop.

        Nothing here ever *lowers* an existing floor: a source's configured
        delay and a backoff already earned are minimums, and a host claiming a
        generous budget is not a reason to overrule an operator who asked for a
        slow rate.
        """
        remaining = _header_int(headers, "x-ratelimit-remaining", "ratelimit-remaining")
        limit = _header_int(headers, "x-ratelimit-limit", "ratelimit-limit")
        window = _header_float(headers, "x-ratelimit-reset", "ratelimit-reset")

        shopify = headers.get("x-shopify-shop-api-call-limit") if hasattr(headers, "get") else None
        if shopify and "/" in str(shopify):
            used, _, total = str(shopify).partition("/")
            try:
                remaining, limit = int(total) - int(used), int(total)
            except ValueError:
                pass

        if remaining is None or limit is None or limit <= 0:
            return
        host = urlparse(url).netloc
        key = self._key(host)
        headroom = remaining / limit
        if headroom > self.HEADROOM:
            self.metered.pop(key, None)
            return
        # Spread whatever is left across the window the host named, defaulting
        # to a minute — the usual period when none is given.
        seconds = window if window and window > 0 else 60.0
        gap = seconds / max(1, remaining)
        previous = self.metered.get(key, 0.0)
        self.metered[key] = min(self.BACKOFF_MAX, max(previous, gap))
        if self.metered[key] != previous:
            LOGGER.info(
                "host=%s reports %d/%d of its rate limit left; pacing at %.2fs",
                host, remaining, limit, self.metered[key],
            )


def _header_int(headers: Any, *names: str) -> int | None:
    for name in names:
        value = headers.get(name) if hasattr(headers, "get") else None
        if value is not None:
            try:
                return int(str(value).strip())
            except ValueError:
                continue
    return None


def _header_float(headers: Any, *names: str) -> float | None:
    value = _header_int(headers, *names)
    return float(value) if value is not None else None


class _Gate:
    """A semaphore whose size can change while requests are in flight."""

    def __init__(self, limit: int) -> None:
        self.limit = max(1, limit)
        self.active = 0
        self.condition = asyncio.Condition()

    def resize(self, limit: int) -> None:
        self.limit = max(1, limit)

    async def __aenter__(self) -> _Gate:
        async with self.condition:
            while self.active >= self.limit:
                await self.condition.wait()
            self.active += 1
        return self

    async def __aexit__(self, *_: Any) -> None:
        async with self.condition:
            self.active -= 1
            self.condition.notify()


class ImpersonatingClient:
    """A real browser's TLS handshake, for hosts that reject Python's.

    Several CDNs decide before a single header is read. They fingerprint the TLS
    ClientHello — cipher order, extensions, ALPN, the JA3 of it — and refuse
    anything that is not a browser, which is why `BROWSER_USER_AGENT` alone gets
    a 403 from clay-king.com and nmclay.com while an ordinary Chrome sails
    through. Sending a browser's *name* is not sending a browser's *handshake*.

    It is worth being exact about what this is not. It is **not** a challenge
    solver and holds no cookies: measured against nmclay.com, a `cf_clearance`
    taken from a browser that had solved the challenge made no difference in
    either direction — `httpx` was refused with it and this client was served
    without it. The handshake is the whole of the difference, so there is
    nothing to harvest, store or keep fresh.

    curl_cffi is an optional dependency for the same reason camoufox is: most
    sources never need it, and an image that does not carry it should degrade to
    "this host refuses us" rather than fail to import.
    """

    #: curl-impersonate target. A major-version name ("chrome") tracks whatever
    #: the installed curl_cffi considers current, which is what we want: a
    #: fingerprint pinned to a browser three years out of date is its own tell.
    PROFILE = "chrome"

    #: Socket timeout for one impersonated request. A plain attribute rather
    #: than a parameter: this is curl's own timeout, not an asyncio deadline,
    #: and taking it as an argument to an async def invites the two to be
    #: confused for one another.
    REQUEST_TIMEOUT = 30.0

    def __init__(self, enabled: bool = True, proxy_url: str | None = None) -> None:
        self.enabled = enabled
        self.proxy_url = proxy_url
        self._unavailable: str | None = None

    @property
    def available(self) -> bool:
        return self.enabled and self._unavailable is None

    def _session(self) -> Any:
        try:
            from curl_cffi import requests as curl_requests
        except ImportError as error:  # pragma: no cover - environment dependent
            self._unavailable = str(error)
            raise Blocked(
                "curl_cffi is not installed; run 'uv sync --directory catalogue-dump "
                "--extra impersonate' to reach hosts that fingerprint the TLS handshake"
            ) from error
        return curl_requests

    def _blocking_get(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
        json_body: Any,
        timeout: float,
    ) -> tuple[int, bytes, dict[str, str], str]:
        requests = self._session()
        response = requests.request(
            method, url, params=params, headers=headers, json=json_body,
            impersonate=self.PROFILE, timeout=timeout, proxy=self.proxy_url,
        )
        return response.status_code, response.content, dict(response.headers), str(response.url)

    async def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> httpx.Response:
        """Issue one request and return it in the shape every caller expects.

        curl_cffi is synchronous, so it runs on a worker thread; the alternative
        is blocking the event loop for every other source in the run.
        """
        if not self.enabled:
            raise Blocked("TLS impersonation disabled (use --impersonate auto or always)")
        status, content, response_headers, final_url = await asyncio.to_thread(
            self._blocking_get, method, url, params, headers, json_body, self.REQUEST_TIMEOUT,
        )
        return httpx.Response(
            status,
            content=content,
            headers=self._decoded_headers(response_headers),
            request=httpx.Request(method, final_url or url),
        )

    @staticmethod
    def _decoded_headers(headers: dict[str, str]) -> dict[str, str]:
        """Drop the encoding headers curl_cffi has already honoured.

        `response.content` comes back decompressed, but the origin's
        `content-encoding: gzip` rides along with it. httpx believes the header
        and inflates the body a second time, which fails as "Error -3 while
        decompressing data: incorrect header check" — a refusal indistinguishable
        from the block this client exists to get past. It cost every gzipped
        sitemap on a host that fingerprints the handshake.
        """
        return {
            key: value for key, value in headers.items()
            if key.lower() not in {"content-encoding", "content-length"}
        }


class _BrowserSubrequestMeter:
    """Own one authorization token for every continued page request."""

    def __init__(
        self,
        authorizer: BrowserSubrequestAuthorizer,
        accounting: BrowserNetworkAccounting,
    ) -> None:
        self._authorizer = authorizer
        self._accounting = accounting
        self._pending: dict[int, tuple[BrowserSubrequestAuthorization, int]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._route_tasks: set[asyncio.Task[Any]] = set()
        self._failure: BaseException | None = None

    async def continue_request(
        self, route: Any, request: Any, transmitted_bytes: int
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._route_tasks.add(task)
        try:
            try:
                authorization = await self._authorizer.authorize(transmitted_bytes)
            except BaseException as error:
                self._remember_failure(error)
                await route.abort()
                raise
            if authorization is None:
                denied = ProxyBudgetExhausted()
                self._remember_failure(denied)
                await route.abort()
                raise denied
            key = id(request)
            try:
                self._accounting.record(transmitted_bytes, 0, 1)
                self._pending[key] = (authorization, transmitted_bytes)
            except BaseException:
                await asyncio.shield(authorization.release())
                raise
            try:
                # Calling continue transfers ownership to the browser. Even if it
                # then raises or is cancelled, the attempt may have been dispatched.
                await route.continue_()
            except BaseException as error:
                await asyncio.shield(
                    self._resolve(
                        request,
                        BrowserSubrequestOutcome(
                            classification=(
                                "cancelled"
                                if isinstance(error, asyncio.CancelledError)
                                else "transport_failure"
                            )
                        )
                    )
                )
                raise
        finally:
            if task is not None:
                self._route_tasks.discard(task)

    def request_finished(self, request: Any) -> None:
        self._schedule(self._finish(request))

    def request_failed(self, request: Any) -> None:
        self._schedule(self._failed(request))

    async def drain(self) -> None:
        await self._wait_tasks()
        if self._failure is not None:
            raise self._failure

    async def _wait_tasks(self) -> None:
        current = asyncio.current_task()
        while self._tasks or any(task is not current for task in self._route_tasks):
            pending = tuple(self._tasks) + tuple(
                task for task in self._route_tasks if task is not current
            )
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def close(self, classification: str) -> None:
        await self._wait_tasks()
        for key, (authorization, transmitted_bytes) in tuple(self._pending.items()):
            if self._pending.pop(key, None) is None:
                continue
            try:
                await authorization.reconcile(
                    BrowserSubrequestOutcome(
                        transmitted_bytes=transmitted_bytes,
                        classification=classification,
                    )
                )
            except BaseException as error:  # noqa: BLE001 - token failures fail closed
                self._remember_failure(error)
        if self._failure is not None:
            raise self._failure

    async def _finish(self, request: Any) -> None:
        response = await request.response()
        status = response.status if response is not None else None
        length = self._content_length(response.headers if response is not None else {})
        classification = (
            "blocked"
            if status == 403
            else "rate_limited"
            if status == 429
            else "success"
            if status is not None and status < 400
            else "http_error"
        )
        await self._resolve(
            request,
            BrowserSubrequestOutcome(
                status=status,
                received_bytes=length,
                classification=classification,
            ),
        )

    async def _failed(self, request: Any) -> None:
        await self._resolve(
            request,
            BrowserSubrequestOutcome(classification="transport_failure"),
        )

    async def _resolve(
        self, request: Any, outcome: BrowserSubrequestOutcome
    ) -> None:
        pending = self._pending.pop(id(request), None)
        if pending is None:
            return
        authorization, transmitted_bytes = pending
        self._accounting.record(0, outcome.received_bytes, 0)
        try:
            await authorization.reconcile(
                outcome.model_copy(
                    update={
                        "transmitted_bytes": max(
                            outcome.transmitted_bytes, transmitted_bytes
                        )
                    }
                )
            )
        except BaseException as error:  # noqa: BLE001 - token failures fail closed
            self._remember_failure(error)

    def _schedule(self, coroutine: Any) -> None:
        task = asyncio.create_task(self._run_event(coroutine))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_event(self, coroutine: Any) -> None:
        try:
            await coroutine
        except BaseException as error:  # noqa: BLE001 - event task must retain failure
            self._remember_failure(error)

    def _remember_failure(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error

    @staticmethod
    def _content_length(headers: Any) -> int:
        try:
            return max(0, int(headers.get("content-length", "0")))
        except (TypeError, ValueError):
            return 0


class BrowserRenderer:
    """One lazily started Camoufox instance, shared by the scrapers that need it.

    "Shared" used to mean shared by the scrapers within one job, because the
    session that owns it is built per job. A worker with four job slots
    therefore ran four browsers, and four such workers ran sixteen — which is
    what happened on 2026-08-10, and it did not fail so much as congeal: the
    same pages, the same request counts, and a single render going from three
    seconds to nine minutes. Two sources stopped finishing inside their
    deadline at all.

    So a renderer may now be built once per *process* and handed to each
    session (`open_session(..., browser=...)`), and `pages` bounds how many
    renders it will run at once. The lock is only around starting the browser;
    holding it across a whole page load, as it used to, made the bound one
    everywhere and hid the cost of the extra instances.
    """

    backend: Literal["camoufox"] = "camoufox"

    def __init__(
        self,
        enabled: bool,
        pages: int = 1,
        proxy_lease: ProxyLease | None = None,
        *,
        proxy_configuration: dict[str, str] | None = None,
        network_accounting: BrowserNetworkAccounting | None = None,
        subrequest_authorizer: BrowserSubrequestAuthorizer | None = None,
    ) -> None:
        if proxy_lease is not None and proxy_configuration is not None:
            raise ValueError("browser proxy lease and neutral configuration are exclusive")
        if proxy_configuration is None and network_accounting is not None:
            raise ValueError("browser accounting requires a proxy configuration")
        if subrequest_authorizer is not None and network_accounting is None:
            raise ValueError("browser authorization requires network accounting")
        self.enabled = enabled
        self.proxy_lease = proxy_lease
        self.proxy_configuration = proxy_configuration
        self.network_accounting = network_accounting
        self.subrequest_authorizer = subrequest_authorizer
        self.manager: Any = None
        self.browser: Any = None
        #: Held only while the browser is starting, never across a page load.
        self.lock = asyncio.Lock()
        #: How many pages this instance will have open at once. One by default,
        #: which is what a single job's renders were always limited to.
        self.pages = asyncio.Semaphore(max(1, pages))
        self._pages: dict[str, Any] = {}
        self._network_meters: dict[int, _BrowserSubrequestMeter] = {}
        self._page_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def _start(self) -> Any:
        if self.browser is None:
            try:
                from camoufox.async_api import AsyncCamoufox
            except ImportError as error:  # pragma: no cover - environment dependent
                raise BrowserUnavailable(
                    "camoufox is not installed; run 'uv sync --directory catalogue-dump'"
                ) from error
            # Camoufox warns that blocking images is itself a WAF signal, so
            # images are loaded normally even though the dump never reads them.
            self.manager = AsyncCamoufox(
                headless=True,
                block_images=(
                    self.proxy_lease is not None
                    or self.proxy_configuration is not None
                ),
                humanize=False,
                proxy=(
                    self.proxy_lease.browser_proxy
                    if self.proxy_lease
                    else self.proxy_configuration
                ),
            )
            try:
                self.browser = await self.manager.__aenter__()
            except Exception as error:  # pragma: no cover - environment dependent
                # The package can be importable while the browser itself was
                # never fetched — `camoufox fetch` downloads it separately, so a
                # `pip install` alone leaves this exact gap. Same remedy as a
                # missing import: run this job somewhere that has one.
                self.manager = None
                raise BrowserUnavailable(f"camoufox could not start: {error}") from error
        return self.browser

    async def _meter_page(self, page: Any) -> _BrowserSubrequestMeter | None:
        """Block paid browser noise and meter every allowed subrequest."""
        if self.proxy_lease is None and self.network_accounting is None:
            return None
        blocked = {"image", "media", "font"}
        blocked_hosts = ("google-analytics.com", "googletagmanager.com", "doubleclick.net")
        meter = (
            _BrowserSubrequestMeter(
                self.subrequest_authorizer,
                self.network_accounting,
            )
            if self.subrequest_authorizer is not None
            and self.network_accounting is not None
            else None
        )

        async def route_request(route: Any, request: Any) -> None:
            if request.resource_type in blocked or any(host in request.url for host in blocked_hosts):
                await route.abort()
                return
            transmitted = (
                len(request.method.encode()) + len(request.url.encode()) + 64
            )
            if self.proxy_lease is not None:
                self.proxy_lease.ensure_request_allowed()
                self.proxy_lease.account(transmitted, 0, 1)
            elif meter is not None:
                await meter.continue_request(route, request, transmitted)
                return
            else:
                assert self.network_accounting is not None
                self.network_accounting.record(transmitted, 0, 1)
            await route.continue_()

        def response_received(response: Any) -> None:
            if meter is not None:
                return
            headers = response.headers
            try:
                length = int(headers.get("content-length", "0"))
            except (TypeError, ValueError):
                length = 0
            if self.proxy_lease is not None:
                self.proxy_lease.account(0, length, 0)
            else:
                assert self.network_accounting is not None
                self.network_accounting.record(0, length, 0)

        await page.route("**/*", route_request)
        page.on("response", response_received)
        if meter is not None:
            page.on("requestfinished", meter.request_finished)
            page.on("requestfailed", meter.request_failed)
            self._network_meters[id(page)] = meter
        return meter

    async def _started(self) -> Any:
        """Start the browser if it is not running, once however many ask at once."""
        async with self.lock:
            return await self._start()

    async def request_json(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        """Issue a request from inside a loaded page.

        Some CDNs fingerprint the TLS handshake and reject any Python HTTP
        client while serving the same public endpoint to a real browser. Running
        the request in the page reuses the browser's own connection and session
        instead of imitating one.
        """
        if not self.enabled:
            raise Blocked("browser rendering disabled (use --browser auto or always)")
        origin = urlparse(page_url).netloc
        # This one keeps its page open between calls, so unlike `render` it also
        # needs the origin's own lock: two calls arriving together would
        # otherwise each open a page and one would be left orphaned in the
        # browser with nothing holding it.
        async with self.pages, self._page_locks[origin]:
            await self._started()
            page = self._pages.get(origin)
            if page is None or page.is_closed():
                page = await self.browser.new_page()
                meter = await self._meter_page(page)
                try:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=45_000)
                    if meter is not None:
                        await meter.drain()
                except BaseException:
                    await page.close()
                    if meter is not None:
                        await meter.close("cancelled")
                        self._network_meters.pop(id(page), None)
                    raise
                self._pages[origin] = page
            result = await page.evaluate(
                """async ({endpoint, method, headers, body}) => {
                    const response = await fetch(endpoint, {
                        method, headers,
                        body: body === null ? undefined : JSON.stringify(body),
                        credentials: 'include',
                    });
                    return {status: response.status, text: await response.text()};
                }""",
                {"endpoint": endpoint, "method": method, "headers": headers or {}, "body": body},
            )
            meter = self._network_meters.get(id(page))
            if meter is not None:
                await meter.drain()
            if result["status"] >= 400:
                raise Blocked(
                    f"{endpoint} returned {result['status']} in the browser context: "
                    f"{result['text'][:400]}"
                )
            try:
                return json_lib.loads(result["text"])
            except json_lib.JSONDecodeError as error:
                raise Blocked(f"{endpoint} did not return JSON in the browser context") from error

    async def render(self, url: str, wait_ms: int = 1500, wait_for: str | None = None) -> str:
        if not self.enabled:
            raise Blocked("browser rendering disabled (use --browser auto or always)")
        async with self.pages:
            await self._started()
            page = await self.browser.new_page()
            meter: _BrowserSubrequestMeter | None = None
            try:
                meter = await self._meter_page(page)
                await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                if wait_for:
                    try:
                        await page.wait_for_selector(wait_for, timeout=15_000)
                    except Exception:  # noqa: BLE001 - a missing selector is not fatal
                        LOGGER.debug("selector %s never appeared on %s", wait_for, url)
                await page.wait_for_timeout(wait_ms)
                if meter is not None:
                    await meter.drain()
                return await page.content()
            finally:
                await page.close()
                if meter is not None:
                    await asyncio.shield(meter.close("cancelled"))
                    self._network_meters.pop(id(page), None)

    async def evaluate(self, url: str, script: str, wait_ms: int = 2000, wait_for: str | None = None) -> Any:
        return (await self.evaluate_result(url, script, wait_ms, wait_for)).value

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        """Load a page and run a script in it, returning a JSON-safe value.

        Used where a supplier computes what it sells in the page itself, so the
        only way to read a published figure is to let the page produce it.
        """
        if not self.enabled:
            raise Blocked("browser rendering disabled (use --browser auto or always)")
        async with self.pages:
            await self._started()
            page = await self.browser.new_page()
            meter: _BrowserSubrequestMeter | None = None
            try:
                meter = await self._meter_page(page)
                await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                if wait_for:
                    try:
                        await page.wait_for_selector(wait_for, timeout=15_000)
                    except Exception:  # noqa: BLE001
                        LOGGER.debug("selector %s never appeared on %s", wait_for, url)
                await page.wait_for_timeout(wait_ms)
                if meter is not None:
                    await meter.drain()
                value = await page.evaluate(script)
                if meter is not None:
                    await meter.drain()
                return BrowserEvaluationResult(
                    value=value,
                    final_url=str(page.url),
                )
            finally:
                await page.close()
                if meter is not None:
                    await asyncio.shield(meter.close("cancelled"))
                    self._network_meters.pop(id(page), None)

    async def close(self) -> None:
        if self.manager is not None:
            pages, self._pages = tuple(self._pages.values()), {}
            for page in pages:
                if not page.is_closed():
                    await page.close()
            meters, self._network_meters = tuple(self._network_meters.values()), {}
            for meter in meters:
                await asyncio.shield(meter.close("cancelled"))
            await self.manager.__aexit__(None, None, None)
            self.manager = self.browser = None

    async def shutdown(self) -> None:
        """Implement the process-owned :class:`BrowserBackend` contract."""
        await self.close()

    @asynccontextmanager
    async def open_session(
        self, job: BrowserJobContext | None = None,
    ) -> AsyncIterator[BrowserSession]:
        """Open a job-owned view over this shared Camoufox process.

        `job` is intentionally unused by Camoufox, but accepted so this backend
        has the same lifecycle contract as externally provisioned CDP sessions.
        Each view owns its persistent origin pages; sequential or concurrent
        jobs therefore never inherit page cookies through a reused tab.
        """
        session = CamoufoxBrowserSession(self)
        try:
            yield session
        finally:
            await session.close()


class CamoufoxBrowserSession:
    """Job-scoped pages backed by a process-scoped :class:`BrowserRenderer`."""

    def __init__(self, backend: BrowserRenderer) -> None:
        self.backend = backend
        self._pages: dict[str, Any] = {}
        self._page_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._closed = False

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None,
    ) -> str:
        self._ensure_open()
        return await self.backend.render(url, wait_ms, wait_for)

    async def evaluate(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> Any:
        self._ensure_open()
        return await self.backend.evaluate(url, script, wait_ms, wait_for)

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        self._ensure_open()
        return await self.backend.evaluate_result(url, script, wait_ms, wait_for)

    async def request_json(
        self, page_url: str, endpoint: str, *, method: str = "POST",
        headers: dict[str, str] | None = None, body: Any = None,
    ) -> Any:
        response = await self.request(
            page_url,
            endpoint,
            method=method,
            headers=headers,
            json_body=body,
        )
        if response.status >= 400:
            raise Blocked(
                f"{endpoint} returned {response.status} in the browser context"
            )
        try:
            return json_lib.loads(response.content)
        except json_lib.JSONDecodeError as error:
            raise Blocked(
                f"{endpoint} did not return JSON in the browser context"
            ) from error

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse:
        """Issue an HTTP request inside this isolated browser context."""
        self._ensure_open()
        if not self.backend.enabled:
            raise Blocked("browser rendering disabled (use --browser auto or always)")
        origin = urlparse(page_url).netloc
        async with self.backend.pages, self._page_locks[origin]:
            await self.backend._started()
            page = self._pages.get(origin)
            if page is None or page.is_closed():
                page = await self.backend.browser.new_page()
                meter = await self.backend._meter_page(page)
                try:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=45_000)
                    if meter is not None:
                        await meter.drain()
                except BaseException:
                    await page.close()
                    if meter is not None:
                        await meter.close("cancelled")
                        self.backend._network_meters.pop(id(page), None)
                    raise
                self._pages[origin] = page
            result = await page.evaluate(
                """async ({endpoint, method, headers, body}) => {
                    const response = await fetch(endpoint, {
                        method, headers,
                        body: body === null ? undefined : JSON.stringify(body),
                        credentials: 'include',
                    });
                    return {
                        status: response.status,
                        headers: Object.fromEntries(response.headers.entries()),
                        text: await response.text(),
                        url: response.url,
                    };
                }""",
                {
                    "endpoint": endpoint,
                    "method": method,
                    "headers": headers or {},
                    "body": json_body,
                },
            )
            meter = self.backend._network_meters.get(id(page))
            if meter is not None:
                await meter.drain()
            return BrowserFetchResponse(
                status=int(result["status"]),
                headers=MappingProxyType(
                    {
                        str(key): str(value)
                        for key, value in result["headers"].items()
                    }
                ),
                content=str(result["text"]).encode("utf-8"),
                final_url=str(result["url"]),
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrowserUnavailable("browser session is closed")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        pages, self._pages = list(self._pages.values()), {}
        for page in pages:
            if not page.is_closed():
                await page.close()
            meter = self.backend._network_meters.pop(id(page), None)
            if meter is not None:
                await asyncio.shield(meter.close("cancelled"))


def _headers_size(headers: Any) -> int:
    return sum(len(str(name).encode()) + len(str(value).encode()) + 4 for name, value in headers.items())


def _response_size(response: httpx.Response) -> int:
    """Estimate billed transfer bytes without charging for decompression.

    HTTPX exposes the bytes read from the network separately from
    ``response.content``, which is already decoded.  Large HTML pages from the
    Ceramic Shop expand by more than thirty times after gzip decoding; using
    that in-memory size exhausted a 25 MB paid-proxy reservation after Decodo
    had billed less than 1 MB.  Hand-built responses may not carry transport
    telemetry, so retain the decoded length as a conservative fallback.
    """
    downloaded = response.num_bytes_downloaded
    body = downloaded if downloaded > 0 else len(response.content)
    return body + _headers_size(response.headers)


def _request_size(method: str, target: str, headers: dict[str, str], body: Any) -> int:
    encoded_body = json_lib.dumps(body, default=str).encode() if body is not None else b""
    return len(method.encode()) + len(target.encode()) + _headers_size(headers) + len(encoded_body) + 16


def _outcome(status: int) -> str:
    if status == 403:
        return "403"
    if status == 429:
        return "429"
    return f"{status // 100}xx"


@dataclass
class TransportStats:
    http_tx_bytes_estimated: int = 0
    http_rx_bytes_estimated: int = 0
    browser_tx_bytes_estimated: int = 0
    browser_rx_bytes_estimated: int = 0
    direct_requests: int = 0
    impersonated_requests: int = 0
    browser_requests: int = 0
    proxy_requests: int = 0
    not_modified: int = 0
    bytes_saved_304: int = 0
    outcomes: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def merge(self, other: TransportStats) -> None:
        for name in (
            "http_tx_bytes_estimated", "http_rx_bytes_estimated",
            "browser_tx_bytes_estimated", "browser_rx_bytes_estimated",
            "direct_requests", "impersonated_requests", "browser_requests", "proxy_requests",
            "not_modified", "bytes_saved_304",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        for outcome, count in other.outcomes.items():
            self.outcomes[outcome] += count

    def copy_to(self, result: Any) -> None:
        for name in (
            "http_tx_bytes_estimated", "http_rx_bytes_estimated",
            "browser_tx_bytes_estimated", "browser_rx_bytes_estimated",
            "direct_requests", "impersonated_requests", "browser_requests", "proxy_requests",
        ):
            setattr(result, name, getattr(self, name))
        result.outcome_counts = dict(self.outcomes)
        result.outcome_counts["304_reused"] = self.not_modified
        result.outcome_counts["bytes_saved_304"] = self.bytes_saved_304


class Fetcher:
    """Polite HTTP access with robots handling, retries and a browser fallback."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        limiter: HostLimiter,
        browser: BrowserSession,
        browser_policy: str = "auto",
        cache: ResponseCache | None = None,
        impersonate_policy: str = "auto",
        impersonator: ImpersonatingClient | None = None,
        robots_policy: str = "ignore",
        stale_on_error: bool = False,
        proxy_lease: ProxyLease | None = None,
        proxy_fallback: Fetcher | None = None,
    ) -> None:
        self.client = client
        self.limiter = limiter
        self.browser = browser
        self.browser_policy = browser_policy
        self.impersonate_policy = impersonate_policy
        self.robots_policy = robots_policy
        self.proxy_lease = proxy_lease
        self.proxy_fallback = proxy_fallback
        self.impersonator = impersonator or ImpersonatingClient(
            impersonate_policy != "never", proxy_lease.url if proxy_lease else None
        )
        self.cache = cache or ResponseCache(".", mode="off")
        self.stale_on_error = stale_on_error
        self._robots: dict[str, urllib.robotparser.RobotFileParser] = {}
        self._sitemaps: dict[str, list[str]] = {}
        self._robots_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._proxy_until = 0.0
        self.stats = TransportStats()

    @property
    def proxy_bytes_remaining(self) -> int | None:
        """Return this request path's remaining paid-proxy reservation."""
        if self.proxy_lease is not None:
            return max(0, self.proxy_lease.max_bytes - self.proxy_lease.used_bytes)
        if self.proxy_fallback is not None:
            return self.proxy_fallback.proxy_bytes_remaining
        return None

    @staticmethod
    def _record_network(url: str, outcome: str, started: float, status: int | None = None) -> None:
        host = (urlparse(url).hostname or urlparse(url).netloc).lower()
        source = CURRENT_SOURCE.get() or None
        metrics.request(source, host, outcome)
        metrics.request_duration(host, time.monotonic() - started)
        if status is not None and status >= 400:
            metrics.http_error(host, status)

    async def rotate_client(self) -> None:
        """Replace this fetcher's HTTP session without losing crawl state.

        Used by large, public Shopify inventory joins: some storefront sessions
        stop completing requests after a healthy burst even though a fresh
        session from the same address continues immediately. The limiter,
        cache, statistics and proxy lease stay attached to this Fetcher.
        """
        previous = self.client
        if self.proxy_lease:
            # Decodo's sticky identity lives in the username. Replacing only
            # the HTTP connection keeps the same residential IP and therefore
            # keeps Shopify's IP-level throttle. Batch rotation is a safe point
            # to request a fresh identity: all requests using the old client
            # have completed, while accounting remains on the same lease.
            self.proxy_lease.rotate_session()
        self.client = httpx.AsyncClient(
            headers=dict(previous.headers), timeout=previous.timeout,
            follow_redirects=True,
            proxy=self.proxy_lease.url if self.proxy_lease else None,
        )
        await previous.aclose()
        if self.proxy_fallback:
            await self.proxy_fallback.rotate_client()

    async def robots(self, url: str) -> tuple[urllib.robotparser.RobotFileParser, list[str]]:
        origin = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        async with self._robots_lock[origin]:
            if origin in self._robots:
                return self._robots[origin], self._sitemaps[origin]
            parser, sitemaps = await self._read_robots(origin)
            self._robots[origin] = parser
            self._sitemaps[origin] = sitemaps
            return parser, sitemaps

    async def _read_robots(self, origin: str) -> tuple[urllib.robotparser.RobotFileParser, list[str]]:
        """Fetch and interpret robots.txt following RFC 9309.

        RobotFileParser.can_fetch returns False for a parser that was never
        given any content, so every unavailable robots.txt would otherwise
        silently disallow an entire site. The status code decides:
        2xx applies the rules, 4xx means "no restrictions published", and 5xx is
        treated conservatively as a full disallow.
        """
        parser = urllib.robotparser.RobotFileParser()
        url = f"{origin}/robots.txt"
        response: httpx.Response | None = None
        for browser_agent in (False, True):
            try:
                if self.proxy_lease:
                    self.proxy_lease.ensure_request_allowed()
                headers = {"user-agent": BROWSER_USER_AGENT} if browser_agent else None
                request_started = time.monotonic()
                response = await self.client.get(url, timeout=15, headers=headers)
                self._record_network(url, _outcome(response.status_code), request_started, response.status_code)
                tx = _request_size("GET", url, headers or {}, None)
                rx = _response_size(response)
                self.stats.http_tx_bytes_estimated += tx
                self.stats.http_rx_bytes_estimated += rx
                self.stats.outcomes[_outcome(response.status_code)] += 1
                if self.proxy_lease:
                    self.proxy_lease.account(tx, rx)
                    self.stats.proxy_requests += 1
                else:
                    self.stats.direct_requests += 1
            except (httpx.HTTPError, UnicodeError) as error:
                self._record_network(
                    url,
                    "timeout" if isinstance(error, httpx.TimeoutException) else "transport_error",
                    request_started,
                )
                LOGGER.warning("robots.txt unreachable at %s (%s); proceeding unrestricted", origin, error)
                parser.allow_all = True
                return parser, []
            # A CDN rejecting our declared agent is not the site's crawl policy;
            # ask once more as an ordinary browser before drawing a conclusion.
            if response.status_code not in (401, 403) or browser_agent:
                break
        assert response is not None

        if response.is_success:
            body = response.text
            if "html" in response.headers.get("content-type", "").lower() and "<html" in body[:400].lower():
                LOGGER.warning("robots.txt at %s returned HTML; proceeding unrestricted", origin)
                parser.allow_all = True
                return parser, []
            parser.parse(body.splitlines())
            sitemaps = [
                line.split(":", 1)[1].strip()
                for line in body.splitlines()
                if line.lower().startswith("sitemap:") and ":" in line and line.split(":", 1)[1].strip()
            ]
            return parser, sitemaps
        if 400 <= response.status_code < 500:
            parser.allow_all = True
            return parser, []
        LOGGER.warning("robots.txt at %s returned %d; treating as disallow", origin, response.status_code)
        parser.disallow_all = True
        return parser, []

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        """Whether to fetch this URL, and at what pace to remember it wants.

        robots.txt is read under either policy, because `Crawl-delay` and
        `Sitemap` are worth having whatever we do about `Disallow`. Only the
        Disallow is conditional: under `robots=ignore` the rate limiter is what
        keeps us welcome — a host's own `X-RateLimit` accounting, its
        `Retry-After`, and slots that halve on any error — rather than a file
        that cannot express any of that.
        """
        parser, _ = await self.robots(url)
        # A published Crawl-delay is remembered, not applied: a host that is
        # answering happily is crawled at the operator's pace, and only one
        # that starts erroring gets the pace it asked for. Disallow is a rule
        # and is obeyed either way.
        published = parser.crawl_delay(USER_AGENT)
        self.limiter.remember_crawl_delay(url, published)
        if obey_robots:
            # A source may ask to be held to the file even where the fleet is
            # not. The strictest request wins, which is the same rule
            # `set_delay` already follows for pace.
            return parser.can_fetch(USER_AGENT, url)
        if ignore_robots or self.robots_policy == "ignore":
            # Not obeying Disallow and also ignoring the pace the host asked for
            # would be taking the whole file as noise. A Crawl-delay is the one
            # thing in robots.txt that says what the shop can actually stand, so
            # it becomes a floor here rather than something only adopted after
            # the host has started refusing us. It is a floor, so it can only
            # ever slow us down relative to the configured delay.
            if published:
                self.limiter.set_delay(url, float(published))
            return True
        return parser.can_fetch(USER_AGENT, url)

    async def text(
        self,
        url: str,
        *,
        browser_user_agent: bool = False,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
        headers: dict[str, str] | None = None,
        allow_proxy_fallback: bool = True,
    ) -> str:
        response = await self.response(
            url, browser_user_agent=browser_user_agent, params=params, accept=accept,
            headers=headers,
            allow_proxy_fallback=allow_proxy_fallback,
        )
        return response.text

    async def json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        browser_user_agent: bool = False,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = await self.response(
            url, params=params, browser_user_agent=browser_user_agent,
            accept="application/json", headers=headers,
        )
        try:
            return response.json()
        except json_lib.JSONDecodeError as error:
            raise Blocked(f"{url} did not return JSON ({response.headers.get('content-type')})") from error

    async def response(
        self,
        url: str,
        *,
        browser_user_agent: bool = False,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
        method: str = "GET",
        json_body: Any = None,
        headers: dict[str, str] | None = None,
        allow_proxy_fallback: bool = True,
    ) -> httpx.Response:
        request_headers: dict[str, str] = dict(headers or {})
        if browser_user_agent:
            request_headers["user-agent"] = BROWSER_USER_AGENT
        if accept:
            request_headers["accept"] = accept

        # The query string is part of what was asked for, so the display and the
        # cache both see the whole request rather than a bare path.
        target = str(httpx.URL(url, params=params)) if params else url
        key = self.cache.key(
            "http", url, method=method, params=params, body=json_body, agent=browser_user_agent,
        )
        if stored := self.cache.read(key, url):
            ACTIVITY.finished(target, "cached")
            return httpx.Response(
                stored.status,
                content=stored.body.encode("utf-8"),
                headers=stored.headers,
                request=httpx.Request(method, stored.url or url),
                extensions={
                    "catalogue_cache_provenance": (
                        "replayed" if self.cache.mode == "replay" else "fresh"
                    )
                },
            )
        if self.cache.mode == "replay":
            raise NotCached(f"{method} {url} is not in the cache")

        stale = self.cache.stale_read(key, url)
        if stale:
            if etag := stale.headers.get("etag"):
                request_headers.setdefault("if-none-match", etag)
            if modified := stale.headers.get("last-modified"):
                request_headers.setdefault("if-modified-since", modified)

        if (
            self.proxy_fallback is not None
            and allow_proxy_fallback
            and time.monotonic() < self._proxy_until
        ):
            if stale is not None and self.stale_on_error and method == "GET":
                self.stats.outcomes["stale_on_error"] += 1
                return self._stale_response(stale, method, url)
            return await self.proxy_fallback.response(
                url, browser_user_agent=browser_user_agent, params=params,
                accept=accept, method=method, json_body=json_body, headers=headers,
            )

        response: httpx.Response | None = None
        for attempt in range(4):
            if self.proxy_lease:
                self.proxy_lease.ensure_request_allowed()
            async with self.limiter.gate(url):
                await self.limiter.wait(url)
                ACTIVITY.started(target)
                request_started = time.monotonic()
                try:
                    response = await self.client.request(
                        method, url, params=params, json=json_body, headers=request_headers or None,
                    )
                    self._record_network(
                        url, _outcome(response.status_code), request_started, response.status_code
                    )
                    ACTIVITY.finished(target, str(response.status_code))
                    if self.proxy_lease is None:
                        self.stats.direct_requests += 1
                    self.stats.http_tx_bytes_estimated += _request_size(
                        method, target, request_headers, json_body
                    )
                    rx = _response_size(response)
                    self.stats.http_rx_bytes_estimated += rx
                    self.stats.outcomes[_outcome(response.status_code)] += 1
                    if self.proxy_lease:
                        tx = _request_size(method, target, request_headers, json_body)
                        self.proxy_lease.account(tx, rx)
                        self.stats.proxy_requests += 1
                except (httpx.HTTPError, UnicodeError) as error:
                    self._record_network(
                        url,
                        "timeout" if isinstance(error, httpx.TimeoutException) else "transport_error",
                        request_started,
                    )
                    ACTIVITY.finished(target, type(error).__name__)
                    # A refused or timed-out request is the host telling us the
                    # pace is wrong just as clearly as a 429 does. Crawling with
                    # no wait between requests makes that answer common, so the
                    # page is asked for again once the backoff has taken effect
                    # rather than dropped — going fast must not cost coverage.
                    self.limiter.record_failure(url, type(error).__name__)
                    self.stats.outcomes["timeout" if isinstance(error, httpx.TimeoutException) else "transport_error"] += 1
                    if attempt == 3:
                        if stale is not None and self.stale_on_error and method == "GET":
                            self.stats.outcomes["stale_on_error"] += 1
                            return self._stale_response(stale, method, url)
                        if self.proxy_fallback is not None and allow_proxy_fallback:
                            return await self.proxy_fallback.response(
                                url, browser_user_agent=browser_user_agent, params=params,
                                accept=accept, method=method, json_body=json_body, headers=headers,
                            )
                        raise
                    pause = self.limiter.spacing(urlparse(url).netloc) or 1.0
                    LOGGER.warning(
                        "transport=%s url=%s retry=%d pause=%.1fs",
                        type(error).__name__, url, attempt + 1, pause,
                    )
                    await asyncio.sleep(pause)
                    continue
            # Read the meter on every answer, including the refusals: a 429
            # usually carries the same headers and is the clearest statement of
            # the pace a host wants.
            self.limiter.observe_headers(url, response.headers)
            if response.status_code != 429 and response.status_code < 500:
                self.limiter.record_success(url)
                break
            self.limiter.record_failure(url, response.status_code)
            retry_after = response.headers.get("retry-after")
            if response.status_code == 429:
                # A host that says how long to wait has said what its limit is.
                # Hold that as a floor rather than only sleeping once, so the
                # rest of the source is paced instead of racing back into it.
                try:
                    if retry_after and float(retry_after) > 0:
                        self.limiter.set_delay(url, min(float(retry_after), HostLimiter.BACKOFF_MAX))
                except ValueError:
                    pass
            rate_limited = response.status_code == 429 or (
                response.status_code >= 500 and bool(retry_after)
            )
            prefer_stale = stale is not None and self.stale_on_error and method == "GET"
            if (
                rate_limited
                and self.proxy_fallback is not None
                and allow_proxy_fallback
                and not prefer_stale
            ):
                # A 429, or an edge 5xx carrying Retry-After, is conclusive
                # evidence that this network identity is throttled. Repeating
                # the same request through several multi-minute waits cannot
                # add evidence; use the already-approved bounded fallback now.
                try:
                    requested_cooldown = float(retry_after) if retry_after else 0.0
                except ValueError:
                    requested_cooldown = 0.0
                self._proxy_until = max(
                    self._proxy_until, time.monotonic() + max(60.0, requested_cooldown),
                )
                return await self.proxy_fallback.response(
                    url, browser_user_agent=browser_user_agent, params=params,
                    accept=accept, method=method, json_body=json_body, headers=headers,
                )
            if attempt == 3:
                break
            try:
                pause = float(retry_after) if retry_after else min(2 ** attempt, 8)
            except ValueError:
                pause = min(2 ** attempt, 8)
            LOGGER.warning("status=%d url=%s retry=%d pause=%.1fs", response.status_code, url, attempt + 1, pause)
            await asyncio.sleep(max(0.0, pause))
        assert response is not None
        if response.status_code == 304 and stale is not None:
            stale.fetched_at = time.time()
            stale.headers.update(
                {
                    name: value
                    for name, value in response.headers.items()
                    if name.lower() in ("content-type", "content-language", "last-modified", "etag")
                }
            )
            self.cache.write(key, stale)
            self.stats.not_modified += 1
            self.stats.bytes_saved_304 += len(stale.body.encode("utf-8"))
            return httpx.Response(
                stale.status,
                content=stale.body.encode("utf-8"),
                headers=stale.headers,
                request=httpx.Request(method, stale.url or url),
                extensions={"catalogue_cache_provenance": "fresh"},
            )
        if (
            stale is not None
            and self.stale_on_error
            and method == "GET"
            and (response.status_code in (408, 425, 429) or response.status_code >= 500)
        ):
            self.stats.outcomes["stale_on_error"] += 1
            return self._stale_response(stale, method, url)
        # A retry-exhausted rate limit is exactly the classified failure that a
        # fallback proxy is meant to escape.  Shopify also reports some edge
        # throttles as a 5xx with Retry-After, so treat that explicit pacing
        # signal like a 429.  Ordinary 5xx responses remain direct-only: a new
        # IP cannot repair an unhealthy origin and would only spend the paid
        # budget needlessly.
        rate_limited = response.status_code == 429 or (
            response.status_code >= 500 and bool(response.headers.get("retry-after"))
        )
        if rate_limited and self.proxy_fallback is not None and allow_proxy_fallback:
            return await self.proxy_fallback.response(
                url, browser_user_agent=browser_user_agent, params=params,
                accept=accept, method=method, json_body=json_body, headers=headers,
            )
        if response.status_code in (401, 403, 406):
            # Three rungs, cheapest first. Several CDNs reject a declared
            # research agent but serve the same public page to an ordinary
            # browser string; the ones that are still refusing after that are
            # usually reading the TLS handshake rather than any header, and the
            # only answer to that is a real browser's handshake.
            if not browser_user_agent:
                return await self.response(
                    url, browser_user_agent=True, params=params, accept=accept,
                    method=method, json_body=json_body, headers=headers,
                    allow_proxy_fallback=allow_proxy_fallback,
                )
            impersonated = await self._impersonate(
                url, method=method, params=params, headers=request_headers, json_body=json_body,
            )
            if impersonated is not None:
                response = impersonated
            elif (
                self.proxy_fallback is not None
                and allow_proxy_fallback
                and response.status_code in (403, 406)
            ):
                return await self.proxy_fallback.response(
                    url, browser_user_agent=True, params=params, accept=accept,
                    method=method, json_body=json_body, headers=headers,
                )
        response.raise_for_status()
        if refusal := looks_like_a_block(response.text, response.headers.get("content-type", "")):
            # A refusal wearing a 200. Raised rather than returned so it travels
            # the same path as any other Blocked — recorded against the source,
            # survivable page by page — instead of being mistaken for a real
            # page that happens to contain nothing.
            self.stats.outcomes["block_page"] += 1
            if self.proxy_fallback is not None and allow_proxy_fallback:
                return await self.proxy_fallback.response(
                    url, browser_user_agent=True, params=params, accept=accept,
                    method=method, json_body=json_body, headers=headers,
                )
            raise Blocked(f"{url} returned a block page with status 200: {refusal!r}")
        # Only a response the site actually completed is worth replaying; a
        # retry-exhausted 5xx would otherwise be served back as if it were the
        # page. Recording the final URL keeps redirects out of the next run.
        self.cache.write(key, CachedResponse(
            status=response.status_code,
            url=str(response.url),
            body=response.text,
            headers={
                name: value for name, value in response.headers.items()
                if name.lower() in ("content-type", "content-language", "last-modified", "etag")
            },
            fetched_at=time.time(),
        ))
        return response

    async def render_through_proxy(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None,
    ) -> str:
        if self.proxy_fallback is None:
            raise Blocked("no proxy fallback is configured")
        return await self.proxy_fallback.render(url, wait_ms, wait_for)

    @staticmethod
    def _stale_response(stale: CachedResponse, method: str, url: str) -> httpx.Response:
        return httpx.Response(
            stale.status,
            content=stale.body.encode("utf-8"),
            headers=stale.headers,
            request=httpx.Request(method, stale.url or url),
            extensions={
                "catalogue_cache_provenance": "stale",
                "catalogue_stale_on_error": True,
            },
        )

    async def _impersonate(
        self,
        url: str,
        *,
        method: str,
        params: dict[str, Any] | None,
        headers: dict[str, str],
        json_body: Any,
    ) -> httpx.Response | None:
        """Re-ask with a browser's TLS fingerprint. None means it did not help.

        Deliberately quiet about failure: this is the last rung of a fallback
        ladder, and the caller already holds a 403 to raise. Whatever goes wrong
        here — the package missing, the host refusing this handshake too — the
        honest outcome is the refusal the site gave us, not an error about our
        own tooling.
        """
        if not self.impersonator.available:
            return None
        target = str(httpx.URL(url, params=params)) if params else url
        ACTIVITY.started(target)
        request_started = time.monotonic()
        async with self.limiter.gate(url):
            await self.limiter.wait(url)
            try:
                response = await self.impersonator.request(
                    url, method=method, params=params,
                    headers={**headers, "user-agent": BROWSER_USER_AGENT},
                    json_body=json_body,
                )
            except (Blocked, Exception) as error:  # noqa: BLE001 - curl_cffi raises its own
                self._record_network(url, "transport_error", request_started)
                ACTIVITY.finished(target, "impersonate-error")
                LOGGER.debug("impersonation failed for %s (%s)", url, error)
                return None
        ACTIVITY.finished(target, f"{response.status_code} (impersonated)")
        self._record_network(url, _outcome(response.status_code), request_started, response.status_code)
        self.stats.impersonated_requests += 1
        rx = _response_size(response)
        self.stats.http_rx_bytes_estimated += rx
        self.stats.http_tx_bytes_estimated += _request_size(method, target, headers, json_body)
        self.stats.outcomes[_outcome(response.status_code)] += 1
        if self.proxy_lease:
            tx = _request_size(method, target, headers, json_body)
            self.proxy_lease.account(tx, rx)
            self.stats.proxy_requests += 1
        if response.status_code >= 400:
            return None
        LOGGER.info("host=%s served an impersonated handshake after refusing ours", urlparse(url).netloc)
        return response

    async def render(self, url: str, wait_ms: int = 1500, wait_for: str | None = None) -> str:
        if self.browser_policy == "never":
            raise Blocked("browser rendering disabled")
        # A rendered page is cached like any other response: replaying it is the
        # only way to reparse a browser-only source (amaco, ceramicolours)
        # without starting a browser at all.
        key = self.cache.key("render", url, wait_ms=wait_ms, wait_for=wait_for)
        if stored := self.cache.read(key, url):
            # `url`, not the `target` the HTTP path builds: a render takes no
            # query parameters of its own, and naming the other variable here
            # raised NameError on every cache hit — so replaying a rendered
            # source, which is the one thing this cache entry exists for, could
            # never actually work.
            ACTIVITY.finished(url, "cached")
            return stored.body
        if self.cache.mode == "replay":
            raise NotCached(f"render {url} is not in the cache")
        ACTIVITY.started(url)
        metrics.browser_render(CURRENT_SOURCE.get() or None)
        proxy_requests_before = self.proxy_lease.requests if self.proxy_lease else 0
        # Through the limiter, exactly like an HTTP request. A rendered page is
        # a request to someone's shop and it was the only kind that skipped
        # this: the host's slot count, its gap, its backoff and its published
        # Crawl-delay all applied to `response()` and none of them applied here.
        # What made that survivable was an accident — `BrowserRenderer` held one
        # lock across a whole page load, so a process could only ever render one
        # page at a time — and raising the page limit to make renders concurrent
        # turned the accident into two unpaced requests at one shop. ceram-decor
        # answered that with 403s and 45-second timeouts and its record count
        # halved.
        async with self.limiter.gate(url):
            await self.limiter.wait(url)
            try:
                document = await self.browser.render(url, wait_ms, wait_for)
            except Exception as error:
                ACTIVITY.finished(url, "browser-error")
                # A timeout or a refusal in the browser says what the host wants
                # as clearly as a 429 does, so it has to reach the limiter or
                # the next page repeats it.
                self.limiter.record_failure(url, type(error).__name__)
                raise
            self.limiter.record_success(url)
        ACTIVITY.finished(url, "rendered")
        self.stats.browser_requests += 1
        if self.proxy_lease:
            self.stats.proxy_requests += self.proxy_lease.requests - proxy_requests_before
        self.stats.browser_rx_bytes_estimated += len(document.encode("utf-8"))
        self.cache.write(key, CachedResponse(
            status=200, url=url, body=document, headers={}, fetched_at=time.time(), kind="render",
        ))
        return document

    async def evaluate_in_browser(
        self,
        url: str,
        script: str,
        wait_ms: int = 2000,
        wait_for: str | None = None,
        *,
        action_id: str = "legacy-evaluate.v1",
    ) -> Any:
        key = self.cache.key(
            "browser-evaluate",
            url,
            action_id=action_id,
            script_sha256=hashlib.sha256(script.encode()).hexdigest(),
            wait_ms=wait_ms,
            wait_for=wait_for,
        )
        if stored := self.cache.read(key, url):
            return json_lib.loads(stored.body)
        if self.cache.mode == "replay":
            raise NotCached(f"browser evaluation for {url} is not in the cache")
        if self.browser_policy == "never":
            raise Blocked("browser rendering disabled")
        proxy_requests_before = self.proxy_lease.requests if self.proxy_lease else 0
        async with self.limiter.gate(url):
            await self.limiter.wait(url)
            try:
                result = await self.browser.evaluate(url, script, wait_ms, wait_for)
            except Exception as error:
                self.limiter.record_failure(url, type(error).__name__)
                raise
            self.limiter.record_success(url)
        encoded = json_lib.dumps(result, ensure_ascii=False, separators=(",", ":"))
        self.stats.browser_requests += 1
        if self.proxy_lease:
            self.stats.proxy_requests += self.proxy_lease.requests - proxy_requests_before
        self.stats.browser_rx_bytes_estimated += len(encoded.encode())
        self.cache.write(
            key,
            CachedResponse(
                status=200,
                url=url,
                body=encoded,
                headers={"content-type": "application/json"},
                fetched_at=time.time(),
                kind="browser-evaluate",
            ),
        )
        return result

    async def request_json_in_browser(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        key = self.cache.key("browser-json", endpoint, page=page_url, method=method, body=body)
        if stored := self.cache.read(key, endpoint):
            return json_lib.loads(stored.body)
        if self.cache.mode == "replay":
            raise NotCached(f"{method} {endpoint} (in browser) is not in the cache")
        if self.browser_policy == "never":
            raise Blocked("browser rendering disabled")
        async with self.limiter.gate(endpoint):
            await self.limiter.wait(endpoint)
            try:
                payload = await self.browser.request_json(
                    page_url, endpoint, method=method, headers=headers, body=body,
                )
            except Exception as error:
                self.limiter.record_failure(endpoint, type(error).__name__)
                raise
            self.limiter.record_success(endpoint)
        self.cache.write(key, CachedResponse(
            status=200, url=endpoint, body=json_lib.dumps(payload), headers={},
            fetched_at=time.time(), kind="browser-json",
        ))
        return payload


@dataclass
class ScrapeResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    requests: int = 0
    rendered_pages: int = 0
    discovered: int = 0
    truncated: bool = False
    notes: list[str] = field(default_factory=list)
    http_tx_bytes_estimated: int = 0
    http_rx_bytes_estimated: int = 0
    browser_tx_bytes_estimated: int = 0
    browser_rx_bytes_estimated: int = 0
    cache_bytes_read: int = 0
    direct_requests: int = 0
    impersonated_requests: int = 0
    browser_requests: int = 0
    proxy_requests: int = 0
    proxy_bytes_reserved: int = 0
    proxy_bytes_estimated: int = 0
    browser_gain: int = 0
    browser_zero_gain: int = 0
    #: Rows the extractor read and the scope filter then dropped. Counted so a
    #: source that produced nothing can say *why* it produced nothing.
    filtered: int = 0
    #: Rows the extractor produced that lacked a usable identity or price.
    #: These are parser/data-quality failures, not materials-scope decisions.
    invalid: int = 0
    outcome_counts: dict[str, int] = field(default_factory=dict)


class Scraper(ABC):
    """One supplier's collection strategy.

    Implementations fetch as narrowly as the site allows, emit one row per
    purchasable variant through record.build, and never invent a field the
    supplier did not publish.
    """

    platform: str = "custom"
    #: api_json, graphql, jsonld, dom or browser - recorded on every row.
    method: str = "dom"

    def __init__(self, name: str, config: dict[str, Any], fetcher: Fetcher) -> None:
        self.name = name
        self.config = config
        self.fetcher = fetcher
        self.result = ScrapeResult()
        #: Whether a failure right now means the catalogue was not fully listed.
        #: True by default, and see `extracting` for why that direction.
        self._enumerating = True
        self.base_url = config.get("url", "")
        if edge := SHARED_EDGES.get(self.platform):
            fetcher.limiter.join_group(self.base_url, edge)
        if delay := config.get("delay"):
            fetcher.limiter.set_delay(self.base_url, float(delay))

    # -- helpers shared by every implementation ---------------------------

    @property
    def ignore_robots(self) -> bool:
        return bool(self.config.get("ignore_robots"))

    @property
    def obey_robots(self) -> bool:
        """This source obeys Disallow even where the run policy does not."""
        return bool(self.config.get("obey_robots"))

    @property
    def strict_scope(self) -> bool:
        return self.config.get("scope", "materials") == "materials"

    def origin(self, url: str | None = None) -> str:
        parsed = urlparse(url or self.base_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def note(self, message: str) -> None:
        LOGGER.info("source=%s %s", self.name, message)
        self.result.notes.append(message)

    @contextmanager
    def extracting(self) -> Iterator[None]:
        """Mark work that reads one already-listed product, not the listing.

        Failures divide in two, and only one of them bears on retirement. A
        *listing* request that fails — a sitemap, a category page, a page of
        pagination — leaves an unknown number of products unseen, so the dump is
        not the catalogue and `plan_load` must not retire against it. A *product
        page* that fails costs one row we already knew about.

        Enumeration is therefore the default and this is the exception, which is
        the safe direction round. The same bug was fixed three times in three
        places — Shopify and Woo pagination, `pagecrawl` discovery, then the
        three scrapers overriding discovery that the second fix missed — and
        every fix was a call site remembering to say something. A scraper
        written tomorrow will not remember and has no reason to know the rule
        exists. Getting this wrong now means over-reporting truncation, which
        costs a retirement that waits for the next run; the other direction
        withdraws a live catalogue.
        """
        previous = self._enumerating
        self._enumerating = False
        try:
            yield
        finally:
            self._enumerating = previous

    def fail(self, url: str, error: Exception | str) -> None:
        self.result.errors.append({"url": url, "error": str(error)})
        if self._enumerating:
            # Not a judgement about this page. While enumerating, a page we
            # could not read is a branch of the catalogue nobody listed.
            self.result.truncated = True
        LOGGER.warning("source=%s url=%s error=%s", self.name, url, error)

    def enumeration_failed(self, url: str, error: Exception | str) -> None:
        """Record a failure that ended the walk before the catalogue did.

        The distinction from `fail` is not about severity, it is about what the
        file that comes out means. A product page that fails costs one known
        row. A *listing* request that fails costs an unknown number of rows
        nobody ever enumerated, so the dump is no longer the whole catalogue —
        and `plan_load` reads `truncated` as its permission to retire anything
        missing from it. Leaving the flag off after a failed page of pagination
        hands the loader a half-catalogue labelled complete, and everything past
        the failure gets withdrawn as though the shop had stopped selling it.

        This is not hypothetical: soundingstone.com and seattlepotterysupply.com
        both answered 429 partway through their pagination on the first run of
        the US sources, returned about two thirds of their rows, and reported
        `truncated: false`. Nothing was retired only because both were new
        sources with nothing yet to retire.
        """
        self.fail(url, error)
        self.result.truncated = True

    def category_allows(self, *values: Any) -> bool | None:
        """Match a product's categories against this source's materials allowlist.

        Returns None when the source declares no allowlist, so the caller falls
        back to keyword classification instead of assuming a match.
        """
        allowed = self.config.get("material_categories")
        if not allowed:
            return None
        haystack = domain.fold(" ".join(domain.clean(value) for value in values if value))
        return any(domain.fold(entry) in haystack for entry in allowed)

    def excluded(self, *values: Any) -> bool:
        blocked = self.config.get("excluded_categories") or []
        haystack = domain.fold(" ".join(domain.clean(value) for value in values if value))
        return any(domain.fold(entry) in haystack for entry in blocked)

    def keep(self, row: dict[str, Any], category_match: bool | None = None) -> bool:
        """Apply validity and the ceramic-materials scope to one candidate row."""
        if not record_module.is_valid(row):
            return False
        if not self.strict_scope:
            return True
        categories = " ".join(row.get("category_path") or [])
        if self.excluded(categories, row.get("name")):
            return False
        if category_match is True:
            # An allowlisted category is authoritative, but still drop obvious
            # equipment filed inside it (a glaze brush under "Glazes").
            return not domain.looks_non_material(row.get("name"))
        if category_match is False:
            return False
        return record_module.in_scope(row, strict=True)

    def add(self, row: dict[str, Any] | None, category_match: bool | None = None) -> None:
        if not row:
            return
        if not record_module.is_valid(row):
            self.result.invalid += 1
            return
        if self.keep(row, category_match):
            self.result.records.append(row)
        else:
            self.result.filtered += 1

    async def sitemap_urls(self, sitemap_urls: list[str], pattern: str | None = None) -> list[str]:
        """Walk sitemap indexes and return matching product URLs."""
        found: list[str] = []
        seen: set[str] = set()
        queue = list(dict.fromkeys(sitemap_urls))
        compiled = re.compile(pattern) if pattern else None
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            try:
                response = await self.fetcher.response(url, accept="application/xml,text/xml")
                self.result.requests += 1
                document = self._sitemap_text(response)
            except (httpx.HTTPError, Blocked, UnicodeError) as error:
                self.fail(url, error)
                continue
            locations = [
                domain.clean(value)
                for value in re.findall(r"<loc>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</loc>", document, re.I)
            ]
            if re.search(r"<sitemapindex\b", document, re.I):
                queue.extend(locations)
                continue
            found.extend(value for value in locations if not compiled or compiled.search(value))
        return list(dict.fromkeys(found))

    @staticmethod
    def _sitemap_text(response: httpx.Response) -> str:
        """Decode a sitemap, decompressing the .xml.gz form many shops publish.

        httpx only unwraps gzip used as a transfer encoding; a .gz file arrives
        as compressed bytes and has to be inflated here.
        """
        body = response.content
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        return body.decode(response.encoding or "utf-8", errors="replace")

    @abstractmethod
    async def scrape(self, limit: int | None = None) -> ScrapeResult:
        """Collect the source and return its records."""

    async def run(self, limit: int | None = None) -> ScrapeResult:
        start = self.base_url
        if start and not await self.fetcher.may_fetch(start, self.ignore_robots, self.obey_robots):
            self.result.errors.append({"url": start, "error": "robots.txt disallows this crawler"})
            self.note("skipped: robots.txt disallows this crawler")
            return self.result
        if self.ignore_robots:
            self.note("robots.txt intentionally not applied for this source (operator decision)")
        try:
            await self.scrape(limit)
        except (httpx.HTTPError, Blocked, ProxyDenied, OSError, UnicodeError) as error:
            self.fail(start, error)
        self.result.records = self.deduplicate(self.result.records)
        self.fetcher.stats.copy_to(self.result)
        if self.fetcher.proxy_fallback:
            self.fetcher.stats.merge(self.fetcher.proxy_fallback.stats)
            self.fetcher.stats.copy_to(self.result)
        self.result.outcome_counts["browser_gain"] = self.result.browser_gain
        self.result.outcome_counts["browser_zero_gain"] = self.result.browser_zero_gain
        self.result.cache_bytes_read = self.fetcher.cache.bytes_read
        proxy_lease = self.fetcher.proxy_lease or (
            self.fetcher.proxy_fallback.proxy_lease if self.fetcher.proxy_fallback else None
        )
        if proxy_lease:
            self.result.proxy_bytes_reserved = proxy_lease.max_bytes
            self.result.proxy_bytes_estimated = proxy_lease.used_bytes
        return self.result

    @staticmethod
    def deduplicate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in records:
            merged.setdefault(record_module.dedupe_key(row), row)
        return list(merged.values())


def absolute(base: str, url: Any) -> str | None:
    text = domain.clean(url)
    return urljoin(base, text) if text else None
