# Proxy integration

The scraper library defines a provider-neutral data plane. The application
owns vendor accounts, secret loading, billing reservations, durable usage, and
operator policy. The core runtime sees only `ProxyPool`, `ProxyLease`, routing
policy, and transport factories.

```text
operator policy + secret store + billing ledger  (application)
                    |
             provider adapter: ProxyPool
                    |
CommerceScraper -> RoutedTransport -> HTTP/browser transport bound to ProxyLease
```

The catalogue implementations are useful references:

- [`PostgresDecodoProxyPool`](../../catalogue-dump/src/mb_ceramics_catalogue/ops/commerce_scraper_proxy.py)
  demonstrates durable authorization and reconciliation;
- [`WebshareGatewayPool`](../../catalogue-dump/src/mb_ceramics_catalogue/ops/commerce_scraper_webshare.py)
  demonstrates provider capability and credential projection over neutral
  accounting;
- [`CamoufoxProxyBrowserTransportFactory`](../../catalogue-dump/src/mb_ceramics_catalogue/ops/commerce_scraper_browser.py)
  binds browser traffic and every browser subrequest to the same lease.

Provider grammar belongs in those application adapters, never in
`mb_commerce_scraper.proxy`.

## Start with a local/static pool

`StaticProxyPool` is suitable for deterministic tests and deployments whose
budgets do not require cross-process durability:

```python
from pydantic import SecretStr

from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig
from mb_commerce_scraper.proxy import (
    ProxyCredentials,
    ProxyEndpoint,
    ProxyKind,
    StaticProxyPool,
    StaticRoute,
)
from mb_commerce_scraper.runtime import build_http_scraper

route = StaticRoute(
    endpoint=ProxyEndpoint(
        provider="example",
        endpoint_id="fr-1",
        protocol="socks5",
        host="127.0.0.1",
        port=1080,
        kind=ProxyKind.RESIDENTIAL,
        countries=frozenset({"FR"}),
    ),
    credentials=ProxyCredentials(
        username=SecretStr("runtime-user"),
        password=SecretStr("runtime-password"),
    ),
)

async with build_http_scraper(
    allowed_origins=("https://shop.example",),
    proxy_pool=StaticProxyPool((route,)),
    proxy_policy=ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK,
        country="FR",
        maximum_requests=100,
        maximum_bytes=25_000_000,
    ),
) as scraper:
    async for page in scraper.collect(source):
        consume(page)
```

Production hosts and credentials must come from an application secret store;
the literals above are deliberately local-only placeholders. Keep the origin
allowlist narrow. A proxy is not an SSRF bypass: redirects and DNS resolution
remain subject to the shared URL policy.

Static routes can prove proxy kind, country, and provider identity. They do not
declare region, city, or session-duration capabilities, so `StaticProxyPool`
rejects requests carrying those constraints before selecting a route or
checking out a lease. Use an application-owned provider adapter when those
constraints are required; silently relaxing paid-route policy is not allowed.

## Provider adapter contract

Implement all `ProxyPool` lifecycle operations:

| Operation | Required behavior |
|---|---|
| `acquire(request)` | Validate kind/geography/session/provider capabilities, reserve ownership, return one sticky lease. |
| `authorize(lease, estimated_bytes)` | Atomically reserve one physical attempt and expected bytes before dispatch; return `None` on denial. |
| authorization `reconcile(outcome)` | Resolve exactly once with actual physical requests and transmitted/received bytes. |
| authorization `release()` | Resolve exactly once only when dispatch never began. |
| `report(lease, outcome)` | Update bounded, reason-aware route/target health without charging again. |
| `rotate(lease, reason)` | Invalidate the old identity and preserve collection usage across the replacement. |
| `release(lease)` | Be idempotent, revoke ownership, and free external resources. |

`can_start()` is only a non-authoritative preview. The atomic `authorize()`
decision is the paid-traffic boundary. Database-backed implementations must
make authorization and its audit record one transaction and fail closed when
the ledger or audit append is unavailable.

Each lease exposes secret-aware HTTP and browser credential projections.
Route metadata contains provider, endpoint, and lease identifiers, never
usernames or passwords. Do not place credentials in URLs stored in config,
diagnostics, cache keys, checkpoints, exceptions, or telemetry.

## Routing and retry ownership

Choose routing explicitly with `ProxyPolicyConfig`. `ProxyRouting` remains the
low-level transport contract for callers that compose `RoutedTransport`
directly; high-level runtime composition uses one policy object so route and
budget fields cannot drift:

- `never`: direct only;
- `always`: proxy only;
- `fallback`: direct first, then proxy only after a typed block or transport
  classification;
- `failover`: proxy route rotation after an eligible proxy outcome;
- `round_robin`: proxy-only selection; the pool may distribute new leases,
  while each collection keeps its acquired lease sticky.

The high-level builder and client retain `routing`,
`proxy_maximum_requests`, and `proxy_maximum_bytes` as a compatibility path for
the initial release. Do not mix those arguments with `proxy_policy`; mixed
configuration is rejected. Cap-only legacy configuration is also rejected
because a limit without active routing cannot authorize or constrain traffic.
Pass an explicit `ProxyPolicyConfig(mode=ProxyMode.NEVER)` to disable routing
for one collection without touching an available proxy backend.

`RoutedTransport` performs one physical attempt. `MiddlewareTransport` owns
retry transitions. An adapter must not add an independent retry loop or charge
both a logical connector call and its physical attempts.

For browser use, implement `ProxyBrowserTransportFactory.build(lease,
authorizer)`. The backend must advertise `browser_subrequests_authorized=True`
only when every continued browser network request obtains authorization before
dispatch and reconciles exactly once. An unmarked backend retains the outer
logical browser-call authorization; it does not establish a hard cap before
each physical subrequest. A production application that requires hard
subrequest caps must set
`require_proxy_browser_subrequest_authorization=True` on `CommerceScraper` or
`build_http_scraper`; composition then rejects an unmarked proxy-browser
factory before acquiring a lease. Blocked resource types should be aborted
before authorization.

## Essential integration tests

Use a real loopback proxy protocol at the network seam, and fakes elsewhere.
The library suite already provides HTTP, authenticated SOCKS5, and two-provider
failover examples in
[`test_http_proxy_integration.py`](../tests/test_http_proxy_integration.py).
For a new adapter, add only the provider-specific cases:

1. request capability validation and credential projection;
2. sticky acquisition, rotation, and exactly-once cleanup;
3. atomic request/byte denial and actual-byte reconciliation;
4. typed block/transport classification and next-provider selection;
5. cancellation plus browser-subrequest authorization if supported;
6. serialized outputs, telemetry, and errors do not contain test secrets.

```console
uv --directory commerce-scraper run --extra dev -- \
  pytest tests/test_http_proxy_integration.py tests/test_proxy_transport.py
make scraper-check
```

## Tracing and incident debugging

Pass one telemetry sink at composition. The runtime emits stable route,
authorization, rotation, retry, and collection events with collection/source
context. Keep protocol detail at `debug`, lifecycle and selected route at
`info`, and denial, exhaustion, rotation failure, or degraded completeness at
`warning`. Diagnose a request by `collection_id` and `request_id`; the same
request identity and explicit attempt number cross retry, direct/proxy rate
gates, browser dispatch, proxy acquire/rotation/outcome, and terminal request
events. Retry events include the selected backoff. Browser events identify the
typed HTTP/browser decision or denial reason. Continue with provider, endpoint,
lease, classification, byte totals, and elapsed time.

Never emit proxy URLs with user information, raw headers, secret-file paths,
provider response bodies, or exception messages that may echo credentials.
Telemetry URL fields preserve bounded paths and query key names only; all query
values are redacted and fragments removed. Invalid or unbounded event names are
replaced rather than echoed.
Register application secrets with the logging redactor before composition and
keep telemetry best-effort so an exporter failure cannot change collection
correctness.

## Production gate and rollback

Before enabling paid traffic, prove local protocol behavior, durable
authorization, secret redaction, cleanup, and a bounded provider sandbox/pilot.
Snapshot the exact provider/profile/route/country/session/budget policy into the
job. Ordinary run parameters may disable or lower that policy; they must never
enable a provider or raise spend.

Canary one source with a small byte cap and a checked-in eligibility flag.
Record request and byte totals by route, rotations, failures, emitted entities,
and connector version. Roll back by disabling proxy routing or returning the
source to its previous pipeline. Revoke active leases only when interrupting
in-flight work is intended. Catalogue operators should also follow
[`ops-proxy-manager.md`](../../docs/ops-proxy-manager.md).
