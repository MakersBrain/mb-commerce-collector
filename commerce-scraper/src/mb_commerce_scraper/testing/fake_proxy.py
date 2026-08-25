from pydantic import SecretStr

from mb_commerce_scraper.proxy import ProxyCredentials, ProxyEndpoint, ProxyKind, StaticProxyPool, StaticRoute


def fake_proxy_pool(*providers: str) -> StaticProxyPool:
    routes = tuple(
        StaticRoute(
            endpoint=ProxyEndpoint(provider=provider, endpoint_id=f"{provider}-1", protocol="http", host=f"{provider}.proxy.test", port=8080, kind=ProxyKind.RESIDENTIAL, countries=frozenset({"FR", "US"})),
            credentials=ProxyCredentials(username=SecretStr(f"{provider}-user"), password=SecretStr(f"{provider}-password")),
        )
        for provider in providers
    )
    return StaticProxyPool(routes)

