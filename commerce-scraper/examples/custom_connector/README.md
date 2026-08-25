# Example external connector

This directory is a separate Python distribution. It imports only public
`mb-commerce-scraper` contracts and publishes `example-feed` through the
`mb_commerce_scraper.connectors` entry-point group. Installing it does not
modify the library and importing it performs no discovery or I/O.

Install both distributions, then load plugins explicitly:

```console
uv add mb-commerce-scraper ./examples/custom_connector
```

```python
registry = ConnectorRegistry.with_builtins()
registry.load_entry_points(strict=True)
assert "example-feed" in registry.names()
```

The implementation validates its own options, uses the injected transport and
clock, checks cancellation before I/O, declares only the fields it supports,
and emits neutral product models. See `../../docs/custom-shops.md` for the
authoring contract and a complete runtime example.
