# Clean external consumer

This application uses only installed public APIs. The repository verifier copies
it outside the source tree, installs the built library wheel and packaged example
connector, then runs both the built-in Shopify connector and the plugin through
an application-defined proxy transport factory.

The focused smoke flow also covers one middleware retry and proxy rotation,
lease/transport cleanup, explicit entry-point discovery, and credential
containment. It uses deterministic in-process responses and performs no network
I/O.
