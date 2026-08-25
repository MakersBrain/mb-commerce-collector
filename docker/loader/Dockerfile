# The importer, as an installed package.
#
# This was `FROM postgres:17-alpine` plus a Python interpreter, because the load
# drove `COPY` through `psql` and needed the client binary. Phase 3 moved it to
# psycopg's `cursor.copy()`, so the base is now an ordinary Python image and the
# PostgreSQL client is gone — which is also what lets the same code run inside
# the worker instead of only in a container built around `psql`.
FROM python:3.12-slim

WORKDIR /src
COPY commerce-scraper/pyproject.toml commerce-scraper/README.md ./commerce-scraper/
COPY commerce-scraper/src ./commerce-scraper/src
COPY catalogue-dump/pyproject.toml catalogue-dump/sources.json ./catalogue-dump/
COPY catalogue-dump/src ./catalogue-dump/src
COPY docker/verify-python-distributions.py /usr/local/bin/verify-python-distributions.py

RUN pip install --no-cache-dir ./commerce-scraper "./catalogue-dump[postgres]" \
 && python -I /usr/local/bin/verify-python-distributions.py \
 && rm -rf /root/.cache

# Not root: it reads NDJSON and writes to one database, and needs nothing else.
RUN useradd --system --uid 10001 catalogue
USER catalogue

WORKDIR /work
ENTRYPOINT ["catalogue-load"]
