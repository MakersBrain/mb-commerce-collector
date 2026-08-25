# The catalogue read API, under its generated contract.
#
# This was `pip install psycopg` plus one `app.py`, because the service was a
# hand-rolled `do_GET` over one view. It is now a Starlette application (§10.2):
# a spec generated from a BaseHTTPRequestHandler is a spec written by hand and
# hoped to be true, and a published contract is worth the dependency.
FROM python:3.12-slim

WORKDIR /src
COPY commerce-scraper/pyproject.toml commerce-scraper/README.md ./commerce-scraper/
COPY commerce-scraper/src ./commerce-scraper/src
# The shared OpenAPI registry lives in the collection package, so it is copied
# in as a path dependency. Nothing else of it is imported.
COPY catalogue-dump/pyproject.toml catalogue-dump/sources.json ./catalogue-dump/
COPY catalogue-dump/src ./catalogue-dump/src
COPY catalogue-service/pyproject.toml catalogue-service/catalogue.openapi.json ./catalogue-service/
COPY catalogue-service/src ./catalogue-service/src
COPY docker/verify-python-distributions.py /usr/local/bin/verify-python-distributions.py

RUN pip install --no-cache-dir ./commerce-scraper ./catalogue-dump ./catalogue-service \
 && python -I /usr/local/bin/verify-python-distributions.py \
 && rm -rf /root/.cache /src

# Not root: it reads one database and serves JSON, and needs nothing else.
RUN useradd --system --uid 10001 catalogue
USER catalogue

EXPOSE 8686
CMD ["catalogue-service"]
