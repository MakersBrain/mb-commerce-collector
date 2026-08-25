# The operator control plane.
#
# Not published on the host by the compose file: the only things that need it
# are the workers' database (which it does not go through) and the explorer's
# server-side proxy. That is defence in depth — the bearer token is the
# authentication boundary, and the service refuses to start without one.
FROM python:3.12-slim

WORKDIR /src
COPY commerce-scraper/pyproject.toml commerce-scraper/README.md ./commerce-scraper/
COPY commerce-scraper/src ./commerce-scraper/src
COPY catalogue-dump/pyproject.toml catalogue-dump/sources.json ./catalogue-dump/
COPY catalogue-dump/src ./catalogue-dump/src
COPY catalogue-control/pyproject.toml catalogue-control/catalogue-ops.openapi.json ./catalogue-control/
COPY catalogue-control/src ./catalogue-control/src
COPY docker/verify-python-distributions.py /usr/local/bin/verify-python-distributions.py

RUN pip install --no-cache-dir ./commerce-scraper ./catalogue-dump ./catalogue-control \
 && python -I /usr/local/bin/verify-python-distributions.py \
 && rm -rf /root/.cache /src

RUN useradd --system --uid 10001 catalogue
COPY docker/control/entrypoint.py /usr/local/bin/catalogue-control-entrypoint.py

ENV CATALOGUE_LOG_JSON=true
EXPOSE 8687
ENTRYPOINT ["python3", "/usr/local/bin/catalogue-control-entrypoint.py"]
