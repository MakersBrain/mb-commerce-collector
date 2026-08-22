# Backup and restore for the catalogue: restic, pg_dump/pg_restore, and the
# tool that knows the two have to be consistent with each other.
#
# Separate from the control image on purpose. This one is scheduled rather than
# served, it needs the PostgreSQL client tools and the restic binary that the
# control image has no business carrying, and it reads the artifact volume where
# control mounts it read-only. Keeping them apart also means a backup run cannot
# be reached from the control API's network surface.
FROM python:3.12-slim

# restic from Debian rather than a GitHub release: it is the version the distro
# security team tracks, and this container's whole job is to be boring.
RUN apt-get update -qq -o Acquire::Languages=none \
 && apt-get install -y -qq --no-install-recommends \
      restic postgresql-client ca-certificates \
 && rm -rf /var/lib/apt/lists/*

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

RUN useradd --system --uid 10002 catalogue-backup
USER catalogue-backup

ENV CATALOGUE_LOG_JSON=true \
    CATALOGUE_ARTIFACTS_DIR=/var/lib/catalogue/dumps

ENTRYPOINT ["catalogue-backup"]
CMD ["backup"]
