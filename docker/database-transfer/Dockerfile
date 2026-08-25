FROM postgres:17@sha256:e38411452a464af89e5adadb8d223bf53b898d47d6ef918b2d58c08707350449

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv ca-certificates \
 && useradd --uid 1000 --create-home transfer \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY commerce-scraper/pyproject.toml commerce-scraper/README.md ./commerce-scraper/
COPY commerce-scraper/src ./commerce-scraper/src
COPY catalogue-dump/pyproject.toml catalogue-dump/sources.json ./catalogue-dump/
COPY catalogue-dump/src ./catalogue-dump/src
COPY docker/verify-python-distributions.py /usr/local/bin/verify-python-distributions.py
RUN python3 -m venv /opt/catalogue \
 && /opt/catalogue/bin/pip install --no-cache-dir ./commerce-scraper "./catalogue-dump[postgres]" \
 && /opt/catalogue/bin/python -I /usr/local/bin/verify-python-distributions.py

ENV PATH=/opt/catalogue/bin:/usr/lib/postgresql/17/bin:/usr/local/bin:/usr/bin:/bin
ENTRYPOINT ["catalogue-db-transfer"]
