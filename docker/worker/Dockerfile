# The collection worker.
#
# Two images are built from this one file, and the difference is the whole
# reason capabilities exist (§5.5): camoufox is most of the image and most of
# the memory, and only two of the eighty sources need it.
#
#   docker build --target worker          -> the plain worker
#   docker build --target worker-browser  -> `--capabilities browser`
#
# The plain worker must not inherit the browser's dependency set, which is why
# the extras are declared separately in pyproject.toml rather than as one
# dependency list.

FROM python:3.12-slim AS base

# libpq is not needed — psycopg[binary] carries its own — but ca-certificates
# is, or every https fetch fails with a verification error that looks like the
# shop's fault.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY commerce-scraper/pyproject.toml commerce-scraper/README.md ./commerce-scraper/
COPY commerce-scraper/src ./commerce-scraper/src
COPY catalogue-dump/pyproject.toml catalogue-dump/sources.json ./catalogue-dump/
COPY catalogue-dump/src ./catalogue-dump/src
COPY docker/verify-python-distributions.py /usr/local/bin/verify-python-distributions.py

RUN useradd --system --uid 10001 --create-home catalogue

# Shared volumes. The cache is per-host sharded so two workers never write one
# entry; the dumps are namespaced <run-id>/<job-id>/ so two runs of one source
# never write one path.
ENV CATALOGUE_CACHE_DIR=/var/lib/catalogue/cache \
    CATALOGUE_DUMPS_DIR=/var/lib/catalogue/dumps \
    CATALOGUE_LOG_JSON=true
RUN mkdir -p /var/lib/catalogue/cache /var/lib/catalogue/dumps \
 && chown -R catalogue /var/lib/catalogue


FROM base AS worker

# curl_cffi is a few megabytes and no browser, so the plain worker carries it:
# without it every source behind a TLS-fingerprinting WAF fails on this worker
# and only succeeds on the browser one, which is the expensive way to fetch a
# page that a handshake alone would have opened.
RUN pip install --no-cache-dir ./commerce-scraper "./catalogue-dump[postgres,otel,impersonate]" \
 && python -I /usr/local/bin/verify-python-distributions.py \
 && rm -rf /root/.cache
COPY docker/worker/entrypoint.py /usr/local/bin/catalogue-worker-entrypoint.py
WORKDIR /var/lib/catalogue
# SIGTERM is handled as a drain: it finishes the current source, requeues
# anything unfinished and marks the worker stopped. Give it room to do that
# rather than the default ten seconds.
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "/usr/local/bin/catalogue-worker-entrypoint.py"]


FROM base AS worker-browser

# camoufox pulls a full browser. Kept in its own stage so the plain worker's
# image does not carry it.
# Camoufox ships a Firefox binary and none of the system libraries it links
# against, and python:3.12-slim carries none of them either. Without these the
# package imports, `camoufox fetch` reports a version, and the browser dies at
# launch with "libgtk-3.so.0: cannot open shared object file" — which reads as a
# per-page error and is really this image being unable to render anything at
# all. The set is the one catalogue-dump/flake.nix already lists for the
# development shell, in Debian package names.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libgtk-3-0 libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 \
      libxfixes3 libxrandr2 libxt6 libnss3 libnspr4 libpango-1.0-0 \
      libcairo2 libasound2t64 libdbus-glib-1-2 libfontconfig1 libfreetype6 \
      libglib2.0-0 libxkbcommon0 libdrm2 libgbm1 fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir ./commerce-scraper "./catalogue-dump[postgres,otel,browser,impersonate]" \
 && python -I /usr/local/bin/verify-python-distributions.py \
 && python -c "import camoufox" \
 && rm -rf /root/.cache
USER catalogue
WORKDIR /var/lib/catalogue

# pip brings the launcher, not the browser: `camoufox fetch` downloads that
# separately. Without it `import camoufox` still succeeds — which is why the
# check above was not enough — and every render fails at runtime with
# "official/stable is not installed", on an image whose entrypoint advertises
# `--capabilities browser`.
#
# It runs after `USER catalogue` on purpose: the download lands in
# $HOME/.cache/camoufox, so fetching as root would put it in a directory this
# process cannot read and leave the image just as broken.
RUN python -m camoufox fetch \
 && python -c "from camoufox.pkgman import installed_verstr; print(installed_verstr())"

# The two checks above both passed on an image whose browser could not start:
# the package imported, `fetch` reported 152.0.4-beta.28, and every launch died
# on a missing libgtk. A version string is not a working browser, so the build
# now starts one and loads a page. It is the only check here that would have
# caught it, and it fails the build rather than the 03:00 run.
RUN python - <<'SMOKE'
import asyncio
from camoufox.async_api import AsyncCamoufox


async def main() -> None:
    async with AsyncCamoufox(headless=True, block_images=False, humanize=False) as browser:
        page = await browser.new_page()
        await page.goto("data:text/html,<title>ok</title>", wait_until="domcontentloaded")
        assert await page.title() == "ok"
        await page.close()
    print("camoufox launches and renders")


asyncio.run(main())
SMOKE

COPY docker/worker/entrypoint.py /usr/local/bin/catalogue-worker-entrypoint.py
USER root
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "/usr/local/bin/catalogue-worker-entrypoint.py", "--capabilities", "browser"]
