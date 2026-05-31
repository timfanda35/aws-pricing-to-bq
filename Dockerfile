## Builder stage
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY requirements.txt ./
RUN pip install -r requirements.txt

# Fail the build if ijson didn't land its C backend. The manylinux wheel bundles
# its own libyajl, so on python:3.12-slim (Debian glibc) this should Just Work —
# but if a future base-image change loses wheel compatibility, ijson silently
# falls back to a pure-Python parser that's 3-10x slower on EC2-class offers.
# Better a loud build failure than a slow Cloud Run Job.
RUN python -c "import ijson, sys; \
  backend = ijson.backend; \
  print(f'ijson backend at build time: {backend}'); \
  sys.exit(0 if backend == 'yajl2_c' else \
    f'ijson backend is {backend!r}, expected yajl2_c. Install libyajl-dev or pin a wheel-compatible ijson version.')"

## Runtime stage
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH="/app" \
    LOG_LEVEL=INFO

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY app/ ./app/
COPY aws_pricing_to_bq/ ./aws_pricing_to_bq/
COPY run_job.py docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

USER appuser

ENTRYPOINT ["/app/docker-entrypoint.sh"]
# Default is the Cloud Run Job entry point. Override CMD to inspect runs etc.
CMD ["python", "run_job.py"]
