FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml /app/
COPY market_platform /app/market_platform
COPY apps/trader-dashboard/static /app/apps/trader-dashboard/static
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
