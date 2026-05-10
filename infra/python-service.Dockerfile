FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml /app/
COPY market_platform /app/market_platform
COPY apps/trader-dashboard/static /app/apps/trader-dashboard/static
COPY contracts /app/contracts
COPY docs /app/docs
COPY lakehouse/contracts /app/lakehouse/contracts
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
