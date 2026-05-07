#!/usr/bin/env bash
set -euo pipefail

docker compose -f infra/docker-compose.yml --profile flink up --build --scale stream-processor=0
