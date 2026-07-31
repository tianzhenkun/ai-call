#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
project_root="$(cd "$script_dir/.." && pwd -P)"
compose_file="$project_root/deploy/ai-call-runtime-postgres-test.compose.yml"
compose_project="${AI_CALL_TEST_COMPOSE_PROJECT:-ai-call-runtime-test-$$}"

cleanup() {
    docker compose -p "$compose_project" -f "$compose_file" down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose -p "$compose_project" -f "$compose_file" up --detach --wait --wait-timeout 60

published_endpoint="$(docker compose -p "$compose_project" -f "$compose_file" port postgres 5432)"
test_port="${published_endpoint##*:}"
if [[ ! "$test_port" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine the PostgreSQL test port from: $published_endpoint" >&2
    exit 1
fi

export AI_CALL_TEST_POSTGRES_DSN="postgresql+asyncpg://ai_call_runtime_test:ai_call_runtime_test@127.0.0.1:${test_port}/ai_call_runtime_test"

cd "$project_root"
if (($# > 0)); then
    uv run pytest "$@"
else
    uv run pytest tests/postgres/test_ai_call_runtime_control_postgres.py
fi
