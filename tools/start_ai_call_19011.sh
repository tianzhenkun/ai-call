#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
project_root="$(cd "$script_dir/.." && pwd -P)"
env_file="$project_root/env/.env.dev"
postgres_container="ai-call-ed81-owner-19011-postgres"
uvicorn="$project_root/.venv/bin/uvicorn"
mode="${1:-start}"

if (($# > 1)) || [[ "$mode" != "start" && "$mode" != "--check" ]]; then
    echo "Usage: $0 [--check]" >&2
    exit 2
fi

for command in docker lsof curl; do
    command -v "$command" >/dev/null || {
        echo "Missing required command: $command" >&2
        exit 1
    }
done

[[ -f "$env_file" ]] || {
    echo "Missing local environment file: $env_file" >&2
    exit 1
}
[[ -x "$uvicorn" ]] || {
    echo "Missing project runtime: $uvicorn" >&2
    exit 1
}
bash -n "$env_file"

container_status="$(docker inspect --format '{{.State.Status}}' "$postgres_container")"
container_health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$postgres_container")"
if [[ "$container_status" != "running" || "$container_health" == "unhealthy" ]]; then
    echo "PostgreSQL container is not ready: status=$container_status health=$container_health" >&2
    exit 1
fi

container_env="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$postgres_container")"
database_username="$(sed -n 's/^POSTGRES_USER=//p' <<<"$container_env")"
database_password="$(sed -n 's/^POSTGRES_PASSWORD=//p' <<<"$container_env")"
database_name="$(sed -n 's/^POSTGRES_DB=//p' <<<"$container_env")"
database_endpoint="$(docker port "$postgres_container" 5432/tcp | head -1)"
database_port="${database_endpoint##*:}"

if [[ -z "$database_username" || -z "$database_password" || -z "$database_name" ]]; then
    echo "PostgreSQL container is missing required local credentials" >&2
    exit 1
fi
if [[ ! "$database_port" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine PostgreSQL port from: $database_endpoint" >&2
    exit 1
fi

docker exec "$postgres_container" pg_isready \
    -U "$database_username" \
    -d "$database_name" >/dev/null

set -a
source "$env_file"
set +a

export ENVIRONMENT=dev
export SERVER_PORT=19011
export ROOT_PATH=
export DATABASE_TYPE=postgres
export DATABASE_HOST=127.0.0.1
export DATABASE_PORT="$database_port"
export DATABASE_NAME="$database_name"
export DATABASE_USER="$database_username"
export DATABASE_PASSWORD="$database_password"
export REDIS_ENABLE=false

listener_pid="$(lsof -nP -tiTCP:19011 -sTCP:LISTEN | head -1 || true)"
if [[ -n "$listener_pid" ]]; then
    listener_cwd="$(lsof -a -p "$listener_pid" -d cwd -Fn | sed -n 's/^n//p')"
    if [[ "$mode" != "--check" ]]; then
        echo "Port 19011 is already listening: pid=$listener_pid cwd=$listener_cwd" >&2
        exit 1
    fi
    [[ "$listener_cwd" == "$project_root" ]] || {
        echo "Port 19011 belongs to another checkout: pid=$listener_pid cwd=$listener_cwd" >&2
        exit 1
    }
    curl -fsS --max-time 5 http://127.0.0.1:19011/ai-call/health >/dev/null
fi

if [[ "$mode" == "--check" ]]; then
    echo "19011 preflight ok: project=$project_root database=$database_name port=$database_port listener=${listener_pid:-none}"
    exit 0
fi

cd "$project_root"
echo "Starting 19011: project=$project_root database=$database_name port=$database_port"
exec "$uvicorn" main:create_app \
    --factory \
    --host 0.0.0.0 \
    --port 19011 \
    --timeout-graceful-shutdown 10
