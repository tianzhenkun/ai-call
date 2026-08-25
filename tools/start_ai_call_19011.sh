#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
project_root="$(cd "$script_dir/.." && pwd -P)"
env_file="${AI_CALL_RUNTIME_ENV_FILE:-$project_root/env/.env.dev}"
knowledge_env_file="${AI_CALL_KNOWLEDGE_ENV_FILE:-}"
postgres_container="ai-call-ed81-owner-19011-postgres"
redis_container="codex-ruoyi-redis-6379"
uvicorn="$project_root/.venv/bin/uvicorn"
mode="start"
profile="real"

for argument in "$@"; do
    case "$argument" in
        --check) mode="check" ;;
        --safe) profile="safe" ;;
        *)
            echo "Usage: $0 [--safe] [--check]" >&2
            exit 2
            ;;
    esac
done

for command in docker lsof curl ps; do
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

redis_status="$(docker inspect --format '{{.State.Status}}' "$redis_container")"
redis_command="$(docker inspect --format '{{range .Config.Cmd}}{{println .}}{{end}}' "$redis_container")"
redis_password="$(sed -n '/^--requirepass$/{n;p;q;}' <<<"$redis_command")"
redis_endpoint="$(docker port "$redis_container" 6379/tcp | head -1)"
redis_port="${redis_endpoint##*:}"
if [[ "$redis_status" != "running" || -z "$redis_password" || ! "$redis_port" =~ ^[0-9]+$ ]]; then
    echo "Redis container is not ready: status=$redis_status port=$redis_port" >&2
    exit 1
fi
docker exec -e REDISCLI_AUTH="$redis_password" "$redis_container" \
    redis-cli ping >/dev/null

set -a
source "$env_file"
set +a

read_env_value() {
    local key="$1" value
    value="$(sed -n "s/^${key}=//p" "$knowledge_env_file" | tail -n 1)"
    value="${value%$'\r'}"
    if [[ "$value" == \"*\" ]]; then
        value="${value#\"}"
        value="${value%\"}"
    elif [[ "$value" == \'*\' ]]; then
        value="${value#\'}"
        value="${value%\'}"
    fi
    printf '%s' "$value"
}

if [[ -n "$knowledge_env_file" ]]; then
    [[ -f "$knowledge_env_file" ]] || {
        echo "Missing knowledge environment file: $knowledge_env_file" >&2
        exit 1
    }
    export AI_CALL_KNOWLEDGE_COS_SECRET_ID="${AI_CALL_KNOWLEDGE_COS_SECRET_ID:-$(read_env_value COS_SECRET_ID)}"
    export AI_CALL_KNOWLEDGE_COS_SECRET_KEY="${AI_CALL_KNOWLEDGE_COS_SECRET_KEY:-$(read_env_value COS_SECRET_KEY)}"
    export AI_CALL_KNOWLEDGE_COS_BUCKET="${AI_CALL_KNOWLEDGE_COS_BUCKET:-$(read_env_value COS_BUCKET)}"
    export AI_CALL_KNOWLEDGE_COS_REGION="${AI_CALL_KNOWLEDGE_COS_REGION:-$(read_env_value COS_REGION)}"
    export AI_CALL_KNOWLEDGE_COS_PREFIX="${AI_CALL_KNOWLEDGE_COS_PREFIX:-$(read_env_value COS_PREFIX)}"
fi

export ENVIRONMENT=dev
export SERVER_PORT=19011
export ROOT_PATH=
export DATABASE_TYPE=postgres
export DATABASE_HOST=127.0.0.1
export DATABASE_PORT="$database_port"
export DATABASE_NAME="$database_name"
export DATABASE_USER="$database_username"
export DATABASE_PASSWORD="$database_password"
export REDIS_ENABLE=true
export REDIS_HOST=127.0.0.1
export REDIS_PORT="$redis_port"
export REDIS_USER=
export REDIS_PASSWORD="$redis_password"
if [[ "$profile" == "safe" ]]; then
    for variable in \
        AI_CALL_KNOWLEDGE_COS_SECRET_ID \
        AI_CALL_KNOWLEDGE_COS_SECRET_KEY \
        AI_CALL_KNOWLEDGE_COS_BUCKET \
        AI_CALL_KNOWLEDGE_COS_REGION \
        AI_CALL_KNOWLEDGE_COS_PREFIX; do
        [[ -n "${!variable:-}" ]] || {
            echo "Missing required knowledge setting: $variable" >&2
            exit 1
        }
    done
    export AI_CALL_PROCESS_ROLES=api
    export AI_CALL_OWNER_COMMAND_V1_ENTRIES=
    export AI_CALL_RUNTIME_INSTANCE_ID=knowledge-safe-19011
    export AI_CALL_RUNTIME_PROVIDER_MODE=stub
    export AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED=false
    export AI_CALL_OUTBOUND_EXECUTOR_ENABLED=false
    export AI_CALL_OUTBOUND_DIALER_MODE=mock
    export AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=false
    export AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE=
    export AI_CALL_SIP_OUTBOUND_ENABLED=false
else
    export AI_CALL_PROCESS_ROLES=api,runtime,dispatcher,jobs,outbound
    export AI_CALL_OWNER_COMMAND_V1_ENTRIES=outbound
    export AI_CALL_RUNTIME_INSTANCE_ID=ed81-owner-19011
    export AI_CALL_RUNTIME_PROVIDER_MODE=livekit
    export AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED=true
    export AI_CALL_OUTBOUND_EXECUTOR_ENABLED=true
    export AI_CALL_OUTBOUND_DIALER_MODE=sip
    export AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=true
    export AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE=19900001001
fi

listener_pid="$(lsof -nP -tiTCP:19011 -sTCP:LISTEN | head -1 || true)"
if [[ -n "$listener_pid" ]]; then
    listener_cwd="$(lsof -a -p "$listener_pid" -d cwd -Fn | sed -n 's/^n//p')"
    if [[ "$mode" != "check" ]]; then
        echo "Port 19011 is already listening: pid=$listener_pid cwd=$listener_cwd" >&2
        exit 1
    fi
    [[ "$listener_cwd" == "$project_root" ]] || {
        echo "Port 19011 belongs to another checkout: pid=$listener_pid cwd=$listener_cwd" >&2
        exit 1
    }
    curl -fsS --max-time 5 http://127.0.0.1:19011/ai-call/health >/dev/null
    if [[ "$profile" == "safe" ]]; then
        process_command="$(ps eww -p "$listener_pid" -o command=)"
        for expected in \
            AI_CALL_PROCESS_ROLES=api \
            AI_CALL_RUNTIME_PROVIDER_MODE=stub \
            AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED=false \
            AI_CALL_OUTBOUND_EXECUTOR_ENABLED=false \
            AI_CALL_OUTBOUND_DIALER_MODE=mock \
            AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=false \
            AI_CALL_SIP_OUTBOUND_ENABLED=false; do
            [[ " $process_command " == *" $expected "* ]] || {
                echo "19011 is not running in safe mode: expected $expected" >&2
                exit 1
            }
        done
    fi
fi

if [[ "$mode" == "check" ]]; then
    echo "19011 preflight ok: profile=$profile project=$project_root database=$database_name port=$database_port redis_port=$redis_port listener=${listener_pid:-none}"
    exit 0
fi

cd "$project_root"
echo "Starting 19011: profile=$profile project=$project_root database=$database_name port=$database_port"
exec "$uvicorn" main:create_app \
    --factory \
    --host 0.0.0.0 \
    --port 19011 \
    --timeout-graceful-shutdown 10
