#!/usr/bin/env bash
set -euo pipefail

deploy_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$deploy_dir"
test -f .env || { echo '.env is required'; exit 1; }

set -a
. ./.env
set +a

for name in LIVEKIT_API_KEY LIVEKIT_API_SECRET REDIS_PASSWORD; do
  value=${!name:-}
  case "$value" in ''|REPLACE_WITH_*) echo "$name is required"; exit 1 ;; esac
done

escape_sed() { printf '%s' "$1" | sed 's/[\\/&]/\\&/g'; }
render() {
  input=$1
  output=$2
  livekit_api_key=$(escape_sed "$LIVEKIT_API_KEY")
  livekit_api_secret=$(escape_sed "$LIVEKIT_API_SECRET")
  redis_password=$(escape_sed "$REDIS_PASSWORD")
  sed \
    -e "s/__LIVEKIT_API_KEY__/${livekit_api_key}/g" \
    -e "s/__LIVEKIT_API_SECRET__/${livekit_api_secret}/g" \
    -e "s/__REDIS_PASSWORD__/${redis_password}/g" \
    "$input" > "$output"
}

mkdir -p runtime
umask 077
render config/livekit.yaml.template runtime/livekit.yaml
render config/egress.yaml.template runtime/egress.yaml
render config/sip.yaml.template runtime/sip.yaml
