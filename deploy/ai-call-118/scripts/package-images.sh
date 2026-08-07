#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
version=${1:-20260807}
bundle_dir=${2:-"$repo_root/image-bundles"}

mkdir -p "$bundle_dir"
docker build --tag "ai-call-transfer/api:${version}" \
  --file "$repo_root/deploy/ai-call-118/Dockerfile.api" "$repo_root"
docker build --tag "ai-call-transfer/freeswitch:${version}" \
  --file "$repo_root/deploy/ai-call-118/Dockerfile.freeswitch" "$repo_root"
for image in "ai-call-transfer/api:${version}" "ai-call-transfer/freeswitch:${version}"; do
  test "$(docker image inspect "$image" --format '{{.Architecture}}')" = amd64
done
docker save --output "$bundle_dir/ai-call-app-amd64-${version}.tar" \
  "ai-call-transfer/api:${version}" \
  "ai-call-transfer/freeswitch:${version}"
sha256sum "$bundle_dir/ai-call-app-amd64-${version}.tar"
