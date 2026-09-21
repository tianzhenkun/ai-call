#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  ''|--check) ;;
  *) echo '用法：render-configs.sh [--check]' >&2; exit 1 ;;
esac

deploy_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$deploy_dir"
test -f .env || { echo '.env is required'; exit 1; }

set -a
. ./.env
set +a

for name in LIVEKIT_API_KEY LIVEKIT_API_SECRET REDIS_PASSWORD LIVEKIT_TURN_DOMAIN; do
  value=${!name:-}
  case "$value" in ''|REPLACE_WITH_*) echo "$name is required"; exit 1 ;; esac
done

turn_error() { echo "TURN/TLS 校验失败：$1" >&2; exit 1; }
command -v openssl >/dev/null || turn_error '需要安装 OpenSSL'
[[ ${#LIVEKIT_TURN_DOMAIN} -le 253 && "$LIVEKIT_TURN_DOMAIN" =~ ^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,63}$ ]] \
  || turn_error 'LIVEKIT_TURN_DOMAIN 必须是域名，不能包含协议、端口或路径'
turn_cert=runtime/turn/fullchain.pem
turn_key=runtime/turn/privkey.pem
test -f "$turn_cert" && test -r "$turn_cert" || turn_error "缺少可读证书 $turn_cert"
test -f "$turn_key" && test -r "$turn_key" || turn_error "缺少可读私钥 $turn_key"
openssl x509 -in "$turn_cert" -checkend 604800 -noout >/dev/null \
  || turn_error '证书无效、已过期或将在 7 天内到期'
openssl verify -purpose sslserver -verify_hostname "$LIVEKIT_TURN_DOMAIN" \
  -untrusted "$turn_cert" "$turn_cert" >/dev/null \
  || turn_error '证书链不受信任、尚未生效或域名不匹配'
cert_public_key=$(openssl x509 -in "$turn_cert" -pubkey -noout \
  | openssl pkey -pubin -outform DER | openssl dgst -sha256) \
  || turn_error '无法读取证书公钥'
key_public_key=$(openssl pkey -in "$turn_key" -passin pass: -pubout -outform DER \
  | openssl dgst -sha256) || turn_error '私钥无效或需要交互式口令'
test "$cert_public_key" = "$key_public_key" || turn_error '证书与私钥不匹配'
if [[ "${1:-}" == --check ]]; then
  echo 'TURN/TLS 域名、证书链、有效期及私钥校验通过；未修改配置，未验证公网可达性。'
  exit 0
fi

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
    -e "s/__LIVEKIT_TURN_DOMAIN__/${LIVEKIT_TURN_DOMAIN}/g" \
    "$input" > "$output"
}

mkdir -p runtime
umask 077
render config/livekit.yaml.template runtime/livekit.yaml
render config/egress.yaml.template runtime/egress.yaml
render config/sip.yaml.template runtime/sip.yaml

# livekit-egress 使用 uid 1001、root 组，配置需要允许组读取。
chmod 640 runtime/egress.yaml
