#!/bin/sh
set -eu

: "${FS_DEFAULT_PASSWORD:?FS_DEFAULT_PASSWORD is required}"
: "${FS_EXTERNAL_IP:?FS_EXTERNAL_IP is required}"

if [ ! -f /etc/freeswitch/freeswitch.xml ]; then
  mkdir -p /etc/freeswitch
  cp -a /usr/share/freeswitch/conf/vanilla/. /etc/freeswitch/
  case "$FS_DEFAULT_PASSWORD" in *[!A-Za-z0-9_-]*) echo 'FS_DEFAULT_PASSWORD must be alphanumeric, _ or -'; exit 1 ;; esac
  sed -i 's/default_password=[^"]*/default_password='"$FS_DEFAULT_PASSWORD"'/' /etc/freeswitch/vars.xml
  sed -i "s#external_rtp_ip=stun:stun.freeswitch.org#external_rtp_ip=${FS_EXTERNAL_IP}#" /etc/freeswitch/vars.xml
  sed -i "s#external_sip_ip=stun:stun.freeswitch.org#external_sip_ip=${FS_EXTERNAL_IP}#" /etc/freeswitch/vars.xml
  sed -i 's#<param name="listen-ip" value="::"/>#<param name="listen-ip" value="0.0.0.0"/>#' /etc/freeswitch/autoload_configs/event_socket.conf.xml
  sed -i 's#<!-- <param name="rtp-start-port" value="16384"/> -->#<param name="rtp-start-port" value="16384"/>#' /etc/freeswitch/autoload_configs/switch.conf.xml
  sed -i 's#<!-- <param name="rtp-end-port" value="32768"/> -->#<param name="rtp-end-port" value="16484"/>#' /etc/freeswitch/autoload_configs/switch.conf.xml
fi

trap '/usr/bin/freeswitch -stop' TERM
/usr/bin/freeswitch -nc -nf -nonat &
pid="$!"
wait "$pid"
