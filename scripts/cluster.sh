#!/usr/bin/env bash
# Run a local 3-broker StreamHub cluster as separate processes.
#
#   scripts/cluster.sh start     build + start brokers 1..3
#   scripts/cluster.sh stop      stop all brokers
#   scripts/cluster.sh status    show processes and /healthz
#   scripts/cluster.sh kill N    SIGKILL broker N (failure testing)
#   scripts/cluster.sh restart N start broker N again
#   scripts/cluster.sh clean     stop and delete all data
#
# Brokers listen on 127.0.0.1:9092-9094, HTTP on 8081-8083.
# Data: ./cluster-data/broker-N, logs: ./cluster-data/broker-N.log
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/streamhub"
DATA="${STREAMHUB_DATA:-$ROOT/cluster-data}"
N=3
PEERS="1=127.0.0.1:9092,2=127.0.0.1:9093,3=127.0.0.1:9094"

build() {
  mkdir -p "$ROOT/bin"
  (cd "$ROOT" && go build -o "$BIN" ./cmd/streamhub)
}

pidfile() { echo "$DATA/broker-$1.pid"; }

start_one() {
  local id=$1
  local port=$((9091 + id)) http=$((8080 + id))
  mkdir -p "$DATA/broker-$id"
  if [[ -f $(pidfile "$id") ]] && kill -0 "$(cat "$(pidfile "$id")")" 2>/dev/null; then
    echo "broker $id already running (pid $(cat "$(pidfile "$id")"))"
    return
  fi
  nohup "$BIN" broker \
    --id "$id" \
    --listen "127.0.0.1:$port" \
    --http "127.0.0.1:$http" \
    --data-dir "$DATA/broker-$id" \
    --peers "$PEERS" \
    --default-replication-factor 3 \
    --min-insync-replicas 2 \
    >"$DATA/broker-$id.log" 2>&1 &
  echo $! >"$(pidfile "$id")"
  echo "broker $id started: 127.0.0.1:$port (http :$http) pid $!"
}

wait_ready() {
  for _ in $(seq 1 60); do
    local ready=0
    for id in $(seq 1 $N); do
      if curl -fs "http://127.0.0.1:$((8080 + id))/readyz" >/dev/null 2>&1; then
        ready=$((ready + 1))
      fi
    done
    if [[ $ready -eq $N ]]; then
      echo "cluster ready ($N/$N brokers)"
      return 0
    fi
    sleep 0.5
  done
  echo "cluster not ready after 30s; see $DATA/*.log" >&2
  return 1
}

stop_one() {
  local f
  f=$(pidfile "$1")
  if [[ -f $f ]]; then
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
    echo "broker $1 stopped"
  fi
}

case "${1:-}" in
  start)
    build
    mkdir -p "$DATA"
    for id in $(seq 1 $N); do start_one "$id"; done
    wait_ready
    echo "export STREAMHUB_BOOTSTRAP=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
    ;;
  stop)
    for id in $(seq 1 $N); do stop_one "$id"; done
    ;;
  kill)
    f=$(pidfile "${2:?broker id}")
    kill -9 "$(cat "$f")" && rm -f "$f" && echo "broker $2 killed (SIGKILL)"
    ;;
  restart)
    start_one "${2:?broker id}"
    ;;
  status)
    for id in $(seq 1 $N); do
      printf "broker %s: " "$id"
      curl -s "http://127.0.0.1:$((8080 + id))/healthz" || printf "down"
      echo
    done
    ;;
  clean)
    for id in $(seq 1 $N); do stop_one "$id"; done
    rm -rf "$DATA"
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac
