#!/usr/bin/env bash
# Starts the replica sets the tests need: rs_1 on 27017-27019 (tagged members, test commands on)
# and stepdown on 27217-27219 for the step-down test. Usage: mongodb-test-env.sh up|down
set -euo pipefail

IMAGE="mongo:${MONGO_VERSION:-7.0.15}"
NAMES=(drv-rs1-0 drv-rs1-1 drv-rs1-2 drv-stepdown-0 drv-stepdown-1 drv-stepdown-2)

mongo_shell() {
  # mongo:5.0 still ships the legacy shell next to mongosh.
  docker exec "$1" sh -c "\$(command -v mongosh || command -v mongo) --quiet --port $2 --eval \"\$0\"" "$3"
}

wait_for() {
  for _ in $(seq 1 120); do
    if [ "$(mongo_shell "$1" "$2" "$3" 2>/dev/null | tail -1)" = "true" ]; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $1: $3" >&2
  docker logs --tail 50 "$1" >&2
  return 1
}

start_set() {
  local set=$1 prefix=$2 port=$3 members=$4 ready=$5
  for i in 0 1 2; do
    docker run -d --name "$prefix-$i" --network host "$IMAGE" --replSet "$set" --bind_ip 127.0.0.1 \
      --port $((port + i)) --wiredTigerCacheSizeGB 0.25 --setParameter enableTestCommands=1 >/dev/null
  done
  wait_for "$prefix-0" "$port" "db.adminCommand({ping: 1}).ok == 1"
  mongo_shell "$prefix-0" "$port" "rs.initiate({_id: '$set', members: [$members]})" >/dev/null
  wait_for "$prefix-0" "$port" "$ready"
}

up() {
  # Tags as test/mongo/read_preferences_test.exs expects; member 0 stays primary.
  start_set rs_1 drv-rs1 27017 "
    {_id: 0, host: '127.0.0.1:27017', priority: 2, tags: {dc: 'east', usage: 'production'}},
    {_id: 1, host: '127.0.0.1:27018', tags: {dc: 'east', usage: 'reporting'}},
    {_id: 2, host: '127.0.0.1:27019', tags: {dc: 'west', usage: 'production'}}" \
    "db.adminCommand({hello: 1}).isWritablePrimary"
  start_set stepdown drv-stepdown 27217 "
    {_id: 0, host: '127.0.0.1:27217'}, {_id: 1, host: '127.0.0.1:27218'}, {_id: 2, host: '127.0.0.1:27219'}" \
    "db.adminCommand({hello: 1}).primary !== undefined"
  echo "MONGO_STEP_DOWN_SEEDS=127.0.0.1:27217,127.0.0.1:27218,127.0.0.1:27219"
  echo "MONGO_STEP_DOWN_SET=stepdown"
}

down() {
  docker rm -f "${NAMES[@]}" >/dev/null 2>&1 || true
}

case "${1:-}" in
  up) down && up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2 && exit 1 ;;
esac
