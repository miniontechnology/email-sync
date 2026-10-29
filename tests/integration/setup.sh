#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMPOSE="$ROOT/docker-compose.yml"
docker compose -f "$COMPOSE" up -d

python3 - <<'PY'
import socket
import sys
import time

ports = (18080, 3143, 15232)
deadline = time.monotonic() + 120
pending = set(ports)
while pending and time.monotonic() < deadline:
    for port in tuple(pending):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pending.remove(port)
        except OSError:
            pass
    time.sleep(1)
if pending:
    sys.exit(f"Integration services did not start on ports: {sorted(pending)}")
PY

docker run --rm --network host \
  --volume "$ROOT:/work:ro" --workdir /work \
  ghcr.io/stalwartlabs/cli \
  --url http://127.0.0.1:18080 --user admin --password adminpass \
  apply --file /work/stalwart-plan.ndjson
