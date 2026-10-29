# Integration services

The integration compose project uses local-only host ports:

| Service | Host port | Test account |
| --- | ---: | --- |
| Stalwart JMAP / DAV | `18080` | `alice@example.org` / `Vx7-quartz-Falcon-river` |
| Stalwart IMAPS | `10993` | `alice@example.org` / `Vx7-quartz-Falcon-river` |
| Stalwart HTTPS | `18443` | admin recovery account `admin` / `adminpass` |
| GreenMail IMAP | `3143` | `alice` / `sourcepass` |
| Radicale CardDAV / CalDAV | `15232` | `alice` / `radicalepass` |

These are throwaway credentials for the isolated integration stack only. The stack binds its
ports to loopback. `setup.sh` starts the compose services, waits for their ports, and provisions
the Stalwart domain and account using the Stalwart CLI image.

```sh
docker compose -f tests/integration/docker-compose.yml up -d
tests/integration/setup.sh
.venv/bin/pytest -q -m integration
docker compose -f tests/integration/docker-compose.yml down
```
