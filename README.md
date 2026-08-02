# LoadForge

LoadForge is a load-testing stack with an API server, orchestrator, dynamically provisioned workers, an aggregator, and downloadable reports.

## Local Docker Quickstart

The Docker Compose profile is the easiest way to run the full API -> Orchestrator -> Worker -> Aggregator -> Report loop locally.

```sh
docker compose -f deployments/docker-compose.yaml build worker
docker compose -f deployments/docker-compose.yaml up -d --build
```

Create a local API key. The Compose file uses `API_KEY_PEPPER=change-me-before-production`, so this stores the token `00000000-0000-4000-8000-000000000001` for local development:

```sh
printf '%s\0%s' 'change-me-before-production' '00000000-0000-4000-8000-000000000001' | sha256sum
docker compose -f deployments/docker-compose.yaml exec -T postgres psql -U loadforge -d loadforge -c "INSERT INTO api_keys(id, name, token_hash) VALUES ('00000000-0000-4000-8000-000000000001', 'local-dev', decode('fc385368d79323d068c1e2488628ed7098041b29260da076ab0af59be28ef389', 'hex')) ON CONFLICT (id) DO UPDATE SET token_hash = EXCLUDED.token_hash, revoked_at = NULL"
```

Run the smoke plan and download the report:

```sh
go run ./cmd/loadforge --token 00000000-0000-4000-8000-000000000001 run --ci examples/httpbin-smoke.yaml
go run ./cmd/loadforge --token 00000000-0000-4000-8000-000000000001 report <run-id>
```

During the run, Docker-provisioned worker containers are labeled with `loadforge.io/run-id=<run-id>`:

```sh
docker ps --filter label=loadforge.io/run-id=<run-id>
```
