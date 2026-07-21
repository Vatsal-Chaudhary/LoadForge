.PHONY: build proto test docker-build helm-install clean dev

REGISTRY    := ghcr.io/vatsalchaudhary
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GOFLAGS     := -ldflags="-X main.Version=$(VERSION)"

# ── Proto generation ────────────────────────────────────────────────────────
proto:
	protoc \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  proto/worker.proto

# ── Build all binaries ──────────────────────────────────────────────────────
build:
	go build $(GOFLAGS) -o bin/orchestrator ./cmd/orchestrator
	go build $(GOFLAGS) -o bin/worker        ./cmd/worker
	go build $(GOFLAGS) -o bin/aggregator    ./cmd/aggregator
	go build $(GOFLAGS) -o bin/apiserver     ./cmd/apiserver
	go build $(GOFLAGS) -o bin/loadforge     ./cmd/loadforge

# ── Test ────────────────────────────────────────────────────────────────────
test:
	go test ./... -race -cover -count=1

test-integration:
	go test ./... -tags=integration -race -count=1

# ── Docker ──────────────────────────────────────────────────────────────────
docker-build:
	docker build -f Dockerfile.orchestrator -t $(REGISTRY)/loadforge-orchestrator:$(VERSION) .
	docker build -f Dockerfile.worker       -t $(REGISTRY)/loadforge-worker:$(VERSION) .
	docker build -f Dockerfile.aggregator   -t $(REGISTRY)/loadforge-aggregator:$(VERSION) .
	docker build -f Dockerfile.apiserver    -t $(REGISTRY)/loadforge-apiserver:$(VERSION) .

docker-push:
	docker push $(REGISTRY)/loadforge-orchestrator:$(VERSION)
	docker push $(REGISTRY)/loadforge-worker:$(VERSION)
	docker push $(REGISTRY)/loadforge-aggregator:$(VERSION)
	docker push $(REGISTRY)/loadforge-apiserver:$(VERSION)

# ── Local dev with docker-compose ───────────────────────────────────────────
dev:
	docker compose up --build

dev-down:
	docker compose down -v

# ── Helm ────────────────────────────────────────────────────────────────────
helm-lint:
	helm lint deployments/helm/loadforge

helm-install:
	helm upgrade --install loadforge deployments/helm/loadforge \
	  --namespace loadforge --create-namespace \
	  --set image.tag=$(VERSION) \
	  -f deployments/helm/loadforge/values.yaml

helm-uninstall:
	helm uninstall loadforge --namespace loadforge

# ── DB migrations ───────────────────────────────────────────────────────────
migrate-up:
	migrate -path internal/store/postgres/migrations \
	        -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path internal/store/postgres/migrations \
	        -database "$(DATABASE_URL)" down 1

# ── Clean ───────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/
