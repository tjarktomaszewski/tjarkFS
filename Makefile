run:
	@go run ./cmd/client $(ARGS)

build:
	@mkdir -p bin
	@go build -o bin/tjarkfs ./cmd/client

test:
	@go test ./... -cover

test-race:
	@go test ./... -cover -race

# proto/sqlc/up/chaos/bench laut docs/STRUCTURE.md, sobald die jeweiligen
# Komponenten (buf, sqlc, docker-compose, test/chaos, bench/) existieren.

.PHONY: run build test test-race