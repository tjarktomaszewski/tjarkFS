run:
	@echo Run tjarkfs
	@go run ./cmd/tjarkfs $(ARGS)

build:
	@echo Build tjarkfs
	@mkdir -p bin
	@go build -o bin/tjarkfs ./cmd/tjarkfs

test:
	@echo Run tests
	@go test ./... -cover

test-race:
	@echo Run tests
	@go test ./... -cover -race

.PHONY: run build test test-race