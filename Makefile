run:
	@go run ./cmd/client $(ARGS)

run-node:
	@go run ./cmd/storagenode $(ARGS)

build:
	@mkdir -p bin
	@go build -o bin/tjarkfs ./cmd/client
	@go build -o bin/tjarkfs-storagenode ./cmd/storagenode

test:
	@go test ./... -cover

test-race:
	@go test ./... -cover -race


# macOS/Homebrew vs. Linux: well-known-types-Include different path
PROTOC_INCLUDE ?= $(shell brew --prefix protobuf 2>/dev/null || echo /usr/include)/include

# protoc-gen-go / protoc-gen-go-grpc live in $(go env GOPATH)/bin, which is not
# always on PATH (go install does not touch it).
export PATH := $(PATH):$(shell go env GOPATH)/bin

proto:
	@mkdir -p gen
	protoc -I $(PROTOC_INCLUDE) -I api/proto \
		--go_out=. --go_opt=module=github.com/tjarktomaszewski/tjarkFS \
		--go-grpc_out=. --go-grpc_opt=module=github.com/tjarktomaszewski/tjarkFS \
		api/proto/tjarkfs/v1/*.proto

.PHONY: run run-node build test test-race proto
