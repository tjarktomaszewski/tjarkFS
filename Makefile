run:
	@go run ./cmd/client $(ARGS)

build:
	@mkdir -p bin
	@go build -o bin/tjarkfs ./cmd/client

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

.PHONY: run build test test-race proto
