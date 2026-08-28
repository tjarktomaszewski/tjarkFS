test:
	@echo Run tests
	@go test ./... -cover

test-race:
	@echo Run tests
	@go test ./... -cover -race