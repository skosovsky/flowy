.PHONY: test lint bench fuzz cover

test:
	@go test -race -count=1 ./...

lint:
	@golangci-lint run ./...

bench:
	@go test -bench=. -benchmem ./...

fuzz:
	@go test -fuzz=. -fuzztime=30s .

cover:
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out
