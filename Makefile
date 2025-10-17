.PHONY: test test-race tidy vendor generate install-mockgen

test: tidy vendor
	CGO_ENABLED=0 go test -mod=vendor -v -count=1 ./...

test-race: tidy vendor
	CGO_ENABLED=1 go test -race -mod=vendor -v -count=1 ./...

tidy:
	@go mod tidy

vendor:
	@go mod vendor

generate:
	go generate ./...

install-mockgen:
	go install go.uber.org/mock/mockgen@v0.6.0