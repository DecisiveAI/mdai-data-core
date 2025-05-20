.PHONY: test
test: tidy vendor
        CGO_ENABLED=0 go test -mod=vendor -v -count=1 ./...

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: vendor
vendor:
	@go mod vendor
