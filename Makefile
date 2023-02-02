SHELL := /bin/bash

GOCMD=go
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install

TEST_REGEX=.
TEST_COUNT=1

.PHONY: test
test:
	$(GOTEST) -run=$(TEST_REGEX) -count=$(TEST_COUNT) ./...

# lint runs all local linters that must pass before pushing
.PHONY: lint lint-install lint-fast
lint: lint-install
	goimports -local github.com/TykTechnologies -w .
	gofmt -w .
	faillint -ignore-tests -paths "$(shell grep -v '^#' .faillint | xargs echo | sed 's/ /,/g')" ./...

lint-fast:
	go generate ./...
	go fmt ./...
	go mod tidy

lint-install: lint-fast
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45.0
	go install github.com/fatih/faillint@latest
