.PHONY: all build test lint fmt vet clean run deps tidy cover pre-commit docker-build \
        build-linux build-darwin build-all-platforms

BINARY   := triangular-arbitrage-bot
BUILD_DIR:= build
GO_FLAGS := -trimpath -buildvcs=false

VERSION   ?= dev
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LD_FLAGS  := -ldflags="\
	-s -w \
	-X triangular-arbitrage-bot/internal/version.Version=$(VERSION) \
	-X triangular-arbitrage-bot/internal/version.Commit=$(COMMIT) \
	-X triangular-arbitrage-bot/internal/version.BuildTime=$(BUILD_TIME)"

all: fmt vet lint test build

build:
	@mkdir -p $(BUILD_DIR)
	go build $(GO_FLAGS) $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY) .

run: build
	./$(BUILD_DIR)/$(BINARY) -config config.json

test:
	go test -race -count=1 ./...

cover:
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

fmt:
	gofmt -s -w .

vet:
	go vet ./...

tidy:
	go mod tidy

deps:
	go mod download

pre-commit:
	@echo "Installing pre-commit hook..."
	@ln -sf ../../scripts/pre-commit.sh .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."

docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(BINARY):$(VERSION) .

docker-run:
	docker compose up --build -d

docker-stop:
	docker compose down

clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

# Cross-compilation
build-linux:
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 go build $(GO_FLAGS) $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY)-linux-amd64 .

build-darwin:
	@mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=arm64 go build $(GO_FLAGS) $(LD_FLAGS) -o $(BUILD_DIR)/$(BINARY)-darwin-arm64 .

build-all-platforms: build-linux build-darwin
	@echo "Cross-platform builds complete"
