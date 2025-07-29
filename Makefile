# Triangular Arbitrage Bot Makefile

# Variables
BINARY_NAME=triangular-arbitrage-bot
BUILD_DIR=build
MAIN_FILE=main.go
GO_FILES=$(shell find . -name "*.go" -not -path "./.git/*")

# Go build flags
LDFLAGS=-ldflags="-s -w"
BUILD_FLAGS=-trimpath

# Default target
.PHONY: all
all: build

# Build the application
.PHONY: build
build: clean
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) .
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# Build for different platforms
.PHONY: build-all
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	
	# Linux
	GOOS=linux GOARCH=amd64 go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 .
	
	# Windows
	GOOS=windows GOARCH=amd64 go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe .
	
	# macOS
	GOOS=darwin GOARCH=amd64 go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .
	
	@echo "Multi-platform build complete!"

# Run the application
.PHONY: run
run: build
	@echo "Running $(BINARY_NAME)..."
	./$(BUILD_DIR)/$(BINARY_NAME)



# Run without building (if binary exists)
.PHONY: run-only
run-only:
	@if [ -f "$(BUILD_DIR)/$(BINARY_NAME)" ]; then \
		echo "Running $(BINARY_NAME)..."; \
		./$(BUILD_DIR)/$(BINARY_NAME); \
	else \
		echo "Binary not found. Run 'make build' first."; \
		exit 1; \
	fi

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@go clean -cache
	@echo "Clean complete!"

# Install dependencies (if using go modules)
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	@go mod tidy
	@go mod download
	@echo "Dependencies installed!"

# Initialize go module (if not already done)
.PHONY: init
init:
	@if [ ! -f "go.mod" ]; then \
		echo "Initializing Go module..."; \
		go mod init triangular-arbitrage-bot; \
		go mod tidy; \
	else \
		echo "Go module already initialized."; \
	fi

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Code formatting complete!"

# Run linter
.PHONY: lint
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Install it with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	@go test -v ./...
	@echo "Tests complete!"

# Run tests with coverage
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Install the binary to system
.PHONY: install
install: build
	@echo "Installing $(BINARY_NAME) to system..."
	@sudo cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/
	@echo "Installation complete!"

# Uninstall the binary from system
.PHONY: uninstall
uninstall:
	@echo "Uninstalling $(BINARY_NAME) from system..."
	@sudo rm -f /usr/local/bin/$(BINARY_NAME)
	@echo "Uninstallation complete!"

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build          - Build the application"
	@echo "  build-all      - Build for multiple platforms (Linux, Windows, macOS)"
	@echo "  run            - Build and run the application"
	@echo "  run-only       - Run the application (if already built)"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Install dependencies"
	@echo "  init           - Initialize Go module"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
	@echo "  test           - Run tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  install        - Install binary to system"
	@echo "  uninstall      - Remove binary from system"
	@echo "  help           - Show this help message"

# Development mode - build and run with file watching (requires fswatch)
.PHONY: dev
dev:
	@echo "Starting development mode..."
	@if command -v fswatch >/dev/null 2>&1; then \
		echo "Watching for changes..."; \
		fswatch -o . --exclude=build --exclude=.git | xargs -n1 -I{} make run; \
	else \
		echo "fswatch not found. Install it for file watching capability."; \
		echo "On Ubuntu/Debian: sudo apt-get install fswatch"; \
		echo "On macOS: brew install fswatch"; \
	fi

# Check if .env file exists
.PHONY: check-env
check-env:
	@if [ ! -f ".env" ]; then \
		echo "Warning: .env file not found!"; \
		echo "Please create a .env file with your API credentials:"; \
		echo "COINEX_API_KEY=your_api_key_here"; \
		echo "COINEX_SECRET_ID=your_secret_id_here"; \
		exit 1; \
	else \
		echo ".env file found ✓"; \
	fi

# Setup development environment
.PHONY: setup
setup: check-env init deps
	@echo "Development environment setup complete!"
	@echo "You can now run: make build && make run" 