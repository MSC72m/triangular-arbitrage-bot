# Triangular Arbitrage Bot Makefile

# Variables
BINARY_NAME=triangular-arbitrage-bot
BUILD_DIR=build
MAIN_FILE=main.go
GO_FILES=$(shell find . -name "*.go" -not -path "./.git/*")

# Go build flags
LDFLAGS=-ldflags="-s -w"
BUILD_FLAGS=-trimpath -buildvcs=false

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

# Run with logging to file
.PHONY: run-log
run-log: build
	@echo "Running $(BINARY_NAME) with logging..."
	@mkdir -p logs
	@echo "Logs will be saved to logs/bot-$(shell date +%Y%m%d-%H%M%S).log"
	@echo "Press Ctrl+C to stop gracefully..."
	@echo '#!/bin/bash' > /tmp/run-bot.sh && \
	echo 'set -e' >> /tmp/run-bot.sh && \
	echo 'LOG_FILE="logs/bot-$$(date +%Y%m%d-%H%M%S).log"' >> /tmp/run-bot.sh && \
	echo 'echo "Starting bot with logging to $$LOG_FILE"' >> /tmp/run-bot.sh && \
	echo 'BOT_PID=""' >> /tmp/run-bot.sh && \
	echo 'TEE_PID=""' >> /tmp/run-bot.sh && \
	echo 'cleanup() {' >> /tmp/run-bot.sh && \
	echo '  echo "Shutting down gracefully..."' >> /tmp/run-bot.sh && \
	echo '  if [ ! -z "$$BOT_PID" ]; then' >> /tmp/run-bot.sh && \
	echo '    echo "Sending SIGTERM to bot process ($$BOT_PID)..."' >> /tmp/run-bot.sh && \
	echo '    kill -TERM $$BOT_PID 2>/dev/null || true' >> /tmp/run-bot.sh && \
	echo '    echo "Waiting for bot to shutdown..."' >> /tmp/run-bot.sh && \
	echo '    timeout 10s tail --pid=$$BOT_PID -f /dev/null 2>/dev/null || true' >> /tmp/run-bot.sh && \
	echo '    echo "Checking if bot process is still running..."' >> /tmp/run-bot.sh && \
	echo '    if kill -0 $$BOT_PID 2>/dev/null; then' >> /tmp/run-bot.sh && \
	echo '      echo "Bot still running, sending SIGKILL..."' >> /tmp/run-bot.sh && \
	echo '      kill -KILL $$BOT_PID 2>/dev/null || true' >> /tmp/run-bot.sh && \
	echo '    fi' >> /tmp/run-bot.sh && \
	echo '  fi' >> /tmp/run-bot.sh && \
	echo '  if [ ! -z "$$TEE_PID" ]; then' >> /tmp/run-bot.sh && \
	echo '    echo "Stopping tee process ($$TEE_PID)..."' >> /tmp/run-bot.sh && \
	echo '    kill -TERM $$TEE_PID 2>/dev/null || true' >> /tmp/run-bot.sh && \
	echo '  fi' >> /tmp/run-bot.sh && \
	echo '  echo "Shutdown complete."' >> /tmp/run-bot.sh && \
	echo '  exit 0' >> /tmp/run-bot.sh && \
	echo '}' >> /tmp/run-bot.sh && \
	echo 'trap cleanup INT TERM' >> /tmp/run-bot.sh && \
	echo './$(BUILD_DIR)/$(BINARY_NAME) 2>&1 | tee "$$LOG_FILE" &' >> /tmp/run-bot.sh && \
	echo 'TEE_PID=$$!' >> /tmp/run-bot.sh && \
	echo 'sleep 0.1' >> /tmp/run-bot.sh && \
	echo 'BOT_PID=$$(pgrep -f "$(BINARY_NAME)" | head -1)' >> /tmp/run-bot.sh && \
	echo 'if [ -z "$$BOT_PID" ]; then' >> /tmp/run-bot.sh && \
	echo '  echo "Warning: Could not find bot process ID"' >> /tmp/run-bot.sh && \
	echo '  BOT_PID=$$TEE_PID' >> /tmp/run-bot.sh && \
	echo 'fi' >> /tmp/run-bot.sh && \
	echo 'echo "Bot process ID: $$BOT_PID"' >> /tmp/run-bot.sh && \
	echo 'echo "Tee process ID: $$TEE_PID"' >> /tmp/run-bot.sh && \
	echo 'wait $$TEE_PID' >> /tmp/run-bot.sh && \
	chmod +x /tmp/run-bot.sh && \
	/tmp/run-bot.sh

# Run with logging to file (simple version)
.PHONY: run-log-simple
run-log-simple: build
	@echo "Running $(BINARY_NAME) with logging (simple version)..."
	@mkdir -p logs
	@echo "Logs will be saved to logs/bot-$(shell date +%Y%m%d-%H%M%S).log"
	@echo "Press Ctrl+C to stop gracefully..."
	@echo '#!/bin/bash' > /tmp/run-bot-simple.sh && \
	echo 'set -e' >> /tmp/run-bot-simple.sh && \
	echo 'LOG_FILE="logs/bot-$$(date +%Y%m%d-%H%M%S).log"' >> /tmp/run-bot-simple.sh && \
	echo 'echo "Starting bot with logging to $$LOG_FILE"' >> /tmp/run-bot-simple.sh && \
	echo 'cleanup() {' >> /tmp/run-bot-simple.sh && \
	echo '  echo "Shutting down gracefully..."' >> /tmp/run-bot-simple.sh && \
	echo '  echo "Shutdown complete."' >> /tmp/run-bot-simple.sh && \
	echo '  exit 0' >> /tmp/run-bot-simple.sh && \
	echo '}' >> /tmp/run-bot-simple.sh && \
	echo 'trap cleanup INT TERM' >> /tmp/run-bot-simple.sh && \
	echo 'exec ./$(BUILD_DIR)/$(BINARY_NAME) 2>&1 | tee "$$LOG_FILE"' >> /tmp/run-bot-simple.sh && \
	chmod +x /tmp/run-bot-simple.sh && \
	/tmp/run-bot-simple.sh


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
	@echo "  run-log        - Build and run with logging to file (robust shutdown)"
	@echo "  run-log-simple - Build and run with logging to file (simple version)"
	@echo "  search-logs    - Search through log files (use: make search-logs SEARCH='term')"
	@echo "  view-logs      - View recent log files and content"
	@echo "  clean-logs     - Remove all log files"
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

# Search logs
.PHONY: search-logs
search-logs:
	@echo "Usage: make search-logs SEARCH='your search term'"
	@echo "Example: make search-logs SEARCH='ARBITRAGE'"
	@if [ -z "$(SEARCH)" ]; then \
		echo "Error: Please provide a search term"; \
		echo "Example: make search-logs SEARCH='ARBITRAGE'"; \
		exit 1; \
	fi
	@echo "Searching logs for: $(SEARCH)"
	@find logs -name "bot-*.log" -type f -exec grep -H "$(SEARCH)" {} \; | head -50

# View recent logs
.PHONY: view-logs
view-logs:
	@echo "Recent log files:"
	@ls -la logs/bot-*.log 2>/dev/null | head -10 || echo "No log files found"
	@echo ""
	@echo "Latest log content (last 50 lines):"
	@if [ -f "logs/bot-$$(ls -t logs/bot-*.log 2>/dev/null | head -1 | sed 's/.*bot-\(.*\)\.log/\1/')".log ]; then \
		tail -50 "logs/bot-$$(ls -t logs/bot-*.log 2>/dev/null | head -1 | sed 's/.*bot-\(.*\)\.log/\1/')".log; \
	else \
		echo "No log files found"; \
	fi

# Clean logs
.PHONY: clean-logs
clean-logs:
	@echo "Removing all log files..."
	@rm -f logs/bot-*.log
	@echo "Logs cleaned." 