# Triangular Arbitrage Bot

A Go-based cryptocurrency triangular arbitrage bot designed to identify and execute profitable trading opportunities across multiple exchanges. This bot specifically targets the CoinEx exchange API to monitor price differences and execute trades when profitable opportunities arise.

## 🚀 Features

- **Real-time Market Monitoring**: Continuously monitors cryptocurrency prices across different trading pairs
- **Triangular Arbitrage Detection**: Identifies profitable triangular arbitrage opportunities
- **Automated Trading**: Executes trades automatically when profitable opportunities are detected
- **Multi-threaded Architecture**: Uses goroutines for concurrent market data processing
- **Secure API Integration**: Secure handling of API credentials and authentication
- **Cross-platform Support**: Builds for Linux, Windows, and macOS

## 📋 Prerequisites

- **Go 1.19+**: Required for building and running the application
- **CoinEx API Credentials**: You'll need API key and secret from CoinEx
- **Git**: For cloning the repository

### Installing Go

#### Ubuntu/Debian:
```bash
sudo apt update
sudo apt install golang-go
```

#### macOS:
```bash
brew install go
```

#### Windows:
Download from [golang.org](https://golang.org/dl/)

## 🛠️ Installation & Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd triangular-arbitrage-bot
```

### 2. Environment Configuration
Create a `.env` file in the project root with your CoinEx API credentials:

```env
COINEX_API_KEY=your_api_key_here
COINEX_SECRET_ID=your_secret_id_here
```

**⚠️ Important**: Never commit your `.env` file to version control. It's already included in `.gitignore`.

### 3. Initialize the Project
```bash
make setup
```

This command will:
- Check for the `.env` file
- Initialize the Go module
- Install dependencies
- Set up the development environment

## 📖 Makefile Guide

The project includes a comprehensive Makefile that simplifies building, testing, and managing the application. Here's a detailed guide to all available commands:

### 🏗️ Build Commands

#### `make build`
Builds the application for your current platform.
```bash
make build
# Output: build/triangular-arbitrage-bot
```

#### `make build-all`
Builds the application for multiple platforms (Linux, Windows, macOS).
```bash
make build-all
# Output: 
# - triangular-arbitrage-bot-linux-amd64
# - triangular-arbitrage-bot-windows-amd64.exe
# - triangular-arbitrage-bot-darwin-amd64
# - triangular-arbitrage-bot-darwin-arm64
```

### 🚀 Run Commands

#### `make run`
Builds and runs the application.
```bash
make run
```

#### `make run-only`
Runs the application without rebuilding (if already built).
```bash
make run-only
```

### 🧹 Clean Commands

#### `make clean`
Removes all build artifacts and cleans Go cache.
```bash
make clean
```

### 🔧 Development Commands

#### `make setup`
Complete development environment setup.
```bash
make setup
# Performs: check-env + init + deps
```

#### `make init`
Initializes the Go module (creates go.mod if it doesn't exist).
```bash
make init
```

#### `make deps`
Installs and updates dependencies.
```bash
make deps
```

#### `make fmt`
Formats all Go code according to Go standards.
```bash
make fmt
```

#### `make lint`
Runs the linter to check code quality (requires golangci-lint).
```bash
make lint
```

#### `make test`
Runs all tests.
```bash
make test
```

#### `make test-coverage`
Runs tests and generates a coverage report.
```bash
make test-coverage
# Generates: coverage.html
```

### 🛠️ System Installation

#### `make install`
Installs the binary to your system (requires sudo).
```bash
make install
# Installs to: /usr/local/bin/triangular-arbitrage-bot
```

#### `make uninstall`
Removes the binary from your system.
```bash
make uninstall
```

### 🔍 Utility Commands

#### `make check-env`
Verifies that your `.env` file exists and is properly configured.
```bash
make check-env
```

#### `make help`
Shows all available Makefile commands.
```bash
make help
```

### 🔄 Development Mode

#### `make dev`
Starts development mode with file watching (requires fswatch).
```bash
make dev
```

**Note**: To use development mode, install fswatch:
- **Ubuntu/Debian**: `sudo apt-get install fswatch`
- **macOS**: `brew install fswatch`

## 🏃‍♂️ Quick Start Guide

### First Time Setup
```bash
# 1. Clone the repository
git clone <repository-url>
cd triangular-arbitrage-bot

# 2. Create your .env file
cp .env.example .env
# Edit .env with your API credentials

# 3. Setup the development environment
make setup

# 4. Build and run
make run
```

### Daily Development Workflow
```bash
# Build the application
make build

# Run the application
make run

# Format code before committing
make fmt

# Run tests
make test

# Clean up when done
make clean
```

## 📁 Project Structure

```
triangular-arbitrage-bot/
├── main.go              # Application entry point
├── coinex.go            # CoinEx API client implementation
├── network.go           # HTTP client and network utilities
├── utils.go             # Utility functions (env loading, hashing)
├── apiInterface.go      # API interface definitions
├── .env                 # Environment variables (not in git)
├── .gitignore          # Git ignore rules
├── Makefile            # Build and development commands
├── go.mod              # Go module definition
├── go.sum              # Go module checksums
└── README.md           # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `COINEX_API_KEY` | Your CoinEx API key | Yes |
| `COINEX_SECRET_ID` | Your CoinEx API secret | Yes |

### Build Configuration

The Makefile includes several build optimizations:

- **Stripped Binaries**: Reduces binary size
- **Trimmed Paths**: Removes file paths for security
- **Cross-platform Support**: Builds for multiple operating systems

## 🧪 Testing

### Running Tests
```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage
```

### Test Coverage
The test coverage report is generated as `coverage.html` and can be opened in your browser.

## 🚀 Deployment

### Local Installation
```bash
make install
```

### Cross-platform Distribution
```bash
make build-all
```

This creates binaries for:
- Linux (AMD64)
- Windows (AMD64)
- macOS (Intel and ARM)

## 🔍 Troubleshooting

### Common Issues

#### Build Errors
```bash
# If you get "undefined" errors:
make init
make deps
make build
```

#### Missing Dependencies
```bash
# Install Go linter
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Install file watcher (for dev mode)
sudo apt-get install fswatch  # Ubuntu/Debian
brew install fswatch          # macOS
```

#### Environment Issues
```bash
# Check if .env file exists
make check-env

# Re-setup environment
make setup
```

### Debug Mode
To run with debug information:
```bash
go run . -debug
```

## 📝 Development Guidelines

### Code Style
- Use `make fmt` before committing
- Run `make lint` to check code quality
- Follow Go naming conventions

### Adding New Features
1. Create your feature branch
2. Implement the feature
3. Add tests
4. Run `make test`
5. Format code with `make fmt`
6. Submit a pull request

### Dependencies
- Add new dependencies: `go get <package>`
- Update dependencies: `make deps`
- Check for security issues: `go list -m all`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Format code: `make fmt`
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## ⚠️ Disclaimer

This software is for educational and research purposes. Cryptocurrency trading involves significant risk. Use at your own risk and never invest more than you can afford to lose.

## 🆘 Support

If you encounter any issues:

1. Check the troubleshooting section above
2. Run `make help` to see all available commands
3. Check the logs for error messages
4. Open an issue on GitHub with detailed information

---

**Happy Trading! 🚀** 