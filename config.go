package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// FOKOrderSettings configures Fill-or-Kill order handling
type FOKOrderSettings struct {
	PollingFrequencyHz           float64 `json:"pollingFrequencyHz"`           // Order status polling frequency (Hz)
	OrderTimeoutSeconds          int     `json:"orderTimeoutSeconds"`          // Timeout before canceling order
	MaxRetryAttempts             int     `json:"maxRetryAttempts"`             // Max attempts to retry arbitrage
	EnableAggressiveReEvaluation bool    `json:"enableAggressiveReEvaluation"` // Enable aggressive profit re-evaluation
	CancelAllOnSingleFailure     bool    `json:"cancelAllOnSingleFailure"`     // Cancel all orders if one fails
}

// OrderExecutionSettings configures order execution rate and amount management
type OrderExecutionSettings struct {
	MaxOrdersPerSecond     float64 `json:"maxOrdersPerSecond"`     // Maximum orders per second (e.g., 0.5 = 1 order every 2 seconds)
	MaxConcurrentOrders    int     `json:"maxConcurrentOrders"`    // Maximum concurrent orders across all markets
	OrderAmountType        string  `json:"orderAmountType"`        // "static" or "dynamic"
	StaticOrderAmount      float64 `json:"staticOrderAmount"`      // Fixed USD amount per order (when static)
	DynamicOrderPercentage float64 `json:"dynamicOrderPercentage"` // Percentage of available balance (when dynamic)
	MaxVolumeFraction      float64 `json:"maxVolumeFraction"`      // Maximum fraction of available order book depth to use (0.0-1.0)
	MaxDailySpend          float64 `json:"maxDailySpend"`          // Maximum USD to spend per day
	AccountBalance         float64 `json:"accountBalance"`         // Current account balance (USD)
	EnableSpendingLimits   bool    `json:"enableSpendingLimits"`   // Enable spending protection
	MinOrderAmount         float64 `json:"minOrderAmount"`         // Minimum order amount (USD)
	MaxOrderAmount         float64 `json:"maxOrderAmount"`         // Maximum order amount (USD)
}

type Config struct {
	// API Configuration
	APIKey       string `json:"apiKey"`
	SecretID     string `json:"secretID"`
	APIBaseURL   string `json:"apiBaseURL"`
	APIBaseURLV2 string `json:"apiBaseURLV2"`

	// Trading Configuration
	ProfitThreshold float64  `json:"profitThreshold"` // Minimum profit % to execute trades
	QuoteCurrencies []string `json:"quoteCurrencies"` // Base currencies for triangular arbitrage
	CriticalMarkets []string `json:"criticalMarkets"` // Critical markets for triangular arbitrage
	IncludeBTC      bool     `json:"includeBTC"`      // Include BTC pairs in arbitrage

	// Order Execution & Asset Locking Configuration
	EnableAssetLocking          bool `json:"enableAssetLocking"`          // Enable asset locking during order execution
	OrderExecutionTimeMs        int  `json:"orderExecutionTimeMs"`        // Simulated order execution time in milliseconds
	MaxConcurrentOrdersPerAsset int  `json:"maxConcurrentOrdersPerAsset"` // Maximum concurrent orders per asset (0 = unlimited)
	OrderTimeoutSeconds         int  `json:"orderTimeoutSeconds"`         // Order timeout in seconds
	EnableOrderSimulation       bool `json:"enableOrderSimulation"`       // Enable realistic order execution simulation

	// Risk Management
	OrderBookDepthLimit int `json:"orderBookDepthLimit"` // Number of order book levels to analyze

	// Rate Limiting
	RateLimitPerSecond int `json:"rateLimitPerSecond"` // API calls per second
	RateLimitPerMinute int `json:"rateLimitPerMinute"` // API calls per minute

	// Performance & Latency
	MaxLatencyMs             int `json:"maxLatencyMs"`             // Maximum acceptable latency for execution
	ConcurrentScans          int `json:"concurrentScans"`          // Number of concurrent arbitrage scans
	wsSubscriptionsBatchSize int `json:"wsSubscriptionsBatchSize"` // Number of markets to subscribe to in a single batch

	// Logging
	LogLevel string `json:"logLevel"` // debug, info, warn, error

	// Trading Fees (per market, configurable)
	TradingFees       map[string]float64 `json:"tradingFees"`       // Market-specific trading fees
	DefaultTradingFee float64            `json:"defaultTradingFee"` // Default trading fee percentage

	// WebSocket Configuration
	WebSocketTimeout  int `json:"webSocketTimeout"`  // WebSocket timeout in seconds
	ReconnectAttempts int `json:"reconnectAttempts"` // Max reconnection attempts

	// Execution Configuration
	OrderTimeout       int  `json:"orderTimeout"`       // Order timeout in seconds
	EnablePaperTrading bool `json:"enablePaperTrading"` // Enable paper trading mode
	SimulationMode     bool `json:"simulationMode"`     // Run in simulation mode only

	// FOK Order Configuration
	FOKOrderSettings FOKOrderSettings `json:"fokOrderSettings"`

	// Order Execution Configuration
	OrderExecutionSettings OrderExecutionSettings `json:"orderExecutionSettings"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		// BASE API URL
		APIBaseURL:   "https://api.coinex.com/v1",
		APIBaseURLV2: "https://api.coinex.com/v2/",

		// Trading defaults
		ProfitThreshold: 0.0001, // 0.01%
		QuoteCurrencies: []string{"USDT", "USDC"},
		CriticalMarkets: []string{"USDCUSDT"},
		IncludeBTC:      false,

		// Order execution defaults
		EnableAssetLocking:          true, // Enable realistic asset locking
		OrderExecutionTimeMs:        3000, // 3 second simulated execution time
		MaxConcurrentOrdersPerAsset: 1,    // Only 1 order per asset at a time
		OrderTimeoutSeconds:         30,   // 30 second order timeout
		EnableOrderSimulation:       true, // Enable realistic order simulation

		// Risk management defaults
		OrderBookDepthLimit: 5, // Analyze top 5 levels

		// Rate limiting defaults
		RateLimitPerSecond: 15,
		RateLimitPerMinute: 300,

		// Performance defaults
		MaxLatencyMs:             200, // 200ms max latency
		ConcurrentScans:          250, // 250 concurrent scanners
		wsSubscriptionsBatchSize: 50,  // 50 markets per batch

		// Logging defaults
		LogLevel: "debug",

		// Trading fees (0.2% = 0.002)
		TradingFees: map[string]float64{
			"USDT": 0.002,
			"USDC": 0.002,
		},
		DefaultTradingFee: 0.002,

		// WebSocket defaults
		WebSocketTimeout:  30,
		ReconnectAttempts: 5,

		// Execution defaults
		OrderTimeout:       30,
		EnablePaperTrading: false,
		SimulationMode:     false,

		// FOK Order Settings
		FOKOrderSettings: FOKOrderSettings{
			PollingFrequencyHz:           2.5,  // Poll every 400ms
			OrderTimeoutSeconds:          2,    // 2 second timeout
			MaxRetryAttempts:             3,    // 3 retry attempts
			EnableAggressiveReEvaluation: true, // Re-evaluate profit after each order
			CancelAllOnSingleFailure:     true, // Cancel all orders if one fails
		},

		// Order Execution Settings
		OrderExecutionSettings: OrderExecutionSettings{
			MaxOrdersPerSecond:     0.5, // 1 order every 2 seconds
			MaxConcurrentOrders:    5,   // 5 concurrent orders max
			OrderAmountType:        "static",
			StaticOrderAmount:      3.5,  // $3.50 per order
			DynamicOrderPercentage: 0.25, // 25% of available balance
			MaxVolumeFraction:      0.5,  // Use up to 50% of order book depth
			MaxDailySpend:          20.0, // $20 max per day
			AccountBalance:         1.5,  // $1.50 available balance
			EnableSpendingLimits:   true, // Enable spending protection
			MinOrderAmount:         0.5,  // $0.50 minimum order
			MaxOrderAmount:         10.0, // $10.00 maximum order
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	// Start with defaults
	config := DefaultConfig()

	// Try to load from file
	file, err := os.Open(path)
	if err != nil {
		// Return defaults if file doesn't exist
		return config, nil
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}

	env := loadEnvConfig()
	config.APIKey = env["COINEX_API_KEY"]
	config.SecretID = env["COINEX_SECRET_ID"]

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	fmt.Printf(" Loaded configuration:\n")
	fmt.Printf("    Profit Threshold: %.4f%%\n", config.ProfitThreshold*100)
	fmt.Printf("    Quote Currencies: %v\n", config.QuoteCurrencies)
	fmt.Printf("    Include BTC: %v\n", config.IncludeBTC)
	fmt.Printf("   🔒 Asset Locking Enabled: %v\n", config.EnableAssetLocking)
	fmt.Printf("   ⏱️  Order Execution Time: %dms\n", config.OrderExecutionTimeMs)
	fmt.Printf("    Max Orders Per Asset: %d\n", config.MaxConcurrentOrdersPerAsset)
	fmt.Printf("    Order Simulation: %v\n", config.EnableOrderSimulation)
	fmt.Printf("    Order Book Depth Limit: %d\n", config.OrderBookDepthLimit)
	fmt.Printf("    Rate Limit Per Second: %d\n", config.RateLimitPerSecond)
	fmt.Printf("    Rate Limit Per Minute: %d\n", config.RateLimitPerMinute)
	fmt.Printf("    Max Latency Ms: %d\n", config.MaxLatencyMs)
	fmt.Printf("    Concurrent Scans: %d\n", config.ConcurrentScans)
	fmt.Printf("    Log Level: %s\n", config.LogLevel)
	fmt.Printf("    Default Trading Fee: %.4f%%\n", config.DefaultTradingFee*100)
	fmt.Printf("    FOK Order Settings:\n")
	fmt.Printf("       Polling Frequency: %.1fHz\n", config.FOKOrderSettings.PollingFrequencyHz)
	fmt.Printf("       Order Timeout: %d seconds\n", config.FOKOrderSettings.OrderTimeoutSeconds)
	fmt.Printf("       Max Retry Attempts: %d\n", config.FOKOrderSettings.MaxRetryAttempts)
	fmt.Printf("       Enable Aggressive Re-evaluation: %v\n", config.FOKOrderSettings.EnableAggressiveReEvaluation)
	fmt.Printf("       Cancel All on Single Failure: %v\n", config.FOKOrderSettings.CancelAllOnSingleFailure)
	fmt.Printf("    Order Execution Settings:\n")
	fmt.Printf("       Max Orders Per Second: %.2f\n", config.OrderExecutionSettings.MaxOrdersPerSecond)
	fmt.Printf("       Max Concurrent Orders: %d\n", config.OrderExecutionSettings.MaxConcurrentOrders)
	fmt.Printf("       Order Amount Type: %s\n", config.OrderExecutionSettings.OrderAmountType)
	fmt.Printf("       Dynamic Order Percentage: %.2f%%\n", config.OrderExecutionSettings.DynamicOrderPercentage*100)
	fmt.Printf("       Max Volume Fraction: %.2f%%\n", config.OrderExecutionSettings.MaxVolumeFraction*100)
	fmt.Printf("       Max Daily Spend: %.2f\n", config.OrderExecutionSettings.MaxDailySpend)
	fmt.Printf("       Account Balance: %.2f\n", config.OrderExecutionSettings.AccountBalance)
	fmt.Printf("       Enable Spending Limits: %v\n", config.OrderExecutionSettings.EnableSpendingLimits)
	fmt.Printf("       Min Order Amount: %.2f\n", config.OrderExecutionSettings.MinOrderAmount)
	fmt.Printf("       Max Order Amount: %.2f\n", config.OrderExecutionSettings.MaxOrderAmount)

	return config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.APIKey == "" {
		return fmt.Errorf("apiKey is required")
	}
	if c.SecretID == "" {
		return fmt.Errorf("secretID is required")
	}

	// Additional validation for real trading mode
	if !c.SimulationMode {
		if len(c.APIKey) < 10 {
			return fmt.Errorf("apiKey appears to be invalid (too short) - required for real trading")
		}
		if len(c.SecretID) < 10 {
			return fmt.Errorf("secretID appears to be invalid (too short) - required for real trading")
		}

		fmt.Printf("  REAL TRADING MODE VALIDATION:\n")
		fmt.Printf("   🔑 API Key: %s...%s (length: %d)\n",
			c.APIKey[:4], c.APIKey[len(c.APIKey)-4:], len(c.APIKey))
		fmt.Printf("   🔐 Secret Key: %s...%s (length: %d)\n",
			c.SecretID[:4], c.SecretID[len(c.SecretID)-4:], len(c.SecretID))
		fmt.Printf("    API credentials appear valid for real trading\n")
	}

	if c.ProfitThreshold <= 0 {
		return fmt.Errorf("profitThreshold must be positive")
	}
	if len(c.QuoteCurrencies) == 0 {
		return fmt.Errorf("at least one quote currency is required")
	}
	if c.RateLimitPerSecond <= 0 {
		return fmt.Errorf("rateLimitPerSecond must be positive")
	}

	// Validate FOK order settings
	if c.FOKOrderSettings.PollingFrequencyHz <= 0 || c.FOKOrderSettings.PollingFrequencyHz > 100 {
		return fmt.Errorf("fokOrderSettings.pollingFrequencyHz must be between 0 and 100")
	}
	if c.FOKOrderSettings.OrderTimeoutSeconds <= 0 || c.FOKOrderSettings.OrderTimeoutSeconds > 300 {
		return fmt.Errorf("fokOrderSettings.orderTimeoutSeconds must be between 1 and 300")
	}
	if c.FOKOrderSettings.MaxRetryAttempts < 0 || c.FOKOrderSettings.MaxRetryAttempts > 10 {
		return fmt.Errorf("fokOrderSettings.maxRetryAttempts must be between 0 and 10")
	}
	if c.FOKOrderSettings.EnableAggressiveReEvaluation {
		if c.ProfitThreshold <= 0 || c.ProfitThreshold > 0.01 {
			return fmt.Errorf("profitThreshold must be positive and less than or equal to 0.01 for aggressive re-evaluation")
		}
	}

	// Validate Order Execution Settings
	if c.OrderExecutionSettings.MaxOrdersPerSecond <= 0 {
		return fmt.Errorf("orderExecutionSettings.maxOrdersPerSecond must be positive")
	}
	if c.OrderExecutionSettings.MaxConcurrentOrders <= 0 {
		return fmt.Errorf("orderExecutionSettings.maxConcurrentOrders must be positive")
	}
	if c.OrderExecutionSettings.DynamicOrderPercentage <= 0 || c.OrderExecutionSettings.DynamicOrderPercentage > 1 {
		return fmt.Errorf("orderExecutionSettings.dynamicOrderPercentage must be between 0 and 1")
	}
	if c.OrderExecutionSettings.MaxVolumeFraction < 0 || c.OrderExecutionSettings.MaxVolumeFraction > 1 {
		return fmt.Errorf("orderExecutionSettings.maxVolumeFraction must be between 0 and 1")
	}
	if c.OrderExecutionSettings.MaxDailySpend <= 0 {
		return fmt.Errorf("orderExecutionSettings.maxDailySpend must be positive")
	}
	if c.OrderExecutionSettings.AccountBalance <= 0 {
		return fmt.Errorf("orderExecutionSettings.accountBalance must be positive")
	}
	if c.OrderExecutionSettings.MinOrderAmount <= 0 {
		return fmt.Errorf("orderExecutionSettings.minOrderAmount must be positive")
	}
	if c.OrderExecutionSettings.MaxOrderAmount <= 0 {
		return fmt.Errorf("orderExecutionSettings.maxOrderAmount must be positive")
	}

	return nil
}

func (c *Config) GetCriticalMarkets() []string {
	// Return the critical markets from config, with fallback to USDCUSDT
	if len(c.CriticalMarkets) > 0 {
		return c.CriticalMarkets
	}
	// Fallback to USDCUSDT if no critical markets configured
	return []string{"USDCUSDT"}
}
