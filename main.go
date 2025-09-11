package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Set up structured logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[ARBITRAGE] ")

	log.Println("Starting Triangular Arbitrage Bot...")

	// Load configuration
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration: API Key length=%d, Quote currencies=%v, Profit threshold=%.6f%%",
		len(config.APIKey), config.QuoteCurrencies, config.ProfitThreshold*100)

	if config.SimulationMode {
		log.Println("SIMULATION MODE ENABLED - No real trades will be executed")
		log.Println("   FOK orders will be simulated")
		log.Println("   Enable real trading by setting simulationMode: false in config.json")
	} else {
		log.Println("REAL TRADING MODE ENABLED - FOK orders will use CoinEx API")
		log.Printf("   FOK Polling Frequency: %.1fHz (every %.0fms)",
			config.FOKOrderSettings.PollingFrequencyHz,
			1000/config.FOKOrderSettings.PollingFrequencyHz)
		log.Printf("   FOK Order Timeout: %ds", config.FOKOrderSettings.OrderTimeoutSeconds)
		log.Printf("   Max Retry Attempts: %d", config.FOKOrderSettings.MaxRetryAttempts)
		log.Println("   REAL MONEY WILL BE USED!")
	}

	// Display order execution settings
	log.Printf("ORDER EXECUTION SETTINGS:")
	log.Printf("   Max Orders Per Second: %.2f (1 order every %.1fs)",
		config.OrderExecutionSettings.MaxOrdersPerSecond,
		1.0/config.OrderExecutionSettings.MaxOrdersPerSecond)
	log.Printf("   Order Amount Type: %s", config.OrderExecutionSettings.OrderAmountType)

	if config.OrderExecutionSettings.OrderAmountType == "static" {
		log.Printf("   Static Order Amount: $%.2f per order", config.OrderExecutionSettings.StaticOrderAmount)
	} else {
		log.Printf("   Dynamic Order Percentage: %.2f%% of available balance",
			config.OrderExecutionSettings.DynamicOrderPercentage*100)
	}

	log.Printf("   Account Balance: $%.2f", config.OrderExecutionSettings.AccountBalance)
	log.Printf("   Max Daily Spend: $%.2f", config.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   Spending Limits: %s", map[bool]string{true: "ENABLED", false: "DISABLED"}[config.OrderExecutionSettings.EnableSpendingLimits])
	log.Printf("   Min Order Amount: $%.2f", config.OrderExecutionSettings.MinOrderAmount)
	log.Printf("   Max Order Amount: $%.2f", config.OrderExecutionSettings.MaxOrderAmount)

	// Initialize HTTP client with WebSocket support and optimized settings
	httpClient := newHttpClient(config)
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TriangularArbitrageBot/2.0",
		"Connection":   "keep-alive",           // Enable connection reuse
		"Keep-Alive":   "timeout=30, max=1000", // Keep connections alive
	}).setClient(&http.Client{
		Timeout: 10 * time.Second, // Reduced timeout for faster failure detection
		Transport: &http.Transport{
			MaxIdleConns:        100,              // Increase connection pool
			MaxIdleConnsPerHost: 50,               // More connections per host
			IdleConnTimeout:     90 * time.Second, // Keep connections alive longer
			DisableKeepAlives:   false,            // Enable keep-alive
			DisableCompression:  false,            // Enable compression
		},
	})

	// Initialize exchange client
	coinexClient := NewCoinexClient(httpClient, config)

	// Display current order execution state
	concurrent, dailySpent, availableBalance := coinexClient.GetOrderExecutionStats()
	log.Printf("CURRENT ORDER EXECUTION STATE:")
	log.Printf("   Active Concurrent Arbitrage Cycles: %d/%d", concurrent, config.OrderExecutionSettings.MaxConcurrentArbitrages)
	log.Printf("   Daily Spent: $%.2f/$%.2f", dailySpent, config.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   Available Balance: $%.2f", availableBalance)

	if config.OrderExecutionSettings.OrderAmountType == "dynamic" {
		nextOrderValue := availableBalance * config.OrderExecutionSettings.DynamicOrderPercentage
		log.Printf("   Next Order Value: $%.2f (%.1f%% of available)",
			nextOrderValue, config.OrderExecutionSettings.DynamicOrderPercentage*100)
	}

	// Test connection
	log.Println("🔗 Testing connection...")
	connectionInfo, err := coinexClient.TestConnection()
	if err != nil {
		log.Fatalf("❌ Connection test failed: %v", err)
	}
	log.Printf("✅ Connection successful: %s", connectionInfo)

	// Get initial order execution stats
	concurrent, totalSpent, avgOrderValue := coinexClient.GetOrderExecutionStats()
	log.Printf("📊 Initial order stats - Active: %d | Total spent: $%.2f | Avg order: $%.2f",
		concurrent, totalSpent, avgOrderValue)

	// Initialize core components
	marketDepths := NewMarketDepths()
	metrics := NewMetrics()
	arbitrageEngine := NewArbitrageEngine(config, marketDepths, metrics, coinexClient)

	// Set market depths reference in HttpClient for resubscription
	httpClient.SetMarketDepths(marketDepths)

	// STEP 1: Get REST API data FIRST (for all markets in parallel)
	log.Println("🔄 STEP 1: Getting REST API data for ALL markets in parallel...")

	// Get arbitrage markets from REST API
	log.Println("🔄 Getting arbitrage markets from REST API...")
	arbitrageMarkets, completeAssets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		log.Fatalf("Failed to get arbitrage markets: %v", err)
	}
	log.Printf("✅ REST API: Found %d markets for arbitrage (%d complete assets)", len(arbitrageMarkets), len(completeAssets))

	// Get initial order book snapshots for critical markets (USDC/USDT pairs) in parallel
	log.Println("🔄 Getting initial order book snapshots for critical markets in parallel...")
	criticalMarkets := config.CriticalMarkets
	criticalResults := make(chan struct {
		market    string
		orderBook *OrderBook
		err       error
	}, len(criticalMarkets))

	for _, market := range criticalMarkets {
		go func(m string) {
			orderBook, err := httpClient.GetOrderBookREST(m)
			criticalResults <- struct {
				market    string
				orderBook *OrderBook
				err       error
			}{m, orderBook, err}
		}(market)
	}

	// Collect critical market results
	for i := 0; i < len(criticalMarkets); i++ {
		result := <-criticalResults
		if result.err != nil {
			log.Printf("⚠️ Failed to get snapshot for %s: %v", result.market, result.err)
		} else {
			marketDepths.Store(result.market, result.orderBook)
			log.Printf("✅ Stored snapshot for %s: %d bids, %d asks", result.market, len(result.orderBook.Bids), len(result.orderBook.Asks))
		}
	}

	// Get initial order book snapshots for ALL arbitrage markets in parallel
	log.Printf("🔄 Getting initial order book snapshots for ALL %d arbitrage markets in parallel...", len(arbitrageMarkets))

	// Temporarily disable rate limiting for initial data loading
	log.Printf("🔓 Temporarily disabling rate limiting for initial data loading...")
	httpClient.DisableRateLimiting()
	defer httpClient.EnableRateLimiting()

	// Create worker pool for parallel processing
	numWorkers := 20 // Increased back to 20 since we disabled rate limiting
	marketChan := make(chan string, len(arbitrageMarkets))
	results := make(chan struct {
		market    string
		orderBook *OrderBook
		err       error
	}, len(arbitrageMarkets))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for market := range marketChan {
				log.Printf("🔗 Worker %d: Fetching %s", workerID, market)
				orderBook, err := httpClient.GetOrderBookREST(market)
				results <- struct {
					market    string
					orderBook *OrderBook
					err       error
				}{market, orderBook, err}
			}
		}(i)
	}

	// Send all markets to workers
	for _, market := range arbitrageMarkets {
		marketChan <- market
	}
	close(marketChan)

	// Wait for all workers to complete
	log.Printf("⏳ Waiting for all workers to complete...")
	wg.Wait()
	close(results)

	// Collect results
	successfulSnapshots := 0
	failedSnapshots := 0
	for result := range results {
		if result.err != nil {
			log.Printf("⚠️ Failed to get snapshot for %s: %v", result.market, result.err)
			failedSnapshots++
		} else {
			marketDepths.Store(result.market, result.orderBook)
			successfulSnapshots++

			// Log first few markets with order book details
			if successfulSnapshots <= 10 {
				log.Printf("✅ Stored snapshot for %s: %d bids, %d asks", result.market, len(result.orderBook.Bids), len(result.orderBook.Asks))
			}
		}

		// Progress logging every 25 markets
		if (successfulSnapshots+failedSnapshots)%25 == 0 {
			log.Printf("📊 Progress: %d successful, %d failed", successfulSnapshots, failedSnapshots)
		}
	}

	log.Printf("✅ REST API completed: %d/%d markets successfully loaded (%d failed)", successfulSnapshots, len(arbitrageMarkets), failedSnapshots)
	log.Printf("🔒 Re-enabling rate limiting for normal operations...")

	// Log discovered markets for debugging
	log.Printf("DISCOVERED MARKETS:")
	for i, market := range arbitrageMarkets {
		if i < 20 { // Show first 20 markets
			log.Printf("   %s", market)
		}
	}
	if len(arbitrageMarkets) > 20 {
		log.Printf("   ... and %d more markets", len(arbitrageMarkets)-20)
	}

	// Log complete assets
	log.Printf("COMPLETE ASSETS: %v", completeAssets)

	// STEP 2: Connect to WebSocket AFTER REST API is complete
	log.Println("🔗 STEP 2: Connecting to WebSocket...")
	if err := httpClient.ConnectWebSocket(); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	log.Println("✅ WebSocket connected successfully")

	// Test WebSocket connection
	log.Println("🔍 Testing WebSocket connection...")
	if err := httpClient.TestWebSocketConnection(); err != nil {
		log.Printf("WebSocket test failed: %v", err)
		log.Println("Continuing anyway...")
	}

	// Wait for WebSocket connection to be fully stable
	log.Println("⏳ Waiting for WebSocket connection to stabilize...")
	time.Sleep(3 * time.Second)

	// STEP 3: Start market data processor for WebSocket messages
	log.Println("📡 STEP 3: Starting market data processor...")
	messageCounter := uint64(0)
	go func() {
		log.Println("Starting market data processor...")
		dataFeed := httpClient.GetWebSocketDataFeed()
		log.Printf("Data feed channel ready, waiting for messages...")

		for msg := range dataFeed {
			messageCounter++

			// Log first few messages for debugging
			if messageCounter <= 3 {
				log.Printf("Processing message #%d: %s", messageCounter, string(msg)[:Min(200, len(msg))])
			}

			// Process WebSocket message
			if err := processWebSocketMessage(msg, marketDepths, metrics, httpClient); err != nil {
				if messageCounter%1000 == 0 { // Reduced logging frequency to improve performance
					log.Printf("WebSocket message processing error: %v | Message preview: %s",
						err, string(msg)[:Min(200, len(msg))])
				}
				continue
			}

			// Log processing stats periodically
			if messageCounter%5000 == 0 || messageCounter <= 5 { // Reduced logging frequency
				log.Printf("Processed %d WebSocket messages successfully", messageCounter)
			}
		}
		log.Printf("Market data processor exited - data feed channel closed")
	}()

	// STEP 4: Subscribe to all markets via WebSocket (since we already have all REST data)
	log.Println("📡 STEP 4: Subscribing to all markets via WebSocket...")
	allMarkets := make([]string, 0, len(arbitrageMarkets)+len(criticalMarkets))
	allMarkets = append(allMarkets, arbitrageMarkets...)
	allMarkets = append(allMarkets, criticalMarkets...)

	log.Printf("Subscribing to %d markets via WebSocket for real-time updates", len(allMarkets))
	if err := httpClient.SubscribeWebSocketWithFullFlow(allMarkets, marketDepths); err != nil {
		log.Printf("❌ WebSocket subscription failed: %v", err)
		log.Fatal("Cannot proceed without WebSocket subscriptions")
	}
	log.Printf("✅ WebSocket subscription completed for %d markets", len(allMarkets))

	log.Printf("Final active markets count: %d", len(allMarkets))
	arbitrageEngine.UpdateTriangularPaths(allMarkets, completeAssets)

	// Wait for initial market data to arrive
	log.Println("⏳ Waiting for initial market data...")
	time.Sleep(3 * time.Second) // Wait for data to arrive

	// Check available markets
	availableMarkets := marketDepths.GetAvailableMarkets()
	log.Printf("Available markets: %d", len(availableMarkets))
	if len(availableMarkets) > 0 {
		log.Printf("Sample markets: %v", availableMarkets[:Min(5, len(availableMarkets))])
	}

	// Start arbitrage engine
	log.Println("Starting arbitrage engine...")
	arbitrageEngine.Start()

	// Start simple periodic metrics logging
	metricsStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-metricsStopChan:
				log.Printf("🛑 Metrics logging stopped")
				return
			case <-ticker.C:
				logBasicMetrics(metrics, marketDepths)
			}
		}
	}()

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Triangular Arbitrage Bot is running!")
	log.Printf("Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-quit
	log.Println("🛑 Shutdown signal received, starting graceful shutdown...")

	// Step 1: Stop arbitrage engine
	log.Println("🛑 Step 1: Stopping arbitrage engine...")
	arbitrageEngine.Stop()

	// Step 2: Stop metrics logging
	log.Println("🛑 Step 2: Stopping metrics logging...")
	close(metricsStopChan)

	// Step 3: Close WebSocket connection
	log.Println("🛑 Step 3: Closing WebSocket connection...")
	if err := httpClient.CloseWebSocket(); err != nil {
		log.Printf("❌ Error closing WebSocket: %v", err)
	}

	// Step 4: Final metrics log
	log.Println("📊 Final metrics:")
	logSimpleMetrics(metrics, marketDepths)

	log.Println("🛑 Triangular Arbitrage Bot stopped")
}
