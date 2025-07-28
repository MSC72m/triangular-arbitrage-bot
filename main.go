package main

import (
	"context"
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

	log.Println("🔺 Starting Triangular Arbitrage Bot...")

	// Load configuration
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Loaded configuration: API Key length=%d, Quote currencies=%v, Profit threshold=%.6f%%",
		len(config.APIKey), config.QuoteCurrencies, config.ProfitThreshold*100)

	if config.SimulationMode {
		log.Println("⚠️  SIMULATION MODE ENABLED - No real trades will be executed")
		log.Println("   📝 FOK orders will be simulated")
		log.Println("   🔧 Enable real trading by setting simulationMode: false in config.json")
	} else {
		log.Println("🚀 REAL TRADING MODE ENABLED - FOK orders will use CoinEx API")
		log.Printf("   ⚡ FOK Polling Frequency: %.1fHz (every %.0fms)",
			config.FOKOrderSettings.PollingFrequencyHz,
			1000/config.FOKOrderSettings.PollingFrequencyHz)
		log.Printf("   ⏰ FOK Order Timeout: %ds", config.FOKOrderSettings.OrderTimeoutSeconds)
		log.Printf("   🔄 Max Retry Attempts: %d", config.FOKOrderSettings.MaxRetryAttempts)
		log.Println("   ⚠️  REAL MONEY WILL BE USED!")
	}

	// Display order execution settings
	log.Printf("💰 ORDER EXECUTION SETTINGS:")
	log.Printf("   📊 Max Orders Per Second: %.2f (1 order every %.1fs)",
		config.OrderExecutionSettings.MaxOrdersPerSecond,
		1.0/config.OrderExecutionSettings.MaxOrdersPerSecond)
	log.Printf("   🔢 Max Concurrent Orders: %d", config.OrderExecutionSettings.MaxConcurrentOrders)
	log.Printf("   💵 Order Amount Type: %s", config.OrderExecutionSettings.OrderAmountType)

	if config.OrderExecutionSettings.OrderAmountType == "static" {
		log.Printf("   💲 Static Order Amount: $%.2f per order", config.OrderExecutionSettings.StaticOrderAmount)
	} else {
		log.Printf("   📈 Dynamic Order Percentage: %.2f%% of available balance",
			config.OrderExecutionSettings.DynamicOrderPercentage*100)
	}

	log.Printf("   🏦 Account Balance: $%.2f", config.OrderExecutionSettings.AccountBalance)
	log.Printf("   💸 Max Daily Spend: $%.2f", config.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   🛡️  Spending Limits: %s", map[bool]string{true: "ENABLED", false: "DISABLED"}[config.OrderExecutionSettings.EnableSpendingLimits])
	log.Printf("   ⬇️  Min Order Amount: $%.2f", config.OrderExecutionSettings.MinOrderAmount)
	log.Printf("   ⬆️  Max Order Amount: $%.2f", config.OrderExecutionSettings.MaxOrderAmount)

	// Initialize HTTP client with WebSocket support
	httpClient := newHttpClient(config)
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TriangularArbitrageBot/2.0",
	}).setClient(&http.Client{
		Timeout: 30 * time.Second,
	}).SetWebSocketUrl("socket.coinex.com")

	// Initialize exchange client
	coinexClient := NewCoinexClient(httpClient, config)

	// Display current order execution state
	concurrent, dailySpent, availableBalance := coinexClient.GetOrderExecutionStats()
	log.Printf("💳 CURRENT ORDER EXECUTION STATE:")
	log.Printf("   🔢 Active Concurrent Orders: %d/%d", concurrent, config.OrderExecutionSettings.MaxConcurrentOrders)
	log.Printf("   💸 Daily Spent: $%.2f/$%.2f", dailySpent, config.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   💰 Available Balance: $%.2f", availableBalance)

	if config.OrderExecutionSettings.OrderAmountType == "dynamic" {
		nextOrderValue := availableBalance * config.OrderExecutionSettings.DynamicOrderPercentage
		log.Printf("   📊 Next Order Value: $%.2f (%.1f%% of available)",
			nextOrderValue, config.OrderExecutionSettings.DynamicOrderPercentage*100)
	}

	// Test connection
	log.Println("🔗 Testing exchange connection...")
	testResponse, err := coinexClient.TestConnection()
	if err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}
	log.Printf("✅ Connection test successful: %s", testResponse[:Min(100, len(testResponse))])

	// Initialize core components
	marketDepths := NewMarketDepths()
	metrics := NewMetrics()
	arbitrageEngine := NewArbitrageEngine(config, marketDepths, metrics, coinexClient)

	// Connect to WebSocket using integrated HttpClient
	log.Println("🔌 Connecting to WebSocket...")
	if err := httpClient.ConnectWebSocket(); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	log.Println("✅ WebSocket connected successfully")

	// Get markets suitable for triangular arbitrage
	log.Println("🔍 Discovering arbitrage markets...")
	arbitrageMarkets, completeAssets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		log.Fatalf("Failed to get arbitrage markets: %v", err)
	}

	if len(arbitrageMarkets) == 0 {
		log.Fatal("❌ No suitable markets found for triangular arbitrage")
	}

	log.Printf("✅ Found %d markets for arbitrage (%d complete assets)", len(arbitrageMarkets), len(completeAssets))

	// Log all discovered markets for debugging
	log.Printf("🔍 DISCOVERED MARKETS:")
	for i, market := range arbitrageMarkets {
		if i < 20 { // Show first 20 markets
			log.Printf("   📊 %s", market)
		}
	}
	if len(arbitrageMarkets) > 20 {
		log.Printf("   ... and %d more markets", len(arbitrageMarkets)-20)
	}

	// Log complete assets
	log.Printf("🎯 COMPLETE ASSETS: %v", completeAssets)

	// Start market data processor BEFORE subscriptions
	messageCounter := uint64(0)
	go func() {
		log.Println("🔄 Starting market data processor...")
		dataFeed := httpClient.GetWebSocketDataFeed()
		log.Printf("📡 Data feed channel ready, waiting for messages...")

		for msg := range dataFeed {
			messageCounter++

			// Log first few messages for debugging
			if messageCounter <= 5 {
				log.Printf("📨 Processing message #%d: %s", messageCounter, string(msg)[:Min(200, len(msg))])
			}

			// Process WebSocket message
			if err := processWebSocketMessage(msg, marketDepths, metrics, httpClient); err != nil {
				if messageCounter%100 == 0 { // Log errors more frequently for debugging
					log.Printf("⚠️  WebSocket message processing error: %v | Message preview: %s",
						err, string(msg)[:Min(200, len(msg))])
				}
				continue
			}

			// Log processing stats periodically
			if messageCounter%1000 == 0 || messageCounter <= 10 {
				log.Printf("📈 Processed %d WebSocket messages successfully", messageCounter)
			}
		}
		log.Printf("⚠️  Market data processor exited - data feed channel closed")
	}()

	// Subscribe to markets with progressive batching approach (prevents WebSocket overload)
	log.Printf("📡 Subscribing to %d markets with progressive batching...", len(arbitrageMarkets))

	successfulSubscriptions, failedSubscriptions := httpClient.SubscribeWebSocketProgressive(arbitrageMarkets, 10)

	log.Printf("📊 PROGRESSIVE Subscription Results:")
	log.Printf("   ✅ Working with data: %d", len(successfulSubscriptions))
	log.Printf("   ❌ Failed/dead: %d", len(failedSubscriptions))
	log.Printf("   📈 Success rate: %.1f%%", float64(len(successfulSubscriptions))/float64(len(arbitrageMarkets))*100)

	if len(failedSubscriptions) > 0 {
		log.Printf("   🚫 Failed markets (first 10): %v", failedSubscriptions[:Min(10, len(failedSubscriptions))])
	}

	// Update arbitrageMarkets to only include successfully subscribed markets
	arbitrageMarkets = successfulSubscriptions

	if len(arbitrageMarkets) == 0 {
		log.Fatal("❌ No markets successfully subscribed - cannot proceed")
	}

	log.Printf("✅ Proceeding with %d working subscriptions", len(arbitrageMarkets))

	// Wait for initial market data and verify it's flowing with proper synchronization
	log.Println("⏳ Waiting for initial market data...")

	var wg sync.WaitGroup
	marketDataReady := make(chan bool, 1)

	// Start goroutine to monitor market data availability
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		timeout := time.After(30 * time.Second) // 30 second timeout

		for {
			select {
			case <-timeout:
				log.Printf("⚠️  Timeout waiting for initial market data")
				marketDataReady <- false
				return
			case <-ticker.C:
				initialMarkets := marketDepths.GetAvailableMarkets()
				log.Printf("🔍 MARKET DATA CHECK: %d markets available", len(initialMarkets))

				// Check if we have the critical USDCUSDT pair and at least 2 other markets
				hasUSDCUSDT := false
				for _, market := range initialMarkets {
					if market == "USDCUSDT" || market == "USDTUSDC" {
						hasUSDCUSDT = true
						break
					}
				}

				if len(initialMarkets) >= 3 && hasUSDCUSDT { // Reduced from 5 to 3, but require USDC pair
					log.Printf("✅ Initial market data ready with %d markets (including USDC/USDT pair)", len(initialMarkets))
					log.Printf("✅ Market data flowing: %v", initialMarkets[:Min(10, len(initialMarkets))])
					marketDataReady <- true
					return
				} else if len(initialMarkets) >= 3 {
					log.Printf("⚠️  Have %d markets but missing critical USDC/USDT pair", len(initialMarkets))
				}
			}
		}
	}()

	// Wait for market data to be ready
	wg.Wait()
	ready := <-marketDataReady
	if !ready {
		log.Printf("❌ WARNING: Proceeding without sufficient market data...")
		retryMarkets := marketDepths.GetAvailableMarkets()
		log.Printf("   📊 Available markets: %d", len(retryMarkets))
		if len(retryMarkets) > 0 {
			log.Printf("   📈 Available: %v", retryMarkets[:Min(5, len(retryMarkets))])
		}
	}

	// Update triangular paths in the arbitrage engine
	arbitrageEngine.UpdateTriangularPaths(arbitrageMarkets, completeAssets)

	// Enhanced WebSocket data validation and fallback
	log.Printf("🔍 Validating WebSocket data health...")

	// Wait a bit more for WebSocket data to stabilize
	time.Sleep(5 * time.Second)

	// Validate actual data delivery vs subscriptions
	activeWsMarkets, deadWsMarkets := httpClient.ValidateWebSocketDataHealth(arbitrageMarkets)

	log.Printf("📊 WEBSOCKET DATA HEALTH:")
	log.Printf("   📡 Subscribed markets: %d", len(arbitrageMarkets))
	log.Printf("   ✅ Delivering data: %d (%.1f%%)", len(activeWsMarkets), float64(len(activeWsMarkets))/float64(len(arbitrageMarkets))*100)
	log.Printf("   ❌ Dead subscriptions: %d", len(deadWsMarkets))

	if len(deadWsMarkets) > 0 {
		log.Printf("   🚫 Dead markets (first 10): %v", deadWsMarkets[:Min(10, len(deadWsMarkets))])
	}

	// Define critical markets that MUST have data - only quote currency variations
	criticalMarkets := config.CriticalMarkets

	// Check which critical markets are missing WebSocket data
	criticalMissing := []string{}

	for _, critical := range criticalMarkets {
		// Check if market has data in marketDepths (from WebSocket)
		if orderBook, exists := marketDepths.Load(critical); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				// Market has WebSocket data, skip
				continue
			}
		}
		// Market missing data, add to REST fallback list
		criticalMissing = append(criticalMissing, critical)
	}

	if len(criticalMissing) > 0 {
		log.Printf("🚨 CRITICAL MARKETS MISSING WEBSOCKET DATA: %v", criticalMissing)
		log.Printf("🔄 Using REST API fallback for missing critical markets...")

		// Use REST API fallback for critical markets missing WebSocket data
		coinexClient.EnsureCriticalMarketData(criticalMissing, marketDepths)

		// Re-validate after fallback
		activeWsMarkets, deadWsMarkets = httpClient.ValidateWebSocketDataHealth(arbitrageMarkets)
		log.Printf("🔄 After REST fallback - Active: %d, Dead: %d", len(activeWsMarkets), len(deadWsMarkets))
	} else {
		log.Printf("✅ All critical markets have WebSocket data - no REST fallback needed")
	}

	// Update triangular paths with only markets that actually have data
	log.Printf("🔄 Updating arbitrage engine with validated markets...")

	// Combine WebSocket active markets with critical markets that have REST fallback data
	allActiveMarkets := append([]string{}, activeWsMarkets...)

	// Add critical markets that have REST fallback data
	for _, critical := range criticalMarkets {
		// Check if critical market now has data (either WebSocket or REST)
		if orderBook, exists := marketDepths.Load(critical); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				// Add to active markets if not already present
				found := false
				for _, active := range allActiveMarkets {
					if active == critical {
						found = true
						break
					}
				}
				if !found {
					allActiveMarkets = append(allActiveMarkets, critical)
					log.Printf("✅ Added critical market to active list: %s (via REST fallback)", critical)
				}
			}
		}
	}

	log.Printf("📊 Final active markets count: %d (WebSocket: %d + REST fallback: %d)",
		len(allActiveMarkets), len(activeWsMarkets), len(allActiveMarkets)-len(activeWsMarkets))

	arbitrageEngine.UpdateTriangularPaths(allActiveMarkets, completeAssets)

	// Start arbitrage engine
	log.Println("🚀 Starting arbitrage engine...")
	arbitrageEngine.Start()

	// Start simple periodic metrics logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		priceTicker := time.NewTicker(2 * time.Minute)         // Log prices less frequently
		debugTicker := time.NewTicker(15 * time.Second)        // Debug market data status frequently
		criticalDataTicker := time.NewTicker(10 * time.Second) // Fetch critical data every 10 seconds
		defer ticker.Stop()
		defer priceTicker.Stop()
		defer debugTicker.Stop()
		defer criticalDataTicker.Stop()

		for {
			select {
			case <-ticker.C:
				logBasicMetrics(metrics, marketDepths)
			case <-priceTicker.C:
				logMarketPrices(marketDepths)
			case <-debugTicker.C:
				logMarketDataStatus(marketDepths, arbitrageMarkets)

				// Log WebSocket connection status
				if httpClient.IsWebSocketConnected() {
					log.Printf("🔗 WebSocket Status: Connected ✅")
				} else {
					log.Printf("🔗 WebSocket Status: Disconnected ❌")
				}
			case <-criticalDataTicker.C:
				// Check if critical markets have data, use REST only if WebSocket data is missing
				// REST API is used as fallback, not primary data source
				log.Printf("🔍 Checking critical markets data availability...")
			}
		}
	}()

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("✅ Triangular Arbitrage Bot is running!")
	log.Printf("🛑 Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-quit
	log.Println("🛑 Shutdown signal received, stopping bot...")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop arbitrage engine
	log.Println("⏹️  Stopping arbitrage engine...")
	arbitrageEngine.Stop()

	// Wait for active FOK orders to complete their FULL 3-leg cycles
	log.Println("⏳ Waiting for ALL active FOK orders to complete...")
	activeTrackers := coinexClient.GetActiveTrackers()

	if len(activeTrackers) > 0 {
		log.Printf("📊 Found %d active FOK orders - waiting for COMPLETE execution...", len(activeTrackers))

		// Wait indefinitely for ALL orders to complete (no timeout pressure)
		// Each FOK order has max 2s timeout, but we want full 3-leg cycle completion
		waitStart := time.Now()

		for {
			activeTrackers = coinexClient.GetActiveTrackers()
			if len(activeTrackers) == 0 {
				log.Printf("✅ ALL FOK orders completed successfully in %v", time.Since(waitStart))
				break
			}

			// Log every 2 seconds what we're waiting for
			if int(time.Since(waitStart).Seconds())%2 == 0 {
				log.Printf("⏳ Still waiting for %d FOK orders to complete... (%v elapsed)",
					len(activeTrackers), time.Since(waitStart))

				// Show which orders we're waiting for
				for i, tracker := range activeTrackers {
					if i < 3 { // Show first 3
						log.Printf("   📋 Order %d: %s | Market: %s | Age: %v",
							i+1, tracker.OrderID, tracker.Market, time.Since(tracker.CreatedAt))
					}
				}
				if len(activeTrackers) > 3 {
					log.Printf("   ... and %d more orders", len(activeTrackers)-3)
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	} else {
		log.Println("✅ No active FOK orders to wait for")
	}

	// Close WebSocket connection
	log.Println("📪 Closing WebSocket connection...")
	if err := httpClient.CloseWebSocket(); err != nil {
		log.Printf("⚠️  Error closing WebSocket: %v", err)
	}

	// Final metrics log
	log.Println("📊 Final metrics:")
	logSimpleMetrics(metrics, marketDepths)

	// Wait for shutdown or timeout
	done := make(chan struct{})
	go func() {
		// Simulate cleanup completion
		time.Sleep(1 * time.Second)
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ Graceful shutdown completed")
	case <-shutdownCtx.Done():
		log.Println("⚠️  Shutdown timeout exceeded")
	}

	log.Println("👋 Triangular Arbitrage Bot stopped")
}
