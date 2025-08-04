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
	log.Printf("   Max Concurrent Arbitrage Cycles: %d", config.OrderExecutionSettings.MaxConcurrentArbitrages)
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

	// Initialize HTTP client with WebSocket support
	httpClient := newHttpClient(config)
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TriangularArbitrageBot/2.0",
	}).setClient(&http.Client{
		Timeout: 30 * time.Second,
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

	// Reset concurrent orders counter to ensure clean state
	log.Println("🔄 Resetting concurrent orders counter to ensure clean state...")
	coinexClient.resetConcurrentOrders()
	coinexClient.syncConcurrentOrders()

	// Get initial order execution stats
	concurrent, totalSpent, avgOrderValue := coinexClient.GetOrderExecutionStats()
	log.Printf("📊 Initial order stats - Active: %d/%d | Total spent: $%.2f | Avg order: $%.2f",
		concurrent, config.OrderExecutionSettings.MaxConcurrentArbitrages, totalSpent, avgOrderValue)

	// Debug: Check and sync concurrent orders state
	log.Println("🔍 Checking concurrent orders state...")
	concurrent, _, _ = coinexClient.GetOrderExecutionStats()
	debugActiveTrackers := coinexClient.GetActiveTrackers()
	log.Printf("🔍 CONCURRENT ORDERS DEBUG | Counter: %d | Active trackers: %d", concurrent, len(debugActiveTrackers))

	if concurrent != len(debugActiveTrackers) {
		log.Printf("⚠️ MISMATCH DETECTED | Syncing concurrent orders counter...")
		coinexClient.syncConcurrentOrders()
		concurrent, _, _ = coinexClient.GetOrderExecutionStats()
		log.Printf("✅ SYNCED | Counter: %d | Active trackers: %d", concurrent, len(debugActiveTrackers))
	} else {
		log.Printf("✅ CONCURRENT ORDERS SYNCED | Counter: %d | Active trackers: %d", concurrent, len(debugActiveTrackers))
	}

	// Initialize core components
	marketDepths := NewMarketDepths()
	metrics := NewMetrics()
	arbitrageEngine := NewArbitrageEngine(config, marketDepths, metrics, coinexClient)

	// Set market depths reference in HttpClient for resubscription
	httpClient.SetMarketDepths(marketDepths)

	// Connect to WebSocket using integrated HttpClient
	log.Println("Connecting to WebSocket...")
	if err := httpClient.ConnectWebSocket(); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	log.Println("WebSocket connected successfully")

	// Start retry queue processor for failed WebSocket markets
	httpClient.StartRetryQueueProcessor(marketDepths)
	log.Println("Retry queue processor started")

	// Test WebSocket connection with a single subscription
	log.Println("Testing WebSocket connection...")
	if err := httpClient.TestWebSocketConnection(); err != nil {
		log.Printf("WebSocket test failed: %v", err)
		log.Println("Continuing anyway...")
	}

	// Wait for WebSocket connection to be fully stable
	log.Println("Waiting for WebSocket connection to stabilize...")
	time.Sleep(5 * time.Second)

	// Get markets suitable for triangular arbitrage
	log.Println("Discovering arbitrage markets...")
	arbitrageMarkets, completeAssets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		log.Fatalf("Failed to get arbitrage markets: %v", err)
	}

	if len(arbitrageMarkets) == 0 {
		log.Fatal("No suitable markets found for triangular arbitrage")
	}

	log.Printf("Found %d markets for arbitrage (%d complete assets)", len(arbitrageMarkets), len(completeAssets))

	// Log all discovered markets for debugging
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

	// Subscribe to all markets IMMEDIATELY after discovering them
	log.Printf("📡 Subscribing to %d markets immediately after discovery with full flow...", len(arbitrageMarkets))
	if err := httpClient.SubscribeWebSocketWithFullFlow(arbitrageMarkets, marketDepths); err != nil {
		log.Printf("❌ Immediate subscription failed: %v", err)
		log.Fatal("Cannot proceed without market subscriptions")
	}
	log.Printf("✅ Immediate subscription completed for %d markets", len(arbitrageMarkets))

	// Start market data processor BEFORE subscriptions
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

	// Wait for initial market data and verify it's flowing with proper synchronization
	log.Println("Waiting for initial market data...")

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
				log.Printf("Timeout waiting for initial market data")
				marketDataReady <- false
				return
			case <-ticker.C:
				initialMarkets := marketDepths.GetAvailableMarkets()
				log.Printf("MARKET DATA CHECK: %d markets available", len(initialMarkets))

				// Check if we have the critical USDCUSDT pair and at least 2 other markets
				// For critical markets, we assume they exist - only check other markets
				nonCriticalMarkets := 0
				for _, market := range initialMarkets {
					if !coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(market) {
						nonCriticalMarkets++
					}
				}

				if nonCriticalMarkets >= 2 { // Reduced from 3 to 2, critical markets assumed to exist
					log.Printf("Initial market data ready with %d non-critical markets (critical markets assumed to exist)", nonCriticalMarkets)
					log.Printf("Market data flowing: %v", initialMarkets[:Min(10, len(initialMarkets))])
					marketDataReady <- true
					return
				} else {
					log.Printf("Have %d non-critical markets but need at least 2", nonCriticalMarkets)
				}
			}
		}
	}()

	// Wait for market data to be ready
	wg.Wait()
	ready := <-marketDataReady
	if !ready {
		log.Printf("WARNING: Proceeding without sufficient market data...")
		retryMarkets := marketDepths.GetAvailableMarkets()
		log.Printf("   Available markets: %d", len(retryMarkets))
		if len(retryMarkets) > 0 {
			log.Printf("   Available: %v", retryMarkets[:Min(5, len(retryMarkets))])
		}
	}

	// Wait for initial WebSocket data to arrive before checking market data
	log.Printf("Waiting 300ms for initial WebSocket data to arrive...")
	time.Sleep(300 * time.Millisecond)

	// Update triangular paths in the arbitrage engine
	arbitrageEngine.UpdateTriangularPaths(arbitrageMarkets, completeAssets)

	// Enhanced WebSocket data validation and fallback
	log.Printf("Validating WebSocket data health...")

	// Wait a bit more for WebSocket data to stabilize
	time.Sleep(5 * time.Second)

	// Validate actual data delivery vs subscriptions
	activeWsMarkets, deadWsMarkets := httpClient.ValidateWebSocketDataHealth(arbitrageMarkets)

	log.Printf("WEBSOCKET DATA HEALTH:")
	log.Printf("   Subscribed markets: %d", len(arbitrageMarkets))
	log.Printf("   Delivering data: %d (%.1f%%)", len(activeWsMarkets), float64(len(activeWsMarkets))/float64(len(arbitrageMarkets))*100)
	log.Printf("   Dead subscriptions: %d", len(deadWsMarkets))

	if len(deadWsMarkets) > 0 {
		log.Printf("   Dead markets (first 10): %v", deadWsMarkets[:Min(10, len(deadWsMarkets))])
	}

	// Define critical markets that MUST have WebSocket data
	criticalMarkets := config.CriticalMarkets

	// Subscribe to critical markets via WebSocket
	log.Printf("CRITICAL MARKETS: %v (subscribing via WebSocket)", criticalMarkets)
	log.Printf("   All critical markets must have WebSocket data - no REST fallback")

	// Subscribe to critical markets via WebSocket
	coinexClient.FetchCriticalMarketsViaWebSocket(criticalMarkets, marketDepths)

	// Combine all markets for final arbitrage markets
	allActiveMarkets := make([]string, 0, len(arbitrageMarkets)+len(criticalMarkets))
	allActiveMarkets = append(allActiveMarkets, arbitrageMarkets...)

	// Add critical markets to active list (subscribed via WebSocket)
	for _, critical := range criticalMarkets {
		allActiveMarkets = append(allActiveMarkets, critical)
		log.Printf("Added critical market to active list: %s (subscribed via WebSocket)", critical)
	}

	log.Printf("Final active markets count: %d (WebSocket only: %d)",
		len(allActiveMarkets), len(allActiveMarkets))

	arbitrageEngine.UpdateTriangularPaths(allActiveMarkets, completeAssets)

	// Start arbitrage engine
	log.Println("Starting arbitrage engine...")
	arbitrageEngine.Start()

	// Start simple periodic metrics logging
	metricsStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		priceTicker := time.NewTicker(2 * time.Minute)          // Log prices less frequently
		debugTicker := time.NewTicker(15 * time.Second)         // Debug market data status frequently
		criticalDataTicker := time.NewTicker(10 * time.Second)  // Fetch critical data every 10 seconds
		dataValidationTicker := time.NewTicker(1 * time.Minute) // Validate data accuracy every minute
		wsHealthTicker := time.NewTicker(30 * time.Second)      // Monitor WebSocket health
		defer ticker.Stop()
		defer priceTicker.Stop()
		defer debugTicker.Stop()
		defer criticalDataTicker.Stop()
		defer dataValidationTicker.Stop()
		defer wsHealthTicker.Stop()

		lastMessageCount := int64(0)
		lastHealthCheck := time.Now()

		for {
			select {
			case <-metricsStopChan:
				log.Printf("🛑 Metrics logging stopped")
				return
			case <-ticker.C:
				logBasicMetrics(metrics, marketDepths)
			case <-priceTicker.C:
				logMarketPrices(marketDepths)
			case <-debugTicker.C:
				logMarketDataStatus(marketDepths, arbitrageMarkets)

				// Log WebSocket connection status
				if httpClient.IsWebSocketConnected() {
					log.Printf("WebSocket Status: Connected")
				} else {
					log.Printf("WebSocket Status: Disconnected")
				}
			case <-criticalDataTicker.C:
				// Monitor critical markets via WebSocket
				log.Printf("Monitoring critical markets via WebSocket: %v", config.CriticalMarkets)
				// Check if critical markets have WebSocket data
				for _, critical := range config.CriticalMarkets {
					if orderBook, exists := marketDepths.Load(critical); exists && orderBook != nil {
						if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
							log.Printf("✅ Critical market %s has WebSocket data | Bids: %d | Asks: %d",
								critical, len(orderBook.Bids), len(orderBook.Asks))
						} else {
							log.Printf("⚠️ Critical market %s has empty order book", critical)
							// Add to retry queue
							// httpClient.AddFailedMarket(critical) // COMMENTED OUT: No retry queue
						}
					} else {
						log.Printf("❌ Critical market %s has no WebSocket data", critical)
						// Add to retry queue
						// httpClient.AddFailedMarket(critical) // COMMENTED OUT: No retry queue
					}
				}
			case <-dataValidationTicker.C:
				// Validate WebSocket data accuracy against REST API
				httpClient.ValidateWebSocketDataAccuracy(arbitrageMarkets)
			case <-wsHealthTicker.C:
				// Monitor WebSocket data flow
				currentMessageCount := metrics.GetSnapshot().MessagesProcessed
				if currentMessageCount == lastMessageCount && time.Since(lastHealthCheck) > 2*time.Minute {
					log.Printf(" WARNING: No WebSocket messages received in last 2 minutes!")
					log.Printf("   Last message count: %d | Current: %d", lastMessageCount, currentMessageCount)
					log.Printf("   WebSocket connected: %v", httpClient.IsWebSocketConnected())
				}
				lastMessageCount = currentMessageCount
				lastHealthCheck = time.Now()

				// Periodic concurrent orders health check
				concurrent, _, _ := coinexClient.GetOrderExecutionStats()
				activeTrackers := coinexClient.GetActiveTrackers()
				if concurrent != len(activeTrackers) {
					log.Printf("⚠️ CONCURRENT ORDERS MISMATCH | Counter: %d | Active trackers: %d | Auto-syncing...",
						concurrent, len(activeTrackers))
					coinexClient.syncConcurrentOrders()
				}

				// Check if counter is stuck at maximum limit
				if concurrent >= config.OrderExecutionSettings.MaxConcurrentArbitrages && len(activeTrackers) == 0 {
					log.Printf("🚨 CONCURRENT ORDERS STUCK | Counter: %d/%d | Active trackers: 0 | Resetting counter...",
						concurrent, config.OrderExecutionSettings.MaxConcurrentArbitrages)
					coinexClient.resetConcurrentOrders()
				}
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

	// Graceful shutdown with proper timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 1: Stop arbitrage engine FIRST to prevent new opportunities from being queued
	log.Println("🛑 Step 1: Stopping arbitrage engine...")
	arbitrageEngine.Stop()

	// Step 2: Stop WebSocket data processing to prevent new market data updates
	log.Println("🛑 Step 2: Stopping WebSocket data processing...")
	httpClient.StopWebSocketProcessing()

	// Step 2.5: Stop metrics logging to prevent continued output
	log.Println("🛑 Step 2.5: Stopping metrics logging...")
	close(metricsStopChan)

	// Step 3: Wait for active FOK orders to complete their FULL 3-leg cycles
	log.Println("🛑 Step 3: Waiting for ALL active FOK orders to complete...")
	activeTrackers := coinexClient.GetActiveTrackers()

	// Check for any open orders on the exchange that might not be tracked
	exchangeOrders, err := coinexClient.GetOpenOrders()
	if err != nil {
		log.Printf("⚠️ Cannot check exchange orders: %v", err)
		exchangeOrders = []map[string]interface{}{} // Empty slice to avoid nil
	}

	totalActiveOrders := len(activeTrackers) + len(exchangeOrders)

	if totalActiveOrders > 0 {
		log.Printf("🔄 %d tracked orders + %d exchange orders = %d total active orders - waiting for completion...",
			len(activeTrackers), len(exchangeOrders), totalActiveOrders)

		// Wait up to 15 seconds for orders to complete
		orderWaitCtx, orderCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer orderCancel()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-orderWaitCtx.Done():
				log.Printf("⏰ Timeout waiting for orders - forcing cancellation")
				// Cancel all remaining orders
				coinexClient.CancelAllActiveOrders()
				break
			case <-ticker.C:
				remainingTrackers := coinexClient.GetActiveTrackers()
				remainingExchangeOrders, _ := coinexClient.GetOpenOrders()
				totalRemaining := len(remainingTrackers) + len(remainingExchangeOrders)

				if totalRemaining == 0 {
					log.Println("✅ All orders completed successfully")
					break
				}
				log.Printf("⏳ Still waiting for %d tracked + %d exchange = %d total orders...",
					len(remainingTrackers), len(remainingExchangeOrders), totalRemaining)
			}
		}
	} else {
		log.Println("✅ No active orders to wait for")
	}

	// Step 3.5: Reset concurrent orders if needed
	log.Println("🛑 Step 3.5: Checking and resetting concurrent orders state...")
	resetConcurrentOrdersIfNeeded(coinexClient)

	// Step 4: Final check for any remaining orders and force cancellation
	finalActiveTrackers := coinexClient.GetActiveTrackers()
	finalExchangeOrders, _ := coinexClient.GetOpenOrders()
	totalFinalOrders := len(finalActiveTrackers) + len(finalExchangeOrders)

	if totalFinalOrders > 0 {
		log.Printf("⚠️ %d tracked + %d exchange = %d total orders still active after engine stop - forcing cancellation",
			len(finalActiveTrackers), len(finalExchangeOrders), totalFinalOrders)

		// Force cancel all remaining tracked orders
		coinexClient.CancelAllActiveOrders()

		// Cancel all exchange orders by market
		marketsToCancel := make(map[string]bool)
		for _, order := range finalExchangeOrders {
			if market, ok := order["market"].(string); ok {
				marketsToCancel[market] = true
			}
		}

		// Cancel all orders for each market
		for market := range marketsToCancel {
			if err := coinexClient.CancelAllOrders(market); err != nil {
				log.Printf("❌ Failed to cancel all orders for market %s: %v", market, err)
			} else {
				log.Printf("✅ Cancelled all orders for market %s", market)
			}
		}

		// Wait up to 5 seconds for cancellation to complete
		remainingWaitCtx, remainingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer remainingCancel()

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-remainingWaitCtx.Done():
				finalRemainingTrackers := coinexClient.GetActiveTrackers()
				finalRemainingExchange, _ := coinexClient.GetOpenOrders()
				totalRemaining := len(finalRemainingTrackers) + len(finalRemainingExchange)
				log.Printf("⏰ Timeout waiting for %d remaining orders - forcing shutdown", totalRemaining)
				goto ClosingProgram
			case <-ticker.C:
				remainingTrackers := coinexClient.GetActiveTrackers()
				remainingExchange, _ := coinexClient.GetOpenOrders()
				totalRemaining := len(remainingTrackers) + len(remainingExchange)

				if totalRemaining == 0 {
					log.Println("✅ All remaining orders cancelled")
					break
				}
				log.Printf("⏳ Still waiting for %d tracked + %d exchange = %d total orders to cancel...",
					len(remainingTrackers), len(remainingExchange), totalRemaining)
			}
		}
	ClosingProgram:
	}

	// Step 5: Close WebSocket connection
	log.Println("🛑 Step 4: Closing WebSocket connection...")
	if err := httpClient.CloseWebSocket(); err != nil {
		log.Printf("❌ Error closing WebSocket: %v", err)
	}

	// Step 6: Final metrics log
	log.Println("📊 Final metrics:")
	logSimpleMetrics(metrics, marketDepths)

	// Step 7: Wait for shutdown or timeout
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
		log.Println("⏰ Shutdown timeout exceeded")
	}

	log.Println("🛑 Triangular Arbitrage Bot stopped")
}

// resetConcurrentOrdersIfNeeded resets the concurrent orders counter if there's a mismatch
func resetConcurrentOrdersIfNeeded(coinexClient *coinexClient) {
	concurrent, _, _ := coinexClient.GetOrderExecutionStats()
	activeTrackers := coinexClient.GetActiveTrackers()

	if concurrent != len(activeTrackers) {
		log.Printf("⚠️ CONCURRENT ORDERS MISMATCH | Counter: %d | Active trackers: %d | Resetting...",
			concurrent, len(activeTrackers))
		coinexClient.resetConcurrentOrders()
		coinexClient.syncConcurrentOrders()
		concurrent, _, _ = coinexClient.GetOrderExecutionStats()
		log.Printf("✅ CONCURRENT ORDERS RESET | Counter: %d | Active trackers: %d", concurrent, len(activeTrackers))
	}
}
