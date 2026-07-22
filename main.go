package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/internal/exchange"
	"triangular-arbitrage-bot/internal/execution"
	"triangular-arbitrage-bot/internal/logging"
	"triangular-arbitrage-bot/internal/market"
	"triangular-arbitrage-bot/internal/network"
	"triangular-arbitrage-bot/internal/version"
	"triangular-arbitrage-bot/pkg/models"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[ARBITRAGE] ")

	configPath := flag.String("config", "config.json", "Path to configuration file")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		log.Printf("triangular-arbitrage-bot %s", version.String())
		return
	}

	log.Printf("Starting Triangular Arbitrage Bot %s", version.String())

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration: API Key length=%d, Quote currencies=%v, Profit threshold=%.6f%%",
		len(cfg.APIKey), cfg.QuoteCurrencies, cfg.ProfitThreshold*100)

	if cfg.SimulationMode {
		log.Println("SIMULATION MODE ENABLED - No real trades will be executed")
	} else {
		log.Println("REAL TRADING MODE ENABLED - FOK orders will use CoinEx API")
		log.Printf("   FOK Polling Frequency: %.1fHz (every %.0fms)",
			cfg.FOKOrderSettings.PollingFrequencyHz,
			1000/cfg.FOKOrderSettings.PollingFrequencyHz)
		log.Printf("   FOK Order Timeout: %ds", cfg.FOKOrderSettings.OrderTimeoutSeconds)
		log.Printf("   Max Retry Attempts: %d", cfg.FOKOrderSettings.MaxRetryAttempts)
		log.Println("   REAL MONEY WILL BE USED!")
	}

	log.Printf("ORDER EXECUTION SETTINGS:")
	log.Printf("   Max Orders Per Second: %.2f (1 order every %.1fs)",
		cfg.OrderExecutionSettings.MaxOrdersPerSecond,
		1.0/cfg.OrderExecutionSettings.MaxOrdersPerSecond)
	log.Printf("   Order Amount Type: %s", cfg.OrderExecutionSettings.OrderAmountType)

	if cfg.OrderExecutionSettings.OrderAmountType == "static" {
		log.Printf("   Static Order Amount: $%.2f per order", cfg.OrderExecutionSettings.StaticOrderAmount)
	} else {
		log.Printf("   Dynamic Order Percentage: %.2f%% of available balance",
			cfg.OrderExecutionSettings.DynamicOrderPercentage*100)
	}

	log.Printf("   Account Balance: $%.2f", cfg.OrderExecutionSettings.AccountBalance)
	log.Printf("   Max Daily Spend: $%.2f", cfg.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   Min Order Amount: $%.2f", cfg.OrderExecutionSettings.MinOrderAmount)
	log.Printf("   Max Order Amount: $%.2f", cfg.OrderExecutionSettings.MaxOrderAmount)

	httpClient := network.NewHttpClient(cfg)
	httpClient.SetHeaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TriangularArbitrageBot/2.0",
	}).SetClient(&http.Client{
		Timeout: 30 * time.Second,
	})

	coinexClient := exchange.NewCoinexClient(httpClient, cfg)

	concurrent, dailySpent, availableBalance := coinexClient.GetOrderExecutionStats()
	log.Printf("CURRENT ORDER EXECUTION STATE:")
	log.Printf("   Active Concurrent Arbitrage Cycles: %d/%d", concurrent, cfg.OrderExecutionSettings.MaxConcurrentArbitrages)
	log.Printf("   Daily Spent: $%.2f/$%.2f", dailySpent, cfg.OrderExecutionSettings.MaxDailySpend)
	log.Printf("   Available Balance: $%.2f", availableBalance)

	log.Println("Testing connection...")
	connectionInfo, err := coinexClient.TestConnection()
	if err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}
	log.Printf("Connection successful: %s", connectionInfo)

	marketDepths := models.NewMarketDepths()
	metrics := models.NewMetrics()
	arbitrageEngine := execution.NewArbitrageEngine(cfg, marketDepths, metrics, coinexClient)
	marketManager := market.NewManager(cfg, httpClient, marketDepths)

	httpClient.SetMarketDepths(marketDepths)

	log.Println("STEP 1: Getting REST API data for ALL markets in parallel...")

	arbitrageMarkets, completeAssets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		log.Fatalf("Failed to get arbitrage markets: %v", err)
	}
	log.Printf("REST API: Found %d markets for arbitrage (%d complete assets)", len(arbitrageMarkets), len(completeAssets))

	criticalMarkets := cfg.CriticalMarkets
	type criticalResult struct {
		market    string
		orderBook models.OrderBook
		err       error
	}
	criticalResults := make(chan criticalResult, len(criticalMarkets))

	for _, mkt := range criticalMarkets {
		go func(m string) {
			orderBook, err := httpClient.GetOrderBookREST(m)
			criticalResults <- criticalResult{m, *orderBook, err}
		}(mkt)
	}

	for i := 0; i < len(criticalMarkets); i++ {
		result := <-criticalResults
		if result.err != nil {
			log.Printf("Failed to get snapshot for %s: %v", result.market, result.err)
		} else {
			marketDepths.Store(result.market, &result.orderBook)
			log.Printf("Stored snapshot for %s: %d bids, %d asks", result.market, len(result.orderBook.Bids), len(result.orderBook.Asks))
		}
	}

	log.Printf("Getting initial order book snapshots for ALL %d arbitrage markets in parallel...", len(arbitrageMarkets))

	log.Printf("Temporarily disabling rate limiting for initial data loading...")
	httpClient.DisableRateLimiting()
	defer httpClient.EnableRateLimiting()

	numWorkers := 20
	marketChan := make(chan string, len(arbitrageMarkets))
	type marketResult struct {
		market    string
		orderBook models.OrderBook
		err       error
	}
	results := make(chan marketResult, len(arbitrageMarkets))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for mkt := range marketChan {
				orderBook, err := httpClient.GetOrderBookREST(mkt)
				results <- marketResult{mkt, *orderBook, err}
			}
		}(i)
	}

	for _, mkt := range arbitrageMarkets {
		marketChan <- mkt
	}
	close(marketChan)

	wg.Wait()
	close(results)

	successfulSnapshots := 0
	failedSnapshots := 0
	for result := range results {
		if result.err != nil {
			log.Printf("Failed to get snapshot for %s: %v", result.market, result.err)
			failedSnapshots++
		} else {
			marketDepths.Store(result.market, &result.orderBook)
			successfulSnapshots++
		}
	}

	log.Printf("REST API completed: %d/%d markets successfully loaded (%d failed)", successfulSnapshots, len(arbitrageMarkets), failedSnapshots)
	log.Printf("Re-enabling rate limiting for normal operations...")

	log.Printf("COMPLETE ASSETS: %v", completeAssets)

	log.Println("STEP 2: Connecting to WebSocket...")
	if err := httpClient.ConnectWebSocket(); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	log.Println("WebSocket connected successfully")

	log.Println("Testing WebSocket connection...")
	if err := httpClient.TestWebSocketConnection(); err != nil {
		log.Printf("WebSocket test failed: %v", err)
		log.Println("Continuing anyway...")
	}

	log.Println("Waiting for WebSocket connection to stabilize...")
	time.Sleep(3 * time.Second)

	log.Println("STEP 3: Starting market data processor...")
	var messageCounter atomic.Uint64

	numWSWorkers := 4
	workerQueue := make(chan []byte, 1000)

	for i := 0; i < numWSWorkers; i++ {
		go func() {
			for msg := range workerQueue {
				_ = marketManager.ProcessWebSocketMessage(msg)
			}
		}()
	}

	go func() {
		dataFeed := httpClient.GetWebSocketDataFeed()

		for msg := range dataFeed {
			count := messageCounter.Add(1)

			select {
			case workerQueue <- msg:
			default:
				if count%5000 == 0 {
					log.Printf("Worker queue full, message %d dropped", count)
				}
			}

			if count%5000 == 0 {
				log.Printf("Processed %d WebSocket messages", count)
			}
		}
		log.Printf("Market data processor exited")
	}()

	log.Println("STEP 4: Subscribing to all markets via WebSocket...")
	allMarkets := make([]string, 0, len(arbitrageMarkets)+len(criticalMarkets))
	allMarkets = append(allMarkets, arbitrageMarkets...)
	allMarkets = append(allMarkets, criticalMarkets...)

	log.Printf("Subscribing to %d markets via WebSocket for real-time updates", len(allMarkets))
	if err := httpClient.SubscribeWebSocketWithFullFlow(allMarkets, marketDepths); err != nil {
		log.Printf("WebSocket subscription failed: %v", err)
		log.Fatal("Cannot proceed without WebSocket subscriptions")
	}
	log.Printf("WebSocket subscription completed for %d markets", len(allMarkets))

	arbitrageEngine.UpdateTriangularPaths(allMarkets, completeAssets)

	log.Println("Waiting for initial market data...")
	time.Sleep(3 * time.Second)

	availableMarkets := marketDepths.GetAvailableMarkets()
	log.Printf("Available markets: %d", len(availableMarkets))

	log.Println("Starting arbitrage engine...")
	arbitrageEngine.Start()

	// Start health check server
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	healthServer := &http.Server{
		Addr:    ":8080",
		Handler: healthMux,
	}
	go func() {
		log.Printf("Health check server starting on :8080")
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	metricsStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-metricsStopChan:
				return
			case <-ticker.C:
				logging.LogBasicMetrics(metrics, marketDepths)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Triangular Arbitrage Bot is running!")
	log.Printf("Press Ctrl+C to stop")

	<-quit
	log.Println("Shutdown signal received...")

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)

		log.Println("Stopping arbitrage engine...")
		arbitrageEngine.Stop()

		log.Println("Stopping metrics logging...")
		close(metricsStopChan)

		log.Println("Closing WebSocket connection...")
		if err := httpClient.CloseWebSocket(); err != nil {
			log.Printf("Error closing WebSocket: %v", err)
		}

		log.Println("Shutting down health server...")
		healthCtx, healthCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer healthCancel()
		healthServer.Shutdown(healthCtx)

		log.Println("Final metrics:")
		logging.LogSimpleMetrics(metrics, marketDepths)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	select {
	case <-shutdownDone:
		log.Println("Shutdown complete")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timed out, forcing exit")
	}
}
