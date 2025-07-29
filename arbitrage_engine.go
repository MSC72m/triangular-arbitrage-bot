package main

import (
	"fmt"
	"log"
	"math/rand" // Added for re-evaluation simulation
	"strconv"
	"strings"
	"sync"
	"time"
)

// AssetLockManager manages asset locking during order execution
type AssetLockManager struct {
	lockedAssets    map[string]int  // asset -> number of active orders
	executingAssets map[string]bool // assets currently in execution channel
	lockMutex       sync.RWMutex    // RW mutex for efficient read access
	executionMutex  sync.RWMutex    // separate mutex for execution cache
	config          *Config

	// Cooldown tracking to prevent repeated execution of same opportunities
	lastExecutionTime map[string]time.Time // asset -> last execution time
	cooldownMutex     sync.RWMutex
}

// NewAssetLockManager creates a new asset lock manager
func NewAssetLockManager(config *Config) *AssetLockManager {
	return &AssetLockManager{
		lockedAssets:      make(map[string]int),
		executingAssets:   make(map[string]bool),
		config:            config,
		lastExecutionTime: make(map[string]time.Time),
	}
}

// TryLockAssets attempts to lock multiple assets for an arbitrage trade
// Returns true if all assets can be locked, false otherwise
func (alm *AssetLockManager) TryLockAssets(assets []string) bool {
	if !alm.config.EnableAssetLocking {
		return true // Asset locking disabled, always allow
	}

	alm.lockMutex.Lock()
	defer alm.lockMutex.Unlock()

	// Check if any market would exceed max concurrent orders
	for _, market := range assets {
		currentOrders := alm.lockedAssets[market]
		if alm.config.MaxConcurrentOrdersPerAsset > 0 &&
			currentOrders >= alm.config.MaxConcurrentOrdersPerAsset {
			return false // Market is at max concurrent orders
		}
	}

	// All markets can be locked, proceed to lock them
	for _, market := range assets {
		alm.lockedAssets[market]++
	}

	log.Printf("🔒 MARKETS LOCKED | %v | Current locks: %v", assets, alm.getLockedAssetsSnapshot())
	return true
}

// UnlockAssets unlocks multiple assets after order completion
func (alm *AssetLockManager) UnlockAssets(assets []string) {
	if !alm.config.EnableAssetLocking {
		return // Asset locking disabled, nothing to unlock
	}

	alm.lockMutex.Lock()
	defer alm.lockMutex.Unlock()

	for _, market := range assets {
		if count, exists := alm.lockedAssets[market]; exists && count > 0 {
			alm.lockedAssets[market]--
			if alm.lockedAssets[market] == 0 {
				delete(alm.lockedAssets, market)
			}
		}
	}

	log.Printf("🔓 MARKETS UNLOCKED | %v | Current locks: %v", assets, alm.getLockedAssetsSnapshot())
}

// IsPathLocked checks if any asset in the arbitrage path is locked
func (alm *AssetLockManager) IsPathLocked(path TriangularPath) bool {
	if !alm.config.EnableAssetLocking {
		return false // Asset locking disabled, never locked
	}

	assets := alm.extractAssetsFromPath(path)

	alm.lockMutex.RLock()
	defer alm.lockMutex.RUnlock()

	for _, asset := range assets {
		if currentOrders := alm.lockedAssets[asset]; alm.config.MaxConcurrentOrdersPerAsset > 0 &&
			currentOrders >= alm.config.MaxConcurrentOrdersPerAsset {
			return true // Asset is locked
		}
	}

	return false
}

// IsPathInExecution checks if any asset in the path is currently being executed
func (alm *AssetLockManager) IsPathInExecution(path TriangularPath) bool {
	if !alm.config.EnableAssetLocking {
		return false
	}

	// Use the base asset (the main asset being arbitraged) as the execution key
	baseAsset := path.Asset1

	alm.executionMutex.RLock()
	defer alm.executionMutex.RUnlock()

	return alm.executingAssets[baseAsset]
}

// MarkAssetInExecution marks an asset as currently being executed
func (alm *AssetLockManager) MarkAssetInExecution(path TriangularPath) bool {
	if !alm.config.EnableAssetLocking {
		return true
	}

	baseAsset := path.Asset1

	// Check cooldown period to prevent repeated execution
	alm.cooldownMutex.RLock()
	lastExec, exists := alm.lastExecutionTime[baseAsset]
	cooldownPeriod := 5 * time.Second // 5 second cooldown between executions
	alm.cooldownMutex.RUnlock()

	if exists && time.Since(lastExec) < cooldownPeriod {
		log.Printf("⏳ COOLDOWN ACTIVE | Asset: %s | Last execution: %v ago | Skipping opportunity",
			baseAsset, time.Since(lastExec))
		return false
	}

	alm.executionMutex.Lock()
	defer alm.executionMutex.Unlock()

	// Check if already in execution
	if alm.executingAssets[baseAsset] {
		return false // Already being executed
	}

	// Mark as executing
	alm.executingAssets[baseAsset] = true

	// ALSO increment the locked assets count for maxConcurrentOrdersPerAsset tracking
	alm.lockMutex.Lock()
	alm.lockedAssets[baseAsset]++
	alm.lockMutex.Unlock()

	// Update last execution time
	alm.cooldownMutex.Lock()
	alm.lastExecutionTime[baseAsset] = time.Now()
	alm.cooldownMutex.Unlock()

	log.Printf("🚀 EXECUTION STARTED | Asset: %s | Path: %s→%s→%s",
		baseAsset, path.Market1, path.Market2, path.Market3)
	return true
}

// UnmarkAssetInExecution removes an asset from execution cache
func (alm *AssetLockManager) UnmarkAssetInExecution(path TriangularPath) {
	if !alm.config.EnableAssetLocking {
		return
	}

	baseAsset := path.Asset1

	alm.executionMutex.Lock()
	defer alm.executionMutex.Unlock()

	delete(alm.executingAssets, baseAsset)

	// ALSO decrement the locked assets count
	alm.lockMutex.Lock()
	if count, exists := alm.lockedAssets[baseAsset]; exists && count > 0 {
		alm.lockedAssets[baseAsset]--
		if alm.lockedAssets[baseAsset] == 0 {
			delete(alm.lockedAssets, baseAsset)
		}
	}
	alm.lockMutex.Unlock()

	log.Printf("✅ EXECUTION COMPLETED | Asset: %s | Available for new opportunities", baseAsset)
}

// GetExecutionStats returns execution cache statistics
func (alm *AssetLockManager) GetExecutionStats() (int, []string) {
	alm.executionMutex.RLock()
	defer alm.executionMutex.RUnlock()

	executingAssets := make([]string, 0, len(alm.executingAssets))
	for asset := range alm.executingAssets {
		executingAssets = append(executingAssets, asset)
	}

	return len(alm.executingAssets), executingAssets
}

// GetAssetOrderCount returns the current number of orders for a specific asset
func (alm *AssetLockManager) GetAssetOrderCount(asset string) int {
	if !alm.config.EnableAssetLocking {
		return 0 // Asset locking disabled, no orders
	}

	alm.lockMutex.RLock()
	defer alm.lockMutex.RUnlock()

	return alm.lockedAssets[asset]
}

// extractAssetsFromPath extracts the specific markets/trading pairs that should be locked
func (alm *AssetLockManager) extractAssetsFromPath(path TriangularPath) []string {
	// Instead of locking base assets, lock the specific trading pairs (markets)
	// This allows the same base asset (e.g., GMT) to trade on different pairs simultaneously
	markets := []string{path.Market1, path.Market2, path.Market3}

	// Remove duplicates if any
	uniqueMarkets := make(map[string]bool)
	for _, market := range markets {
		uniqueMarkets[market] = true
	}

	result := make([]string, 0, len(uniqueMarkets))
	for market := range uniqueMarkets {
		result = append(result, market)
	}

	return result
}

// getLockedAssetsSnapshot returns a snapshot of locked assets (for logging)
func (alm *AssetLockManager) getLockedAssetsSnapshot() map[string]int {
	snapshot := make(map[string]int)
	for asset, count := range alm.lockedAssets {
		snapshot[asset] = count
	}
	return snapshot
}

// GetLockStats returns lock statistics for monitoring
func (alm *AssetLockManager) GetLockStats() (int, []string) {
	alm.lockMutex.RLock()
	defer alm.lockMutex.RUnlock()

	totalLocks := 0
	lockedAssets := make([]string, 0, len(alm.lockedAssets))

	for asset, count := range alm.lockedAssets {
		totalLocks += count
		lockedAssets = append(lockedAssets, fmt.Sprintf("%s:%d", asset, count))
	}

	return totalLocks, lockedAssets
}

// OrderExecutionManager manages order execution and lifecycle
type OrderExecutionManager struct {
	coinexClient     *coinexClient
	assetLockManager *AssetLockManager
	orderResultChan  chan *OrderResult
	config           *Config
}

// NewOrderExecutionManager creates a new order execution manager
func NewOrderExecutionManager(coinexClient *coinexClient, assetLockManager *AssetLockManager, config *Config) *OrderExecutionManager {
	return &OrderExecutionManager{
		coinexClient:     coinexClient,
		assetLockManager: assetLockManager,
		orderResultChan:  make(chan *OrderResult, 2000), // Buffered channel for order results
		config:           config,
	}
}

// ExecuteArbitrageOrders executes all three legs of an arbitrage trade with advanced FOK handling
func (oem *OrderExecutionManager) ExecuteArbitrageOrders(opportunity ArbitrageOpportunity) {
	path := opportunity.Path
	markets := oem.assetLockManager.extractAssetsFromPath(path)

	// Always unmark from execution cache at the end
	defer oem.assetLockManager.UnmarkAssetInExecution(path)

	// Try to lock markets
	if !oem.assetLockManager.TryLockAssets(markets) {
		log.Printf("🚫 ARBITRAGE BLOCKED | Path: %s→%s→%s | Markets locked: %v",
			path.Market1, path.Market2, path.Market3, markets)
		return
	}

	executionMode := map[bool]string{true: "SIMULATION", false: "REAL API"}[oem.config.SimulationMode]
	log.Printf("🚀 EXECUTING FOK ARBITRAGE (%s) | Path: %s→%s→%s | Markets: %v | Expected Profit: %.6f%%",
		executionMode, path.Market1, path.Market2, path.Market3, markets, opportunity.NetProfit*100)

	// Start FOK arbitrage execution with retry logic
	oem.executeFOKArbitrageWithRetry(opportunity, markets, 0)
}

// executeFOKArbitrageWithRetry executes FOK arbitrage with intelligent retry and re-evaluation
func (oem *OrderExecutionManager) executeFOKArbitrageWithRetry(opportunity ArbitrageOpportunity, markets []string, attemptNumber int) {
	log.Printf("🔄 FOK ARBITRAGE EXECUTION | Attempt: %d/%d | Path: %s→%s→%s | Profit: %.6f%%",
		attemptNumber, oem.config.FOKOrderSettings.MaxRetryAttempts,
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.EstimatedProfit*100)

	// Unlock assets when done
	defer func() {
		oem.assetLockManager.UnlockAssets(markets)
		log.Printf("🔓 ASSETS UNLOCKED | Markets: %v", markets)
	}()

	// Step 3: Place FOK orders for all three legs
	log.Printf("📤 PLACING 3 FOK ORDERS | Attempt: %d/%d", attemptNumber, oem.config.FOKOrderSettings.MaxRetryAttempts)

	orderTrackers := make([]*FOKOrderTracker, 3)
	orderResults := make(map[int]*OrderResult)
	resultMutex := sync.RWMutex{}

	// Place all three orders
	for i := 0; i < 3; i++ {
		var market, orderType string
		var amount, price float64

		// Determine order parameters for each leg
		switch i {
		case 0: // First leg
			market = opportunity.Path.Market1
			orderType = "buy"
			amount = opportunity.Volume
			price = opportunity.Price1
		case 1: // Second leg
			market = opportunity.Path.Market2
			orderType = "sell"
			amount = opportunity.Volume
			price = opportunity.Price2
		case 2: // Third leg
			market = opportunity.Path.Market3
			orderType = "buy"
			amount = opportunity.Volume
			price = opportunity.Price3
		}

		// Create result channel for this order
		orderResultChan := make(chan *OrderResult, 1)

		// Place FOK order
		log.Printf("🔄 ATTEMPTING ORDER %d | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
			i+1, market, orderType, amount, price)

		tracker := oem.coinexClient.PlaceFOKOrder(market, orderType, amount, price, orderResultChan)
		if tracker == nil {
			log.Printf("❌ ORDER %d REJECTED | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
				i+1, market, orderType, amount, price)
			log.Printf("💡 POSSIBLE REASONS: Rate limit (5/sec), Spending limit ($%.2f balance), Concurrent limit (%d)",
				oem.config.OrderExecutionSettings.AccountBalance, oem.config.OrderExecutionSettings.MaxConcurrentOrders)

			// Cancel any orders that were already placed
			for j := 0; j < i; j++ {
				if orderTrackers[j] != nil {
					orderTrackers[j].mu.Lock()
					if orderTrackers[j].CancelChan != nil {
						select {
						case orderTrackers[j].CancelChan <- struct{}{}:
						default:
						}
					}
					orderTrackers[j].mu.Unlock()
				}
			}

			log.Printf("🛑 ARBITRAGE CANCELLED | Order %d rejected, cancelling previous orders", i+1)
			return
		}

		orderTrackers[i] = tracker
		log.Printf("✅ ORDER %d PLACED | ID: %s | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
			i+1, tracker.OrderID, market, orderType, amount, price)

		// Start result collector goroutine for this order
		go func(orderIndex int, resultChan <-chan *OrderResult) {
			result := <-resultChan
			resultMutex.Lock()
			orderResults[orderIndex] = result
			resultMutex.Unlock()
			log.Printf("📥 ORDER %d RESULT | ID: %s | Status: %s", orderIndex+1, result.OrderID, result.Status)
		}(i, orderResultChan)
	}

	// Step 4: Wait for all results with timeout
	timeout := time.Duration(oem.config.FOKOrderSettings.OrderTimeoutSeconds+1) * time.Second
	waitStart := time.Now()

	log.Printf("⏳ WAITING FOR ALL ORDER RESULTS | Timeout: %v", timeout)

	for time.Since(waitStart) < timeout {
		resultMutex.RLock()
		resultCount := len(orderResults)
		resultMutex.RUnlock()

		if resultCount == 3 {
			log.Printf("✅ ALL ORDER RESULTS RECEIVED | Time: %v", time.Since(waitStart))
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Step 5: Analyze results and decide next action
	resultMutex.RLock()
	results := make(map[int]*OrderResult)
	for k, v := range orderResults {
		results[k] = v
	}
	resultMutex.RUnlock()

	oem.analyzeFOKResultsAndDecide(opportunity, markets, results, attemptNumber)
}

// analyzeFOKResultsAndDecide analyzes FOK results and decides whether to retry or complete
func (oem *OrderExecutionManager) analyzeFOKResultsAndDecide(opportunity ArbitrageOpportunity, markets []string, results map[int]*OrderResult, attemptNumber int) {
	successfulOrders := 0
	failedOrders := 0
	totalFilled := 0.0
	totalPnL := 0.0
	totalFees := 0.0

	// Analyze each order result
	for i := 0; i < 3; i++ {
		if result, exists := results[i]; exists {
			if result.Status == OrderStatusFilled {
				successfulOrders++
				totalFilled += result.FilledAmount * result.AvgPrice
				totalFees += result.Fee

				// Calculate PnL contribution
				if strings.Contains(result.Type, "sell") {
					totalPnL += result.FilledAmount * result.AvgPrice
				} else {
					totalPnL -= result.FilledAmount * result.AvgPrice
				}
			} else {
				failedOrders++
			}

			log.Printf("📊 LEG %d ANALYSIS | ID: %s | Status: %s | Filled: %.6f | Fee: %.6f %s",
				i+1, result.OrderID, result.Status, result.FilledAmount, result.Fee, result.FeeCurrency)
		} else {
			failedOrders++
			log.Printf("📊 LEG %d ANALYSIS | Missing result", i+1)
		}
	}

	log.Printf("📊 FOK ARBITRAGE ANALYSIS | Attempt %d | Successful: %d/3 | Failed: %d/3 | Total PnL: $%.4f | Total Fees: $%.4f",
		attemptNumber+1, successfulOrders, failedOrders, totalPnL, totalFees)

	// Check if arbitrage was successful
	if successfulOrders == 3 {
		// Complete success
		netPnL := totalPnL - totalFees
		log.Printf("✅ FOK ARBITRAGE COMPLETE | Attempt %d | Expected: $%.4f | Actual: $%.4f | Net: $%.4f",
			attemptNumber+1, opportunity.NetProfit*opportunity.Volume, totalPnL, netPnL)

		// Reset rejection count on success
		oem.coinexClient.resetRejectionCount()

		// Unlock markets - arbitrage completed successfully
		oem.assetLockManager.UnlockAssets(markets)
		return
	}

	// Partial or complete failure - decide whether to retry
	shouldRetry := false
	retryReason := ""

	if attemptNumber < oem.config.FOKOrderSettings.MaxRetryAttempts && oem.config.FOKOrderSettings.EnableAggressiveReEvaluation {
		// Re-evaluate profitability with current market conditions
		log.Printf("🔄 RE-EVALUATING PROFIT | Attempt %d | Checking if retry is worthwhile", attemptNumber+1)

		newOpportunity := oem.reEvaluateOpportunity(opportunity)
		if newOpportunity != nil {
			profitDifference := newOpportunity.NetProfit - opportunity.NetProfit

			// Check if profit is still above threshold and hasn't degraded too much
			if newOpportunity.NetProfit >= oem.config.ProfitThreshold &&
				profitDifference >= -oem.config.ProfitThreshold {

				shouldRetry = true
				retryReason = fmt.Sprintf("Still profitable: %.6f%% (change: %+.6f%%)",
					newOpportunity.NetProfit*100, profitDifference*100)
				opportunity = *newOpportunity // Update opportunity with new prices
			} else {
				retryReason = fmt.Sprintf("Profit degraded: %.6f%% → %.6f%% (change: %+.6f%%)",
					opportunity.NetProfit*100, newOpportunity.NetProfit*100, profitDifference*100)
			}
		} else {
			retryReason = "Market data unavailable for re-evaluation"
		}
	} else if attemptNumber >= oem.config.FOKOrderSettings.MaxRetryAttempts {
		retryReason = "Maximum retry attempts reached"
	} else if !oem.config.FOKOrderSettings.EnableAggressiveReEvaluation {
		retryReason = "Aggressive re-evaluation disabled"
	}

	if shouldRetry {
		log.Printf("🔄 RETRYING FOK ARBITRAGE | Attempt %d → %d | Reason: %s",
			attemptNumber+1, attemptNumber+2, retryReason)

		// Small delay before retry to let market conditions stabilize
		time.Sleep(100 * time.Millisecond)

		// Retry with updated opportunity
		oem.executeFOKArbitrageWithRetry(opportunity, markets, attemptNumber+1)
	} else {
		log.Printf("🛑 FOK ARBITRAGE ABANDONED | After %d attempts | Reason: %s",
			attemptNumber+1, retryReason)

		// Cancel all remaining active orders if configured to do so
		if oem.config.FOKOrderSettings.CancelAllOnSingleFailure {
			log.Printf("🛑 CANCELLING ALL REMAINING ORDERS | Per configuration")
			oem.coinexClient.CancelAllActiveOrders()
		}

		// Unlock markets - arbitrage abandoned
		oem.assetLockManager.UnlockAssets(markets)
	}
}

// reEvaluateOpportunity re-evaluates the arbitrage opportunity with current market data
func (oem *OrderExecutionManager) reEvaluateOpportunity(originalOpportunity ArbitrageOpportunity) *ArbitrageOpportunity {
	// This would typically be done by the arbitrage engine
	// For now, we'll simulate a re-evaluation with some price movement

	// Simulate slight price movements (±0.01%) since original calculation
	priceMovement1 := 0.9999 + rand.Float64()*0.0002 // ±0.01%
	priceMovement2 := 0.9999 + rand.Float64()*0.0002
	priceMovement3 := 0.9999 + rand.Float64()*0.0002

	newOpportunity := originalOpportunity
	newOpportunity.Price1 *= priceMovement1
	newOpportunity.Price2 *= priceMovement2
	newOpportunity.Price3 *= priceMovement3

	// Recalculate profit based on direction
	path := originalOpportunity.Path
	var roundTripRate float64

	if path.Direction1 == "buy" && path.Direction2 == "sell" && path.Direction3 == "sell" {
		roundTripRate = (1.0 / newOpportunity.Price1) * newOpportunity.Price2 * newOpportunity.Price3
	} else if path.Direction1 == "buy" && path.Direction2 == "buy" && path.Direction3 == "sell" {
		roundTripRate = (1.0 / newOpportunity.Price1) * (1.0 / newOpportunity.Price2) * newOpportunity.Price3
	} else {
		// Handle other direction combinations
		roundTripRate = 1.0 // Fallback
	}

	estimatedProfit := roundTripRate - 1.0

	// Subtract fees
	fee1 := oem.getTradingFee(path.Market1)
	fee2 := oem.getTradingFee(path.Market2)
	fee3 := oem.getTradingFee(path.Market3)
	totalFees := fee1 + fee2 + fee3

	newOpportunity.EstimatedProfit = estimatedProfit
	newOpportunity.NetProfit = estimatedProfit - totalFees
	newOpportunity.Timestamp = time.Now()

	log.Printf("🔄 PROFIT RE-EVALUATION | Original: %.6f%% | New: %.6f%% | Change: %+.6f%% | Prices: %.8f→%.8f, %.8f→%.8f, %.8f→%.8f",
		originalOpportunity.NetProfit*100, newOpportunity.NetProfit*100,
		(newOpportunity.NetProfit-originalOpportunity.NetProfit)*100,
		originalOpportunity.Price1, newOpportunity.Price1,
		originalOpportunity.Price2, newOpportunity.Price2,
		originalOpportunity.Price3, newOpportunity.Price3)

	return &newOpportunity
}

// getTradingFee returns the trading fee for a market
func (oem *OrderExecutionManager) getTradingFee(market string) float64 {
	if fee, exists := oem.config.TradingFees[market]; exists {
		return fee
	}
	return oem.config.DefaultTradingFee
}

// executionWorker handles the execution of arbitrage opportunities
func (ae *ArbitrageEngine) executionWorker() {
	for {
		select {
		case <-ae.stopChan:
			return
		case opportunity := <-ae.executionChan:
			if ae.config.SimulationMode {
				ae.simulateExecution(opportunity)
			} else {
				// Use the new order execution manager
				ae.orderExecutionManager.ExecuteArbitrageOrders(opportunity)
			}
		}
	}
}

// simulateExecution simulates the execution for testing purposes
func (ae *ArbitrageEngine) simulateExecution(opportunity ArbitrageOpportunity) {
	path := opportunity.Path
	markets := ae.assetLockManager.extractAssetsFromPath(path)

	// Always unmark from execution cache at the end
	defer ae.assetLockManager.UnmarkAssetInExecution(path)

	// Check asset locking in simulation mode too
	if !ae.assetLockManager.TryLockAssets(markets) {
		log.Printf("🚫 SIMULATION BLOCKED | Path: %s→%s→%s | Markets locked: %v",
			path.Market1, path.Market2, path.Market3, markets)
		return
	}

	log.Printf("💰 SIMULATION TRADE | Path: %s→%s→%s | Expected Profit: %.6f%% | Volume: $%.2f | Est. PnL: $%.4f | Markets: %v",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.NetProfit*100,
		opportunity.Volume,
		opportunity.NetProfit*opportunity.Volume,
		markets)

	// Log the three legs of the trade
	log.Printf("   🔄 Leg 1: %s %s at price %.8f",
		opportunity.Path.Direction1, opportunity.Path.Market1, opportunity.Price1)
	log.Printf("   🔄 Leg 2: %s %s at price %.8f",
		opportunity.Path.Direction2, opportunity.Path.Market2, opportunity.Price2)
	log.Printf("   🔄 Leg 3: %s %s at price %.8f",
		opportunity.Path.Direction3, opportunity.Path.Market3, opportunity.Price3)

	// Simulate execution time
	if ae.config.EnableOrderSimulation {
		executionTime := time.Duration(ae.config.OrderExecutionTimeMs) * time.Millisecond
		log.Printf("⏳ SIMULATING EXECUTION | Markets: %v | Duration: %v", markets, executionTime)
		time.Sleep(executionTime)
	}

	ae.metrics.IncrementTrades()
	ae.metrics.AddPnL(opportunity.NetProfit * opportunity.Volume)

	// Unlock markets after simulation
	ae.assetLockManager.UnlockAssets(markets)

	log.Printf("✅ SIMULATION COMPLETE | Total trades: %d | Total PnL: $%.4f | Markets unlocked: %v",
		ae.metrics.GetSnapshot().TradesExecuted,
		ae.metrics.GetSnapshot().TotalPnL,
		markets)
}

// ArbitrageEngine manages triangular arbitrage detection and execution
type ArbitrageEngine struct {
	config                *Config
	marketDepths          *MarketDepths
	metrics               *Metrics
	coinexClient          *coinexClient
	rateLimiter           *RateLimiter
	assetLockManager      *AssetLockManager
	orderExecutionManager *OrderExecutionManager

	// Triangular paths cache
	triangularPaths []TriangularPath
	pathsLock       sync.RWMutex

	// Active trades tracking
	activeTrades map[string]*ArbitrageTrade
	// Execution channel
	executionChan chan ArbitrageOpportunity
	stopChan      chan struct{}

	// Loop detection
	lastOpportunityTime time.Time
	opportunityCount    int
	loopMutex           sync.RWMutex
}

// NewArbitrageEngine creates a new arbitrage engine
func NewArbitrageEngine(config *Config, marketDepths *MarketDepths, metrics *Metrics, coinexClient *coinexClient) *ArbitrageEngine {
	assetLockManager := NewAssetLockManager(config)
	orderExecutionManager := NewOrderExecutionManager(coinexClient, assetLockManager, config)

	return &ArbitrageEngine{
		config:                config,
		marketDepths:          marketDepths,
		metrics:               metrics,
		coinexClient:          coinexClient,
		rateLimiter:           NewRateLimiter(config.RateLimitPerSecond, config.RateLimitPerSecond),
		assetLockManager:      assetLockManager,
		orderExecutionManager: orderExecutionManager,
		activeTrades:          make(map[string]*ArbitrageTrade),
		executionChan:         make(chan ArbitrageOpportunity, 2000),
		stopChan:              make(chan struct{}),
	}
}

// Start begins the arbitrage detection and execution
func (ae *ArbitrageEngine) Start() {
	log.Printf("Starting arbitrage engine with %d concurrent scans", ae.config.ConcurrentScans)

	// Start execution worker
	go ae.executionWorker()

	// Start concurrent arbitrage scanners
	for i := 0; i < ae.config.ConcurrentScans; i++ {
		go ae.arbitrageScanner(i)
	}
}

// Stop stops the arbitrage engine
func (ae *ArbitrageEngine) Stop() {
	close(ae.stopChan)
}

// UpdateTriangularPaths updates the cache of triangular arbitrage paths
func (ae *ArbitrageEngine) UpdateTriangularPaths(markets []string, completeAssets []string) {
	// Use WaitGroup to wait for market data to become available
	log.Printf("⏳ Waiting for market data before creating triangular paths...")

	var wg sync.WaitGroup
	marketDataReady := make(chan bool, 1)

	// Start goroutine to monitor market data availability
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		timeout := time.After(30 * time.Second) // 30 second timeout

		for {
			select {
			case <-timeout:
				log.Printf("⚠️  Timeout waiting for market data")
				marketDataReady <- false
				return
			case <-ticker.C:
				availableMarkets := ae.marketDepths.GetAvailableMarkets()
				if len(availableMarkets) >= 3 { // Reduced from 10 to 3 - many markets are illiquid
					log.Printf("✅ Market data ready with %d markets", len(availableMarkets))

					// Check critical quote pairs
					for _, pair := range ae.config.CriticalMarkets {
						// Skip checks for critical markets - assume they exist
						log.Printf("   ✅ %s: Assumed to exist (critical market)", pair)
					}

					marketDataReady <- true
					return
				}
			}
		}
	}()

	// Wait for market data to be ready
	wg.Wait()
	ready := <-marketDataReady
	if !ready {
		log.Printf("❌ Failed to get sufficient market data, proceeding anyway...")
	}

	// Get available markets from market depths (markets that actually have data)
	availableMarkets := ae.marketDepths.GetAvailableMarkets()
	availableMarketSet := make(map[string]bool)
	for _, market := range availableMarkets {
		availableMarketSet[market] = true
	}

	log.Printf("🔍 MARKET VALIDATION:")
	log.Printf("   📊 Expected markets: %d", len(markets))
	log.Printf("   ✅ Available markets with data: %d", len(availableMarkets))

	// Show some examples of available vs expected
	log.Printf("   📈 Available markets (first 10): %v", availableMarkets[:Min(10, len(availableMarkets))])

	// Check specifically for critical quote pairs
	log.Printf("   🔍 Critical quote pair check:")
	for _, critical := range ae.config.CriticalMarkets {
		// Skip checks for critical markets - assume they exist
		log.Printf("      ✅ %s: Assumed to exist (critical market)", critical)
	}

	// Check if critical markets exist in the original markets list
	log.Printf("   🔍 Checking original markets list for critical pairs:")
	criticalInOriginal := make(map[string]bool)
	for _, critical := range ae.config.CriticalMarkets {
		criticalInOriginal[critical] = false
	}

	for _, market := range markets {
		if criticalInOriginal[market] {
			criticalInOriginal[market] = true
			log.Printf("      ✅ %s found in original markets list", market)
		}
	}

	for critical, found := range criticalInOriginal {
		if !found {
			log.Printf("      ❌ %s NOT found in original markets list", critical)
		}
	}

	// Filter markets to only include those with actual data AND liquidity
	validMarkets := []string{}
	liquidMarkets := []string{}
	missingMarkets := []string{}

	for _, market := range markets {
		// Skip all checks for critical markets - assume they exist and have liquidity
		if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(market) {
			validMarkets = append(validMarkets, market)
			liquidMarkets = append(liquidMarkets, market)
			continue
		}

		if availableMarketSet[market] {
			// Check if market has actual liquidity (order book data from WebSocket)
			if orderBook, exists := ae.marketDepths.Load(market); exists && orderBook != nil {
				if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					liquidMarkets = append(liquidMarkets, market)
				}
			}
			validMarkets = append(validMarkets, market)
		} else {
			missingMarkets = append(missingMarkets, market)
		}
	}

	log.Printf("   ✅ Valid markets: %d", len(validMarkets))
	log.Printf("   �� Liquid markets: %d", len(liquidMarkets))
	log.Printf("   ❌ Missing markets: %d", len(missingMarkets))

	if len(missingMarkets) > 0 {
		log.Printf("   🚫 Missing markets (first 10): %v", missingMarkets[:Min(10, len(missingMarkets))])
	}

	// Create triangular arbitrage paths using liquid markets only
	// Look for cycles like USDT → Asset → USDC → USDT
	log.Printf("🔍 Creating triangular paths from %d liquid markets", len(liquidMarkets))
	if len(liquidMarkets) > 0 {
		log.Printf("   📊 Sample liquid markets: %v", liquidMarkets[:Min(10, len(liquidMarkets))])
	}

	// Check if critical markets are in liquid markets
	log.Printf("   ✅ Critical markets assumed to exist: %v", ae.config.CriticalMarkets)

	paths := ae.discoverTriangularArbitrageCycles(liquidMarkets)

	ae.pathsLock.Lock()
	ae.triangularPaths = paths
	ae.pathsLock.Unlock()

	log.Printf("🔄 Updated triangular paths: %d arbitrage cycles discovered", len(paths))

	// Debug: Why no paths?
	if len(paths) == 0 {
		log.Printf("🔍 DEBUG: Why no triangular paths found?")
		log.Printf("   - Available markets: %d", len(liquidMarkets))
		log.Printf("   - Complete assets: %d", len(completeAssets))

		if len(liquidMarkets) > 0 {
			log.Printf("   - Sample markets: %v", liquidMarkets[:Min(5, len(liquidMarkets))])
		}
		if len(completeAssets) > 0 {
			log.Printf("   - Complete assets: %v", completeAssets)
		}
	}

	// Log first few paths for debugging
	for i, path := range paths {
		if i < 5 { // Show first 5 paths
			log.Printf("   📊 Cycle %d: %s (%s) → %s (%s) → %s (%s) → %s",
				i+1,
				path.BaseAsset,
				path.Direction1,
				path.Asset1,
				path.Direction2,
				path.Asset2,
				path.Direction3,
				path.BaseAsset)
		}
	}

	if len(paths) > 5 {
		log.Printf("   ... and %d more cycles", len(paths)-5)
	}
}

// discoverTriangularArbitrageCycles discovers triangular arbitrage cycles like USDT → Asset → USDC → USDT
func (ae *ArbitrageEngine) discoverTriangularArbitrageCycles(availableMarkets []string) []TriangularPath {
	var paths []TriangularPath

	log.Printf("🔍 Discovering triangular arbitrage cycles from %d available markets", len(availableMarkets))

	// Create market lookup sets for configured quote currencies only
	marketSet := make(map[string]bool)
	usdtMarkets := make(map[string]bool) // Asset/USDT pairs
	usdcMarkets := make(map[string]bool) // Asset/USDC pairs

	for _, market := range availableMarkets {
		marketSet[market] = true

		// Only process markets for configured quote currencies
		for _, quote := range ae.config.QuoteCurrencies {
			if strings.HasSuffix(market, quote) {
				asset := strings.TrimSuffix(market, quote)
				if asset != "" {
					if quote == "USDT" && asset != "USDC" {
						usdtMarkets[asset] = true
					} else if quote == "USDC" && asset != "USDT" {
						usdcMarkets[asset] = true
					}
				}
				break
			}
		}
	}

	log.Printf("   📊 USDT pairs: %d, USDC pairs: %d", len(usdtMarkets), len(usdcMarkets))

	// Find triangular cycles: USDT → Asset → USDC → USDT
	cycleCount := 0

	// Only create cycles if we have both quote currencies configured
	if len(ae.config.QuoteCurrencies) >= 2 {
		// Check if we have a direct USDC/USDT pair
		hasDirectPair := false
		for _, critical := range ae.config.CriticalMarkets {
			if marketSet[critical] {
				hasDirectPair = true
				break
			}
		}

		if hasDirectPair {
			// Use direct USDC/USDT pair if available
			var quotePair string
			var quoteDirection string

			// Find the available critical market
			for _, critical := range ae.config.CriticalMarkets {
				if marketSet[critical] {
					quotePair = critical
					if strings.HasSuffix(critical, "USDT") {
						quoteDirection = "sell" // Selling USDC for USDT
					} else {
						quoteDirection = "buy" // Buying USDC with USDT
					}
					log.Printf("   🎯 Using direct quote pair: %s (%s)", critical, quoteDirection)
					break
				}
			}

			for asset := range usdtMarkets {
				if usdcMarkets[asset] { // Asset has both USDT and USDC pairs
					// Forward cycle: USDT → Asset → USDC → USDT
					path1 := TriangularPath{
						BaseAsset:  "USDT",
						Asset1:     asset,
						Asset2:     "USDC",
						Market1:    asset + "USDT", // Buy asset with USDT
						Market2:    asset + "USDC", // Sell asset for USDC
						Market3:    quotePair,      // Convert USDC back to USDT
						Direction1: "buy",
						Direction2: "sell",
						Direction3: quoteDirection, // Correct direction based on available pair
					}

					// Reverse cycle: USDT → USDC → Asset → USDT
					var reverseDirection1 string
					if quotePair == "USDCUSDT" {
						reverseDirection1 = "buy" // Buy USDC with USDT
					} else {
						reverseDirection1 = "sell" // Sell USDT for USDC
					}

					path2 := TriangularPath{
						BaseAsset:  "USDT",
						Asset1:     "USDC",
						Asset2:     asset,
						Market1:    quotePair,      // Convert USDT to USDC
						Market2:    asset + "USDC", // Buy asset with USDC
						Market3:    asset + "USDT", // Sell asset for USDT
						Direction1: reverseDirection1,
						Direction2: "buy",
						Direction3: "sell",
					}

					paths = append(paths, path1, path2)
					cycleCount += 2

					log.Printf("   ✅ Created cycles for %s: USDT→%s→USDC→USDT and reverse (using %s)", asset, asset, quotePair)
				}
			}
		} else {
			// No direct USDC/USDT pair - create triangular cycles using only USDT pairs
			// This creates cycles like: USDT → Asset1 → Asset2 → USDT (where both assets have USDT pairs)
			log.Printf("   🎯 No direct USDC/USDT pair found - creating USDT-only triangular cycles")

			// Find assets that have USDT pairs
			usdtAssets := []string{}
			for asset := range usdtMarkets {
				usdtAssets = append(usdtAssets, asset)
			}

			log.Printf("   📊 Assets with USDT pairs: %d", len(usdtAssets))

			if len(usdtAssets) < 2 {
				log.Printf("   ⚠️  Need at least 2 assets with USDT pairs for triangular arbitrage")
				return paths
			}

			// Create triangular cycles using three different USDT pairs
			// Example: USDT → BTC → ETH → USDT (all using USDT pairs)
			for i, asset1 := range usdtAssets {
				for j, asset2 := range usdtAssets {
					if i != j { // Use different assets
						// Cycle: USDT → Asset1 → Asset2 → USDT
						path1 := TriangularPath{
							BaseAsset:  "USDT",
							Asset1:     asset1,
							Asset2:     asset2,
							Market1:    asset1 + "USDT", // Buy asset1 with USDT
							Market2:    asset1 + "USDT", // Sell asset1 for USDT (same market, different side)
							Market3:    asset2 + "USDT", // Buy asset2 with USDT
							Direction1: "buy",
							Direction2: "sell",
							Direction3: "buy", // Buy asset2 with USDT
						}

						// Reverse cycle: USDT → Asset2 → Asset1 → USDT
						path2 := TriangularPath{
							BaseAsset:  "USDT",
							Asset1:     asset2,
							Asset2:     asset1,
							Market1:    asset2 + "USDT", // Buy asset2 with USDT
							Market2:    asset2 + "USDT", // Sell asset2 for USDT (same market, different side)
							Market3:    asset1 + "USDT", // Buy asset1 with USDT
							Direction1: "buy",
							Direction2: "sell",
							Direction3: "buy", // Buy asset1 with USDT
						}

						paths = append(paths, path1, path2)
						cycleCount += 2

						if cycleCount <= 10 { // Limit logging to first 10 cycles
							log.Printf("   ✅ Created cycle: USDT→%s→%s→USDT", asset1, asset2)
						}
					}
				}
			}
		}
	} else {
		log.Printf("   ⚠️  Need at least 2 quote currencies for triangular arbitrage (configured: %v)", ae.config.QuoteCurrencies)
	}

	log.Printf("✅ Created %d triangular arbitrage cycles", cycleCount)
	return paths
}

// arbitrageScanner continuously scans for arbitrage opportunities
func (ae *ArbitrageEngine) arbitrageScanner(scannerID int) {
	ticker := time.NewTicker(50 * time.Millisecond) // Increased frequency from 100ms to 50ms for faster opportunity detection
	defer ticker.Stop()

	scanCount := 0

	for {
		select {
		case <-ae.stopChan:
			return
		case <-ticker.C:
			scanCount++
			ae.scanForOpportunities(scannerID, scanCount)
		}
	}
}

// scanForOpportunities scans for profitable arbitrage opportunities
func (ae *ArbitrageEngine) scanForOpportunities(scannerID int, scanCount int) {
	startTime := time.Now()
	defer func() {
		detectionTime := time.Since(startTime)
		if detectionTime > time.Duration(ae.config.MaxLatencyMs)*time.Millisecond {
			log.Printf("⚠️  SLOW SCAN | Scanner %d | Latency: %v (limit: %dms)",
				scannerID, detectionTime, ae.config.MaxLatencyMs)
		}
	}()

	ae.pathsLock.RLock()
	paths := make([]TriangularPath, len(ae.triangularPaths))
	copy(paths, ae.triangularPaths)
	ae.pathsLock.RUnlock()

	if len(paths) == 0 {
		if scanCount%1000 == 0 && scannerID == 0 { // Log occasionally from scanner 0 only
			log.Printf("⚠️  NO PATHS | Scanner %d | No valid triangular paths available for scanning", scannerID)

			// Get available markets for debugging
			availableMarkets := ae.marketDepths.GetAvailableMarkets()
			log.Printf("   📊 Available markets with data: %d", len(availableMarkets))
			if len(availableMarkets) > 0 {
				log.Printf("   📈 Sample available markets: %v", availableMarkets[:Min(5, len(availableMarkets))])
			} else {
				log.Printf("   ❌ No market data available at all - WebSocket may be disconnected")
			}
		}
		return
	}

	// Get snapshot of market depths
	snapshot := ae.marketDepths.GetSnapshot()

	// Critical markets are assumed to exist - no need to fetch price data
	// The arbitrage engine will handle them differently in path discovery

	// Debug: Show asset lock status periodically
	if scanCount%1000 == 0 && scannerID == 0 { // Log every 1000 scans from scanner 0
		totalLocks, lockedAssets := ae.assetLockManager.GetLockStats()
		executingCount, executingAssets := ae.assetLockManager.GetExecutionStats()

		if totalLocks > 0 || executingCount > 0 {
			log.Printf("🔒 ASSET STATUS | Locked: %d assets: %v | Executing: %d assets: %v",
				totalLocks, lockedAssets[:Min(10, len(lockedAssets))],
				executingCount, executingAssets)
		}
	}

	opportunitiesFound := 0
	pathsChecked := 0
	pathsWithData := 0
	pathsBlocked := 0
	pathsInExecution := 0
	pathsBelowThreshold := 0
	pathsLosing := 0

	// Track which assets are being scanned vs blocked vs executing
	assetsScanned := make(map[string]int)
	assetsBlocked := make(map[string]int)
	assetsExecuting := make(map[string]int)

	for _, path := range paths {
		pathsChecked++

		// Extract assets for this path
		baseAsset := path.Asset1 // The main asset being arbitraged (e.g., GMT, ONE, etc.)

		// FIRST PRIORITY: Check maxConcurrentOrdersPerAsset limit
		if ae.assetLockManager.GetAssetOrderCount(baseAsset) >= ae.config.MaxConcurrentOrdersPerAsset {
			pathsBlocked++
			assetsBlocked[baseAsset]++
			continue
		}

		// Check if this asset is already in execution (SECOND PRIORITY)
		if ae.assetLockManager.IsPathInExecution(path) {
			pathsInExecution++
			assetsExecuting[baseAsset]++
			continue
		}

		// Check if this path is blocked by asset locking (THIRD PRIORITY)
		if ae.assetLockManager.IsPathLocked(path) {
			pathsBlocked++
			assetsBlocked[baseAsset]++
			continue
		}

		// Track that we're scanning this asset
		assetsScanned[baseAsset]++

		opportunity := ae.calculateOpportunity(path, snapshot, startTime)

		if opportunity != nil {
			pathsWithData++
			profitPercent := opportunity.NetProfit * 100

			if opportunity.NetProfit >= ae.config.ProfitThreshold {
				opportunitiesFound++
				ae.metrics.IncrementOpportunities()

				// Detect potential loops
				ae.detectLoop()

				log.Printf("🚨 OPPORTUNITY FOUND | Scanner %d | Path: %s→%s→%s | Profit: +%.6f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
					scannerID,
					path.Market1, path.Market2, path.Market3,
					profitPercent,
					opportunity.Volume,
					opportunity.Price1, opportunity.Price2, opportunity.Price3)

				// Mark asset as in execution before sending to channel
				if ae.assetLockManager.MarkAssetInExecution(path) {
					// Send to execution channel (non-blocking)
					select {
					case ae.executionChan <- *opportunity:
						log.Printf("✅ QUEUED | Scanner %d | Opportunity sent to execution", scannerID)
					default:
						// If channel is full, unmark the asset since we're not executing
						ae.assetLockManager.UnmarkAssetInExecution(path)
						log.Printf("⚠️  DROPPED | Scanner %d | Execution channel full", scannerID)
					}
				} else {
					log.Printf("🔄 DUPLICATE SKIPPED | Scanner %d | Asset %s already in execution", scannerID, baseAsset)
				}
			} else if opportunity.NetProfit > 0 {
				pathsBelowThreshold++
			} else {
				pathsLosing++
			}
		}
	}

	// Enhanced scan summary with asset lock information and asset diversity
	if scanCount%1000 == 0 && scannerID == 0 {
		log.Printf("🔍 SCAN SUMMARY | Paths: %d | Blocked: %d | In Execution: %d | With data: %d | Opportunities: %d | Below threshold: %d | Losing: %d | Detection time: %v",
			pathsChecked, pathsBlocked, pathsInExecution, pathsWithData, opportunitiesFound, pathsBelowThreshold, pathsLosing, time.Since(startTime))

		// Show asset diversity
		if len(assetsScanned) > 0 {
			scannedList := make([]string, 0, len(assetsScanned))
			for asset, count := range assetsScanned {
				scannedList = append(scannedList, fmt.Sprintf("%s:%d", asset, count))
			}
			log.Printf("   📊 Assets scanned: %v", scannedList[:Min(10, len(scannedList))])
		}

		if len(assetsBlocked) > 0 {
			blockedList := make([]string, 0, len(assetsBlocked))
			for asset, count := range assetsBlocked {
				blockedList = append(blockedList, fmt.Sprintf("%s:%d", asset, count))
			}
			log.Printf("   🚫 Assets blocked: %v", blockedList[:Min(10, len(blockedList))])
		}

		if len(assetsExecuting) > 0 {
			executingList := make([]string, 0, len(assetsExecuting))
			for asset, count := range assetsExecuting {
				executingList = append(executingList, fmt.Sprintf("%s:%d", asset, count))
			}
			log.Printf("   🔄 Assets in execution: %v", executingList[:Min(10, len(executingList))])
		}

		// Show execution cache status
		executingCount, executingAssets := ae.assetLockManager.GetExecutionStats()
		if executingCount > 0 {
			log.Printf("   🚀 Currently executing: %d assets: %v", executingCount, executingAssets)
		}
	}
}

// calculateOpportunity calculates the profitability of a triangular arbitrage path
func (ae *ArbitrageEngine) calculateOpportunity(path TriangularPath, snapshot map[string]*OrderBook, detectionStart time.Time) *ArbitrageOpportunity {
	market1Data, ok1 := snapshot[path.Market1]
	market2Data, ok2 := snapshot[path.Market2]
	market3Data, ok3 := snapshot[path.Market3]

	// This should no longer happen since we now filter to only valid markets
	if !ok1 || !ok2 || !ok3 {
		// Check if any of the missing markets are critical markets (should be ignored)
		missing := []string{}
		hasCriticalMissing := false

		if !ok1 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market1) {
				hasCriticalMissing = true
			} else {
				missing = append(missing, path.Market1)
			}
		}
		if !ok2 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market2) {
				hasCriticalMissing = true
			} else {
				missing = append(missing, path.Market2)
			}
		}
		if !ok3 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market3) {
				hasCriticalMissing = true
			} else {
				missing = append(missing, path.Market3)
			}
		}

		// If only critical markets are missing, ignore the check
		if hasCriticalMissing && len(missing) == 0 {
			// Critical markets assumed to exist - continue processing
		} else if len(missing) > 0 {
			// Reduced logging frequency to avoid spam
			if detectionStart.UnixNano()%10000 == 0 { // Log ~0.01% of these
				log.Printf("⚠️  TEMP MISSING | Path: %s→%s→%s | Missing: %v (temporary data gap)",
					path.Market1, path.Market2, path.Market3, missing)
			}
			return nil
		}
	}

	if len(market1Data.Asks) == 0 || len(market1Data.Bids) == 0 ||
		len(market2Data.Asks) == 0 || len(market2Data.Bids) == 0 ||
		len(market3Data.Asks) == 0 || len(market3Data.Bids) == 0 {

		// Check if any of the empty markets are critical markets (should be ignored)
		emptyMarkets := []string{}
		hasCriticalEmpty := false

		if len(market1Data.Asks) == 0 || len(market1Data.Bids) == 0 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market1) {
				hasCriticalEmpty = true
			} else {
				emptyMarkets = append(emptyMarkets, path.Market1)
			}
		}
		if len(market2Data.Asks) == 0 || len(market2Data.Bids) == 0 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market2) {
				hasCriticalEmpty = true
			} else {
				emptyMarkets = append(emptyMarkets, path.Market2)
			}
		}
		if len(market3Data.Asks) == 0 || len(market3Data.Bids) == 0 {
			if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market3) {
				hasCriticalEmpty = true
			} else {
				emptyMarkets = append(emptyMarkets, path.Market3)
			}
		}

		// If only critical markets are empty, ignore the check and continue processing
		if hasCriticalEmpty && len(emptyMarkets) == 0 {
			// Critical markets assumed to exist - continue processing without logging
		} else if len(emptyMarkets) > 0 {
			// Reduce logging frequency for empty orderbooks to avoid spam
			if detectionStart.UnixNano()%50000 == 0 { // Log only ~0.002% of these
				log.Printf("⚠️  EMPTY ORDERBOOK | Path: %s→%s→%s | Empty markets: %v",
					path.Market1, path.Market2, path.Market3, emptyMarkets)
			}
			return nil
		}
	}

	// Helper function to get price for a market, handling critical markets with assumed prices
	getPrice := func(market string, marketData *OrderBook, direction string) (float64, error) {
		// Check if this is a critical market with empty order book
		if ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(market) {
			if direction == "buy" && len(marketData.Asks) == 0 {
				// Use assumed price for critical market
				if assumedPrice, exists := ae.coinexClient.criticalMarketPriceManager.GetAssumedPrice(market); exists {
					return assumedPrice, nil
				}
			} else if direction == "sell" && len(marketData.Bids) == 0 {
				// Use assumed price for critical market
				if assumedPrice, exists := ae.coinexClient.criticalMarketPriceManager.GetAssumedPrice(market); exists {
					return assumedPrice, nil
				}
			}
		}

		// Use actual order book data
		if direction == "buy" {
			if len(marketData.Asks) == 0 {
				return 0, fmt.Errorf("no asks available for %s", market)
			}
			return strconv.ParseFloat(marketData.Asks[0].Price, 64)
		} else {
			if len(marketData.Bids) == 0 {
				return 0, fmt.Errorf("no bids available for %s", market)
			}
			return strconv.ParseFloat(marketData.Bids[0].Price, 64)
		}
	}

	// Get relevant prices based on direction
	var price1, price2, price3 float64
	var err error

	// Get price for market 1
	price1, err = getPrice(path.Market1, market1Data, path.Direction1)
	if err != nil {
		// Only log for non-critical markets
		if !ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market1) {
			log.Printf("⚠️  PRICE ERROR | Path: %s→%s→%s | Market1: %s - %v",
				path.Market1, path.Market2, path.Market3, path.Market1, err)
		}
		return nil
	}

	// Get price for market 2
	price2, err = getPrice(path.Market2, market2Data, path.Direction2)
	if err != nil {
		// Only log for non-critical markets
		if !ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market2) {
			log.Printf("⚠️  PRICE ERROR | Path: %s→%s→%s | Market2: %s - %v",
				path.Market1, path.Market2, path.Market3, path.Market2, err)
		}
		return nil
	}

	// Get price for market 3
	price3, err = getPrice(path.Market3, market3Data, path.Direction3)
	if err != nil {
		// Only log for non-critical markets
		if !ae.coinexClient.criticalMarketPriceManager.AssumeCriticalMarketExists(path.Market3) {
			log.Printf("⚠️  PRICE ERROR | Path: %s→%s→%s | Market3: %s - %v",
				path.Market1, path.Market2, path.Market3, path.Market3, err)
		}
		return nil
	}

	// Validate prices
	if price1 <= 0 || price2 <= 0 || price3 <= 0 {
		log.Printf("⚠️  INVALID PRICE | Path: %s→%s→%s | Prices: %.8f, %.8f, %.8f",
			path.Market1, path.Market2, path.Market3, price1, price2, price3)
		return nil
	}

	// Calculate round-trip rate based on direction
	var roundTripRate float64
	if path.Direction1 == "buy" && path.Direction2 == "sell" && path.Direction3 == "sell" {
		// USDT → Asset → USDC → USDT
		roundTripRate = (1.0 / price1) * price2 * price3
	} else if path.Direction1 == "sell" && path.Direction2 == "buy" && path.Direction3 == "buy" {
		// USDT → USDC → Asset → USDT (reverse)
		roundTripRate = (1.0 / price3) * (1.0 / price2) * price1
	} else if path.Direction1 == "buy" && path.Direction2 == "buy" && path.Direction3 == "sell" {
		// USDT → USDC → Asset → USDT
		roundTripRate = (1.0 / price1) * (1.0 / price2) * price3
	} else if path.Direction1 == "sell" && path.Direction2 == "sell" && path.Direction3 == "buy" {
		// USDT → Asset → USDC → USDT (reverse)
		roundTripRate = price1 * price2 * (1.0 / price3)
	} else if path.Direction1 == "sell" && path.Direction2 == "buy" && path.Direction3 == "sell" {
		// USDT → USDC → Asset → USDT (alternative reverse)
		roundTripRate = price1 * (1.0 / price2) * price3
	} else {
		log.Printf("⚠️  INVALID DIRECTION | Path: %s→%s→%s | Invalid direction combination: %s, %s, %s",
			path.Market1, path.Market2, path.Market3, path.Direction1, path.Direction2, path.Direction3)
		return nil
	}

	estimatedProfit := roundTripRate - 1.0

	// Calculate fees
	fee1 := ae.getTradingFee(path.Market1)
	fee2 := ae.getTradingFee(path.Market2)
	fee3 := ae.getTradingFee(path.Market3)
	totalFees := fee1 + fee2 + fee3

	netProfit := estimatedProfit - totalFees

	// Determine volume based on order execution settings
	var volume float64
	if ae.config.OrderExecutionSettings.OrderAmountType == "static" {
		// Use static order amount from config
		volume = ae.config.OrderExecutionSettings.StaticOrderAmount
	} else {
		// Use dynamic percentage of available balance
		availableBalance := ae.config.OrderExecutionSettings.AccountBalance
		volume = availableBalance * ae.config.OrderExecutionSettings.DynamicOrderPercentage
	}

	if volume <= 0 {
		log.Printf("⚠️  INVALID VOLUME | Path: %s→%s→%s | Invalid volume: %.6f",
			path.Market1, path.Market2, path.Market3, volume)
		return nil
	}

	// Log calculation results based on profitability
	if netProfit >= ae.config.ProfitThreshold {
		// This is a profitable opportunity
		log.Printf("💰 PROFITABLE PATH | %s→%s→%s | Profit: +%.8f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
			path.Market1, path.Market2, path.Market3,
			netProfit*100, volume, price1, price2, price3)
	} else if netProfit > 0 && netProfit < ae.config.ProfitThreshold {
		// Profitable but below threshold - log occasionally to avoid spam
		if detectionStart.UnixNano()%1000 == 0 { // Log ~0.1% of these
			log.Printf("💡 BELOW THRESHOLD | %s→%s→%s | Profit: +%.8f%% (need: %.8f%%) | Volume: $%.2f",
				path.Market1, path.Market2, path.Market3,
				netProfit*100, ae.config.ProfitThreshold*100, volume)
		}
	} else {
		// This is a losing trade - log occasionally for debugging
		if detectionStart.UnixNano()%10000 == 0 { // Log ~0.01% of these
			log.Printf("📉 LOSING PATH | %s→%s→%s | Loss: %.8f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
				path.Market1, path.Market2, path.Market3,
				netProfit*100, volume, price1, price2, price3)
		}
	}

	return &ArbitrageOpportunity{
		Path:            path,
		EstimatedProfit: estimatedProfit,
		NetProfit:       netProfit,
		Volume:          volume,
		Timestamp:       time.Now(),
		DetectionTime:   time.Since(detectionStart),
		Market1Data:     *market1Data,
		Market2Data:     *market2Data,
		Market3Data:     *market3Data,
		Price1:          price1,
		Price2:          price2,
		Price3:          price3,
	}
}

// getTradingFee returns the trading fee for a market
func (ae *ArbitrageEngine) getTradingFee(market string) float64 {
	if fee, exists := ae.config.TradingFees[market]; exists {
		return fee
	}
	return ae.config.DefaultTradingFee
}

// detectLoop checks if the bot is stuck in a loop and logs warnings
func (ae *ArbitrageEngine) detectLoop() {
	ae.loopMutex.Lock()
	defer ae.loopMutex.Unlock()

	now := time.Now()
	ae.opportunityCount++

	// If we've had more than 10 opportunities in the last 10 seconds, we might be stuck
	if ae.opportunityCount >= 10 && now.Sub(ae.lastOpportunityTime) < 10*time.Second {
		log.Printf("⚠️  LOOP DETECTED | %d opportunities in last 10s | Consider checking balance/configuration", ae.opportunityCount)
		ae.opportunityCount = 0 // Reset counter
	}

	ae.lastOpportunityTime = now
}
