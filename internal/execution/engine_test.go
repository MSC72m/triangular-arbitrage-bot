package execution

import (
	"math"
	"testing"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/internal/exchange"
	"triangular-arbitrage-bot/internal/network"
	"triangular-arbitrage-bot/pkg/models"
)

func createTestConfig() *config.Config {
	cfg := config.DefaultConfig()
	cfg.APIKey = "testapikey12345678"
	cfg.SecretID = "testsecret12345678"
	cfg.SimulationMode = true
	cfg.QuoteCurrencies = []string{"USDT", "USDC"}
	cfg.CriticalMarkets = []string{"USDCUSDT"}
	cfg.OrderExecutionSettings.StaticOrderAmount = 3.5
	cfg.OrderExecutionSettings.DynamicOrderPercentage = 0.25
	cfg.OrderExecutionSettings.AccountBalance = 100.0
	cfg.OrderExecutionSettings.MaxOrderAmount = 50.0
	cfg.OrderExecutionSettings.OrderAmountType = "static"
	cfg.DefaultTradingFee = 0.002
	cfg.TradingFees = map[string]float64{
		"USDT": 0.002,
		"USDC": 0.002,
	}
	cfg.ArbitragePriceModifers = config.PriceModifers{
		BasePriceModifer: 0,
		BuyPriceModifer:  0,
		SellPriceModifer: 0,
	}
	return cfg
}

func createTestEngine(cfg *config.Config) *ArbitrageEngine {
	marketDepths := models.NewMarketDepths()
	metrics := models.NewMetrics()
	httpClient := network.NewHttpClient(cfg)
	coinexClient := exchange.NewCoinexClient(httpClient, cfg)
	return NewArbitrageEngine(cfg, marketDepths, metrics, coinexClient)
}

func TestNewArbitrageEngine(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	if engine == nil {
		t.Fatal("NewArbitrageEngine returned nil")
	}
	if engine.config != cfg {
		t.Error("config not set correctly")
	}
	if engine.marketDepths == nil {
		t.Error("marketDepths is nil")
	}
	if engine.metrics == nil {
		t.Error("metrics is nil")
	}
	if engine.executedOpportunities == nil {
		t.Error("executedOpportunities map is nil")
	}
}

func TestCalculateProfitProfitable(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "HBAR",
		Asset2:     "USDC",
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// prices with a 2% favorable cross-rate
	price1 := 0.1   // HBAR/USDT buy price
	price2 := 0.102 // HBAR/USDC sell price (2% higher)
	price3 := 1.0   // USDC/USDT sell price

	snapshot := map[string]*models.OrderBook{}

	roundTripRate, profitPercentage := engine.calculateProfit(path, price1, price2, price3, time.Now(), snapshot)

	// With fees: asset1 = 3.5 / 0.1  0.998 = 34.93
	// usdc = 34.93  0.102  0.998 = 3.555734
	// final = 3.555734  1.0  0.998 = 3.548622
	// netProfit = 3.548622 - 3.5 = 0.048622
	// profitPercentage = 0.048622 / 3.5 = 0.01389...
	if profitPercentage <= 0 {
		t.Errorf("expected positive profit, got %f", profitPercentage)
	}
	if roundTripRate <= 1.0 {
		t.Errorf("expected roundTripRate > 1.0, got %f", roundTripRate)
	}

	// Verify the calculation manually
	initialUSDT := cfg.OrderExecutionSettings.StaticOrderAmount
	fee := cfg.DefaultTradingFee
	expectedAsset1 := initialUSDT / price1 * (1.0 - fee)
	expectedUSDC := expectedAsset1 * price2 * (1.0 - fee)
	expectedFinal := expectedUSDC * price3 * (1.0 - fee)
	expectedRate := expectedFinal / initialUSDT
	expectedProfit := (expectedFinal - initialUSDT) / initialUSDT

	if math.Abs(roundTripRate-expectedRate) > 1e-10 {
		t.Errorf("roundTripRate mismatch: got %f, expected %f", roundTripRate, expectedRate)
	}
	if math.Abs(profitPercentage-expectedProfit) > 1e-10 {
		t.Errorf("profitPercentage mismatch: got %f, expected %f", profitPercentage, expectedProfit)
	}
}

func TestCalculateProfitUnprofitable(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "HBAR",
		Asset2:     "USDC",
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// Equal prices - no arbitrage spread, only fees eat into profit
	price1 := 0.1
	price2 := 0.1
	price3 := 1.0

	snapshot := map[string]*models.OrderBook{}

	roundTripRate, profitPercentage := engine.calculateProfit(path, price1, price2, price3, time.Now(), snapshot)

	if profitPercentage >= 0 {
		t.Errorf("expected negative profit for equal prices, got %f", profitPercentage)
	}
	if roundTripRate >= 1.0 {
		t.Errorf("expected roundTripRate < 1.0 for unprofitable path, got %f", roundTripRate)
	}

	// Verify: (1-0.002)^3 ≈ 0.994012
	expectedRate := (1.0 - 0.002) * (1.0 - 0.002) * (1.0 - 0.002)
	if math.Abs(roundTripRate-expectedRate) > 1e-10 {
		t.Errorf("roundTripRate mismatch: got %f, expected %f", roundTripRate, expectedRate)
	}
}

func TestCalculateProfitBreakEven(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "HBAR",
		Asset2:     "USDC",
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// With zero fees, round trip should be exactly 1.0
	cfg.DefaultTradingFee = 0
	cfg.TradingFees = map[string]float64{}

	price1 := 0.1
	price2 := 0.1
	price3 := 1.0

	snapshot := map[string]*models.OrderBook{}

	roundTripRate, profitPercentage := engine.calculateProfit(path, price1, price2, price3, time.Now(), snapshot)

	if math.Abs(roundTripRate-1.0) > 1e-10 {
		t.Errorf("expected roundTripRate 1.0 with zero fees, got %f", roundTripRate)
	}
	if math.Abs(profitPercentage) > 1e-10 {
		t.Errorf("expected zero profit with zero fees and equal prices, got %f", profitPercentage)
	}
}

func TestCalculateProfitHighSpread(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "BTC",
		Asset2:     "USDC",
		Market1:    "BTCUSDT",
		Market2:    "BTCUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// 10% favorable spread
	price1 := 50000.0
	price2 := 55000.0
	price3 := 1.0

	snapshot := map[string]*models.OrderBook{}

	_, profitPercentage := engine.calculateProfit(path, price1, price2, price3, time.Now(), snapshot)

	// Should be highly profitable
	if profitPercentage <= 0 {
		t.Errorf("expected significant positive profit with 10%% spread, got %f", profitPercentage)
	}

	// With 10% spread and 3x 0.2% fees, profit should be roughly 10% - 0.6% ≈ 9.4%
	if profitPercentage < 0.05 {
		t.Errorf("expected profit > 5%% with 10%% spread, got %f%%", profitPercentage*100)
	}
}

func TestCalculateVolumeStatic(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.OrderAmountType = "static"
	cfg.OrderExecutionSettings.StaticOrderAmount = 5.0
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1: "HBARUSDT",
		Market2: "HBARUSDC",
		Market3: "USDCUSDT",
	}

	snapshot := map[string]*models.OrderBook{}

	volume := engine.calculateVolume(path, snapshot)
	if volume != 5.0 {
		t.Errorf("expected static volume 5.0, got %f", volume)
	}
}

func TestCalculateVolumeDynamic(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.OrderAmountType = "dynamic"
	cfg.OrderExecutionSettings.AccountBalance = 100.0
	cfg.OrderExecutionSettings.DynamicOrderPercentage = 0.25
	cfg.OrderExecutionSettings.MaxOrderAmount = 50.0
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "HBAR",
		Asset2:     "USDC",
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// Create order books with sufficient depth
	snapshot := map[string]*models.OrderBook{
		"HBARUSDT": {
			Bids: []models.Depth{
				{Price: "0.1", Amount: "1000"},
			},
			Asks: []models.Depth{
				{Price: "0.11", Amount: "1000"},
			},
		},
		"HBARUSDC": {
			Bids: []models.Depth{
				{Price: "0.11", Amount: "1000"},
			},
			Asks: []models.Depth{
				{Price: "0.12", Amount: "1000"},
			},
		},
		"USDCUSDT": {
			Bids: []models.Depth{
				{Price: "0.99", Amount: "10000"},
			},
			Asks: []models.Depth{
				{Price: "1.01", Amount: "10000"},
			},
		},
	}

	volume := engine.calculateVolume(path, snapshot)

	// balanceVolume = 100  0.25 = 25.0
	// Should be min(25.0, maxVolumeFromDepth, 50.0) = min(25.0, maxVolumeFromDepth)
	if volume <= 0 {
		t.Errorf("expected positive volume, got %f", volume)
	}
	if volume > 25.0 {
		t.Errorf("expected volume <= 25.0 (balance limit), got %f", volume)
	}
}

func TestCalculateVolumeDynamicMinFloor(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.OrderAmountType = "dynamic"
	cfg.OrderExecutionSettings.AccountBalance = 0.0001
	cfg.OrderExecutionSettings.DynamicOrderPercentage = 0.25
	cfg.OrderExecutionSettings.MaxOrderAmount = 0.0001
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1: "HBARUSDT",
		Market2: "HBARUSDC",
		Market3: "USDCUSDT",
	}

	snapshot := map[string]*models.OrderBook{}

	volume := engine.calculateVolume(path, snapshot)
	if volume < 0.001 {
		t.Errorf("expected volume >= 0.001 minimum floor, got %f", volume)
	}
}

func TestGetExecuteableData(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.StaticOrderAmount = 3.5
	cfg.ArbitragePriceModifers = config.PriceModifers{
		BuyPriceModifer:  0,
		SellPriceModifer: 0,
	}
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		BaseAsset:  "USDT",
		Asset1:     "HBAR",
		Asset2:     "USDC",
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// Create order books with sufficient volume at first level
	market1Data := &models.OrderBook{
		Market: "HBARUSDT",
		Bids: []models.Depth{
			{Price: "0.100", Amount: "100"}, // volume = 0.100  100 = 10 >= 3.5
			{Price: "0.099", Amount: "100"},
			{Price: "0.098", Amount: "100"},
		},
		Asks: []models.Depth{
			{Price: "0.101", Amount: "100"},
			{Price: "0.102", Amount: "100"},
		},
	}

	market2Data := &models.OrderBook{
		Market: "HBARUSDC",
		Bids: []models.Depth{
			{Price: "0.101", Amount: "100"},
		},
		Asks: []models.Depth{
			{Price: "0.102", Amount: "100"}, // volume = 0.102  100 = 10.2 >= 3.5
			{Price: "0.103", Amount: "100"},
		},
	}

	market3Data := &models.OrderBook{
		Market: "USDCUSDT",
		Bids: []models.Depth{
			{Price: "0.999", Amount: "1000"},
		},
		Asks: []models.Depth{
			{Price: "0.999", Amount: "1000"}, // volume = 0.999  1000 = 999 >= 3.5
			{Price: "1.001", Amount: "1000"},
		},
	}

	price1, price2, price3, err := engine.getExecuteableData(market1Data, market2Data, market3Data, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With zero modifiers:
	// buy direction uses bids: first bid has volume, returns bids[1:] → price = 0.099
	// sell direction uses asks: first ask has volume, returns asks[1:] → price2 = 0.103, price3 = 1.001
	if price1 != 0.099 {
		t.Errorf("expected price1 0.099, got %f", price1)
	}
	if price2 != 0.103 {
		t.Errorf("expected price2 0.103, got %f", price2)
	}
	if price3 != 1.001 {
		t.Errorf("expected price3 1.001, got %f", price3)
	}
}

func TestGetExecuteableDataInsufficientVolume(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.StaticOrderAmount = 1000.0 // High amount
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	market1Data := &models.OrderBook{
		Market: "HBARUSDT",
		Bids: []models.Depth{
			{Price: "0.1", Amount: "1"}, // volume = 0.1 < 1000
		},
		Asks: []models.Depth{
			{Price: "0.11", Amount: "1"},
		},
	}

	market2Data := &models.OrderBook{
		Market: "HBARUSDC",
		Asks: []models.Depth{
			{Price: "0.11", Amount: "1"},
		},
		Bids: []models.Depth{
			{Price: "0.1", Amount: "1"},
		},
	}

	market3Data := &models.OrderBook{
		Market: "USDCUSDT",
		Asks: []models.Depth{
			{Price: "1.0", Amount: "1"},
		},
		Bids: []models.Depth{
			{Price: "0.99", Amount: "1"},
		},
	}

	_, _, _, err := engine.getExecuteableData(market1Data, market2Data, market3Data, path)
	if err == nil {
		t.Fatal("expected error for insufficient volume")
	}
}

func TestGetExecuteableDataWithBuyModifier(t *testing.T) {
	cfg := createTestConfig()
	cfg.OrderExecutionSettings.StaticOrderAmount = 3.5
	cfg.ArbitragePriceModifers = config.PriceModifers{
		BuyPriceModifer:  0.01, // 1% premium
		SellPriceModifer: 0,
	}
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	market1Data := &models.OrderBook{
		Market: "HBARUSDT",
		Bids: []models.Depth{
			{Price: "0.100", Amount: "100"},
			{Price: "0.099", Amount: "100"},
		},
		Asks: []models.Depth{
			{Price: "0.101", Amount: "100"},
		},
	}

	market2Data := &models.OrderBook{
		Market: "HBARUSDC",
		Asks: []models.Depth{
			{Price: "0.102", Amount: "100"},
			{Price: "0.103", Amount: "100"},
		},
		Bids: []models.Depth{
			{Price: "0.101", Amount: "100"},
		},
	}

	market3Data := &models.OrderBook{
		Market: "USDCUSDT",
		Asks: []models.Depth{
			{Price: "0.999", Amount: "1000"},
			{Price: "1.001", Amount: "1000"},
		},
		Bids: []models.Depth{
			{Price: "0.999", Amount: "1000"},
		},
	}

	price1, _, _, err := engine.getExecuteableData(market1Data, market2Data, market3Data, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// buy direction: bids[1].Price = 0.099, with 1% modifier: 0.099  1.01 = 0.09999
	expected := 0.099 * 1.01
	if math.Abs(price1-expected) > 1e-10 {
		t.Errorf("expected price1 %f with buy modifier, got %f", expected, price1)
	}
}

func TestDiscoverTriangularArbitrageCycles(t *testing.T) {
	cfg := createTestConfig()
	cfg.QuoteCurrencies = []string{"USDT", "USDC"}
	cfg.CriticalMarkets = []string{"USDCUSDT"}
	engine := createTestEngine(cfg)

	markets := []string{
		"HBARUSDT",
		"HBARUSDC",
		"USDCUSDT",
	}

	paths := engine.discoverTriangularArbitrageCycles(markets)

	if len(paths) != 1 {
		t.Fatalf("expected 1 triangular path, got %d", len(paths))
	}

	path := paths[0]
	if path.BaseAsset != "USDT" {
		t.Errorf("expected BaseAsset 'USDT', got '%s'", path.BaseAsset)
	}
	if path.Asset1 != "HBAR" {
		t.Errorf("expected Asset1 'HBAR', got '%s'", path.Asset1)
	}
	if path.Asset2 != "USDC" {
		t.Errorf("expected Asset2 'USDC', got '%s'", path.Asset2)
	}
	if path.Market1 != "HBARUSDT" {
		t.Errorf("expected Market1 'HBARUSDT', got '%s'", path.Market1)
	}
	if path.Market2 != "HBARUSDC" {
		t.Errorf("expected Market2 'HBARUSDC', got '%s'", path.Market2)
	}
	if path.Market3 != "USDCUSDT" {
		t.Errorf("expected Market3 'USDCUSDT', got '%s'", path.Market3)
	}
	if path.Direction1 != "buy" {
		t.Errorf("expected Direction1 'buy', got '%s'", path.Direction1)
	}
	if path.Direction2 != "sell" {
		t.Errorf("expected Direction2 'sell', got '%s'", path.Direction2)
	}
	if path.Direction3 != "sell" {
		t.Errorf("expected Direction3 'sell', got '%s'", path.Direction3)
	}
}

func TestDiscoverTriangularArbitrageCyclesMultipleAssets(t *testing.T) {
	cfg := createTestConfig()
	cfg.QuoteCurrencies = []string{"USDT", "USDC"}
	cfg.CriticalMarkets = []string{"USDCUSDT"}
	engine := createTestEngine(cfg)

	markets := []string{
		"HBARUSDT",
		"HBARUSDC",
		"BTCUSDT",
		"BTCUSDC",
		"ETHUSDT",
		"ETHUSDC",
		"USDCUSDT",
	}

	paths := engine.discoverTriangularArbitrageCycles(markets)

	// Should find 3 triangular paths (HBAR, BTC, ETH)
	if len(paths) != 3 {
		t.Fatalf("expected 3 triangular paths, got %d", len(paths))
	}

	assetSet := make(map[string]bool)
	for _, p := range paths {
		assetSet[p.Asset1] = true
		if p.BaseAsset != "USDT" {
			t.Errorf("expected BaseAsset 'USDT', got '%s'", p.BaseAsset)
		}
		if p.Market3 != "USDCUSDT" {
			t.Errorf("expected Market3 'USDCUSDT', got '%s'", p.Market3)
		}
	}

	for _, expectedAsset := range []string{"HBAR", "BTC", "ETH"} {
		if !assetSet[expectedAsset] {
			t.Errorf("expected asset '%s' not found in paths", expectedAsset)
		}
	}
}

func TestDiscoverTriangularArbitrageCyclesNoDirectPair(t *testing.T) {
	cfg := createTestConfig()
	cfg.QuoteCurrencies = []string{"USDT", "USDC"}
	cfg.CriticalMarkets = []string{"BTCUSDT"} // No USDCUSDT in critical markets
	engine := createTestEngine(cfg)

	markets := []string{
		"HBARUSDT",
		"HBARUSDC",
		// No USDCUSDT market available
	}

	paths := engine.discoverTriangularArbitrageCycles(markets)

	// Without USDCUSDT, no triangular paths can be formed
	if len(paths) != 0 {
		t.Errorf("expected 0 paths without direct pair, got %d", len(paths))
	}
}

func TestDiscoverTriangularArbitrageCyclesEmpty(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	paths := engine.discoverTriangularArbitrageCycles([]string{})
	if len(paths) != 0 {
		t.Errorf("expected 0 paths for empty markets, got %d", len(paths))
	}
}

func TestGetTradingFee(t *testing.T) {
	cfg := createTestConfig()
	cfg.TradingFees = map[string]float64{
		"BTCUSDT": 0.001,
	}
	cfg.DefaultTradingFee = 0.003
	engine := createTestEngine(cfg)

	// Market-specific fee
	fee := engine.getTradingFee("BTCUSDT")
	if fee != 0.001 {
		t.Errorf("expected market-specific fee 0.001, got %f", fee)
	}

	// Default fee
	fee = engine.getTradingFee("ETHUSDT")
	if fee != 0.003 {
		t.Errorf("expected default fee 0.003, got %f", fee)
	}
}

func TestIsOpportunityAlreadyExecuted(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	opportunity := models.ArbitrageOpportunity{
		Path: models.TriangularPath{
			Market1: "HBARUSDT",
			Market2: "HBARUSDC",
			Market3: "USDCUSDT",
		},
		Price1: 0.1,
		Price2: 0.102,
		Price3: 1.0,
	}

	// Should not be executed yet
	if engine.isOpportunityAlreadyExecuted(opportunity) {
		t.Fatal("opportunity should not be marked as executed")
	}

	// Mark as executed
	engine.markOpportunityAsExecuted(opportunity)

	// Should be executed now
	if !engine.isOpportunityAlreadyExecuted(opportunity) {
		t.Fatal("opportunity should be marked as executed")
	}
}

func TestIsOpportunityAlreadyExecutedDifferentPrices(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	opportunity1 := models.ArbitrageOpportunity{
		Path: models.TriangularPath{
			Market1: "HBARUSDT",
			Market2: "HBARUSDC",
			Market3: "USDCUSDT",
		},
		Price1: 0.1,
		Price2: 0.102,
		Price3: 1.0,
	}

	opportunity2 := models.ArbitrageOpportunity{
		Path: models.TriangularPath{
			Market1: "HBARUSDT",
			Market2: "HBARUSDC",
			Market3: "USDCUSDT",
		},
		Price1: 0.101, // Different price
		Price2: 0.102,
		Price3: 1.0,
	}

	engine.markOpportunityAsExecuted(opportunity1)

	// Same opportunity should be detected
	if !engine.isOpportunityAlreadyExecuted(opportunity1) {
		t.Fatal("same opportunity should be detected")
	}

	// Different prices should NOT be detected as already executed
	if engine.isOpportunityAlreadyExecuted(opportunity2) {
		t.Fatal("different prices should not be detected as same opportunity")
	}
}

func TestUpdateTriangularPaths(t *testing.T) {
	cfg := createTestConfig()
	cfg.QuoteCurrencies = []string{"USDT", "USDC"}
	cfg.CriticalMarkets = []string{"USDCUSDT"}
	engine := createTestEngine(cfg)

	// First, store market data so they're "available"
	engine.marketDepths.Store("HBARUSDT", &models.OrderBook{
		Bids: []models.Depth{{Price: "0.1", Amount: "100"}},
		Asks: []models.Depth{{Price: "0.11", Amount: "100"}},
	})
	engine.marketDepths.Store("HBARUSDC", &models.OrderBook{
		Bids: []models.Depth{{Price: "0.11", Amount: "100"}},
		Asks: []models.Depth{{Price: "0.12", Amount: "100"}},
	})
	engine.marketDepths.Store("USDCUSDT", &models.OrderBook{
		Bids: []models.Depth{{Price: "0.99", Amount: "10000"}},
		Asks: []models.Depth{{Price: "1.01", Amount: "10000"}},
	})

	allMarkets := []string{"HBARUSDT", "HBARUSDC", "USDCUSDT"}
	completeAssets := []string{"HBAR"}

	engine.UpdateTriangularPaths(allMarkets, completeAssets)

	engine.pathsLock.RLock()
	pathCount := len(engine.triangularPaths)
	engine.pathsLock.RUnlock()

	if pathCount != 1 {
		t.Errorf("expected 1 triangular path after update, got %d", pathCount)
	}
}

func TestGetExecuteableDataNilMarketData(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	// Pass nil for market1Data
	_, _, _, err := engine.getExecuteableData(nil, &models.OrderBook{}, &models.OrderBook{}, path)
	if err == nil {
		t.Fatal("expected error for nil market data")
	}
}

func TestGetExecuteableDataEmptyOrderBook(t *testing.T) {
	cfg := createTestConfig()
	engine := createTestEngine(cfg)

	path := models.TriangularPath{
		Market1:    "HBARUSDT",
		Market2:    "HBARUSDC",
		Market3:    "USDCUSDT",
		Direction1: "buy",
		Direction2: "sell",
		Direction3: "sell",
	}

	emptyBook := &models.OrderBook{
		Bids: []models.Depth{},
		Asks: []models.Depth{},
	}

	_, _, _, err := engine.getExecuteableData(emptyBook, emptyBook, emptyBook, path)
	if err == nil {
		t.Fatal("expected error for empty order book")
	}
}
