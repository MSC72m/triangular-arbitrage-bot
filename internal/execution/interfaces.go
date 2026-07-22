package execution

import "triangular-arbitrage-bot/pkg/models"

// ExchangeClient defines the interface the arbitrage engine needs
// from an exchange client. This enables testing with mocks.
type ExchangeClient interface {
	// PlaceFOKOrder places a Fill-or-Kill order and returns a tracker.
	PlaceFOKOrder(market, orderType string, amount, price float64, resultChan chan<- interface{}) interface{}

	// GetArbitrageMarkets returns tradeable markets and complete asset pairs.
	GetArbitrageMarkets() ([]string, []string, error)

	// TestConnection verifies connectivity to the exchange.
	TestConnection() (string, error)

	// GetOrderExecutionStats returns current execution state.
	GetOrderExecutionStats() (concurrent int, dailySpent float64, availableBalance float64)

	// GetBalance returns the account balance as a string.
	GetBalance() (string, error)
}

// MarketDataStore defines the interface for reading and writing
// order book data. Both models.MarketDepths and mock implementations
// satisfy this interface.
type MarketDataStore interface {
	Store(market string, ob models.OrderBook)
	Load(market string) (models.OrderBook, bool)
	GetAvailableMarkets() []string
	GetSnapshot() map[string]models.OrderBook
}
