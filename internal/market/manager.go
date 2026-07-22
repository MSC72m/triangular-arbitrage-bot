package market

import (
	"encoding/json"
	"fmt"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/internal/network"
	"triangular-arbitrage-bot/pkg/models"
)

// Manager handles market data processing and order book management
type Manager struct {
	config       *config.Config
	httpClient   *network.HttpClient
	marketDepths *models.MarketDepths
}

// NewManager creates a new market data manager
func NewManager(config *config.Config, httpClient *network.HttpClient, marketDepths *models.MarketDepths) *Manager {
	return &Manager{
		config:       config,
		httpClient:   httpClient,
		marketDepths: marketDepths,
	}
}

// ProcessWebSocketMessage processes incoming WebSocket market data
func (m *Manager) ProcessWebSocketMessage(msg []byte) error {
	var wsMsg struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}

	if err := json.Unmarshal(msg, &wsMsg); err != nil {
		return fmt.Errorf("failed to unmarshal WebSocket message: %w", err)
	}

	switch wsMsg.Method {
	case "state.update":
		return m.processStateUpdate(wsMsg.Params)
	case "depth.update":
		return m.processDepthUpdate(wsMsg.Params)
	default:
		// Ignore other message types for now
		return nil
	}
}

// processStateUpdate processes state update messages
func (m *Manager) processStateUpdate(params json.RawMessage) error {
	var stateUpdate struct {
		Market string `json:"market"`
		Data   struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
		} `json:"data"`
	}

	if err := json.Unmarshal(params, &stateUpdate); err != nil {
		return fmt.Errorf("failed to unmarshal state update: %w", err)
	}

	// Convert array format to OrderBook
	OrderBook := &models.OrderBook{
		Market:    stateUpdate.Market,
		Timestamp: time.Now(),
	}

	// Process asks (sell orders)
	for _, ask := range stateUpdate.Data.Asks {
		if len(ask) >= 2 {
			OrderBook.Asks = append(OrderBook.Asks, models.Depth{
				Price:  ask[0],
				Amount: ask[1],
			})
		}
	}

	// Process bids (buy orders)
	for _, bid := range stateUpdate.Data.Bids {
		if len(bid) >= 2 {
			OrderBook.Bids = append(OrderBook.Bids, models.Depth{
				Price:  bid[0],
				Amount: bid[1],
			})
		}
	}

	// Store the updated order book
	m.marketDepths.Store(stateUpdate.Market, OrderBook)
	return nil
}

// processDepthUpdate processes depth update messages
func (m *Manager) processDepthUpdate(params json.RawMessage) error {
	// Similar to state update but with different structure
	return m.processStateUpdate(params)
}

// GetOrderBookREST fetches order book data via REST API
func (m *Manager) GetOrderBookREST(market string) (*models.OrderBook, error) {
	url := fmt.Sprintf("%s/market/depth?market=%s&limit=%d&merge=0",
		m.config.APIBaseURL, market, m.config.OrderBookDepthLimit)

	data, err := m.httpClient.Request("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get order book: %w", err)
	}

	var response struct {
		Code int `json:"code"`
		Data struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
		} `json:"data"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal order book response: %w", err)
	}

	if response.Code != 0 {
		return nil, fmt.Errorf("API error: %s", response.Message)
	}

	OrderBook := &models.OrderBook{
		Market:    market,
		Timestamp: time.Now(),
	}

	// Process asks
	for _, ask := range response.Data.Asks {
		if len(ask) >= 2 {
			OrderBook.Asks = append(OrderBook.Asks, models.Depth{
				Price:  ask[0],
				Amount: ask[1],
			})
		}
	}

	// Process bids
	for _, bid := range response.Data.Bids {
		if len(bid) >= 2 {
			OrderBook.Bids = append(OrderBook.Bids, models.Depth{
				Price:  bid[0],
				Amount: bid[1],
			})
		}
	}

	return OrderBook, nil
}
