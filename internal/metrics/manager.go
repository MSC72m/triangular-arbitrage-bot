package metrics

import (
	"log"
	"time"

	"triangular-arbitrage-bot/pkg/models"
)

// Manager handles metrics collection and reporting
type Manager struct {
	metrics *models.Metrics
}

// NewManager creates a new metrics manager
func NewManager() *Manager {
	return &Manager{
		metrics: models.NewMetrics(),
	}
}

// GetMetrics returns the current metrics
func (m *Manager) GetMetrics() *models.Metrics {
	return m.metrics
}

// IncrementOpportunities increments opportunities detected counter
func (m *Manager) IncrementOpportunities() {
	m.metrics.IncrementOpportunities()
}

// IncrementTrades increments trades executed counter
func (m *Manager) IncrementTrades() {
	m.metrics.IncrementTrades()
}

// IncrementSuccessfulTrades increments successful trades counter
func (m *Manager) IncrementSuccessfulTrades() {
	m.metrics.IncrementSuccessfulTrades()
}

// IncrementFailedTrades increments failed trades counter
func (m *Manager) IncrementFailedTrades() {
	m.metrics.IncrementFailedTrades()
}

// AddPnL adds profit/loss to total
func (m *Manager) AddPnL(pnl float64) {
	m.metrics.AddPnL(pnl)
}

// AddFees adds fees to total
func (m *Manager) AddFees(fees float64) {
	m.metrics.AddFees(fees)
}

// UpdateDetectionTime updates average detection time
func (m *Manager) UpdateDetectionTime(detectionTime time.Duration) {
	m.metrics.UpdateDetectionTime(detectionTime)
}

// UpdateExecutionTime updates average execution time
func (m *Manager) UpdateExecutionTime(executionTime time.Duration) {
	m.metrics.UpdateExecutionTime(executionTime)
}

// IncrementMessagesProcessed increments messages processed counter
func (m *Manager) IncrementMessagesProcessed() {
	m.metrics.IncrementMessagesProcessed()
}

// IncrementErrorCount increments error counter
func (m *Manager) IncrementErrorCount() {
	m.metrics.IncrementErrorCount()
}

// UpdateBalance updates current balance
func (m *Manager) UpdateBalance(currency string, balance float64) {
	m.metrics.UpdateBalance(currency, balance)
}

// SetActiveConnections sets active connections count
func (m *Manager) SetActiveConnections(count int) {
	m.metrics.SetActiveConnections(count)
}

// LogBasicMetrics logs basic metrics information
func (m *Manager) LogBasicMetrics(marketDepths *models.MarketDepths) {
	m.metrics.mu.RLock()
	defer m.metrics.mu.RUnlock()

	uptime := time.Since(m.metrics.StartTime)
	log.Printf("📊 METRICS - Uptime: %v", uptime.Round(time.Second))
	log.Printf("   Opportunities: %d | Trades: %d | Success: %d | Failed: %d",
		m.metrics.OpportunitiesDetected, m.metrics.TradesExecuted,
		m.metrics.TradesSuccessful, m.metrics.TradesFailed)
	log.Printf("   PnL: $%.2f | Fees: $%.2f | Markets: %d | Messages: %d",
		m.metrics.TotalPnL, m.metrics.TotalFees, len(marketDepths.GetAvailableMarkets()),
		m.metrics.MessagesProcessed)
	if m.metrics.ErrorCount > 0 {
		log.Printf("   Errors: %d", m.metrics.ErrorCount)
	}
}

// LogSimpleMetrics logs simple metrics summary
func (m *Manager) LogSimpleMetrics(marketDepths *models.MarketDepths) {
	m.metrics.mu.RLock()
	defer m.metrics.mu.RUnlock()

	uptime := time.Since(m.metrics.StartTime)
	log.Printf("📊 FINAL METRICS - Uptime: %v", uptime.Round(time.Second))
	log.Printf("   Total Opportunities: %d", m.metrics.OpportunitiesDetected)
	log.Printf("   Trades Executed: %d (Success: %d, Failed: %d)",
		m.metrics.TradesExecuted, m.metrics.TradesSuccessful, m.metrics.TradesFailed)
	log.Printf("   Total PnL: $%.2f | Total Fees: $%.2f", m.metrics.TotalPnL, m.metrics.TotalFees)
	log.Printf("   Messages Processed: %d | Errors: %d", m.metrics.MessagesProcessed, m.metrics.ErrorCount)
	log.Printf("   Active Markets: %d", len(marketDepths.GetAvailableMarkets()))
}
