package network

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/pkg/models"
)

const (
	GET    = "GET"
	POST   = "POST"
	PUT    = "PUT"
	PATCH  = "PATCH"
	DELETE = "DELETE"
)

type HttpClient struct {
	client  *http.Client
	headers map[string]string
	mu      sync.RWMutex
	config  *config.Config

	// WebSocket fields
	wsConn          *websocket.Conn
	wsSubscriptions map[string]bool
	wsDataFeed      chan []byte
	wsUrl           string
	wsConnected     bool

	// Market data cache with proper synchronization
	marketData   map[string]*models.OrderBook
	marketDataMu sync.RWMutex

	// Rate limiting
	rateLimiter *models.RateLimiter

	// WebSocket connection management
	wsReconnectChan chan bool
	wsStopChan      chan bool
	wsReconnecting  bool
	wsLastPong      time.Time
	wsPongTimeout   time.Duration

	// WebSocket background processes
	wsReadDone     chan bool
	wsPingDone     chan bool
	wsHealthDone   chan bool
	reconnectMutex sync.Mutex

	// Retry queue for failed WebSocket markets
	failedMarkets    map[string]time.Time // market -> last failure time
	failedMarketsMu  sync.RWMutex
	retryInterval    time.Duration
	maxRetryAttempts int

	// Flag to indicate if WebSocket data processing has been stopped
	wsProcessingStopped bool

	// Market depths reference for resubscription
	marketDepths *models.MarketDepths

	// Flag to track initial phase (full updates only) - DEPRECATED
	// We now use REST API for initial data and WebSocket with is_full=false for incremental updates
}

func NewHttpClient(config *config.Config) *HttpClient {
	return &HttpClient{
		config:              config,
		headers:             make(map[string]string),
		wsSubscriptions:     make(map[string]bool),
		wsDataFeed:          make(chan []byte, config.WebSocketBufferSize), // Reasonable buffer for incremental updates
		marketData:          make(map[string]*models.OrderBook),
		rateLimiter:         models.NewRateLimiter(config.RateLimitPerSecond, config.RateLimitPerSecond),
		wsUrl:               "wss://socket.coinex.com/v2/spot", // Original CoinEx spot WebSocket URL
		wsReconnectChan:     make(chan bool, 1),
		wsStopChan:          make(chan bool, 1),
		wsPongTimeout:       45 * time.Second, // Reduced from 60s to 45s for faster detection
		wsReadDone:          make(chan bool, 1),
		wsPingDone:          make(chan bool, 1),
		wsHealthDone:        make(chan bool, 1),
		failedMarkets:       make(map[string]time.Time),
		retryInterval:       5 * time.Second, // Default retry interval
		maxRetryAttempts:    3,
		wsProcessingStopped: false,
	}
}

func (c *HttpClient) SetClient(client *http.Client) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client = client
	return c
}

func (c *HttpClient) setInternalHeader(req *http.Request) {
	c.mu.RLock()
	headersCopy := make(map[string]string)
	for k, v := range c.headers {
		headersCopy[k] = v
	}
	c.mu.RUnlock()

	for key, value := range headersCopy {
		req.Header.Set(key, value)
	}
}

func (c *HttpClient) SetHeaders(headers map[string]string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = make(map[string]string)
	for k, v := range headers {
		c.headers[k] = v
	}
	return c
}

func (c *HttpClient) GetHeaders() http.Header {
	c.mu.RLock()
	defer c.mu.RUnlock()
	headers := make(http.Header)
	for k, v := range c.headers {
		headers.Add(k, v)
	}
	return headers
}

func (c *HttpClient) MergeHeaders(newHeaders map[string]string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headers == nil {
		c.headers = make(map[string]string)
	}
	for key, value := range newHeaders {
		c.headers[key] = value
	}
	return c
}

func (c *HttpClient) SetWebSocketUrl(host string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wsUrl = fmt.Sprintf("wss://%s/", host)
	return c
}

func (c *HttpClient) getQueryString(urlStr string, params map[string]string) string {
	var sb strings.Builder
	sb.WriteString(urlStr)

	// Filter out non-query parameters
	queryParams := make(map[string]string)
	for k, v := range params {
		if k != "url" && k != "method" && k != "body" && k != "query" {
			queryParams[k] = v
		}
	}

	if len(queryParams) == 0 {
		return sb.String()
	}

	// Check if URL already has query parameters
	if strings.Contains(urlStr, "?") {
		sb.WriteString("&")
	} else {
		sb.WriteString("?")
	}

	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(url.QueryEscape(queryParams[k]))
		if i < len(keys)-1 {
			sb.WriteString("&")
		}
	}
	return sb.String()
}

func (c *HttpClient) PerformRequest(params map[string]string, method string) (map[string]interface{}, error) {
	// Apply rate limiting
	if !c.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

	var reqBody io.Reader
	url := params["url"]

	if method == GET {
		reqBody = nil
		url = c.getQueryString(url, params)
	} else {
		if body, exists := params["body"]; exists {
			reqBody = strings.NewReader(body)
		}
	}

	if queryParams, exists := params["query"]; exists {
		if queryParams != "" {
			url += "?" + queryParams
		}
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	// Set proper content type for form data
	if method == POST || method == PUT || method == PATCH || method == DELETE {
		if _, hasBody := params["body"]; hasBody {
			// Check if body looks like JSON (starts with { or [)
			bodyStr := params["body"]
			if strings.HasPrefix(strings.TrimSpace(bodyStr), "{") || strings.HasPrefix(strings.TrimSpace(bodyStr), "[") {
				req.Header.Set("Content-Type", "application/json")
			} else {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		}
	}

	c.setInternalHeader(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyString, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var body map[string]interface{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		log.Printf("Failed to unmarshal JSON response: %v", err)
		log.Printf("Response body: %s", string(bodyString))
		return nil, err
	}

	return body, nil
}

// DoWithRateLimit executes an HTTP request with rate limiting
func (c *HttpClient) DoWithRateLimit(req *http.Request) (*http.Response, error) {
	// Apply rate limiting
	if !c.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

	// Set internal headers
	c.setInternalHeader(req)

	// Execute the request using the underlying http.Client
	return c.client.Do(req)
}

// Request performs an HTTP request and returns the raw response body
func (c *HttpClient) Request(method, url string, extraHeaders map[string]string) ([]byte, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	c.setInternalHeader(req)
	for key, value := range extraHeaders {
		req.Header.Set(key, value)
	}

	if !c.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// DisableRateLimiting temporarily disables rate limiting for bulk operations
func (c *HttpClient) DisableRateLimiting() {
	c.rateLimiter.Disable()
}

// EnableRateLimiting re-enables rate limiting after bulk operations
func (c *HttpClient) EnableRateLimiting() {
	c.rateLimiter.Enable()
}
