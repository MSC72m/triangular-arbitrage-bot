package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Test struct to simulate the FOKOrderTracker
type TestFOKOrderTracker struct {
	OrderID string
	Market  string
	Type    string
	Amount  float64
	Price   float64
}

// Test HTTP client
type TestHttpClient struct {
	apiKey   string
	secretID string
}

func NewTestHttpClient() *TestHttpClient {
	return &TestHttpClient{
		apiKey:   "77F875170F174EF19A696173AD103A63",                 // Replace with your actual API key
		secretID: "FA569E5EF5FB1CCB8CBEC312737A0DE52F497611CC7075E1", // Replace with your actual secret ID
	}
}

// generateRESTSignature creates HMAC-SHA256 signature for CoinEx API v2 REST endpoints
func (c *TestHttpClient) generateRESTSignature(method, requestPath, queryString, body string, timestamp int64) string {
	// Create the string to sign for v2 REST API
	// Format: method + request_path + body + timestamp
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization

	// Build the full request path including query string if present
	fullRequestPath := requestPath
	if queryString != "" {
		fullRequestPath = requestPath + "?" + queryString
	}

	// Create the string to sign: method + request_path + body + timestamp
	var stringToSign string
	if body != "" {
		// For POST/PUT requests with body, include the body as a string literal
		stringToSign = method + fullRequestPath + body + strconv.FormatInt(timestamp, 10)
	} else {
		// For GET/DELETE requests without body
		stringToSign = method + fullRequestPath + strconv.FormatInt(timestamp, 10)
	}

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	// Debug logging
	log.Printf("🔐 REST SIGNATURE DEBUG | Method: %s | Path: %s | Query: %s | Body: %s | Timestamp: %d",
		method, requestPath, queryString, body, timestamp)
	log.Printf("🔐 REST SIGNATURE DEBUG | Full request path: %s", fullRequestPath)
	log.Printf("🔐 REST SIGNATURE DEBUG | String to sign: %s", stringToSign)
	log.Printf("🔐 REST SIGNATURE DEBUG | Signature: %s", signature)
	log.Printf("🔐 REST SIGNATURE DEBUG | Secret key length: %d", len(c.secretID))

	return signature
}

// Test pollRealOrderStatus function
func (c *TestHttpClient) testPollRealOrderStatus(tracker *TestFOKOrderTracker) error {
	log.Printf("🧪 TESTING POLL FUNCTION | ID: %s | Market: %s", tracker.OrderID, tracker.Market)

	// Poll up to 4 times
	for i := 0; i < 4; i++ {
		timestamp := time.Now().UnixMilli()
		method := "GET"
		requestPath := "/v2/spot/order-status" // Keep /v2/ as requested

		// Build query string for order status API - MUST be sorted alphabetically
		queryParams := map[string]string{
			"market":   tracker.Market,
			"order_id": tracker.OrderID,
		}

		// Sort parameters alphabetically (required for signature)
		keys := make([]string, 0, len(queryParams))
		for k := range queryParams {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Build query string in sorted order (no URL encoding for signature)
		var queryParts []string
		for _, key := range keys {
			queryParts = append(queryParts, key+"="+queryParams[key])
		}
		queryString := strings.Join(queryParts, "&")

		log.Printf("🔍 POLLING ORDER | ID: %s | Attempt: %d/4", tracker.OrderID, i+1)
		log.Printf("🔍 POLL DEBUG | Market: %s | OrderID: %s | Query: %s", tracker.Market, tracker.OrderID, queryString)

		// For GET requests: method + request_path + timestamp (no body)
		// Pass query string separately to signature generation
		signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)

		// Set authentication headers
		authHeaders := map[string]string{
			"X-COINEX-KEY":       c.apiKey,
			"X-COINEX-SIGN":      signature,
			"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
		}

		log.Printf("🔍 AUTH HEADERS | Key: %s | Sign: %s | Timestamp: %s",
			authHeaders["X-COINEX-KEY"],
			authHeaders["X-COINEX-SIGN"],
			authHeaders["X-COINEX-TIMESTAMP"])

		// Prepare request parameters
		requestURL := "https://api.coinex.com" + requestPath + "?" + queryString

		log.Printf("🔍 POLL REQUEST | URL: %s", requestURL)

		// Actually send the HTTP request
		req, err := http.NewRequest(method, requestURL, nil)
		if err != nil {
			log.Printf("❌ FAILED TO CREATE REQUEST | Error: %v", err)
			continue
		}

		// Add headers
		for key, value := range authHeaders {
			req.Header.Set(key, value)
		}

		// Send request
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("❌ HTTP REQUEST FAILED | Error: %v", err)
			continue
		}
		defer resp.Body.Close()

		// Read response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ FAILED TO READ RESPONSE | Error: %v", err)
			continue
		}

		log.Printf("📥 RESPONSE STATUS | %s", resp.Status)
		log.Printf("📥 RESPONSE BODY | %s", string(body))

		// Parse response
		var response map[string]interface{}
		if err := json.Unmarshal(body, &response); err != nil {
			log.Printf("❌ FAILED TO PARSE RESPONSE | Error: %v", err)
			continue
		}

		// Check response
		if code, ok := response["code"].(float64); !ok || code != 0 {
			message, _ := response["message"].(string)
			log.Printf("⚠️ API ERROR | Code: %.0f | Message: %s", code, message)

			// If we get "Order not found" instead of "Signature Incorrect", that means signature is working!
			if message == "Order not found" || strings.Contains(message, "not found") {
				log.Printf("✅ SIGNATURE IS WORKING! | Got expected 'not found' error instead of signature error")
				return nil
			} else if strings.Contains(message, "Signature") {
				log.Printf("❌ SIGNATURE STILL INCORRECT | Error: %s", message)
			}
		} else {
			log.Printf("✅ API CALL SUCCESSFUL | Code: %.0f", code)
		}

		// For testing, we'll just log and return after first attempt
		if i == 0 {
			log.Printf("🧪 TEST COMPLETE | First attempt completed")
			return nil
		}

		// If not filled, wait a bit before next attempt
		if i < 3 { // Don't sleep after last attempt
			time.Sleep(200 * time.Millisecond) // 200ms between attempts
		}
	}

	log.Printf("❌ TEST FAILED | All attempts completed")
	return fmt.Errorf("test completed")
}

func main() {
	log.Printf("🧪 STARTING POLL FUNCTION TEST")

	// Create test client
	client := NewTestHttpClient()

	// Create test tracker with fake order ID
	tracker := &TestFOKOrderTracker{
		OrderID: "999999999999", // Fake order ID
		Market:  "BTCUSDT",      // Real market
		Type:    "buy",
		Amount:  0.001,
		Price:   50000.0,
	}

	// Test the polling function
	err := client.testPollRealOrderStatus(tracker)
	if err != nil {
		log.Printf("❌ TEST FAILED | Error: %v", err)
	} else {
		log.Printf("✅ TEST COMPLETED | Check logs above for results")
	}

	log.Printf("🧪 TEST ENDED")
}
