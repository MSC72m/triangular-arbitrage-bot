package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
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
}

func newHttpClient() *HttpClient {
	return &HttpClient{
		headers: make(map[string]string),
	}
}

func (c *HttpClient) setClient(client *http.Client) *HttpClient {
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

func (c *HttpClient) setheaders(headers map[string]string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = make(map[string]string)
	for k, v := range headers {
		c.headers[k] = v
	}
	return c
}

func (c *HttpClient) getHeaders() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]string)
	for k, v := range c.headers {
		result[k] = v
	}
	return result
}

func (c *HttpClient) mergeHeaders(newHeaders map[string]string) *HttpClient {
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

func (c HttpClient) getQueryString(url string, params map[string]string) string {
	var sb strings.Builder
	sb.WriteString(url)
	if len(params) == 0 {
		return sb.String()
	}
	sb.WriteString("?")
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(urlQueryEscape(params[k]))
		if i < len(keys)-1 {
			sb.WriteString("&")
		}
	}
	return sb.String()
}

func (c *HttpClient) performRequest(params map[string]string, method string) (map[string]interface{}, error) {
	fmt.Println("Performing request", params, method)
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
		fmt.Printf("Failed to unmarshal JSON response: %v\n", err)
		fmt.Printf("Response body: %s\n", string(bodyString))
		return nil, err
	}

	fmt.Println("Response length", len(bodyString))

	return body, nil
}
