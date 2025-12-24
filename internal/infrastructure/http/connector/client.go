package connector

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// HTTPClient là client HTTP có thể tái sử dụng cho các connector
// Dựa trên kiến trúc của Confluent HTTP Source Connector
type HTTPClient struct {
	client *http.Client
	config HTTPClientConfig
}

// NewHTTPClient tạo HTTP client mới với cấu hình
func NewHTTPClient(config HTTPClientConfig) *HTTPClient {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.KeepAlive == 0 {
		config.KeepAlive = 30 * time.Second
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 100
	}
	if config.MaxIdleConnsPerHost == 0 {
		config.MaxIdleConnsPerHost = 10
	}
	if config.RetryMaxAttempts == 0 {
		config.RetryMaxAttempts = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = time.Second
	}

	return &HTTPClient{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   config.DialTimeout,
					KeepAlive: config.KeepAlive,
				}).DialContext,
				MaxIdleConns:        config.MaxIdleConns,
				MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
			},
		},
	}
}

// Do thực hiện HTTP request với retry logic
func (c *HTTPClient) Do(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	var lastErr error
	
	for attempt := 0; attempt <= c.config.RetryMaxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt) * c.config.RetryBackoff
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		resp, err := c.doRequest(ctx, req)
		if err == nil {
			return resp, nil
		}
		
		lastErr = err
		// Chỉ retry với server errors (5xx) hoặc network errors
		if resp != nil && resp.StatusCode < 500 {
			return resp, err
		}
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", c.config.RetryMaxAttempts+1, lastErr)
}

// doRequest thực hiện HTTP request đơn lẻ
func (c *HTTPClient) doRequest(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, req.Body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	// Set timeout nếu có
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
		httpReq = httpReq.WithContext(ctx)
	}

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer httpResp.Body.Close()

	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return &HTTPResponse{
		StatusCode: httpResp.StatusCode,
		Headers:    httpResp.Header,
		Body:       bodyBytes,
		Request:    req,
	}, nil
}

// Get thực hiện GET request
func (c *HTTPClient) Get(ctx context.Context, url string, headers map[string]string) (*HTTPResponse, error) {
	return c.Do(ctx, &HTTPRequest{
		Method:  http.MethodGet,
		URL:     url,
		Headers: headers,
	})
}

// Post thực hiện POST request
func (c *HTTPClient) Post(ctx context.Context, url string, headers map[string]string, body io.Reader) (*HTTPResponse, error) {
	return c.Do(ctx, &HTTPRequest{
		Method:  http.MethodPost,
		URL:     url,
		Headers: headers,
		Body:    body,
	})
}

