package client

import (
	"fmt"

	"github.com/go-resty/resty/v2"
)

type Client struct {
	SecretKey   string
	restyClient *resty.Client
	baseUrl     string
}

type RespData struct {
	Value Value `json:"value"`
}
type Value struct {
	Value string `json:"value"`
	TTL   int    `json:"ttl"`
	Key   string `json:"key"`
}

func NewClient(baseUrl string) (client *Client, err error) {
	restyClient := resty.New()
	restyClient.SetBaseURL(baseUrl)

	result := make(map[string]string)

	resp, err := restyClient.R().SetResult(&result).Get("/api/v1/client/new")
	if err != nil || resp.StatusCode() != 200 {
		return nil, err
	}

	key := result["secret-key"]

	restyClient.SetHeader("secret-key", key)
	return &Client{
		SecretKey:   key,
		restyClient: restyClient,
		baseUrl:     baseUrl,
	}, nil
}

// Ping
func (c *Client) Ping() (err error) {
	resp, err := c.restyClient.R().Get("/api/v1/client/ping")
	if err != nil || resp.StatusCode() != 200 {
		return err
	}
	return nil
}

// Set
func (c *Client) Set(key string, value string) (err error) {
	r := c.restyClient.R()
	r.SetBody(map[string]string{"value": value})
	url := fmt.Sprintf("/api/v1/store/set/%s", key)
	resp, err := r.Post(url)
	if err != nil || resp.StatusCode() != 200 {
		return err
	}

	return nil
}

// Get
func (c *Client) Get(key string) (v Value, err error) {
	url := fmt.Sprintf("/api/v1/store/get/%s", key)

	respData := new(RespData)
	resp, err := c.restyClient.R().SetResult(respData).Get(url)
	if err != nil || resp.StatusCode() != 200 {
		return Value{}, err
	}
	return respData.Value, nil
}

// Delete
func (c *Client) Delete(key string) (err error) {
	url := fmt.Sprintf("/api/v1/store/delete/%s", key)
	resp, err := c.restyClient.R().Delete(url)
	if err != nil || resp.StatusCode() != 200 {
		return err
	}
	return nil
}

// GetKeys
func (c *Client) GetKeys() (keys []string, err error) {
	resp, err := c.restyClient.R().Get("/api/v1/store/keys")
	if err != nil || resp.StatusCode() != 200 {
		return nil, err
	}

	return nil, nil
}
