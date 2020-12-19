package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	el "github.com/leapforce-libraries/go_exactonline_new/logistics"
)

type Client struct {
	clientID    string
	exactOnline *eo.ExactOnline
}

func NewClient(clientID string, exactOnline *eo.ExactOnline) *Client {
	return &Client{clientID, exactOnline}
}

func (c *Client) LogisticsClient() *el.Client {
	return c.exactOnline.LogisticsClient
}

func (c *Client) ClientID() string {
	return c.clientID
}
