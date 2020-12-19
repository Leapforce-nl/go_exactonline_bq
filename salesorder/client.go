package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	es "github.com/leapforce-libraries/go_exactonline_new/salesorder"
)

type Client struct {
	clientID    string
	exactOnline *eo.ExactOnline
}

func NewClient(clientID string, exactOnline *eo.ExactOnline) *Client {
	return &Client{clientID, exactOnline}
}

func (c *Client) SalesOrderClient() *es.Client {
	return c.exactOnline.SalesOrderClient
}

func (c *Client) ClientID() string {
	return c.clientID
}
