package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ef "github.com/leapforce-libraries/go_exactonline_new/financialtransaction"
)

type Client struct {
	clientID    string
	exactOnline *eo.ExactOnline
}

func NewClient(clientID string, exactOnline *eo.ExactOnline) *Client {
	return &Client{clientID, exactOnline}
}

func (c *Client) FinancialTransactionClient() *ef.Client {
	return c.exactOnline.FinancialTransactionClient
}

func (c *Client) ClientID() string {
	return c.clientID
}
