package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	eb "github.com/leapforce-libraries/go_exactonline_new/budget"
)

type Client struct {
	clientID    string
	exactOnline *eo.ExactOnline
}

func NewClient(clientID string, exactOnline *eo.ExactOnline) *Client {
	return &Client{clientID, exactOnline}
}

func (c *Client) BudgetClient() *eb.Client {
	return c.exactOnline.BudgetClient
}

func (c *Client) ClientID() string {
	return c.clientID
}
