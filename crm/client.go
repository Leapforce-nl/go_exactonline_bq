package exactonline_bq

import (
	eo "github.com/Leapforce-nl/go_exactonline_new"
)

type Client struct {
	clientID    string
	exactOnline *eo.ExactOnline
}

func NewClient(clientID string, exactOnline *eo.ExactOnline) *Client {
	return &Client{clientID, exactOnline}
}

func (c *Client) ExactOnline() *eo.ExactOnline {
	return c.exactOnline
}
func (c *Client) ClientID() string {
	return c.clientID
}
