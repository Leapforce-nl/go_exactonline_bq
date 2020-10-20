package exactonline_bq2

import (
	eo "github.com/Leapforce-nl/go_exactonline2"
)

type Client struct {
	exactOnline *eo.ExactOnline
}

func NewClient(exactOnline *eo.ExactOnline) *Client {
	return &Client{exactOnline}
}

func (c *Client) ExactOnline() *eo.ExactOnline {
	return c.exactOnline
}
