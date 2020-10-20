package exactonline_bq

import (
	exactonline "github.com/Leapforce-nl/go_exactonline_new"
)

// Client contains ClientID and Insightly APIKey of specific client
//
type Client struct {
	ClientID                string
	ExactOnlineClientID     string
	ExactOnlineClientSecret string
	ExactOnlineDivision     int
	Tables                  string
	ExactOnline             *exactonline.ExactOnline
}
