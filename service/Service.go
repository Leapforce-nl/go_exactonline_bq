package exactonline_bq

import (
	exactonline "github.com/leapforce-libraries/go_exactonline_new"
)

// Service contains ClientID and Insightly APIKey of specific client
//
type Service struct {
	ClientID                string
	ExactOnlineClientID     string
	ExactOnlineClientSecret string
	ExactOnlineDivision     int
	Tables                  string
	ExactOnlineService      *exactonline.Service
}
