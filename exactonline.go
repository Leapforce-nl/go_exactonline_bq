package exactonline_bq2

import (
	salesorder "github.com/Leapforce-nl/exactonline_bq2/salesorder"
	exactonline "github.com/Leapforce-nl/go_exactonline2"
)

type ExactOnline struct {
	SalesOrderClient *salesorder.Client
}

func NewExactOnline(exactOnline *exactonline.ExactOnline) *ExactOnline {
	eo := ExactOnline{}

	eo.SalesOrderClient = salesorder.NewClient(exactOnline)

	return &eo
}
