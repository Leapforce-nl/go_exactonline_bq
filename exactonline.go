package exactonline_bq

import (
	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	budget "github.com/Leapforce-nl/go_exactonline_bq/budget"
	financialtransaction "github.com/Leapforce-nl/go_exactonline_bq/financialtransaction"
	salesorder "github.com/Leapforce-nl/go_exactonline_bq/salesorder"
	exactonline "github.com/Leapforce-nl/go_exactonline_new"
)

type ExactOnline struct {
	BudgetClient               *budget.Client
	FinancialTransactionClient *financialtransaction.Client
	SalesOrderClient           *salesorder.Client
}

func NewExactOnline(clientID string, division int, exactOnlineClientID string, exactOnlineClientSecret string, bigQuery *bigquerytools.BigQuery, isLive bool) (*ExactOnline, error) {
	eo, err := exactonline.NewExactOnline(division, exactOnlineClientID, exactOnlineClientSecret, bigQuery, isLive)
	if err != nil {
		return nil, err
	}

	eo_bq := ExactOnline{}

	eo_bq.BudgetClient = budget.NewClient(clientID, eo)
	eo_bq.FinancialTransactionClient = financialtransaction.NewClient(clientID, eo)
	eo_bq.SalesOrderClient = salesorder.NewClient(clientID, eo)

	return &eo_bq, nil

}
