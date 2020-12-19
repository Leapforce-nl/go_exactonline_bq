package exactonline_bq

import (
	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	errortools "github.com/leapforce-libraries/go_errortools"
	budget "github.com/leapforce-libraries/go_exactonline_bq/budget"
	crm "github.com/leapforce-libraries/go_exactonline_bq/crm"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_bq/financialtransaction"
	logistics "github.com/leapforce-libraries/go_exactonline_bq/logistics"
	salesorder "github.com/leapforce-libraries/go_exactonline_bq/salesorder"
	exactonline "github.com/leapforce-libraries/go_exactonline_new"
)

type ExactOnline struct {
	BudgetClient               *budget.Client
	CRMClient                  *crm.Client
	FinancialTransactionClient *financialtransaction.Client
	LogisticsClient            *logistics.Client
	SalesOrderClient           *salesorder.Client
}

func NewExactOnline(division int32, clientID string, exactOnlineClientID string, exactOnlineClientSecret string, bigQuery *bigquerytools.BigQuery) (*ExactOnline, *errortools.Error) {
	eo, e := exactonline.NewExactOnline(division, exactOnlineClientID, exactOnlineClientSecret, bigQuery)
	if e != nil {
		return nil, e
	}

	eo_bq := ExactOnline{}

	eo_bq.BudgetClient = budget.NewClient(clientID, eo)
	eo_bq.CRMClient = crm.NewClient(clientID, eo)
	eo_bq.FinancialTransactionClient = financialtransaction.NewClient(clientID, eo)
	eo_bq.LogisticsClient = logistics.NewClient(clientID, eo)
	eo_bq.SalesOrderClient = salesorder.NewClient(clientID, eo)

	return &eo_bq, nil

}
