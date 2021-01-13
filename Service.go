package exactonline_bq

import (
	errortools "github.com/leapforce-libraries/go_errortools"
	budget "github.com/leapforce-libraries/go_exactonline_bq/budget"
	crm "github.com/leapforce-libraries/go_exactonline_bq/crm"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_bq/financialtransaction"
	logistics "github.com/leapforce-libraries/go_exactonline_bq/logistics"
	salesorder "github.com/leapforce-libraries/go_exactonline_bq/salesorder"
	exactonline "github.com/leapforce-libraries/go_exactonline_new"
	google "github.com/leapforce-libraries/go_google"
)

type Service struct {
	BudgetService               *budget.Service
	CRMService                  *crm.Service
	FinancialTransactionService *financialtransaction.Service
	LogisticsService            *logistics.Service
	SalesOrderService           *salesorder.Service
}

func NewService(division int32, clientID string, exactOnlineServiceID string, exactOnlineServiceSecret string, bigQuery *google.BigQuery) (*Service, *errortools.Error) {
	exactonlineService, e := exactonline.NewService(division, exactOnlineServiceID, exactOnlineServiceSecret, bigQuery)
	if e != nil {
		return nil, e
	}

	exactonlineBQService := Service{}

	exactonlineBQService.BudgetService = budget.NewService(clientID, exactonlineService)
	exactonlineBQService.CRMService = crm.NewService(clientID, exactonlineService)
	exactonlineBQService.FinancialTransactionService = financialtransaction.NewService(clientID, exactonlineService)
	exactonlineBQService.LogisticsService = logistics.NewService(clientID, exactonlineService)
	exactonlineBQService.SalesOrderService = salesorder.NewService(clientID, exactonlineService)

	return &exactonlineBQService, nil

}
