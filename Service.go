package exactonline_bq

import (
	errortools "github.com/leapforce-libraries/go_errortools"
	budget "github.com/leapforce-libraries/go_exactonline_bq/budget"
	crm "github.com/leapforce-libraries/go_exactonline_bq/crm"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_bq/financialtransaction"
	logistics "github.com/leapforce-libraries/go_exactonline_bq/logistics"
	salesorder "github.com/leapforce-libraries/go_exactonline_bq/salesorder"
	exactonline "github.com/leapforce-libraries/go_exactonline_new"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
)

type Service struct {
	BudgetService               *budget.Service
	CRMService                  *crm.Service
	FinancialTransactionService *financialtransaction.Service
	LogisticsService            *logistics.Service
	SalesOrderService           *salesorder.Service
}

type ServiceConfig struct {
	ClientID                string
	Division                int32
	ExactOnlineClientID     string
	ExactOnlineClientSecret string
	MaxRetries              *uint
	SecondsBetweenRetries   *uint32
}

func NewService(serviceConfig ServiceConfig, bigQueryService *bigquery.Service) (*Service, *errortools.Error) {
	exactOnlineServiceConfig := exactonline.ServiceConfig{
		Division:     serviceConfig.Division,
		ClientID:     serviceConfig.ExactOnlineClientID,
		ClientSecret: serviceConfig.ExactOnlineClientSecret,
	}

	exactonlineService, e := exactonline.NewService(exactOnlineServiceConfig, bigQueryService)
	if e != nil {
		return nil, e
	}

	exactonlineBQService := Service{}

	exactonlineBQService.BudgetService = budget.NewService(serviceConfig.ClientID, exactonlineService)
	exactonlineBQService.CRMService = crm.NewService(serviceConfig.ClientID, exactonlineService)
	exactonlineBQService.FinancialTransactionService = financialtransaction.NewService(serviceConfig.ClientID, exactonlineService)
	exactonlineBQService.LogisticsService = logistics.NewService(serviceConfig.ClientID, exactonlineService)
	exactonlineBQService.SalesOrderService = salesorder.NewService(serviceConfig.ClientID, exactonlineService)

	return &exactonlineBQService, nil

}
