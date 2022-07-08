package exactonline_bq

import (
	errortools "github.com/leapforce-libraries/go_errortools"
	assets "github.com/leapforce-libraries/go_exactonline_bq/assets"
	budget "github.com/leapforce-libraries/go_exactonline_bq/budget"
	cashflow "github.com/leapforce-libraries/go_exactonline_bq/cashflow"
	crm "github.com/leapforce-libraries/go_exactonline_bq/crm"
	financial "github.com/leapforce-libraries/go_exactonline_bq/financial"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_bq/financialtransaction"
	inventory "github.com/leapforce-libraries/go_exactonline_bq/inventory"
	logistics "github.com/leapforce-libraries/go_exactonline_bq/logistics"
	payroll "github.com/leapforce-libraries/go_exactonline_bq/payroll"
	project "github.com/leapforce-libraries/go_exactonline_bq/project"
	purchaseentry "github.com/leapforce-libraries/go_exactonline_bq/purchaseentry"
	purchaseorder "github.com/leapforce-libraries/go_exactonline_bq/purchaseorder"
	salesinvoice "github.com/leapforce-libraries/go_exactonline_bq/salesinvoice"
	salesorder "github.com/leapforce-libraries/go_exactonline_bq/salesorder"
	sync "github.com/leapforce-libraries/go_exactonline_bq/sync"
	exactonline "github.com/leapforce-libraries/go_exactonline_new"
)

type Service struct {
	AssetsService               *assets.Service
	BudgetService               *budget.Service
	CashflowService             *cashflow.Service
	CRMService                  *crm.Service
	FinancialService            *financial.Service
	FinancialTransactionService *financialtransaction.Service
	InventoryService            *inventory.Service
	LogisticsService            *logistics.Service
	PayrollService              *payroll.Service
	ProjectService              *project.Service
	PurchaseEntryService        *purchaseentry.Service
	PurchaseOrderService        *purchaseorder.Service
	SalesInvoiceService         *salesinvoice.Service
	SalesOrderService           *salesorder.Service
	SyncService                 *sync.Service
}

func NewService(exactonlineService *exactonline.Service) (*Service, *errortools.Error) {
	if exactonlineService == nil {
		return nil, nil
	}

	exactonlineBqService := Service{}

	exactonlineBqService.AssetsService = assets.NewService(exactonlineService)
	exactonlineBqService.BudgetService = budget.NewService(exactonlineService)
	exactonlineBqService.CashflowService = cashflow.NewService(exactonlineService)
	exactonlineBqService.CRMService = crm.NewService(exactonlineService)
	exactonlineBqService.FinancialService = financial.NewService(exactonlineService)
	exactonlineBqService.FinancialTransactionService = financialtransaction.NewService(exactonlineService)
	exactonlineBqService.InventoryService = inventory.NewService(exactonlineService)
	exactonlineBqService.LogisticsService = logistics.NewService(exactonlineService)
	exactonlineBqService.PayrollService = payroll.NewService(exactonlineService)
	exactonlineBqService.ProjectService = project.NewService(exactonlineService)
	exactonlineBqService.PurchaseEntryService = purchaseentry.NewService(exactonlineService)
	exactonlineBqService.PurchaseOrderService = purchaseorder.NewService(exactonlineService)
	exactonlineBqService.SalesInvoiceService = salesinvoice.NewService(exactonlineService)
	exactonlineBqService.SalesOrderService = salesorder.NewService(exactonlineService)
	exactonlineBqService.SyncService = sync.NewService(exactonlineService)

	return &exactonlineBqService, nil
}
