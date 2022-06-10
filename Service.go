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

	exactonlineBQService := Service{}

	exactonlineBQService.AssetsService = assets.NewService(exactonlineService)
	exactonlineBQService.BudgetService = budget.NewService(exactonlineService)
	exactonlineBQService.CashflowService = cashflow.NewService(exactonlineService)
	exactonlineBQService.CRMService = crm.NewService(exactonlineService)
	exactonlineBQService.FinancialService = financial.NewService(exactonlineService)
	exactonlineBQService.FinancialTransactionService = financialtransaction.NewService(exactonlineService)
	exactonlineBQService.InventoryService = inventory.NewService(exactonlineService)
	exactonlineBQService.LogisticsService = logistics.NewService(exactonlineService)
	exactonlineBQService.PayrollService = payroll.NewService(exactonlineService)
	exactonlineBQService.ProjectService = project.NewService(exactonlineService)
	exactonlineBQService.PurchaseEntryService = purchaseentry.NewService(exactonlineService)
	exactonlineBQService.PurchaseOrderService = purchaseorder.NewService(exactonlineService)
	exactonlineBQService.SalesInvoiceService = salesinvoice.NewService(exactonlineService)
	exactonlineBQService.SalesOrderService = salesorder.NewService(exactonlineService)
	exactonlineBQService.SyncService = sync.NewService(exactonlineService)

	return &exactonlineBQService, nil
}
