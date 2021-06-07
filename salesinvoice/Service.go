package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	es "github.com/leapforce-libraries/go_exactonline_new/salesinvoice"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnlineService *eo.Service) *Service {
	return &Service{exactOnlineService}
}

func (service *Service) SalesInvoiceService() *es.Service {
	return service.exactOnlineService.SalesInvoiceService
}
