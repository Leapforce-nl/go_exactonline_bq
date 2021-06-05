package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ec "github.com/leapforce-libraries/go_exactonline_new/cashflow"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnline *eo.Service) *Service {
	return &Service{exactOnline}
}

func (service *Service) CashflowService() *ec.Service {
	return service.exactOnlineService.CashflowService
}
