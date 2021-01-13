package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ef "github.com/leapforce-libraries/go_exactonline_new/financialtransaction"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactService *eo.Service) *Service {
	return &Service{clientID, exactService}
}

func (service *Service) FinancialTransactionService() *ef.Service {
	return service.exactOnlineService.FinancialTransactionService
}

func (service *Service) ClientID() string {
	return service.clientID
}
