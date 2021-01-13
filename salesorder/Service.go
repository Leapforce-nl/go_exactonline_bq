package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	es "github.com/leapforce-libraries/go_exactonline_new/salesorder"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnlineService *eo.Service) *Service {
	return &Service{clientID, exactOnlineService}
}

func (service *Service) SalesOrderService() *es.Service {
	return service.exactOnlineService.SalesOrderService
}

func (service *Service) ClientID() string {
	return service.clientID
}
