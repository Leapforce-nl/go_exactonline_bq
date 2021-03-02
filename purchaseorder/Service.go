package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ep "github.com/leapforce-libraries/go_exactonline_new/purchaseorder"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnlineService *eo.Service) *Service {
	return &Service{clientID, exactOnlineService}
}

func (service *Service) PurchaseOrderService() *ep.Service {
	return service.exactOnlineService.PurchaseOrderService
}

func (service *Service) ClientID() string {
	return service.clientID
}
