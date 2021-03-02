package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ee "github.com/leapforce-libraries/go_exactonline_new/purchaseentry"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnlineService *eo.Service) *Service {
	return &Service{clientID, exactOnlineService}
}

func (service *Service) PurchaseEntryService() *ee.Service {
	return service.exactOnlineService.PurchaseEntryService
}

func (service *Service) ClientID() string {
	return service.clientID
}
