package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ee "github.com/leapforce-libraries/go_exactonline_new/purchaseentry"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnlineService *eo.Service) *Service {
	return &Service{exactOnlineService}
}

func (service *Service) PurchaseEntryService() *ee.Service {
	return service.exactOnlineService.PurchaseEntryService
}
