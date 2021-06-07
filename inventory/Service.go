package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ei "github.com/leapforce-libraries/go_exactonline_new/inventory"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnlineService *eo.Service) *Service {
	return &Service{exactOnlineService}
}

func (service *Service) InventoryService() *ei.Service {
	return service.exactOnlineService.InventoryService
}
