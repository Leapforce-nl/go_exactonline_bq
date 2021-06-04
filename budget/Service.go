package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	eb "github.com/leapforce-libraries/go_exactonline_new/budget"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnline *eo.Service) *Service {
	return &Service{exactOnline}
}

func (service *Service) BudgetService() *eb.Service {
	return service.exactOnlineService.BudgetService
}
