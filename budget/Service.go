package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	eb "github.com/leapforce-libraries/go_exactonline_new/budget"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnline *eo.Service) *Service {
	return &Service{clientID, exactOnline}
}

func (service *Service) BudgetService() *eb.Service {
	return service.exactOnlineService.BudgetService
}

func (service *Service) ClientID() string {
	return service.clientID
}
