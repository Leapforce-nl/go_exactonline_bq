package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	el "github.com/leapforce-libraries/go_exactonline_new/logistics"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnlineService *eo.Service) *Service {
	return &Service{clientID, exactOnlineService}
}

func (service *Service) LogisticsService() *el.Service {
	return service.exactOnlineService.LogisticsService
}

func (service *Service) ClientID() string {
	return service.clientID
}
