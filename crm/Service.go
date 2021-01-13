package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ec "github.com/leapforce-libraries/go_exactonline_new/crm"
)

type Service struct {
	clientID           string
	exactOnlineService *eo.Service
}

func NewService(clientID string, exactOnlineService *eo.Service) *Service {
	return &Service{clientID, exactOnlineService}
}

func (service *Service) CRMService() *ec.Service {
	return service.exactOnlineService.CRMService
}
func (service *Service) ClientID() string {
	return service.clientID
}
