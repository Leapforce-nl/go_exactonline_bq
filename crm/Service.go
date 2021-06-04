package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ec "github.com/leapforce-libraries/go_exactonline_new/crm"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnlineService *eo.Service) *Service {
	return &Service{exactOnlineService}
}

func (service *Service) CRMService() *ec.Service {
	return service.exactOnlineService.CRMService
}
