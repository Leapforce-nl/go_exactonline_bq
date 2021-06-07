package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ep "github.com/leapforce-libraries/go_exactonline_new/project"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnlineService *eo.Service) *Service {
	return &Service{exactOnlineService}
}

func (service *Service) ProjectService() *ep.Service {
	return service.exactOnlineService.ProjectService
}
