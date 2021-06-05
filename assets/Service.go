package exactonline_bq

import (
	eo "github.com/leapforce-libraries/go_exactonline_new"
	ea "github.com/leapforce-libraries/go_exactonline_new/assets"
)

type Service struct {
	exactOnlineService *eo.Service
}

func NewService(exactOnline *eo.Service) *Service {
	return &Service{exactOnline}
}

func (service *Service) AssetsService() *ea.Service {
	return service.exactOnlineService.AssetsService
}
