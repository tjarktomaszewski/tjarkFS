package service

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

type ListService struct {
	repo domain.FileRepository
}

func NewListService(repo domain.FileRepository) *ListService {
	return &ListService{
		repo: repo,
	}
}

func (l *ListService) List() ([]domain.File, error) {
	return l.repo.List()
}
