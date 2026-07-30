package metadata

import (
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type SQLiteFileRepository struct {
}

func (r *SQLiteFileRepository) Save(file domain.File) error {

}

func (r *SQLiteFileRepository) Get(id domain.FileID) (*domain.File, error) {

}

func (r *SQLiteFileRepository) Delete(id domain.FileID) {

}
