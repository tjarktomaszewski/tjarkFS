package identity

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

type FileIDGenerator interface {
	Generate() (domain.FileID, error)
}

type UUIDFileIDGenerator struct{}

func (g UUIDFileIDGenerator) Generate() (domain.FileID, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return domain.FileID(uuid.String()), err
	}
	fmt.Printf("generated file id: %x\n", uuid.String())

	return domain.FileID(uuid.String()), nil
}
