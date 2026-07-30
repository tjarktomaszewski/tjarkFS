package domain

type FileIDGenerator interface {
	Generate() (FileID, error)
}
