package domain

type FileRepository interface {
	Save(file File) error
	Get(id FileID) (*File, error)
	Delete(id FileID) error
	List() ([]File, error)
}
