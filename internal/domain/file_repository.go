package domain

type FileRepository interface {
	Save(file File) error
	Get(id FileID) (*File, error)
	// Delete removes the file's metadata and decrements the reference
	// count of the chunks it references. It returns the IDs of the chunks
	// whose reference count dropped to zero; the caller is responsible for
	// physically removing those chunks from the chunk store. Chunks still
	// referenced by other files are left in place. Deleting an unknown
	// file is a no-op.
	Delete(id FileID) ([]ChunkID, error)
	List() ([]File, error)
}
