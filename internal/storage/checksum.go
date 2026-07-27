package storage

import "crypto/sha256"

func SHA256(data []byte) [32]byte {
	return sha256.Sum256(data)
}
