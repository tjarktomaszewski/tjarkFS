package storage

import "crypto/sha256"

func SHA256(data []byte) string {
	sha := sha256.Sum256(data)
	return string(sha[:])
}
