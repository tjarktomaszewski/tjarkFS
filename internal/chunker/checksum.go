package chunker

import (
	"crypto/sha256"
	"encoding/hex"
)

func SHA256(data []byte) string {
	sha := sha256.Sum256(data)
	return hex.EncodeToString(sha[:])
}
