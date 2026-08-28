package metadata

import "github.com/tjarktomaszewski/tjarkFS/internal/domain"

// distinctChunkIDs returns the unique chunk IDs of the given list,
// preserving first-occurrence order. A file references a chunk once, no
// matter how many positions the chunk occupies within the file, so the
// per-chunk reference count counts files, not positions.
func distinctChunkIDs(ids []domain.ChunkID) []domain.ChunkID {
	seen := make(map[domain.ChunkID]bool, len(ids))
	out := make([]domain.ChunkID, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}