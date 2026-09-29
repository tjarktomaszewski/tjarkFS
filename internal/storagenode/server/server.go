package server

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tjarkfs "github.com/tjarktomaszewski/tjarkFS/gen/proto/tjarkfs/v1"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

// blockSize is the payload size of a single GetChunk response. It stays well
// below the 4 MiB a gRPC client accepts per message by default.
const blockSize = 1 << 20

// Fence rejects writes from a client whose lease has been superseded — the
// storage node's half of split-brain protection. The SQLite-backed
// implementation arrives later; NoopFence stands in until then.
type Fence interface {
	CheckAndUpdate(fileID domain.FileID, token int64) error
}

// NoopFence accepts every write. While the controlplane hands out no leases no
// writer can be stale, so there is nothing to reject.
type NoopFence struct{}

// CheckAndUpdate implements Fence.
func (NoopFence) CheckAndUpdate(domain.FileID, int64) error { return nil }

var (
	// errChunkSize marks a client that announced a size it did not deliver.
	errChunkSize = errors.New("chunk size mismatch")
	// errChunkContent marks bytes that do not hash to the chunk id they were
	// sent under.
	errChunkContent = errors.New("chunk content does not match chunk id")
)

type Server struct {
	store        domain.Store
	fence        Fence
	logger       *slog.Logger
	maxRecvBytes int64
	tjarkfs.UnimplementedStorageNodeServer
}

var _ tjarkfs.StorageNodeServer = (*Server)(nil)

// NewStorageServer builds the storage node's gRPC adapter. A nil fence is
// replaced by NoopFence and a nil logger by the default logger, mirroring the
// store constructor. maxRecvBytes caps a single chunk; zero disables the cap.
func NewStorageServer(store domain.Store, fence Fence, maxRecvBytes int64, logger *slog.Logger) *Server {
	if fence == nil {
		fence = NoopFence{}
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		store:        store,
		fence:        fence,
		logger:       logger,
		maxRecvBytes: maxRecvBytes,
	}
}

// PutChunk stores one chunk sent as a client stream: the first request carries
// the metadata, the ones after it carry data. The bytes go into a temporary
// file and become visible under chunkID only after the announced size and the
// SHA256 hash check out, so a reader never sees a half-written or corrupted
// chunk. Storing a chunk that is already there overwrites nothing and reports
// existed=true, which makes the call idempotent and safe to retry.
func (s *Server) PutChunk(stream tjarkfs.StorageNode_PutChunkServer) error {
	start := time.Now()

	header, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "first request must carry the chunk header")
		}
		return err
	}

	chunkID := header.GetChunkId()
	if err := validateChunkID(chunkID); err != nil {
		return err
	}

	size := header.GetSize()
	if size < 0 {
		return status.Errorf(codes.InvalidArgument, "chunk %s: negative size %d", chunkID, size)
	}
	if s.maxRecvBytes > 0 && size > s.maxRecvBytes {
		return status.Errorf(codes.ResourceExhausted,
			"chunk %s: size %d exceeds max-recv-bytes %d", chunkID, size, s.maxRecvBytes)
	}

	// A request without a fence token cannot be stale, so the check is
	// skipped: before the lease exists no client holds one and 0 is the only
	// token in flight.
	if token := header.GetFenceToken(); token > 0 {
		if err := s.fence.CheckAndUpdate(domain.FileID(header.GetFileId()), token); err != nil {
			return fmt.Errorf("fence check for file %s token %d: %w", header.GetFileId(), token, err)
		}
	}

	// Asked before the write: an existing chunk turns this into a no-op that
	// still has to report existed=true.
	existed, err := s.store.Exists(chunkID)
	if err != nil {
		return status.Errorf(codes.Internal, "chunk %s: %v", chunkID, err)
	}

	if err := s.store.PutAtomic(chunkID, func(dst io.Writer) error {
		return s.writeChunk(stream, header, dst, chunkID, size)
	}); err != nil {
		return putError(chunkID, err)
	}

	s.logger.Debug(
		"chunk stored",
		"chunk_id", chunkID,
		"file_id", header.GetFileId(),
		"position", header.GetPosition(),
		"size", size,
		"existed", existed,
		"took", time.Since(start),
	)

	return stream.SendAndClose(&tjarkfs.PutChunkResponse{
		Stored:  !existed,
		Existed: existed,
	})
}

// GetChunk streams a chunk back to the client. The node sends what is on disk
// unchecked — the client verifies SHA256(data) against the chunk id
func (s *Server) GetChunk(req *tjarkfs.GetChunkRequest, stream tjarkfs.StorageNode_GetChunkServer) error {
	start := time.Now()

	chunkID := req.GetChunkId()
	if err := validateChunkID(chunkID); err != nil {
		return err
	}

	sent, err := s.streamChunk(stream, chunkID)
	if err != nil {
		return err
	}

	s.logger.Debug(
		"chunk streamed",
		"chunk_id", chunkID,
		"size", sent,
		"took", time.Since(start),
	)

	return nil
}

// streamChunk copies a chunk from the store to the client in fixed-size
// blocks and reports how many bytes were sent. The first response carries the
// chunk id and its size, the ones after it carry only data. ReplicateChunk
// answers with the same response type and therefore the same stream type, so
// the whole path below is shared between the two RPCs.
func (s *Server) streamChunk(stream tjarkfs.StorageNode_GetChunkServer, chunkID string) (int64, error) {
	chunkReader, err := s.store.Get(chunkID)
	if err != nil {
		return 0, getError(chunkID, err)
	}
	// One file per RPC: the deferred Close is the only thing that hands the
	// descriptor back when the client walks away mid-stream.
	defer chunkReader.Close()

	// The store hands out a plain reader, only a file knows its own size. A
	// reader that cannot answer reports 0, which the client reads as
	// unknown: the authoritative size travels with the file metadata anyway.
	var size int64
	if stater, ok := chunkReader.(interface{ Stat() (fs.FileInfo, error) }); ok {
		info, err := stater.Stat()
		if err != nil {
			return 0, status.Errorf(codes.Internal, "stat chunk %s: %v", chunkID, err)
		}
		size = info.Size()
	}

	buffer := make([]byte, blockSize)
	var sent int64
	for {
		n, readErr := io.ReadFull(chunkReader, buffer)

		// Whatever was read before the error belongs to the chunk:
		// io.ErrUnexpectedEOF is the last, partial block.
		if n > 0 {
			resp := &tjarkfs.GetChunkResponse{Data: buffer[:n]}
			if sent == 0 {
				resp.ChunkId = chunkID
				resp.Size = size
			}
			// The buffer is reused for the next block. That is safe because
			// gRPC marshals the message inside Send and does not read the
			// slice afterwards.
			if err := stream.Send(resp); err != nil {
				return sent, err
			}
			sent += int64(n)
		}

		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return sent, readErr
			}
			break
		}
	}

	// A chunk without bytes still needs one response, or the client never
	// learns the id it asked for.
	if sent == 0 {
		if err := stream.Send(&tjarkfs.GetChunkResponse{ChunkId: chunkID, Size: size}); err != nil {
			return 0, err
		}
	}

	return sent, nil
}

// getError maps a failed chunk read to a status code. An unknown chunk is
// NOT_FOUND so that the client can move on to the next replica;
// everything else is a fault of this node.
func getError(chunkID string, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return status.Errorf(codes.NotFound, "chunk %s: %v", chunkID, err)
	}

	return status.Errorf(codes.Internal, "open chunk %s: %v", chunkID, err)
}

// writeChunk drains the stream into dst and verifies it before returning: the
// number of bytes must match the announced size and their SHA256 must equal
// chunkID. It runs inside the store's temporary file, so returning an error
// leaves the chunk invisible.
func (s *Server) writeChunk(
	stream tjarkfs.StorageNode_PutChunkServer,
	header *tjarkfs.PutChunkRequest,
	dst io.Writer,
	chunkID string,
	size int64,
) error {
	hash := sha256.New()
	// One read feeds file and hash: the MultiWriter costs no second pass.
	sink := io.MultiWriter(dst, hash)

	written, err := writeData(stream, sink, header.GetData())
	if err != nil {
		return err
	}

	if written != size {
		return fmt.Errorf("%w: expected %d bytes, got %d", errChunkSize, size, written)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != chunkID {
		return fmt.Errorf("%w: %s hashes to %s", errChunkContent, chunkID, got)
	}

	return nil
}

// writeData drains the remaining requests into w and reports how many bytes
// were written. The header's data field is written as well, so a client that
// packs header and payload into one request is served correctly; later
// requests may carry empty data parts and repeated metadata, both of which the
// contract allows and both of which are ignored here.
func writeData(stream tjarkfs.StorageNode_PutChunkServer, w io.Writer, first []byte) (int64, error) {
	var written int64
	n, err := w.Write(first)
	written = int64(n)
	if err != nil {
		return written, err
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return written, nil
		}
		if err != nil {
			return written, err
		}

		data := req.GetData()
		if len(data) == 0 {
			continue
		}

		n, err := w.Write(data)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
}

// validateChunkID rejects every id that is not exactly one SHA256 in hex. The
// id becomes a file name, so the check is what keeps a chunk id from walking
// out of the chunk directory. Uppercase hex is rejected as well: it would
// address a second file for content that is already stored.
func validateChunkID(id string) error {
	if len(id) != sha256.Size*2 {
		return status.Errorf(codes.InvalidArgument, "chunk id %q is not a %d character hash", id, sha256.Size*2)
	}
	if _, err := hex.DecodeString(id); err != nil {
		return status.Errorf(codes.InvalidArgument, "chunk id %q is not hex: %v", id, err)
	}
	if strings.ToLower(id) != id {
		return status.Errorf(codes.InvalidArgument, "chunk id %q is not lowercase hex", id)
	}

	return nil
}

// putError maps a failed atomic write to a status code. Size and content
// mismatches are the client's fault. A transport error keeps the status the
// stream already carries — status.Code reports Unknown for anything that is
// not a status error — and everything else is a fault of this node.
func putError(chunkID string, err error) error {
	switch {
	case errors.Is(err, errChunkSize):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, errChunkContent):
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	if status.Code(err) != codes.Unknown {
		return err
	}

	return status.Errorf(codes.Internal, "store chunk %s: %v", chunkID, err)
}
