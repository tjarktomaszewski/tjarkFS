package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tjarkfs "github.com/tjarktomaszewski/tjarkFS/gen/proto/tjarkfs/v1"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/server"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

const testMaxRecvBytes = 8 << 20

// testNode is a storage node behind a real gRPC connection, so the handler
// runs over the same transport a client would use.
type testNode struct {
	client  tjarkfs.StorageNodeClient
	server  *server.Server
	store   *store.FilesystemStore
	dataDir string
}

func newTestNode(t *testing.T) *testNode {
	t.Helper()

	// A bare chunk server; the tests that care about the node's identity or
	// its log build their own node through newNode.
	return newNode(t, server.NodeInfo{}, nil)
}

// putStream streams one chunk: the header first, then one request per data
// part. It is safe to call from several goroutines at once.
func (n *testNode) putStream(
	ctx context.Context,
	header *tjarkfs.PutChunkRequest,
	parts ...[]byte,
) (*tjarkfs.PutChunkResponse, error) {
	stream, err := n.client.PutChunk(ctx)
	if err != nil {
		return nil, err
	}

	if err := stream.Send(header); err != nil {
		// The handler may reject the header and close the stream before the
		// client is done sending.
		_ = stream.CloseSend()
		return nil, err
	}
	for _, part := range parts {
		if err := stream.Send(&tjarkfs.PutChunkRequest{Data: part}); err != nil {
			_ = stream.CloseSend()
			return nil, err
		}
	}

	return stream.CloseAndRecv()
}

func (n *testNode) put(t *testing.T, header *tjarkfs.PutChunkRequest, parts ...[]byte) *tjarkfs.PutChunkResponse {
	t.Helper()

	resp, err := n.putStream(context.Background(), header, parts...)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	return resp
}

func (n *testNode) mustExist(t *testing.T, id string, want bool) {
	t.Helper()

	exists, err := n.store.Exists(id)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists != want {
		t.Fatalf("chunk %s exists=%v, want %v", id, exists, want)
	}
}

func (n *testNode) mustRead(t *testing.T, id string) string {
	t.Helper()

	reader, err := n.store.Get(id)
	if err != nil {
		t.Fatalf("get %s failed: %v", id, err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read %s failed: %v", id, err)
	}

	return string(content)
}

// tempFiles reports temporary files the atomic write should have cleaned up.
func (n *testNode) tempFiles(t *testing.T) []string {
	t.Helper()

	var leftovers []string
	err := filepath.WalkDir(n.dataDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), ".tmp-") {
			leftovers = append(leftovers, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s failed: %v", n.dataDir, err)
	}

	return leftovers
}

func chunkIDOf(content []byte) string {
	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func TestPutChunkStoresChunk(t *testing.T) {
	node := newTestNode(t)
	content := []byte("hello chunk")
	id := chunkIDOf(content)

	resp := node.put(t, &tjarkfs.PutChunkRequest{
		ChunkId:  id,
		Size:     int64(len(content)),
		FileId:   "file-1",
		Position: 3,
	}, content)

	if !resp.GetStored() || resp.GetExisted() {
		t.Fatalf("expected stored=true existed=false, got %+v", resp)
	}
	node.mustExist(t, id, true)
	if got := node.mustRead(t, id); got != string(content) {
		t.Fatalf("expected %q on disk, got %q", content, got)
	}
}

func TestPutChunkReportsExistingChunk(t *testing.T) {
	node := newTestNode(t)
	content := []byte("deduplicated chunk")
	id := chunkIDOf(content)
	header := &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}

	node.put(t, header, content)
	resp := node.put(t, header, content)

	if resp.GetStored() || !resp.GetExisted() {
		t.Fatalf("expected stored=false existed=true, got %+v", resp)
	}
	if got := node.mustRead(t, id); got != string(content) {
		t.Fatalf("expected %q on disk, got %q", content, got)
	}
}

func TestPutChunkDrainsDataFromFollowingParts(t *testing.T) {
	node := newTestNode(t)
	parts := [][]byte{[]byte("first "), []byte("second "), []byte("third")}
	content := bytes.Join(parts, nil)
	id := chunkIDOf(content)

	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, parts...)

	if got := node.mustRead(t, id); got != string(content) {
		t.Fatalf("expected %q on disk, got %q", content, got)
	}
}

func TestPutChunkAcceptsDataInTheHeaderMessage(t *testing.T) {
	node := newTestNode(t)
	content := []byte("packed into a single request")
	id := chunkIDOf(content)

	node.put(t, &tjarkfs.PutChunkRequest{
		ChunkId: id,
		Size:    int64(len(content)),
		Data:    content,
	})

	if got := node.mustRead(t, id); got != string(content) {
		t.Fatalf("expected %q on disk, got %q", content, got)
	}
}

func TestPutChunkRejectsContentThatDoesNotHashToTheChunkID(t *testing.T) {
	node := newTestNode(t)
	expected := []byte("expected content")
	id := chunkIDOf(expected)

	_, err := node.putStream(context.Background(),
		&tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(expected))},
		// Same length, different content: only the hash can catch this.
		bytes.Repeat([]byte("x"), len(expected)),
	)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", got, err)
	}
	node.mustExist(t, id, false)
	if leftovers := node.tempFiles(t); len(leftovers) > 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}

func TestPutChunkRejectsSizeMismatch(t *testing.T) {
	node := newTestNode(t)
	content := []byte("abc")
	id := chunkIDOf(content)

	_, err := node.putStream(context.Background(),
		&tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content) + 7)},
		content,
	)

	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (%v)", got, err)
	}
	node.mustExist(t, id, false)
}

func TestPutChunkRejectsChunkIDsOutsideTheChunkDirectory(t *testing.T) {
	content := []byte("traversal attempt")

	for _, id := range []string{
		"",
		"zz",
		"../../etc/passwd",
		strings.ToUpper(chunkIDOf(content)),
	} {
		t.Run(id, func(t *testing.T) {
			node := newTestNode(t)

			_, err := node.putStream(context.Background(),
				&tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))},
				content,
			)

			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v (%v)", id, got, err)
			}
			if leftovers := node.tempFiles(t); len(leftovers) > 0 {
				t.Errorf("temporary files left behind: %v", leftovers)
			}
		})
	}
}

func TestPutChunkRejectsChunkLargerThanMaxRecvBytes(t *testing.T) {
	node := newTestNode(t)

	_, err := node.putStream(context.Background(), &tjarkfs.PutChunkRequest{
		ChunkId: chunkIDOf([]byte("oversized")),
		Size:    testMaxRecvBytes + 1,
	})

	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v (%v)", got, err)
	}
}

func TestPutChunkServesConcurrentWritersOfTheSameChunk(t *testing.T) {
	node := newTestNode(t)
	content := bytes.Repeat([]byte("a"), 1<<20)
	id := chunkIDOf(content)
	header := &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := node.putStream(context.Background(), header, content)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent put failed: %v", err)
		}
	}
	if got := node.mustRead(t, id); got != string(content) {
		t.Fatalf("expected %d bytes on disk, got %d", len(content), len(got))
	}
	if leftovers := node.tempFiles(t); len(leftovers) > 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}

// readChunk drains a GetChunk stream and returns every response the node sent.
func (n *testNode) readChunk(ctx context.Context, id string) ([]*tjarkfs.GetChunkResponse, error) {
	stream, err := n.client.GetChunk(ctx, &tjarkfs.GetChunkRequest{ChunkId: id})
	if err != nil {
		return nil, err
	}

	var responses []*tjarkfs.GetChunkResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return responses, nil
		}
		if err != nil {
			return responses, err
		}
		responses = append(responses, resp)
	}
}

func joinedData(responses []*tjarkfs.GetChunkResponse) []byte {
	var content []byte
	for _, resp := range responses {
		content = append(content, resp.GetData()...)
	}

	return content
}

func TestGetChunkStreamsSmallChunk(t *testing.T) {
	node := newTestNode(t)
	content := []byte("a small chunk")
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	responses, err := node.readChunk(context.Background(), id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response for a small chunk, got %d", len(responses))
	}
	if responses[0].GetChunkId() != id {
		t.Errorf("first response carries chunk id %q, want %q", responses[0].GetChunkId(), id)
	}
	if responses[0].GetSize() != int64(len(content)) {
		t.Errorf("first response announces %d bytes, want %d", responses[0].GetSize(), len(content))
	}
	if got := joinedData(responses); !bytes.Equal(got, content) {
		t.Fatalf("expected %q, got %q", content, got)
	}
}

func TestGetChunkStreamsLargeChunkInBlocks(t *testing.T) {
	node := newTestNode(t)
	// Two and a half blocks, so the last response is a partial one.
	content := contentOfSize(2<<20 + 1<<19)
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	responses, err := node.readChunk(context.Background(), id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(responses) < 3 {
		t.Fatalf("expected the chunk to arrive in at least 3 blocks, got %d", len(responses))
	}
	for i, resp := range responses {
		if int64(len(resp.GetData())) > 1<<20 {
			t.Errorf("response %d carries %d bytes, more than one block", i, len(resp.GetData()))
		}
		if i == 0 && resp.GetChunkId() != id {
			t.Errorf("first response carries chunk id %q, want %q", resp.GetChunkId(), id)
		}
		if i > 0 && resp.GetChunkId() != "" {
			t.Errorf("response %d repeats the chunk id, only the first one may", i)
		}
	}
	if last := responses[len(responses)-1]; len(last.GetData()) == 0 {
		t.Error("last response carries no bytes, the final block is missing")
	}
	if got := joinedData(responses); !bytes.Equal(got, content) {
		t.Fatalf("expected %d bytes back, got %d", len(content), len(got))
	}
}

func TestGetChunkStreamsHeaderForAnEmptyChunk(t *testing.T) {
	node := newTestNode(t)
	emptySum := sha256.Sum256(nil)
	id := hex.EncodeToString(emptySum[:])
	if err := node.store.PutAtomic(id, func(io.Writer) error { return nil }); err != nil {
		t.Fatalf("put empty chunk failed: %v", err)
	}

	responses, err := node.readChunk(context.Background(), id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected exactly 1 response for an empty chunk, got %d", len(responses))
	}
	if responses[0].GetChunkId() != id || len(responses[0].GetData()) != 0 {
		t.Fatalf("expected a bare header for the empty chunk, got %+v", responses[0])
	}
}

func TestGetChunkUnknownChunk(t *testing.T) {
	node := newTestNode(t)

	_, err := node.readChunk(context.Background(), chunkIDOf([]byte("never stored")))

	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("expected NotFound, got %v (%v)", got, err)
	}
}

func TestGetChunkRejectsChunkIDsOutsideTheChunkDirectory(t *testing.T) {
	node := newTestNode(t)

	for _, id := range []string{"", "../../etc/passwd", strings.ToUpper(chunkIDOf([]byte("x")))} {
		t.Run(id, func(t *testing.T) {
			_, err := node.readChunk(context.Background(), id)

			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v (%v)", id, got, err)
			}
		})
	}
}

func TestGetChunkReleasesTheFileWhenTheClientWalksAway(t *testing.T) {
	node := newTestNode(t)
	// One and a half blocks, so the client can stop after the first response.
	content := contentOfSize(1<<20 + 1<<19)
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	// Warm the connection so its descriptors are not counted as growth.
	if _, err := node.readChunk(context.Background(), id); err != nil {
		t.Fatalf("warmup get failed: %v", err)
	}

	// The finalizer of *os.File closes a leaked descriptor as soon as the
	// garbage collector gets to it, which would make this test pass either
	// way. Switching the collector off for the measurement turns it back
	// into a real check: only the deferred Close hands the file back.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	before := openFileCount(t)

	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := node.client.GetChunk(ctx, &tjarkfs.GetChunkRequest{ChunkId: id})
		if err != nil {
			cancel()
			t.Fatalf("get failed: %v", err)
		}
		if _, err := stream.Recv(); err != nil {
			cancel()
			t.Fatalf("first response failed: %v", err)
		}
		cancel()
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
	}

	// Every aborted stream has to hand its descriptor back; without the
	// deferred Close the node leaks one file per cancelled download.
	if after := openFileCount(t); after > before+5 {
		t.Errorf("open files grew from %d to %d across 20 aborted downloads", before, after)
	}
}

// contentOfSize builds a deterministic payload of exactly n bytes.
func contentOfSize(n int) []byte {
	content := make([]byte, n)
	for i := range content {
		content[i] = byte('a' + i%26)
	}

	return content
}

// openFileCount counts the descriptors of this process. Readdirnames is used
// instead of os.ReadDir because the latter stats every entry, and the
// descriptor of the directory it is reading cannot be stat'ed.
func openFileCount(t *testing.T) int {
	t.Helper()

	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Skipf("cannot count open files on this platform: %v", err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Skipf("cannot count open files on this platform: %v", err)
	}

	return len(names)
}
