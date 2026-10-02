package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tjarkfs "github.com/tjarktomaszewski/tjarkFS/gen/proto/tjarkfs/v1"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/server"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

// Tests for the storage node RPCs around the chunk lifecycle: deleting a
// chunk, pulling a replica off this node, and reporting the node itself.

// newNode starts a storage node with its own data directory. node tells the
// server which node it is; a nil logger keeps the node's per-chunk log lines
// out of the test output. The listener is opened here instead of through
// Start(), so every test gets its own :0 port; the server is then built with
// that address, exactly the way cmd/storagenode wires a real one.
func newNode(t *testing.T, node server.NodeInfo, logger *slog.Logger) *testNode {
	t.Helper()

	dataDir := t.TempDir()
	if node.DataDir == "" {
		node.DataDir = dataDir
	}
	if logger == nil {
		// The node logs one line per stored chunk; the test output stays
		// about the test.
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	chunkStore := store.NewFileSystemStorage(dataDir, logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	node.ListenAddr = listener.Addr().String()

	// The message limit mirrors what main.go passes from --max-recv-bytes;
	// gRPC would otherwise refuse any chunk above 4 MiB.
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(testMaxRecvBytes))
	srv := server.NewStorageServer(chunkStore, nil, testMaxRecvBytes, node, logger)
	tjarkfs.RegisterStorageNodeServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return &testNode{
		client:  tjarkfs.NewStorageNodeClient(conn),
		server:  srv,
		store:   chunkStore,
		dataDir: dataDir,
	}
}

// info reads the node's self-report over the wire.
func (n *testNode) info(t *testing.T) *tjarkfs.GetNodeInfoResponse {
	t.Helper()

	info, err := n.client.GetNodeInfo(context.Background(), &tjarkfs.GetNodeInfoRequest{})
	if err != nil {
		t.Fatalf("get node info failed: %v", err)
	}

	return info
}

// readReplicated drains a ReplicateChunk stream and returns every response
// the node sent.
func (n *testNode) readReplicated(ctx context.Context, id, requestedBy string) ([]*tjarkfs.GetChunkResponse, error) {
	stream, err := n.client.ReplicateChunk(ctx, &tjarkfs.ReplicateChunkRequest{
		ChunkId:     id,
		RequestedBy: requestedBy,
	})
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

func TestDeleteChunkRemovesTheChunkFromTheStore(t *testing.T) {
	node := newTestNode(t)
	content := []byte("about to be deleted")
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	resp, err := node.client.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if !resp.GetDeleted() {
		t.Fatalf("deleted = false for a chunk that was on disk, got %+v", resp)
	}

	// A delete that only answers deleted=true frees nothing. The chunk has to
	// be gone from the store and unreadable over the wire.
	node.mustExist(t, id, false)
	if _, err := node.readChunk(context.Background(), id); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after the delete, got %v (%v)", status.Code(err), err)
	}
}

func TestDeleteChunkReportsAnAlreadyDeletedChunkAsNotDeleted(t *testing.T) {
	node := newTestNode(t)
	content := []byte("deleted twice")
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	if _, err := node.client.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{ChunkId: id}); err != nil {
		t.Fatalf("first delete failed: %v", err)
	}
	resp, err := node.client.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("second delete failed: %v", err)
	}

	// The controlplane deletes a chunk on every node that might have it, and
	// the set of chunks on a node has to become empty at some point. So a
	// chunk that is already gone is an answer, not an error.
	if resp.GetDeleted() {
		t.Fatalf("deleted = true for a chunk that was already gone, got %+v", resp)
	}
}

func TestDeleteChunkRejectsChunkIDsOutsideTheChunkDirectory(t *testing.T) {
	node := newTestNode(t)
	// A delete that trusts the chunk id would remove a file the client named.
	victim := filepath.Join(node.dataDir, "victim.txt")
	if err := os.WriteFile(victim, []byte("not a chunk"), 0o600); err != nil {
		t.Fatalf("write victim failed: %v", err)
	}

	for _, id := range []string{
		"",
		"victim.txt",
		"../victim.txt",
		strings.ToUpper(chunkIDOf([]byte("x"))),
	} {
		t.Run(id, func(t *testing.T) {
			_, err := node.client.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{ChunkId: id})

			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v (%v)", id, got, err)
			}
		})
	}

	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("the file a rejected delete named is gone: %v", err)
	}
}

// failingStore fails every delete with an error that is not fs.ErrNotExist.
// The other methods are never called, so the embedded nil interface is fine.
type failingStore struct{ domain.Store }

func (failingStore) Delete(string) error { return errors.New("device is not ready") }

func TestDeleteChunkReportsAStoreFailureAsAnError(t *testing.T) {
	srv := server.NewStorageServer(failingStore{}, nil, testMaxRecvBytes, server.NodeInfo{}, nil)

	// DeleteChunk is unary, so the handler can be called directly — which is
	// the only way to hand the node a store that fails.
	_, err := srv.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{
		ChunkId: chunkIDOf([]byte("anything")),
	})

	// deleted=false is reserved for a chunk that was not there. A delete that
	// failed on disk has to reach the controlplane as a failure, or the bytes
	// stay on the node while the metadata says they are gone.
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("expected Internal, got %v (%v)", got, err)
	}
}

func TestReplicateChunkStreamsTheSameBytesAsGetChunk(t *testing.T) {
	node := newTestNode(t)
	// Two and a half blocks: the pull path has to reproduce the framing of
	// GetChunk, not just the first response.
	content := contentOfSize(2<<20 + 1<<19)
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	fromGet, err := node.readChunk(context.Background(), id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	fromReplicate, err := node.readReplicated(context.Background(), id, "node-2")
	if err != nil {
		t.Fatalf("replicate failed: %v", err)
	}

	if len(fromReplicate) != len(fromGet) {
		t.Fatalf("replication sent %d responses, GetChunk sent %d", len(fromReplicate), len(fromGet))
	}
	first := fromReplicate[0]
	if first.GetChunkId() != id {
		t.Errorf("first response carries chunk id %q, want %q", first.GetChunkId(), id)
	}
	if first.GetSize() != int64(len(content)) {
		t.Errorf("first response announces %d bytes, want %d", first.GetSize(), len(content))
	}

	replicated := joinedData(fromReplicate)
	if !bytes.Equal(replicated, content) {
		t.Fatalf("replication delivered %d bytes, want %d", len(replicated), len(content))
	}
	// The target node stores what it received under the chunk id it asked
	// for, so the bytes have to hash to that id — the check the repair worker
	// depends on.
	if got := chunkIDOf(replicated); got != id {
		t.Fatalf("replicated bytes hash to %s, want %s", got, id)
	}
}

func TestReplicateChunkRejectsUnknownAndInvalidChunkIDs(t *testing.T) {
	node := newTestNode(t)

	for _, testCase := range []struct {
		name string
		id   string
		want codes.Code
	}{
		{name: "unknown chunk", id: chunkIDOf([]byte("never stored")), want: codes.NotFound},
		{name: "empty id", id: "", want: codes.InvalidArgument},
		{name: "traversal", id: "../../etc/passwd", want: codes.InvalidArgument},
		{name: "uppercase hex", id: strings.ToUpper(chunkIDOf([]byte("x"))), want: codes.InvalidArgument},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := node.readReplicated(context.Background(), testCase.id, "node-2")

			if got := status.Code(err); got != testCase.want {
				t.Fatalf("expected %v for %q, got %v (%v)", testCase.want, testCase.id, got, err)
			}
		})
	}
}

// safeBuffer collects log lines. The handler writes from the gRPC goroutine
// while the test reads after the call returned, so the buffer needs its own
// lock.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func TestReplicateChunkLogsTheRequestingNode(t *testing.T) {
	logs := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	node := newNode(t, server.NodeInfo{}, logger)
	content := []byte("replicated on request")
	id := chunkIDOf(content)
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}, content)

	if _, err := node.readReplicated(context.Background(), id, "node-2"); err != nil {
		t.Fatalf("replicate failed: %v", err)
	}

	// requested_by has no place in the response — the requester knows who it
	// is. The log is the only place an audit trail can come from, so the
	// replication has to name both the chunk and the node that asked.
	if line := logs.String(); !strings.Contains(line, id) || !strings.Contains(line, "node-2") {
		t.Fatalf("replication was not logged with chunk %s and requester node-2, log:\n%s", id, line)
	}
}

func TestGetNodeInfoReportsIdentityAndCapacity(t *testing.T) {
	// A capacity of 0 means "unbestimmt", so the test uses a real number: a
	// node that reports 0 for everything is indistinguishable from a node
	// that reports nothing at all.
	node := newNode(t, server.NodeInfo{NodeID: "node-a", Capacity: 4096}, nil)

	info := node.info(t)

	if info.GetNodeId() != "node-a" {
		t.Errorf("node_id = %q, want %q", info.GetNodeId(), "node-a")
	}
	if info.GetCapacity() != 4096 {
		t.Errorf("capacity = %d, want 4096", info.GetCapacity())
	}
	// A node that has never received a chunk reports zero bytes and zero
	// chunks, not -1 or "unknown".
	if info.GetUsed() != 0 || info.GetChunkCount() != 0 {
		t.Errorf("a node without chunks reports used=%d chunk_count=%d, want 0/0", info.GetUsed(), info.GetChunkCount())
	}
}

func TestGetNodeInfoCountsChunksAndBytes(t *testing.T) {
	node := newNode(t, server.NodeInfo{NodeID: "node-a", Capacity: 4096}, nil)
	first, second, third := []byte("aaa"), []byte("bb"), []byte("cccc")
	for _, content := range [][]byte{first, second, third} {
		node.put(t, &tjarkfs.PutChunkRequest{ChunkId: chunkIDOf(content), Size: int64(len(content))}, content)
	}

	info := node.info(t)
	if want := int64(9); info.GetUsed() != want {
		t.Errorf("used = %d, want %d", info.GetUsed(), want)
	}
	if info.GetChunkCount() != 3 {
		t.Errorf("chunk_count = %d, want 3", info.GetChunkCount())
	}

	// used is what the controlplane plans against, so it has to follow the
	// chunk tree: a deleted chunk is no longer storage this node holds.
	if _, err := node.client.DeleteChunk(context.Background(), &tjarkfs.DeleteChunkRequest{
		ChunkId: chunkIDOf(second),
	}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	info = node.info(t)
	if want := int64(7); info.GetUsed() != want {
		t.Errorf("used = %d after the delete, want %d", info.GetUsed(), want)
	}
	if info.GetChunkCount() != 2 {
		t.Errorf("chunk_count = %d after the delete, want 2", info.GetChunkCount())
	}
}

func TestGetNodeInfoIgnoresNonChunkFilesInTheDataDir(t *testing.T) {
	node := newNode(t, server.NodeInfo{NodeID: "node-a", Capacity: 4096}, nil)
	content := []byte("the only chunk")
	node.put(t, &tjarkfs.PutChunkRequest{ChunkId: chunkIDOf(content), Size: int64(len(content))}, content)

	// Both of these sit next to the chunks in a real node: the identity file
	// main.go writes and the fence sidecar. They are not storage this node
	// holds, so they must not show up in used or in chunk_count.
	foreign := map[string][]byte{
		"node-id":       []byte("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
		"fences.db":     bytes.Repeat([]byte("SQLite format 3"), 64),
		"fences.db-wal": make([]byte, 128),
	}
	for name, payload := range foreign {
		if err := os.WriteFile(filepath.Join(node.dataDir, name), payload, 0o600); err != nil {
			t.Fatalf("write %s failed: %v", name, err)
		}
	}

	info := node.info(t)
	if want := int64(len(content)); info.GetUsed() != want {
		t.Errorf("used = %d, want %d: files that are not chunks are counted", info.GetUsed(), want)
	}
	if info.GetChunkCount() != 1 {
		t.Errorf("chunk_count = %d, want 1: files that are not chunks are counted", info.GetChunkCount())
	}
}

func TestServerStartsActive(t *testing.T) {
	node := newNode(t, server.NodeInfo{NodeID: "node-a"}, nil)

	// Nobody has set a state yet, and a node nobody has spoken about still
	// has to accept writes.
	if got := node.server.NodeState(); got != domain.NodeActive {
		t.Fatalf("a fresh node reports %q, want %q", got, domain.NodeActive)
	}
}

func TestSetNodeStateChangesTheStateTheNodeReports(t *testing.T) {
	node := newNode(t, server.NodeInfo{NodeID: "node-a"}, nil)

	// The state travels back to the node through the heartbeat response, so
	// the enum on the wire and the state in the node must not drift apart.
	for _, testCase := range []struct {
		wire tjarkfs.NodeState
		want domain.NodeState
	}{
		{wire: tjarkfs.NodeState_NODE_STATE_DRAINING, want: domain.NodeDraining},
		{wire: tjarkfs.NodeState_NODE_STATE_DEAD, want: domain.NodeDead},
		{wire: tjarkfs.NodeState_NODE_STATE_ACTIVE, want: domain.NodeActive},
	} {
		t.Run(testCase.wire.String(), func(t *testing.T) {
			_, err := node.client.SetNodeState(context.Background(), &tjarkfs.SetNodeStateRequest{
				NodeId: "node-a",
				State:  testCase.wire,
			})
			if err != nil {
				t.Fatalf("set node state failed: %v", err)
			}

			if got := node.server.NodeState(); got != testCase.want {
				t.Fatalf("node reports %q after SetNodeState(%v), want %q", got, testCase.wire, testCase.want)
			}
		})
	}
}
