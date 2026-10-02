package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tjarkfs "github.com/tjarktomaszewski/tjarkFS/gen/proto/tjarkfs/v1"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/server"
	"github.com/tjarktomaszewski/tjarkFS/internal/storagenode/store"
)

// Tests for the node's lifecycle: Start binding a port and serving on it, and
// GracefulStop letting go of it. The other tests build their own gRPC server,
// so they cannot catch a node that comes up but answers nothing.

// startNode brings a node up the way cmd/storagenode does — through Start — and
// returns a client on the port the kernel handed out. It stops the node when the
// test ends.
func startNode(t *testing.T, info server.NodeInfo, maxRecvBytes int64) (*server.Server, tjarkfs.StorageNodeClient) {
	t.Helper()

	if info.DataDir == "" {
		info.DataDir = t.TempDir()
	}
	if info.ListenAddr == "" {
		info.ListenAddr = "127.0.0.1:0"
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := server.NewStorageServer(
		store.NewFileSystemStorage(info.DataDir, logger),
		nil,
		maxRecvBytes,
		info,
		logger,
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.GracefulStop(ctx); err != nil {
			t.Errorf("GracefulStop during cleanup failed: %v", err)
		}
	})

	conn, err := grpc.NewClient(
		srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialing %s failed: %v", srv.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return srv, tjarkfs.NewStorageNodeClient(conn)
}

func sha256ID(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// A node that listens but answers Unimplemented is indistinguishable from a
// running one until somebody calls it. This is the call that proves the
// handlers are the ones registered.
func TestStartServesTheHandlersItRegistered(t *testing.T) {
	_, client := startNode(t, server.NodeInfo{
		NodeID:     "node-start",
		Capacity:   4096,
		ListenAddr: "127.0.0.1:0",
	}, testMaxRecvBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetNodeInfo(ctx, &tjarkfs.GetNodeInfoRequest{})
	if err != nil {
		t.Fatalf("a started node answered GetNodeInfo with %v (%v)", err, status.Code(err))
	}
	if info.GetNodeId() != "node-start" {
		t.Errorf("node_id = %q, want %q", info.GetNodeId(), "node-start")
	}

	// One chunk through the real transport, back out again.
	content := []byte("start proves the transport")
	id := sha256ID(content)
	stream, err := client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk open failed: %v", err)
	}
	if err := stream.Send(&tjarkfs.PutChunkRequest{ChunkId: id, Size: int64(len(content))}); err != nil {
		t.Fatalf("sending the header failed: %v", err)
	}
	if err := stream.Send(&tjarkfs.PutChunkRequest{ChunkId: id, Data: content}); err != nil {
		t.Fatalf("sending the data failed: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutChunk failed: %v (%v)", err, status.Code(err))
	}
	if !resp.GetStored() || resp.GetExisted() {
		t.Errorf("first write reports stored=%v existed=%v, want true/false",
			resp.GetStored(), resp.GetExisted())
	}
}

// --max-recv-bytes 0 means "no cap" in this project. gRPC's MaxRecvMsgSize(0)
// means the opposite — it rejects every message, including the first one — so
// the option must stay unset in that case.
func TestStartWithoutAMessageCapStillAcceptsChunks(t *testing.T) {
	_, client := startNode(t, server.NodeInfo{NodeID: "node-uncapped"}, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	content := []byte("no cap configured")
	stream, err := client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk open failed: %v", err)
	}
	if err := stream.Send(&tjarkfs.PutChunkRequest{
		ChunkId: sha256ID(content),
		Size:    int64(len(content)),
	}); err != nil {
		t.Fatalf("sending the header failed: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("a header-only chunk was accepted; it should fail the size check")
	}

	// The decisive check: not one message may be refused with ResourceExhausted.
	stream, err = client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk open failed: %v", err)
	}
	if err := stream.Send(&tjarkfs.PutChunkRequest{
		ChunkId: sha256ID(content),
		Size:    int64(len(content)),
	}); err != nil {
		t.Fatalf("sending the header failed: %v", err)
	}
	if err := stream.Send(&tjarkfs.PutChunkRequest{Data: content}); err != nil {
		t.Fatalf("sending the data failed: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("an uncapped node refused a small chunk: %v (%v)", err, status.Code(err))
	}
}

// GracefulStop has to end the serving, not merely sleep. A node that keeps
// accepting calls after a signal never releases its port to the next one.
func TestGracefulStopStopsServing(t *testing.T) {
	srv, client := startNode(t, server.NodeInfo{NodeID: "node-stop"}, testMaxRecvBytes)

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("Addr is empty after Start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.GetNodeInfo(ctx, &tjarkfs.GetNodeInfoRequest{}); err != nil {
		t.Fatalf("the node does not answer before the stop: %v", err)
	}

	if err := srv.GracefulStop(ctx); err != nil {
		t.Fatalf("GracefulStop failed: %v", err)
	}

	// The port has to be free again, otherwise a restarted node cannot bind it.
	probe, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the node still holds %s after GracefulStop: %v", addr, err)
	}
	_ = probe.Close()

	// And a call that raced the shutdown has to fail rather than hang.
	callCtx, cancelCall := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCall()
	if _, err := client.GetNodeInfo(callCtx, &tjarkfs.GetNodeInfoRequest{}); err == nil {
		t.Error("a stopped node answered GetNodeInfo")
	}
}

func TestGracefulStopOnANodeThatNeverStarted(t *testing.T) {
	srv := server.NewStorageServer(
		store.NewFileSystemStorage(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil))),
		nil,
		testMaxRecvBytes,
		server.NodeInfo{},
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.GracefulStop(ctx); err != nil {
		t.Errorf("stopping a node that never started returned %v, want nil", err)
	}
}

// The address other nodes dial is not the address this process binds. Reporting
// the listen address would send every client to a port only this host can reach.
func TestGetNodeInfoReportsTheAdvertiseAddrNotTheListenAddr(t *testing.T) {
	_, client := startNode(t, server.NodeInfo{
		NodeID:        "node-adv",
		ListenAddr:    "127.0.0.1:0",
		AdvertiseAddr: "storage-1.internal:9100",
	}, testMaxRecvBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetNodeInfo(ctx, &tjarkfs.GetNodeInfoRequest{})
	if err != nil {
		t.Fatalf("GetNodeInfo failed: %v", err)
	}
	if info.GetAddress() != "storage-1.internal:9100" {
		t.Errorf("address = %q, want the advertise addr %q", info.GetAddress(), "storage-1.internal:9100")
	}
}

func TestAdvertiseAddrFallsBackToTheListenAddr(t *testing.T) {
	srv, client := startNode(t, server.NodeInfo{
		NodeID:     "node-fallback",
		ListenAddr: "127.0.0.1:0",
	}, testMaxRecvBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetNodeInfo(ctx, &tjarkfs.GetNodeInfoRequest{})
	if err != nil {
		t.Fatalf("GetNodeInfo failed: %v", err)
	}
	if info.GetAddress() != srv.Addr() {
		t.Errorf("address = %q, want the bound address %q", info.GetAddress(), srv.Addr())
	}
}

func TestStartReportsAnUnbindableAddress(t *testing.T) {
	srv := server.NewStorageServer(
		store.NewFileSystemStorage(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil))),
		nil,
		testMaxRecvBytes,
		server.NodeInfo{ListenAddr: "256.256.256.256:1"},
		nil,
	)

	err := srv.Start()
	if err == nil {
		_ = srv.GracefulStop(context.Background())
		t.Fatal("Start succeeded on an address that cannot be bound")
	}
	if srv.Addr() != "" {
		t.Errorf("Addr = %q after a failed Start, want empty", srv.Addr())
	}
}

// A node whose state was set from the controlplane keeps it across the wire;
// this also pins that SetNodeState survives being served by Start().
func TestSetNodeStateThroughAStartedNode(t *testing.T) {
	srv, client := startNode(t, server.NodeInfo{NodeID: "node-draining"}, testMaxRecvBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.SetNodeState(ctx, &tjarkfs.SetNodeStateRequest{
		NodeId: "node-draining",
		State:  tjarkfs.NodeState_NODE_STATE_DRAINING,
	}); err != nil {
		t.Fatalf("SetNodeState failed: %v", err)
	}

	if got := srv.NodeState(); got != domain.NodeDraining {
		t.Errorf("node state = %q, want %q", got, domain.NodeDraining)
	}

	// An unknown state is not an instruction, and not a panic either.
	if _, err := client.SetNodeState(ctx, &tjarkfs.SetNodeStateRequest{
		NodeId: "node-draining",
		State:  tjarkfs.NodeState(99),
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("an undefined state was answered with %v, want InvalidArgument", err)
	}
}
