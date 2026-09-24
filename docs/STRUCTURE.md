# Projektstruktur (Go, Standard-Layout)

Die Struktur folgt dem gängigen Go-Layout (`cmd/`, `internal/`, `api/`). Jede Komponente hat eine klare Grenze, und die Phasen des Umsetzungsplans lassen sich jeweils einem Paket zuordnen.

```
tjarkfs/                          
├── cmd/
│   ├── controlplane/main.go      # Binary: Metadata Node
│   ├── storagenode/main.go       # Binary: Storage Node
│   └── client/main.go        # CLI-Client (upload/download/status)
│
├── api/
│   └── proto/
│       └── tjarkfs/v1/
│           ├── control.proto     # RegisterNode, Heartbeat, GetPlacement, ...
│           ├── storage.proto     # PutChunk (stream), GetChunk (stream), DeleteChunk
│           └── client.proto      # Upload/Download-API (Proxy-Modus)
│
├── gen/                          # generierter Code (protoc / buf, sqlc)
│   ├── proto/tjarkfs/v1/
│   └── sqlc/                     # sqlc-Output
│
├── internal/
│   ├── controlplane/
│   │   ├── server/               # gRPC-Handler (thin)
│   │   ├── metadata/             # Buckets, Files, File→Chunk (sqlc)
│   │   ├── membership/           # Node-Registry, Heartbeats, TTL, ACTIVE/DRAINING/DEAD
│   │   ├── placement/            # Interface + Consistent/Rendezvous Hashing
│   │   ├── repair/               # Self-Healing-Loop
│   │   └── gc/                   # Mark-and-Sweep für Orphaned Chunks
│   │
│   ├── storagenode/
│   │   ├── server/               # gRPC-Handler, Pipeline-Forwarding (A→B→C)
│   │   ├── store/                # ChunkStore-Interface + Disk-Implementierung
│   │   ├── scrubber/             # periodische Bitrot-Prüfung
│   │   └── heartbeat/            # Client-Seite Heartbeat/Register
│   │
│   ├── chunker/                  # Streaming-Chunking + io.MultiWriter-Hashing
│   ├── erasure/                  # Redundancy-Interface
│   │   ├── erasure.go            #   type Codec interface { Encode; Reconstruct }
│   │   ├── replication.go        #   Phase 2: 3x-Replikation
│   │   └── reedsolomon.go        #   Phase 3: klauspost/reedsolomon
│   ├── crypto/                   # AES-256-GCM, DEK/Master-Key (Envelope Encryption)
│   ├── client/                   # Upload-/Download-Orchestrierung, Scatter-Gather
│   ├── metrics/                  # Prometheus-Collector
│   └── config/                   # Config-Structs, Env/Flag-Parsing
│
├── db/
│   ├── migrations/               # 0001_init.up.sql, .down.sql (golang-migrate/goose)
│   └── queries/                  # *.sql für sqlc
│       ├── files.sql
│       ├── chunks.sql
│       └── nodes.sql
│
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile.controlplane
│   │   └── Dockerfile.storagenode
│   ├── docker-compose.yml        # 1 CP, 1 Postgres, 5 Storage Nodes
│   ├── prometheus/prometheus.yml
│   └── grafana/dashboards/*.json
│
├── test/
│   ├── integration/              # Multi-Node-Tests (testcontainers oder compose)
│   ├── chaos/                    # docker kill während Up-/Download, SHA256-Check
│   └── testdata/
│
├── bench/                        # go test -bench, pprof-Profile, Ergebnisse
│
├── docs/
│   ├── architecture.md           # Mermaid: Data Flow vs. Control Flow
│   ├── design-tradeoffs.md
│   ├── failure-modes.md
│   └── adr/                      # Architecture Decision Records (0001-….md)
│
├── buf.yaml / buf.gen.yaml       # Proto-Tooling
├── sqlc.yaml
├── Makefile                      # proto, sqlc, build, test, up, chaos, bench
├── go.mod
├── .golangci.yml
├── .github/workflows/ci.yml
└── README.md
```

## Zentrale Interfaces

Die Grenzen zwischen den Paketen sind bewusst als Interfaces gehalten, damit Implementierungen später austauschbar sind (Replikation → Reed-Solomon, Master-Mapping → Hashing):

```go
// internal/controlplane/placement
type Placer interface {
    GetPlacement(ctx context.Context, chunkID string, n int) ([]NodeID, error)
}

// internal/erasure
type Codec interface {
    Encode(data []byte) (shards [][]byte, err error)
    Reconstruct(shards [][]byte) error // nil-Einträge = fehlende Shards
    DataShards() int
    TotalShards() int
}

// internal/storagenode/store
type ChunkStore interface {
    Put(ctx context.Context, id string, r io.Reader, wantHash []byte) error
    Get(ctx context.Context, id string) (io.ReadCloser, error)
    Delete(ctx context.Context, id string) error
    Stat(ctx context.Context) (used, capacity uint64, err error)
}
```

## Abhängigkeitsregeln

- `cmd/*` verdrahtet nur (Config laden, Dependencies bauen, Server starten), keine Logik.
- `server/`-Pakete sind dünne gRPC-Adapter, die Logik liegt in `metadata`, `membership`, `repair` usw.
- `controlplane` und `storagenode` importieren sich nie gegenseitig, sie kommunizieren ausschließlich über die generierten Proto-Stubs.
- Geteilte Bausteine (`chunker`, `erasure`, `crypto`) haben keine Abhängigkeit auf gRPC oder die Datenbank und sind dadurch leicht unit-testbar.
- `internal/` verhindert, dass externe Module die Interna importieren. Nur wenn ein öffentliches Client-SDK angeboten werden soll, lohnt sich zusätzlich ein `pkg/client`.

## Makefile-Targets

```makefile
proto:   ; buf generate
sqlc:    ; sqlc generate
build:   ; go build -o bin/ ./cmd/...
test:    ; go test ./internal/...
up:      ; docker compose -f deploy/docker-compose.yml up --build
chaos:   ; go test ./test/chaos/... -tags=chaos -timeout 30m
bench:   ; go test ./bench/... -bench=. -benchmem -cpuprofile=bench/cpu.out
```