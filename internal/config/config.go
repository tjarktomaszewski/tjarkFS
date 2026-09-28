// Package config bündelt die Konfiguration der drei Binaries (Client,
// Controlplane, StorageNode): je eine Struct, die die Felder hält, und ein
// *flag.FlagSet, das diese Felder über StringVar/IntVar/Int64Var/Float64Var/
// BoolVar/DurationVar bindet.
//
// Vorrangregel: Flag > Env > hartkodierter Default. Sie entsteht von
// selbst, weil die Env-Variable den Default des Flags bildet: ein explizit
// gesetztes Flag überschreibt den Default, ein nicht gesetztes behält ihn.
// Damit kann die Parse-Logik nicht von der Regel abweichen.
//
// Env-Namen folgen den Flag-Namen in SCREAMING_SNAKE_CASE, also
// --data-dir → DATA_DIR und --heartbeat-interval → HEARTBEAT_INTERVAL.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Client struct {
	DataDir   string // --data-dir
	ChunkSize string // --chunk-size, mit Suffix (z. B. 64k, 1m)
	Verbose   bool   // --verbose

	FlagSet *flag.FlagSet
}

func NewClient() (*Client, error) {
	verbose, err := env("VERBOSE", false, parseBool)
	if err != nil {
		return nil, err
	}

	c := &Client{FlagSet: flag.NewFlagSet("tjarkfs-client", flag.ContinueOnError)}

	c.FlagSet.StringVar(&c.DataDir, "data-dir", envString("DATA_DIR", "./data"),
		"Directory for Chunks and Metadata-DB")
	c.FlagSet.StringVar(&c.ChunkSize, "chunk-size", envString("CHUNK_SIZE", "1m"),
		"Chunk-size (e.g. 64k, 1m, 64m)")
	c.FlagSet.BoolVar(&c.Verbose, "verbose", verbose,
		"verbose logging")

	return c, nil
}

type StorageNode struct {
	ListenAddr        string        // --listen
	DataDir           string        // --data-dir
	NodeID            string        // --node-id, empty = UUID in <data-dir>/node-id
	Capacity          string        // --capacity, with Suffix (e.g. 10G), 0 = undefined
	HeartbeatInterval time.Duration // --heartbeat-interval
	MaxRecvBytes      int64         // --max-recv-bytes
	ScrubInterval     time.Duration // --scrub-interval

	FlagSet *flag.FlagSet
}

func NewStorageNode() (*StorageNode, error) {
	heartbeatInterval, err := env("HEARTBEAT_INTERVAL", 5*time.Second, parseDuration)
	if err != nil {
		return nil, err
	}
	maxRecvBytes, err := env("MAX_RECV_BYTES", int64(8<<20), parseInt64)
	if err != nil {
		return nil, err
	}
	scrubInterval, err := env("SCRUB_INTERVAL", 6*time.Hour, parseDuration)
	if err != nil {
		return nil, err
	}

	s := &StorageNode{FlagSet: flag.NewFlagSet("tjarkfs-storagenode", flag.ContinueOnError)}

	s.FlagSet.StringVar(&s.ListenAddr, "listen", envString("LISTEN_ADDR", ":9100"),
		"Adresse, auf der der StorageNode gRPC annimmt")
	s.FlagSet.StringVar(&s.DataDir, "data-dir", envString("DATA_DIR", "./data/node"),
		"Verzeichnis für Chunks, Fence-Sidecar und Node-ID")
	s.FlagSet.StringVar(&s.NodeID, "node-id", envString("NODE_ID", ""),
		"Node-ID; leer = einmalig generierte UUID in <data-dir>/node-id")
	s.FlagSet.StringVar(&s.Capacity, "capacity", envString("CAPACITY", "0"),
		"Kapazität mit Suffix (z. B. 10G), 0 = unbestimmt; Bytes via config.ParseChunkSize")
	s.FlagSet.DurationVar(&s.HeartbeatInterval, "heartbeat-interval", heartbeatInterval,
		"Takt des Heartbeats zum Controlplane")
	s.FlagSet.Int64Var(&s.MaxRecvBytes, "max-recv-bytes", maxRecvBytes,
		"Maximal empfangene Bytes pro PutChunk-Stream")
	s.FlagSet.DurationVar(&s.ScrubInterval, "scrub-interval", scrubInterval,
		"Takt des Scrubbers")

	return s, nil
}

type ControlPlane struct {
	ListenAddr         string        // --listen
	DataDir            string        // --data-dir
	UploadTTL          time.Duration // --upload-ttl
	HeartbeatTimeout   time.Duration // --heartbeat-timeout
	LeaseTTL           time.Duration // --lease-ttl
	ReplicationTarget  int           // --replication-target
	RebalanceThreshold float64       // --rebalance-threshold
	GCInterval         time.Duration // --gc-interval
	RepairInterval     time.Duration // --repair-interval
	RebalanceInterval  time.Duration // --rebalance-interval
	RebalanceEnabled   bool          // --rebalance-enabled

	FlagSet *flag.FlagSet
}

func NewControlPlane() (*ControlPlane, error) {
	uploadTTL, err := env("UPLOAD_TTL", 24*time.Hour, parseDuration)
	if err != nil {
		return nil, err
	}
	heartbeatTimeout, err := env("HEARTBEAT_TIMEOUT", 15*time.Second, parseDuration)
	if err != nil {
		return nil, err
	}
	leaseTTL, err := env("LEASE_TTL", 60*time.Second, parseDuration)
	if err != nil {
		return nil, err
	}
	replicationTarget, err := env("REPLICATION_TARGET", 3, parseInt)
	if err != nil {
		return nil, err
	}
	rebalanceThreshold, err := env("REBALANCE_THRESHOLD", 0.8, parseFloat64)
	if err != nil {
		return nil, err
	}
	gcInterval, err := env("GC_INTERVAL", time.Minute, parseDuration)
	if err != nil {
		return nil, err
	}
	repairInterval, err := env("REPAIR_INTERVAL", 10*time.Second, parseDuration)
	if err != nil {
		return nil, err
	}
	rebalanceInterval, err := env("REBALANCE_INTERVAL", time.Minute, parseDuration)
	if err != nil {
		return nil, err
	}
	rebalanceEnabled, err := env("REBALANCE_ENABLED", false, parseBool)
	if err != nil {
		return nil, err
	}

	c := &ControlPlane{FlagSet: flag.NewFlagSet("tjarkfs-controlplane", flag.ContinueOnError)}

	c.FlagSet.StringVar(&c.ListenAddr, "listen", envString("LISTEN_ADDR", ":9000"),
		"Adresse, auf der der ControlPlane gRPC annimmt")
	c.FlagSet.StringVar(&c.DataDir, "data-dir", envString("DATA_DIR", "./data/cp"),
		"Verzeichnis für die Metadaten-DB")
	c.FlagSet.DurationVar(&c.UploadTTL, "upload-ttl", uploadTTL,
		"Alter, ab dem ein UPLOADING-File als abgestorben gilt")
	c.FlagSet.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", heartbeatTimeout,
		"Alter des letzten Heartbeats, ab dem ein Node als DEAD gilt")
	c.FlagSet.DurationVar(&c.LeaseTTL, "lease-ttl", leaseTTL,
		"Gültigkeitsdauer einer Lease")
	c.FlagSet.IntVar(&c.ReplicationTarget, "replication-target", replicationTarget,
		"Anzahl angestrebter Replikate pro Chunk")
	c.FlagSet.Float64Var(&c.RebalanceThreshold, "rebalance-threshold", rebalanceThreshold,
		"used/capacity, ab dem ein Node auf DRAINING gesetzt wird")
	c.FlagSet.DurationVar(&c.GCInterval, "gc-interval", gcInterval,
		"Takt des Aufräumers für abgestorbene Uploads")
	c.FlagSet.DurationVar(&c.RepairInterval, "repair-interval", repairInterval,
		"Takt des Repair-Workers")
	c.FlagSet.DurationVar(&c.RebalanceInterval, "rebalance-interval", rebalanceInterval,
		"Takt des Rebalance-Workers")
	c.FlagSet.BoolVar(&c.RebalanceEnabled, "rebalance-enabled", rebalanceEnabled,
		"Load-aware Placement (LoadAware) statt reinem Rendezvous-Hashing")

	return c, nil
}

// ParseChunkSize transforms size indication (optional k/m/g-Suffix)
// to Bytes; without suffix the value already is in bytes. Suffix can be capital or
// lowercase
func ParseChunkSize(chunkSizeStr string) (int64, error) {
	if chunkSizeStr == "" {
		return 0, fmt.Errorf("chunk size cannot be empty")
	}

	lastChar := chunkSizeStr[len(chunkSizeStr)-1]

	// No suffix: interpret as bytes
	if lastChar >= '0' && lastChar <= '9' {
		return strconv.ParseInt(chunkSizeStr, 10, 64)
	}

	value, err := strconv.ParseInt(chunkSizeStr[:len(chunkSizeStr)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse chunk size %q: %w", chunkSizeStr, err)
	}

	switch lastChar {
	case 'k', 'K':
		return value * 1024, nil
	case 'm', 'M':
		return value * 1024 * 1024, nil
	case 'g', 'G':
		return value * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("invalid chunk size suffix: %c", lastChar)
	}
}

// Returns value of name env, or default (def) when empty
func envString(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// env reads name as T with parse and defaults to def if variable is not set
// Unreadable value is an error and not silently skipped or silently returned default
func env[T any](name string, def T, parse func(string) (T, error)) (T, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}

	parsed, err := parse(v)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("env %s=%q: %w", name, v, err)
	}
	return parsed, nil
}

func parseBool(s string) (bool, error)       { return strconv.ParseBool(s) }
func parseInt(s string) (int, error)         { return strconv.Atoi(s) }
func parseInt64(s string) (int64, error)     { return strconv.ParseInt(s, 10, 64) }
func parseFloat64(s string) (float64, error) { return strconv.ParseFloat(s, 64) }
func parseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}
