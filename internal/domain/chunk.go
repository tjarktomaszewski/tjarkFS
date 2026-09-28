package domain

import "time"

type ChunkID string
type NodeID string
type NodeState string

type Node struct {
	ID             NodeID
	Address        string
	Capacity, Used int64
	ChunkCount     int64
	State          NodeState
	LastHeartbeat  time.Time
}

const (
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeDead     NodeState = "dead"
)

const ()

type Chunk struct {
	ID   ChunkID
	Size int64
}
