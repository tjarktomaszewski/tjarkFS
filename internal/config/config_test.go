package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Vorrangregel Flag > Env > Default. Sie ist der Kern des Pakets, weil sie
// nur dadurch entsteht, dass die Env-Variable den Flag-Default bildet.
func TestClientVorrangFlagUeberEnvUeberDefault(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		c, err := NewClient()
		require.NoError(t, err)

		assert.Equal(t, "./data", c.DataDir)
		assert.Equal(t, "1m", c.ChunkSize)
		assert.False(t, c.Verbose)
	})

	t.Run("env fails", func(t *testing.T) {
		t.Setenv("DATA_DIR", "/from/env")
		t.Setenv("CHUNK_SIZE", "64k")
		t.Setenv("VERBOSE", "true")

		c, err := NewClient()
		require.NoError(t, err)

		assert.Equal(t, "/from/env", c.DataDir)
		assert.Equal(t, "64k", c.ChunkSize)
		assert.True(t, c.Verbose)
	})

	t.Run("flag schlägt env", func(t *testing.T) {
		t.Setenv("DATA_DIR", "/from/env")
		t.Setenv("CHUNK_SIZE", "64k")
		t.Setenv("VERBOSE", "true")

		c, err := NewClient()
		require.NoError(t, err)
		require.NoError(t, c.FlagSet.Parse([]string{
			"--data-dir", "/from/flag",
			"--chunk-size", "2m",
			"--verbose=false",
		}))

		assert.Equal(t, "/from/flag", c.DataDir)
		assert.Equal(t, "2m", c.ChunkSize)
		assert.False(t, c.Verbose)
	})
}

// Typed Env-Werte, die der Plan je Binary vorsieht, und ihre Fehlermeldung.
func TestStorageNodeUndControlPlaneDefaults(t *testing.T) {
	s, err := NewStorageNode()
	require.NoError(t, err)
	assert.Equal(t, ":9100", s.ListenAddr)
	assert.Equal(t, "./data/node", s.DataDir)
	assert.Empty(t, s.NodeID)
	assert.Equal(t, "0", s.Capacity)
	assert.Equal(t, 5*time.Second, s.HeartbeatInterval)
	assert.Equal(t, int64(8<<20), s.MaxRecvBytes)
	assert.Equal(t, 6*time.Hour, s.ScrubInterval)

	c, err := NewControlPlane()
	require.NoError(t, err)
	assert.Equal(t, ":9000", c.ListenAddr)
	assert.Equal(t, "./data/cp", c.DataDir)
	assert.Equal(t, 24*time.Hour, c.UploadTTL)
	assert.Equal(t, 15*time.Second, c.HeartbeatTimeout)
	assert.Equal(t, 60*time.Second, c.LeaseTTL)
	assert.Equal(t, 3, c.ReplicationTarget)
	assert.InDelta(t, 0.8, c.RebalanceThreshold, 0.0001)
	assert.False(t, c.RebalanceEnabled)
}

func TestEnvUeberschreibtListenUndKapazitaet(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "127.0.0.1:9999")
	t.Setenv("CAPACITY", "10G")
	t.Setenv("REPLICATION_TARGET", "5")
	t.Setenv("HEARTBEAT_INTERVAL", "250ms")

	s, err := NewStorageNode()
	require.NoError(t, err)
	require.NoError(t, s.FlagSet.Parse([]string{"--capacity", "2G"}))
	assert.Equal(t, "127.0.0.1:9999", s.ListenAddr)
	assert.Equal(t, "2G", s.Capacity, "flag schlägt env")
	assert.Equal(t, 250*time.Millisecond, s.HeartbeatInterval)

	capacity, err := ParseChunkSize(s.Capacity)
	require.NoError(t, err)
	assert.Equal(t, int64(2<<30), capacity, "Sufixe wie --capacity=10G aus Phase 11")

	cp, err := NewControlPlane()
	require.NoError(t, err)
	assert.Equal(t, 5, cp.ReplicationTarget)
}

// Ein Tippfehler in der Umgebung darf nicht wie eine Absicht aussehen: der
// Wert muss scheitern, nicht auf den Default zurückfallen.
func TestUnlesbareEnvIstFehler(t *testing.T) {
	cases := []struct {
		env   string
		value string
		build func() error
	}{
		{"MAX_RECV_BYTES", "viel", func() error { _, err := NewStorageNode(); return err }},
		{"HEARTBEAT_INTERVAL", "5", func() error { _, err := NewStorageNode(); return err }},
		{"REPLICATION_TARGET", "drei", func() error { _, err := NewControlPlane(); return err }},
		{"REBALANCE_THRESHOLD", "hoch", func() error { _, err := NewControlPlane(); return err }},
		{"REBALANCE_ENABLED", "vielleicht", func() error { _, err := NewControlPlane(); return err }},
		{"VERBOSE", "manchmal", func() error { _, err := NewClient(); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)

			err := tc.build()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.env)
		})
	}
}

func TestParseChunkSize(t *testing.T) {
	valid := map[string]int64{
		"1024":    1024,
		"64k":     64 * 1024,
		"1m":      1024 * 1024,
		"2g":      2 * 1024 * 1024 * 1024,
		"0":       0,
		"1048576": 1048576,
		"10G":     10 * 1024 * 1024 * 1024,
	}
	for in, want := range valid {
		got, err := ParseChunkSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	for _, in := range []string{"", "10x", "m", "1.5m", "groß"} {
		_, err := ParseChunkSize(in)
		assert.Error(t, err, in)
	}
}
