package chunker

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkerSplitsData(t *testing.T) {

	input := []byte("Hello Distributed File System")

	chunker, _ := NewChunker(
		bytes.NewReader(input),
		10,
	)

	var chunks [][]byte

	for {
		stream, err := chunker.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf(
				"unexpected error: %v",
				err,
			)
		}

		data, err := io.ReadAll(stream.Reader)

		if err != nil {
			t.Fatalf(
				"failed reading chunk: %v",
				err,
			)
		}

		chunks = append(
			chunks,
			data,
		)
	}

	expected := [][]byte{
		[]byte("Hello Dist"),
		[]byte("ributed Fi"),
		[]byte("le System"),
	}

	if len(chunks) != len(expected) {
		t.Fatalf(
			"expected %d chunks, got %d",
			len(expected),
			len(chunks),
		)
	}

	for i := range expected {

		if !bytes.Equal(
			chunks[i],
			expected[i],
		) {
			t.Errorf(
				"chunk %d mismatch: expected %s got %s",
				i,
				expected[i],
				chunks[i],
			)
		}
	}
}

func TestChunkerConstructor(t *testing.T) {
	input := []byte("Hello Distributed File System")
	_, err := NewChunker(
		bytes.NewReader(input),
		-1,
	)

	assert.Error(t, err)

	_, err = NewChunker(
		bytes.NewReader(input),
		1,
	)

	assert.NoError(t, err)
}
