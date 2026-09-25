# tjarkFS

Distributed filesystem: files are split into SHA256-content-addressed chunks
(deduplication across files), stored on a local filesystem, with file metadata
in SQLite.

## Usage

```
tjarkfs upload <datei> [--name <name>]
tjarkfs download <file-id> [output]
tjarkfs list [--full]
tjarkfs remove <file-id>
```

Global flags (or env): `--data-dir` / `DATA_DIR` (default `./data`),
`--chunk-size` / `CHUNK_SIZE` (e.g. `64k`, `1m`, default `1m`), `--verbose`.

## Development

```
make run ARGS="list"
make build
make test
```

## Structure

See [docs/STRUCTURE.md](docs/STRUCTURE.md) for the target layout
(`cmd/`, `internal/`, `api/`). The current codebase is the single-binary
phase: `cmd/client` wires `internal/client` (upload/download/delete/list
orchestration) together with `internal/chunker`,
`internal/storagenode/store` and `internal/controlplane/metadata`.