# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Monstache is a Go daemon that syncs MongoDB to multiple data stores in real-time. Version 6 is designed for MongoDB 3.6+ and supports Elasticsearch 7.0+ as well as alternative sinks (ClickHouse, Kafka, File, Console).

Key functionality:
- Real-time sync via MongoDB change streams (default) or oplog tailing
- Direct reads for initial bulk sync of existing data
- JavaScript and Go plugin-based document transformation
- Resumable checkpointing for fault tolerance

## Build Commands

```bash
# Build for all platforms (Linux, Windows, macOS)
make all

# Build Linux release only
make release

# Build locally for development
go build -o monstache

# Clean build artifacts
make clean
```

## Running Tests

Integration tests require MongoDB and Elasticsearch running:

```bash
# Run tests locally (requires MongoDB 4.0+ and Elasticsearch 7.0+ on localhost)
go test -v

# Adjust delay between operations if tests fail (default 3 seconds)
go test -v -delay 10

# Environment variables for custom endpoints
MONGO_DB_URL=mongodb://localhost:27017 ELASTIC_SEARCH_URL=http://localhost:9200 go test -v

# Run tests via Docker Compose (isolated environment)
cd docker/test && ./run-tests.sh

# Run specific package tests
go test -v ./pkg/sinks/clickhouse/...
go test -v ./pkg/sinks/bulk/...
```

**Warning**: Tests are destructive - they drop the `test` database in MongoDB and indices prefixed with `test` in Elasticsearch.

## Architecture

### Core Flow

```
MongoDB → GTM (change streams) → Filtering/Transformation → Sink Router → Output Backends
```

### Main Components

- **monstache.go**: Entry point and core daemon logic. `indexClient` struct (line ~147) is the central coordinator managing MongoDB connections, sink connectors, and worker goroutines via channels.

- **pkg/sinks/**: Pluggable output backends
  - `factory.go`: Creates sink connectors based on config
  - `bulk/`: Batch processing service with retry/backoff
  - `clickhouse/`: ClickHouse HTTP client with dynamic schema
  - `kafka/`, `file/`, `console/`: Alternative sinks
  - `SinkConnector` interface: `RouteData()`, `RouteDelete()`, `RouteDrop()`, `Flush()`

- **monstachemap/**: Plugin interface definitions
  - `MapperPluginInput/Output`: For document transformation plugins
  - `ProcessPluginInput`: Extended with Elasticsearch client access
  - Plugins compiled with: `go build -buildmode=plugin -o myplugin.so`

- **GTM dependency** (github.com/rwynn/gtm): Handles MongoDB change stream consumption. Currently using a fork at github.com/XUJiahua/gtm with resumable direct read support.

### Configuration

Config files use TOML format. See:
- `config.toml`: Basic example
- `config.full.toml`: Complete reference with all options

Key config sections:
- MongoDB connection and namespace filtering
- Resume/checkpoint strategy (oplog timestamp or direct read offset)
- Sink-specific settings under `[sink.clickhouse]`, `[sink.kafka]`, etc.
- Bulk processor tuning (workers, batch-size, flush-interval)

### Plugin Development

```go
// Mapper plugin signature
func Map(input *monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error)

// Compile plugin
go build -buildmode=plugin -o myplugin.so myplugin.go

// Enable via CLI or config
monstache -mapper-plugin-path /path/to/myplugin.so
```

### HTTP Server

When `enable-http-server = true`, exposes:
- `/healthz`: Health check endpoint (used by Docker)
- Prometheus metrics at standard endpoint
