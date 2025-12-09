# leo_object_storage

[![Erlang/OTP](https://img.shields.io/badge/Erlang%2FOTP-19+-blue.svg)](https://www.erlang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

**leo_object_storage** is a log-structured object/BLOB storage library for Erlang/OTP applications. It implements Facebook's Haystack-inspired append-only storage format, providing efficient storage and retrieval of unstructured data.

### Key Features

- **Log-structured Storage**: Append-only design for high write performance
- **Haystack Format**: AVS (Append-only Versioned Storage) file format with MD5 checksums
- **Data Compaction**: Background garbage collection with configurable concurrency
- **Metadata Management**: Supports LevelDB or Bitcask as metadata backend
- **Chunked Objects**: Support for large objects split across multiple chunks
- **Diagnosis & Recovery**: Built-in tools for data diagnosis and metadata recovery
- **OTP Standard Logging**: Uses OTP logger for diagnostic output

## Requirements

- Erlang/OTP 21 or later
- rebar3

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {leo_object_storage, {git, "https://github.com/leo-project/leo_object_storage.git", {tag, "2.1.0"}}}
]}.
```

## Build

```bash
$ rebar3 compile
```

## Test

```bash
$ rebar3 eunit
```

## Architecture

### Module Structure

| Module | Description |
|--------|-------------|
| `leo_object_storage_api` | Public API entry point |
| `leo_object_storage_server` | Gen_server managing storage operations |
| `leo_object_storage_haystack` | Log-structured storage implementation |
| `leo_object_storage_sup` | Supervisor for process management |
| `leo_compact_fsm_controller` | FSM controller for compaction orchestration |
| `leo_compact_fsm_worker` | FSM worker for compaction execution |
| `leo_object_storage_transformer` | Data transformation utilities |

### Storage Format

The AVS (Append-only Versioned Storage) format uses 1024-byte headers containing:

- MD5 checksum (128 bits)
- Key size, data size, metadata size
- Offset, address ID, clock timestamp
- Delete flag, chunk information

## Configuration

Configuration options can be set in the application environment:

| Option | Default | Description |
|--------|---------|-------------|
| `metadata_storage` | `leveldb` | Metadata backend (`leveldb` or `bitcask`) |
| `object_storage` | `haystack` | Object storage format |
| `sync_mode` | `none` | Sync mode (`none`, `periodic`, `writethrough`) |
| `sync_interval_in_ms` | `1000` | Sync interval in milliseconds |
| `is_strict_check` | `false` | Enable strict data validation |
| `is_enable_diagnosis_log` | `true` | Enable diagnosis logging |

## Basic Usage

### Initialize Storage

```erlang
%% Start the application
application:start(leo_object_storage),

%% Start object storage with configuration
%% Format: [{NumOfContainers, Path}]
leo_object_storage_api:start([{1, "/path/to/storage"}]).
```

### Store an Object

```erlang
%% Create metadata and store object
Object = #?OBJECT{
    key = <<"my_key">>,
    data = <<"my_data">>,
    addr_id = 1
},
leo_object_storage_api:put({1, <<"my_key">>}, Object).
```

### Retrieve an Object

```erlang
%% Get object by address ID and key
{ok, Metadata, Object} = leo_object_storage_api:get({AddrId, Key}).
```

### Delete an Object

```erlang
%% Logical delete (marks as deleted)
leo_object_storage_api:delete({AddrId, Key}, Object).
```

### Data Compaction

```erlang
%% Run compaction with concurrency level
leo_object_storage_api:compact_data(NumOfConcurrency).

%% Check compaction status
{ok, Status} = leo_object_storage_api:compact_state().
```

### Diagnosis

```erlang
%% Run diagnosis on all containers
leo_object_storage_api:diagnose_data().

%% Recover metadata from AVS files
leo_object_storage_api:recover_metadata().
```

## Usage in LeoFS

**leo_object_storage** is a core component of [LeoFS](https://github.com/leo-project/leofs), used in [leo_storage](https://github.com/leo-project/leofs/tree/v1/apps/leo_storage) to store and manage unstructured data in distributed object storage.

## License

Apache License, Version 2.0

## Sponsors

- LeoProject/LeoFS is sponsored by [Lions Data, Ltd.](https://lions-data.com/) from Jan 2019.
- LeoProject/LeoFS was sponsored by [Rakuten, Inc.](https://global.rakuten.com/corp/) from 2012 to Dec 2018.
