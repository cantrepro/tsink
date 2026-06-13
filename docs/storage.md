# Storage Format Specification

Status: draft v0 (implementation target)

This document defines the tsink on-disk and WAL formats. It is intended to be detailed enough to implement encoder/decoder code without reading other docs.

## Goals

- Chunk/block-centric layout.
- Binary metadata and index files only.
- Efficient replay with WAL frames keyed by `series_id`.
- Crash-safe segment commit protocol.
- Deterministic, idempotent recovery.

## Scope

- Backward compatibility with prior formats is not required.
- This spec covers persisted format and replay semantics, not query API shape.

## Global Conventions

- Endianness: little-endian for all fixed-width numeric fields.
- Signed timestamps: `i64` Unix time in configured precision.
- Variable integers: unsigned LEB128 (`uvarint`) and zigzag LEB128 (`svarint`).
- Checksums:
  - `crc32c` for frame/chunk corruption detection.
  - `xxh64` for whole-file integrity in manifests.
- Magic constants are ASCII byte sequences.
- All lists that are searched by binary search are sorted strictly ascending.

## Directory Layout

```text
<data_path>/
  manifest.current
  wal/
    wal-000000000001.log
    wal-000000000002.log
  segments/
    L0/
      seg-<segment_id>/
        manifest.bin
        chunks.bin
        chunk_index.bin
        series.bin
        postings.bin
    L1/
    L2/
```

### Temporary Files

Writers use `*.tmp` and rename only after fsync. Temporary files are excluded from `effective_bpp` accounting.

## Segment Family

Each immutable segment directory contains exactly 5 files:

- `chunks.bin`: chunk payload stream.
- `chunk_index.bin`: fixed-width lookup entries for chunk locations/time bounds.
- `series.bin`: series dictionary + metric/label dictionaries.
- `postings.bin`: label postings (`label_name`,`label_value`) -> sorted series ids.
- `manifest.bin`: checksums and summary statistics for the segment.

### Segment ID

- `segment_id: u64` is globally unique and monotonically increasing.
- Compaction outputs allocate new segment ids; input segment ids are never reused.

## File: `manifest.bin`

### Header (fixed, 80 bytes)

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `TSM2` |
| version | `u16` | starts at `1` |
| flags | `u16` | reserved |
| segment_id | `u64` | segment identifier |
| level | `u8` | `0..=2` |
| reserved0 | `[u8; 7]` | zero |
| min_ts | `i64` | min timestamp across all chunks |
| max_ts | `i64` | max timestamp across all chunks |
| created_unix_ns | `i64` | wall clock creation time |
| series_count | `u64` | unique series |
| chunk_count | `u64` | total chunks |
| point_count | `u64` | total points |
| wal_highwater_segment | `u64` | last WAL segment fully included |
| wal_highwater_frame | `u64` | last WAL frame seq fully included |
| file_entry_count | `u32` | must be `4` |
| reserved1 | `u32` | zero |

### File Entries (repeated `file_entry_count`)

| Field | Type | Notes |
|---|---:|---|
| file_kind | `u8` | `1=chunks`, `2=chunk_index`, `3=series`, `4=postings` |
| compression | `u8` | `0=none` |
| reserved | `u16` | zero |
| file_len | `u64` | bytes on disk |
| xxh64 | `u64` | hash of full file bytes |

### Trailer

| Field | Type | Notes |
|---|---:|---|
| manifest_crc32c | `u32` | over header + file entries |

Validation fails if any checksum mismatches.

## File: `series.bin`

### Header

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `SRS2` |
| version | `u16` | `1` |
| flags | `u16` | reserved |
| metric_dict_count | `u32` | entries |
| label_name_dict_count | `u32` | entries |
| label_value_dict_count | `u32` | entries |
| series_count | `u64` | entries |

### Dictionaries

For each dictionary entry:

- `id: u32`
- `byte_len: u32`
- `bytes[byte_len]` (UTF-8, no NUL)

Dictionary ids must be dense `0..n-1`.

### Series Entries (sorted by `series_id`)

| Field | Type | Notes |
|---|---:|---|
| series_id | `u64` | unique in DB |
| lane | `u8` | `0=numeric`, `1=blob` |
| reserved | `u8` | zero |
| label_pair_count | `u16` | count |
| metric_id | `u32` | metric dictionary id |
| first_label_pair_offset | `u64` | absolute offset to label pairs |

Label pairs block referenced by `first_label_pair_offset` is `label_pair_count` pairs of:

- `label_name_id: u32`
- `label_value_id: u32`

Label pairs for a series must be sorted by `(label_name_id, label_value_id)` and unique.

## File: `postings.bin`

### Header

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `PST2` |
| version | `u16` | `1` |
| flags | `u16` | reserved |
| postings_count | `u64` | number of posting lists |

### Posting List Entries

| Field | Type | Notes |
|---|---:|---|
| label_name_id | `u32` | key part |
| label_value_id | `u32` | key part |
| series_count | `u32` | list len |
| encoded_len | `u32` | bytes of delta-encoded series ids |
| series_ids_delta_uvarint | `bytes` | sorted unique ids, delta encoded |

Posting list entries are sorted lexicographically by `(label_name_id, label_value_id)`.

## File: `chunks.bin`

### Header

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `CHK2` |
| version | `u16` | `1` |
| flags | `u16` | reserved |
| chunk_count | `u64` | number of chunk records |

### Chunk Record (repeated)

| Field | Type | Notes |
|---|---:|---|
| record_len | `u32` | full record bytes after this field |
| header_crc32c | `u32` | checksum of fixed header below |
| series_id | `u64` | owner series |
| lane | `u8` | `0=numeric`, `1=blob` |
| ts_codec | `u8` | timestamp codec id |
| val_codec | `u8` | value codec id |
| chunk_flags | `u8` | reserved |
| point_count | `u16` | must be `1..=65535` |
| min_ts | `i64` | inclusive |
| max_ts | `i64` | inclusive |
| payload_len | `u32` | encoded payload length |
| payload | `bytes` | codec payload |
| payload_crc32c | `u32` | checksum of payload |

`record_len` must match serialized record size exactly.

## File: `chunk_index.bin`

### Header

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `CID2` |
| version | `u16` | `1` |
| flags | `u16` | reserved |
| entry_count | `u64` | total chunk index entries |
| series_table_count | `u64` | number of per-series range records |

### Chunk Index Entries (fixed-width, sorted by `series_id`,`min_ts`,`max_ts`,`chunk_offset`)

| Field | Type | Notes |
|---|---:|---|
| series_id | `u64` | key |
| min_ts | `i64` | inclusive |
| max_ts | `i64` | inclusive |
| chunk_offset | `u64` | byte offset inside `chunks.bin` to chunk record |
| chunk_len | `u32` | bytes in `chunks.bin` |
| point_count | `u16` | count |
| lane | `u8` | lane |
| ts_codec | `u8` | codec id |
| val_codec | `u8` | codec id |
| level | `u8` | segment level |

### Series Range Table

| Field | Type | Notes |
|---|---:|---|
| series_id | `u64` | series |
| first_entry_index | `u64` | index in chunk index array |
| entry_count | `u32` | number of chunks for series |
| reserved | `u32` | zero |

The series range table is sorted by `series_id` for direct binary search.

## Codec IDs

Timestamp codecs (`ts_codec`):

- `1`: fixed-step RLE
- `2`: delta-of-delta bitpacking
- `3`: delta varint fallback

Value codecs (`val_codec`):

- `1`: Gorilla XOR (`f64`)
- `2`: zigzag delta bitpack (`i64`)
- `3`: delta bitpack (`u64`)
- `4`: constant RLE
- `5`: bool bitpack
- `6`: bytes/string length+payload delta blocks (blob lane)

Codec selection is per chunk and persisted in chunk header/index.

## WAL

WAL directory: `<data_path>/wal`.

### WAL File Header

| Field | Type | Notes |
|---|---:|---|
| magic | `[u8; 4]` | `WAL2` |
| version | `u16` | `1` |
| flags | `u16` | reserved |
| wal_segment_id | `u64` | monotonic |

### Frame Envelope

| Field | Type | Notes |
|---|---:|---|
| frame_len | `u32` | bytes after this field |
| frame_type | `u8` | `1=samples`, `2=checkpoint` |
| frame_flags | `u8` | reserved |
| reserved | `u16` | zero |
| frame_seq | `u64` | monotonic inside WAL segment |
| payload_crc32c | `u32` | payload checksum |
| header_crc32c | `u32` | envelope checksum except this field |
| payload | `bytes` | type-specific |

### Samples Payload (`frame_type=1`)

| Field | Type | Notes |
|---|---:|---|
| batch_count | `u16` | number of series batches |
| reserved | `u16` | zero |
| payload_xxh64 | `u64` | hash over all batch bytes |
| batches | `bytes` | repeated batch records |

Batch record:

| Field | Type | Notes |
|---|---:|---|
| series_id | `u64` | target series |
| lane | `u8` | numeric/blob |
| ts_codec | `u8` | codec used in batch |
| val_codec | `u8` | codec used in batch |
| batch_flags | `u8` | reserved |
| sample_count | `u16` | number of samples |
| base_ts | `i64` | timestamp base |
| ts_payload_len | `u32` | bytes |
| val_payload_len | `u32` | bytes |
| ts_payload | `bytes` | encoded timestamp deltas |
| val_payload | `bytes` | encoded values |

### Replay Semantics

1. Read all segment manifests and derive durable high-water mark `(wal_segment_id, frame_seq)`.
2. Scan WAL files in increasing `wal_segment_id`.
3. For each frame:
   - verify envelope and payload checksums,
   - stop at first corrupt/truncated frame in a WAL file,
   - ignore frames `<=` durable high-water mark,
   - apply later frames to in-memory chunk builders.
4. On checkpoint/flush commit, write new segment files and update manifest high-water mark atomically.
5. Recovery is idempotent because high-water mark prevents double-apply.

Corruption policy: truncate WAL from first invalid frame boundary and keep prior valid frames.

## Crash-Safe Segment Commit Protocol

For each output segment:

1. Write `*.tmp` for `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.
2. `fsync` each file.
3. Compute file hashes and write `manifest.bin.tmp`, then `fsync`.
4. Rename data files into final names.
5. Rename `manifest.bin.tmp` -> `manifest.bin` (last).
6. `fsync` segment directory.
7. Update `manifest.current` via write+fsync+rename.

A segment directory without a valid `manifest.bin` is ignored during startup.

## Versioning Rules

- `version` in each file is a major format version.
- Reader behavior:
  - same version: must decode.
  - higher version: fail fast with explicit unsupported-version error.
- Unknown flags must be zero until explicitly defined.

## Invariants

- `series_id` is globally unique and never reused.
- Every `series_id` in `chunks.bin` and `postings.bin` must exist in `series.bin`.
- `chunk_index.bin` entries must map to valid chunk record boundaries.
- Within a chunk: timestamps are strictly increasing after decode.
- `point_count` in chunk header/index must equal decoded point count.
- Postings lists contain sorted unique series ids.
- Blob-lane values never use numeric codecs.
- Numeric-lane chunks never mix numeric and blob value types.

## Retention and Compaction Expectations

- When retention enforcement is enabled, retention removes whole segments whose `max_ts < retention_cutoff`.
- Compaction rewrites overlapping ranges into higher levels and emits new immutable segments.
- Compaction must preserve per-series timestamp order and exact sample values.
- Manifest statistics (`point_count`, `chunk_count`, `series_count`) must match file contents exactly.
