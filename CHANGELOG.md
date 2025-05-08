## [0.1.0-m1] – 2025-05-08

### Added

- **Page and Segmented files for all events**
  - Introduces segment writer actor which buffers events to pages and flushes them
  to segment files.

- **MMR persistence**  
  - `MmrIndexingActor` now computes per-event leaf hashes (via `sha256(archive(&event))`)  
  - Persists 32-byte MMR leaves into `xaero_mmr-*.seg` using the existing `SegmentWriterActor`  
  - the leaves will be then used to recompute the mmr again during replay or bootup.

- **Actor unit tests**  
  - Tests for in-memory MMR append behavior  
  - Tests capturing exact leaf hashes sent to the MMR segment writer
  - Tests for segment writer
