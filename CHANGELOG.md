# Changelog

All notable changes to Xaeroflux will be documented in this file.

[0.1.0-m2] - 2025-05-18

Added
• SDK API (xaeroflux_sdk): a reactive, Rx-like interface for integrating event streams in client applications.
• Subject::unsafe_run: a one-call entrypoint that wires up the AOF, MMR, and Segment Writer actors behind a Subject sink for full persistence and indexing.
• On-disk event header (XaeroOnDiskEventHeader + EVENT_HEADER_SIZE constant) enabling zero-copy slicing and alignment of archived events in mmap pages.
• Plumbing coverage: SDK API now auto-instantiates and manages the AOF actor, MMR actor, and Segment Writer actor under the hood.

⸻

[0.1.0-m1] - 2025-05-08

Added
 • Page and Segmented files for all events
 • Introduces SegmentWriterActor which buffers events into fixed-size pages and flushes them to segment files.
 • MMR persistence
 • MmrIndexingActor computes per-event leaf hashes (sha256(archive(&event))).
 • Persists 32-byte MMR leaves into xaero_mmr-*.seg via the existing segment writer.
 • Leaves can be replayed to reconstruct the MMR index on startup.
 • Actor unit tests
 • In-memory MMR append behavior.
 • Exact leaf-hash capture for MMR segment writer.
 • Segment writer integration tests.