# Changelog

All notable changes to Xaeroflux will be documented in this file.

[0.3.0-m3] – 2025-06-06

Added
•	SubjectHash: derive a blake3 hash of workspace/wid/Object/oid as a root directory namespace for LMDB and segment files.
•	SubjectTagging: the SubjectHash is now attached to each Subject for filesystem isolation.
•	Control and Data Pipes: introduces per-Subject pipes to route events to appropriate actors:
•	XaeroEvent travels into a Pipe, is picked up by a XAERO_DISPATCHER_POOL thread via execute, delivered to each actor’s listener, and then dispatched to its handler.
•	Control pipes accept only control-plane payloads (e.g., metadata updates, system events).
•	Data pipes accept only data-plane payloads (e.g., CRDT ops, application events).
•	derive(PipeKind) Macro: adds a #[derive(PipeKind)] macro with pipe_kind(Control) attribute that generates ControlNetworkPipe and DataNetworkPipe types.
These network pipes carry NetworkPayload for peer-to-peer flows: control-plane events from peers or data-plane CRDT operations.

This is a stepping-stone release (minor version increment) but remains part of the m3 milestone.

⸻

[0.2.0-m3] – 2025-06-04

Added
•	Introduces subcrate xaeroflux-actors (housing storage and indexing actors; may require cleanup in a future release).
•	Introduces subcrate xaeroflux-core (containing all core structs for events, system calls, hashing utilities, and essential data structures).
•	Introduces xaeroflux-macros, including the subject! macro, which:
•	Instantiates a new Subject with a single call.
•	Automatically emits two SystemPayload events—WorkspaceCreated and ObjectCreated—to the sink before unsafe_run executes.
•	Establishes the concept of Workspace and Object as namespacing constructs to better organize edits and CRDT operations.
•	Adds a P2P layer skeleton with two distinct planes:
•	Control Plane: transmits network payloads for control‐level events (e.g. PeaksWritten, SegmentRolledOver, and other metadata exchanges). This layer enables peers to diff missing data before exchanging actual payloads, thus reducing bandwidth.
•	Data Plane: responsible for transmitting actual data payloads between peers.
•	Provides an abstraction layer allowing the Iroh-based implementation to be swapped out in the future if needed.
•	Includes basic subject! macro test cases.

Next up
•	Implement and test the P2P Control Plane over local Wi-Fi, mDNS, and DHT.

⸻

[0.1.0-m3] - 2025-05-22

Added:
•	Read portion of SDK API - Replayable subject that scans events from segments and relays them through subject booted with Scan operator.
•	Segment Reader plumbing that reads segments for Replay.
•	Segment reading logic.
•	Configuration for files.
•	Cleanup and more test cases.

⸻

[0.1.0-m2] - 2025-05-18

Added
•	SDK API (xaeroflux_sdk): a reactive, Rx-like interface for integrating event streams in client applications.
•	Subject::unsafe_run: a one-call entrypoint that wires up the AOF, MMR, and Segment Writer actors behind a Subject sink for full persistence and indexing.
•	On-disk event header (XaeroOnDiskEventHeader + EVENT_HEADER_SIZE constant) enabling zero-copy slicing and alignment of archived events in mmap pages.
•	Plumbing coverage: SDK API now auto-instantiates and manages the AOF actor, MMR actor, and Segment Writer actor under the hood.

⸻

[0.1.0-m1] - 2025-05-08

Added
•	Page and Segmented files for all events.
•	Introduces SegmentWriterActor which