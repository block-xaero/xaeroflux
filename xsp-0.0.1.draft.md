# Xaeroflux Sync Protocol Specification (Draft 0.0.1)

This draft defines the **Xaeroflux Sync Protocol (XSP)** — a unified, Merkle-powered, event-centric p2p protocol for synchronizing append-only events across native apps, browsers, IoT nodes, and AI models. It supports CRDT-enabled events natively, enabling conflict-free, convergent state for any event type.

---

## 1. Design Principles

1. **Event-First & CRDT-Ready**: Every payload is an *Event*, with an optional CRDT header, ensuring conflict-free merges for distributed state.
2. **Transport-Agnostic**: Operates over any bidirectional byte stream (libp2p, QUIC, WebRTC, TCP).
3. **Merkle Indexing**: Uses Merkle Mountain Range (MMR) roots to efficiently identify missing events.
4. **Low Chattiness**: Two-phase diff (PEAKS ↔ DIFF) plus batched PAGE\_REQUEST/PAGE\_RESPONSE.
5. **Model-Ready (MCP)**: AI nodes share context/tensors as standard events (`MODEL_CTX`) under the same Merkle-backed sync.
6. **Extensible**: New event types or control messages require only new codes—no fundamental protocol changes.

---

## 2. Framing & Message Format

All wire messages share a simple length-framed envelope:

```ascii
┌─────────┬───────────────-┬──────────┐
│ Type    │ Length (varint)│ Payload  │
└─────────┴───────────────-┴──────────┘
```

* **Type** (1 byte): message code (see §3).
* **Length** (varint): number of payload bytes.
* **Payload**: structured per message.

---

## 3. Wire Messages

| Code | Name           | Dir | Payload                                                                  |
| :--: | :------------- | :-- | :----------------------------------------------------------------------- |
| 0x01 | HELLO          | ↔   | `version:u8 ∥ peer_id:32 ∥ cap_count:varint ∥ [cap:u8]…`                 |
| 0x02 | HELLO\_ACK     | ↔   | `version:u8 ∥ cap_count:varint ∥ [cap:u8]…`                              |
| 0x10 | PEAKS          | →   | `count:varint ∥ [hash₀…hashₙ]` (MMR peak hashes)                         |
| 0x11 | DIFF           | ←   | `missing:varint ∥ [idx₀…idxₖ]` (indexes of missing peaks)                |
| 0x20 | PAGE\_REQUEST  | ←   | `segment_id:varint ∥ page_count:varint ∥ [page_idx:varint]…`             |
| 0x21 | PAGE\_RESPONSE | →   | `segment_id ∥ page_count ∥ for each: (page_idx ∥ blob_len ∥ blob_bytes)` |
| 0x30 | SYNC\_DONE     | ↔   | *(no payload)*                                                           |

**Capabilities** (`cap:u8`) include `0x01=SYNC`, `0x02=CRDT`, `0x03=MCP`, etc.

---

## 4. Event Encoding inside Pages

Each `blob_bytes` in `PAGE_RESPONSE` is exactly one fixed-size page (e.g., 16 KiB) containing back-to-back *Event* records with optional CRDT metadata:

```ascii
┌────────────────────┬──────────────┬──────────────────┬──────────-------------┐
│ event_type:u8      │ crdt_type:u8 │ crdt_id:varint   │ length:varint ∥ data… │
└────────────────────┴──────────────┴──────────────────┴────────────────-------┘
```

* **event\_type**:  identifies schema (`0x01=APPLICATION`, `0x02=MODEL_CTX`, `0x03=FEATURE_MAP`, `0x04=IMAGE_POST`, etc.).
* **crdt\_type**:  `0x00=NONE`, `0x01=G_COUNTER`, `0x02=PN_COUNTER`, `0x03=OR_SET`, etc.
* **crdt\_id**:    varint identifier for CRDT instance (e.g., image ID, configuration key).
* **length**:    varint size of `data`.
* **data**:      the serialized payload (Protobuf, FlatBuffers, raw bytes).

Pages are zero-padded after the last event to fill the fixed size.

---

## 5. CRDT Conflict Resolution

XSP embeds CRDT logic directly in event records:

1. **Event Arrival**: On receiving a CRDT-enabled event (`crdt_type ≠ 0`), peers extract `(crdt_type, crdt_id, data)`.
2. **Merge Function**: Apply the CRDT-specific merge:

   * **G\_COUNTER**: maintain map of peer→count, take element-wise max.
   * **PN\_COUNTER**: maintain two G\_COUNTERs for increments/decrements.
   * **OR\_SET**: maintain add/remove sets, reconciled via set union/subtraction.
3. **State Publication**: Local merged state can be re-emitted as a CRDT event, ensuring monotonic convergence.
4. **Idempotence**: Duplicate or out-of-order CRDT events yield the same merged state.

CRDT events flow through the same Merkle-backed sync—PEAKS, DIFF, PAGE\_REQUEST/RESPONSE—so state converges without extra chatter.

---

## 6. Sync Workflow

1. **Handshake**

   * A↔B: HELLO ↔ HELLO\_ACK (negotiate version & capabilities: SYNC, CRDT, MCP).
2. **Merkle Diff**

   * A → B: PEAKS
   * B → A: DIFF
3. **Fetch Pages**

   * B → A: PAGE\_REQUEST
   * A → B: PAGE\_RESPONSE
4. **Page Processing**

   * B parses each page blob, extracts events (with CRDT merge), applies merges, updates AOF & live MMR.
5. **Completion**

   * Either side: SYNC\_DONE, then close stream.

---

## 7. Single-Event Appends & Live MMR Index

On local event creation:

1. **AOF Append & MMR Push**

   * Append event bytes to LMDB AOF.
   * Compute `leaf_hash = sha256(event_bytes)`, call `mmr.push(leaf_hash)`.
   * Broadcast updated peaks (PEAKS) immediately.
2. **Buffer & Page Flush**

   * Buffer events until `buffer_size ≥ page_size` or urgent flush.
   * Write buffered page blob to segment file; roll segment on max pages.

Decoupling live MMR updates from disk flush ensures peers see new events via Merkle roots without waiting for page boundaries.

---

## 8. Model Context Protocol (MCP) Integration

AI-specific context flows as regular `MODEL_CTX` events (`event_type=0x02`) with optional CRDT:

* Models emit context tensors, embeddings, or prompts as event payloads.
* Sync, CRDT merge, and verification apply identically.
* No special wire messages; MCP is a convention of event types within XSP.

---

## 9. Example: Collaborative AI Scoring

1. **User posts**: `IMAGE_POST` (0x04).
2. **Color head**: emits `FEATURE_MAP` + `PARTIAL_SCORE` (crdt\_type=PN\_COUNTER).
3. **Texture head**: emits its own events.
4. **Peers merge**: CRDT counters converge via merges.
5. **Aggregator**: emits `AGGREGATED_SCORE` event when CRDT state stable.

All flows use the XSP sync for delivery and Merkle proofs for integrity.

---

## 10. Versioning & Extensibility

* **Protocol version** in HELLO.
* **Capabilities** enable negotiation of SYNC, CRDT, MCP, etc.
* **New event\_types** or CRDT codes expand without new wire messages.
* **Future messages** (e.g., PROOF\_REQUEST) may be added.

---

## 11. Next Steps

1. Review and finalize event\_type, crdt\_type, and cap codes.
2. Publish as `spec-0.0.1.draft.md` in `xaeroflux-protocol` repo.
3. Implement reference libraries in Rust/TS covering framing, MMR diff, event parsing, CRDT merges.
4. Test convergence across native, WASM, and AI environments.
