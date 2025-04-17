# Indexing storage review (from Proposal)

```ascii

         ┌──────────────────┐            Decoded Pages            ┌────────────────────┐
         │   Reader Loop    │─reader_tx─────────────────────────▶│ Merkle‑Tree Actor  │
         │ (mio + mmap read)│                                    │ (single thread)    │
         └──────────────────┘                                    └───────┬────────────┘
              ▲   │                                                        │
  raw bytes   │   │ apply subtree pages                                   │ dirty page
              │   │                                                        │
         ┌────┴───▼──────────┐                                        ┌───▼────────────┐
         │   On‑Disk File    │                                        │   Writer Loop  │
         │   (mmap'd .bin)   │◀──────────writer_tx──────────────────────▶│(encode + flush)│
         └───────────────────┘                                        └────────────────┘


```

## Reader Loop

Blocks on Poll for READABLE on the mmap'd FD.
Reads one or more 16 KiB slices → OnDiskMerklePage.
Decodes into XaeroMerklePage (header + Vec<XaeroMerkleNode>)
Sends over reader_tx.

## Merkle‑Tree Actor

Receives pages on reader_rx.
Stitches each page's subtree into tree.nodes at subtree_root_idx.
On internal updates, marks affected page dirty and sends its index on writer_tx.
Writer Loop

Receives page‐indices on writer_rx.
Re‑encodes XaeroMerklePage → raw bytes.
Copies into MmapMut at page_index * PAGE_SIZE.
Calls mmap.flush_range(...).
