use std::{
    io::{Read, Seek, SeekFrom},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::Receiver;
use rkyv::Archive;

use crate::size::PAGE_SIZE;

/// Diagnostics related to the network sync subsystem.
#[repr(C)]
#[derive(Clone, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct NetworkDiagnostics {
    /// Number of peers currently connected.
    pub connected_peers: usize,
    /// The peer ID (or identifier) with which the last successful sync occurred.
    pub last_sync_peer: Option<String>,
    /// Current bandwidth utilization in bytes per second.
    pub bandwidth_bytes_per_sec: u64,
}

/// Diagnostics related to the hosting device.
#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct SourceDeviceDiagnostics {
    /// Number of CPU cores available.
    pub cpu_cores: usize,
    /// Current active thread count.
    pub active_threads: usize,
    /// Current memory usage in bytes.
    pub memory_usage_bytes: u64,
    /// Remaining disk space in bytes.
    pub disk_free_bytes: u64,
}

/// Diagnostics for the Merkle tree indexing subsystem.
#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct MerkleIndexDiagnostics {
    /// Average latency to update the Merkle tree index in milliseconds.
    pub indexing_latency_ms: u64,
    /// Average latency to flush the Merkle tree index to disk, in milliseconds.
    pub flush_latency_ms: u64,
    /// Average latency per page write in the Merkle tree, in milliseconds.
    pub page_write_latency_ms: u64,
    /// Total count of proofs generated since start.
    pub proofs_generated: u64,
    /// The timestamp (epoch millis) of the last proof verified.
    pub last_proof_verified: Option<u64>,
}

#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct LMDBDiagnostics {
    /// Average latency for LMDB write operations, in milliseconds.
    pub storage_latency_ms: u64,
    /// Number of write batches executed.
    pub write_batches: u64,
}

/// Diagnostics for the Write-Ahead Log (WAL) subsystem.
#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct WalDiagnostics {
    /// Average latency for writing to the WAL, in milliseconds.
    pub wal_write_latency_ms: u64,
    /// Average latency for flushing the WAL, in milliseconds.
    pub wal_flush_latency_ms: u64,
}

/// The top-level diagnostics structure that aggregates various subsystem metrics.
#[repr(C)]
#[derive(Clone, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct Diagnostics {
    pub network: NetworkDiagnostics,
    pub source_device: SourceDeviceDiagnostics,
    pub merkle_index: MerkleIndexDiagnostics,
    pub lmdb_diagnostics: LMDBDiagnostics,
    pub wal: WalDiagnostics,
}

/// Asynchronously tails a segment file and sends data to the channel
/// it returns immediately with.
/// THIS IS A TESTING OPERATION ONLY - TO USE THIS WE NEED TO BE IN DIAGNOSTIC MODE
pub fn tail_mmr_segment_file(path: String) -> (JoinHandle<()>, Receiver<Vec<u8>>) {
    let (tx, rx) = crossbeam::channel::unbounded();
    let jh = std::thread::spawn(move || {
        let mut read_pos = 0;
        let f = &mut std::fs::File::open(path).expect("segment file cannot be opened");
        loop {
            let current_file_length = f
                .metadata()
                .map(|e| e.len())
                .expect("failed to get file metadata");
            let page_size = *PAGE_SIZE.get().expect("page_size not set!") as u64;
            while read_pos + page_size <= current_file_length {
                f.seek(SeekFrom::Start(read_pos)).expect("failed to seek");
                let mut buffer: Vec<u8> = vec![0; page_size as usize];
                f.read_exact(buffer.as_mut_slice()).expect("failed to read");
                read_pos += page_size;
                tx.send(buffer).expect("failed to send data");
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    (jh, rx)
}

#[cfg(test)]
mod diag_tests {
    use std::{fs::OpenOptions, io::Write, thread, time::Duration};

    use crossbeam::channel::RecvTimeoutError;
    use tempfile::tempdir;

    use super::tail_mmr_segment_file;
    use crate::size::{PAGE_SIZE, init_page_size, init_segment_size};

    #[test]
    fn test_tail_mmr_segment_file_reads_pages() {
        init_page_size();
        init_segment_size();
        // 1) set up temp segment file
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("0000.seg");
        let path = file_path.to_str().expect("failed_to_unwrap").to_string();

        // 2) open & write first page
        let page_size = *PAGE_SIZE.get().expect("PAGE_SIZE not set");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&file_path)
            .expect("open segment file");
        let page1 = vec![0xAA; page_size as usize];
        file.write_all(&page1).expect("write page1");
        file.flush().expect("flush");

        // 3) start tailer
        let (_jh, rx) = tail_mmr_segment_file(path.clone());

        // Give the tail-thread a moment to pick up the first page
        thread::sleep(Duration::from_millis(50));

        // 4) append second page
        let page2 = vec![0xBB; page_size as usize];
        file.write_all(&page2).expect("write page2");
        file.flush().expect("flush");

        // 5) read back pages in order
        let got1 = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("should get first page");
        assert_eq!(got1, page1, "First page payload matches");

        let got2 = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("should get second page");
        assert_eq!(got2, page2, "Second page payload matches");

        // 6) no more data
        match rx.recv_timeout(Duration::from_millis(100)) {
            Err(RecvTimeoutError::Timeout) => {} // good
            Ok(_) => panic!("got unexpected extra data"),
            Err(e) => panic!("channel error: {:?}", e),
        }
    }
}
