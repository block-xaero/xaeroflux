use std::fs::OpenOptions;

use tempfile::NamedTempFile;
use xaeroflux::logs::wal::*;

fn create_test_wal(size: usize) -> Wal {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_path_buf();

    // preallocate file
    {
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(size as u64).unwrap();
    }

    Wal::new(path.to_str().unwrap())
}

#[test]
fn test_write_and_read() {
    let mut wal = create_test_wal(4096);
    let data = b"hello wal!";
    wal.write(data.as_ref());

    let read = wal.read();
    assert_eq!(&read[..data.len()], data);
}

#[test]
fn test_multiple_writes_accumulate() {
    let mut wal = create_test_wal(4096);
    wal.write(b"first".as_ref());
    wal.write(b"second".as_ref());

    let read = wal.read();
    println!("length: {}", read.len());
    assert_eq!(read.len(), 11); // "firstsecond" = 11 bytes
}

#[test]
fn test_flush_does_not_panic() {
    let mut wal = create_test_wal(4096);
    wal.write(b"flush me".as_ref());
    wal.flush();
}

#[test]
fn test_truncate_resets_offset() {
    let mut wal = create_test_wal(4096);
    wal.write(b"will be gone".as_ref());
    wal.truncate();
    assert_eq!(wal.read(), vec![]);
    assert_eq!(wal.offset, 0);
}

#[test]
fn test_write_exceeding_page_size() {
    let mut wal = create_test_wal(4096);
    let big_data = vec![42u8; 8192]; // 2 pages worth
    wal.write(&big_data);

    let read = wal.read();
    assert_eq!(&read[..big_data.len()], &big_data[..]);
}
