use std::{cmp::min, fs::File, os::fd::AsRawFd, sync::Arc, usize};

use memmap2::MmapMut;
use mio::{unix::SourceFd, Events, Interest, Poll, Token};
use tracing::event;

use crate::sys::{get_page_size, mm};

use super::merkle_tree::{MerkleData, XaeroMerkleNode, XaeroMerkleTree};

pub struct XaeroMerkleStorageConfig {
    pub page_size: usize,
    pub max_pages: usize,
    pub nodes_per_page: usize,
    pub file_path: String,
}
pub struct XaeroMerklePage {
    pub page: [u8; 1024 * 16], // 16KB pages
    pub version: u64,
    pub is_dirty: bool,
}

impl TryFrom<&[u8]> for XaeroMerklePage {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 1024 * 16 {
            return Err(anyhow::anyhow!("Invalid page size").into());
        }
        let mut page = [0; 1024 * 16];
        page.copy_from_slice(value);
        Ok(XaeroMerklePage {
            page,
            version: 0,
            is_dirty: false,
        })
    }
}
pub struct XaeroMerklePageDecoder<T>
where
    T: MerkleData,
{
    pub input: crossbeam::channel::Receiver<XaeroMerklePage>,
    pub output: crossbeam::channel::Sender<XaeroMerkleNode<T>>,
    pub config: Arc<XaeroMerkleStorageConfig>,
}

impl<T> XaeroMerklePageDecoder<T>
where
    T: MerkleData,
{
    fn init(
        &self,
        receiver: crossbeam::channel::Receiver<XaeroMerklePage>,
        sender: crossbeam::channel::Sender<XaeroMerkleNode<T>>,
    ) {
        self.input = receiver;
        self.output = sender;
        self.config = Arc::new(XaeroMerkleStorageConfig {
            page_size: get_page_size(),
            max_pages: 1024,
            nodes_per_page: 512,
            file_path: String::new(),
        });
    }

    fn start(&self) {
        loop {
            match self.input.recv() {
                Ok(page) => {
                    let raw_nodes: [u8; self.config.nodes_per_page] = page.page;
                    let mut nodes  = Vec::new();

                }
                Err(err) => {
                    // Handle the error, e.g., log it or break the loop
                    break;
                }
            }

            }
        }
    }
}
pub struct XaeroMerkleStorage {
    pub poll: Arc<Poll>,
    pub events: Events,
    pub pages: (
        crossbeam::channel::Receiver<XaeroMerklePage>,
        crossbeam::channel::Sender<XaeroMerklePage>,
    ),
    pub config: Arc<XaeroMerkleStorageConfig>,
    merkle_mmap_buffer: MmapMut,
}
pub trait XaeroMerkleStorageOps<T>
where
    T: MerkleData,
{
    fn write_pages(&self, tree: Arc<XaeroMerkleTree<T>>) -> anyhow::Result<()>;
    fn read_pages(&self) -> anyhow::Result<XaeroMerkleTree<T>>;
}

impl XaeroMerkleStorage {
    pub fn new(fp: &str) -> Self {
        let file = File::open(fp).expect("Failed to open file");
        let poll = Arc::new(Poll::new().expect("Failed to create poll instance"));
        let mut events = Events::with_capacity(1024);
        let config = XaeroMerkleStorageConfig {
            page_size: get_page_size(),
            max_pages: 1024,
            file_path: fp.to_string(),
        };
        mm(fp);
        let merkle_mmap_buffer = mm(fp);
        Self {
            poll,
            events,
            pages: crossbeam::channel::bounded::<XaeroMerklePage>(1024),
            config: Arc::new(config),
            merkle_mmap_buffer,
        }
    }
    pub fn start(&self, last_read_offset: usize, last_write_offset: usize) -> anyhow::Result<()> {
        let file = File::open(self.config.file_path).expect("Failed to open file");
        let fd = file.as_raw_fd();
        let source = SourceFd(&fd);
        self.poll.registry().register(
            &mut source,
            Token(0),
            Interest::READABLE | Interest::WRITABLE,
        )?;
        loop {
            self.poll.poll(&mut self.events, None)?;
            for event in &self.events {
                match event.token() {
                    Token(0) => {
                        // read the whole file and accumulate pages
                        let read_size = min(
                            self.config.page_size,
                            self.last_read_offset + self.config.page_size,
                        );
                        let page_bytes =
                            self.merkle_mmap_buffer[last_read_offset..last_read_offset + read_size];
                        let page = XaeroMerklePage {
                            page: page_bytes.try_into().unwrap(),
                            version: 0,
                            is_dirty: false,
                        };
                        self.pages.1.send(page);
                        self.last_read_offset += read_size;
                        if self.last_read_offset >= self.merkle_mmap_buffer.len() {
                            self.last_read_offset = 0;
                            self.merkle_mmap_buffer.flush().unwrap();
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
