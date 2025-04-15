use std::{
    cmp::min,
    fs::{self, File},
    os::fd::AsRawFd,
    sync::Arc,
};

use memmap2::MmapMut;
use mio::{Events, Interest, Poll, Token, unix::SourceFd};
use serde::Deserialize;

use super::merkle_tree::{XaeroMerkleNode, XaeroMerkleTree};
use crate::{
    core::XaeroData,
    sys::{get_page_size, mm},
};

pub const XAERO_MERKLE_PAGE_MARKER: &[u8; 4] = b"XAER";
pub const XAERO_MERKLE_NODE_MARKER: &[u8; 2] = b"XN";

#[derive(Deserialize, Debug)]
pub struct XaeroMerkleStorageConfig {
    pub page_size: usize,
    pub max_pages: usize,
    pub nodes_per_page: usize,
    pub file_path: String,
}

impl XaeroMerkleStorageConfig {
    pub fn new(conf_path: &str) -> Self {
        let s = fs::read_to_string(conf_path).expect("Unable to read config file");
        let s: XaeroMerkleStorageConfig = toml::de::from_str(&s).expect("failed to parse config");
        Self {
            page_size: s.page_size,
            max_pages: s.max_pages,
            nodes_per_page: s.nodes_per_page,
            file_path: s.file_path,
        }
    }
}

pub struct XaeroPageHeader {
    pub marker: [u8; 4],
    pub event_type: u8,
}
pub struct XaeroMerklePage {
    pub header: XaeroPageHeader,
    pub page: [u8; 1024 * 16], // 16KB pages
    pub version: u64,
    pub is_dirty: bool,
}

impl TryFrom<&[u8]> for XaeroMerklePage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let h: [u8; 4] = value[..4]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid marker"))?;
        if h != *XAERO_MERKLE_PAGE_MARKER {
            return Err(anyhow::anyhow!("Invalid marker"));
        }

        let event_type_arr: [u8; 1] = value[4..5]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid event type"))?;
        let event_type = event_type_arr[0];
        let header = XaeroPageHeader {
            marker: *XAERO_MERKLE_PAGE_MARKER,
            event_type,
        };

        if value.len() != 1024 * 16 {
            return Err(anyhow::anyhow!("Invalid page size"));
        }
        let mut page = [0; 1024 * 16];
        page.copy_from_slice(value);
        Ok(XaeroMerklePage {
            header,
            page,
            version: 0,
            is_dirty: false,
        })
    }
}
pub struct XaeroMerklePageDecoder {
    pub input: crossbeam::channel::Receiver<XaeroMerklePage>,
    pub output: crossbeam::channel::Sender<XaeroMerkleNode>,
    pub config: Arc<XaeroMerkleStorageConfig>,
}

impl XaeroMerklePageDecoder {
    pub fn init(
        &mut self,
        receiver: crossbeam::channel::Receiver<XaeroMerklePage>,
        sender: crossbeam::channel::Sender<XaeroMerkleNode>,
    ) {
        self.input = receiver;
        self.output = sender;
        self.config = Arc::new(XaeroMerkleStorageConfig {
            page_size: get_page_size(),
            max_pages: 1024,
            nodes_per_page: 512,
            file_path: "merkle_storage.bin".to_string(),
        });
    }

    pub fn start(&self) {
        while let Ok(page) = self.input.recv() {
            let _raw_nodes: [u8; 1024 * 16] = page.page;
            let mut _nodes: Vec<XaeroMerkleNode> = Vec::new();
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
    pub last_read_offset: usize,
    pub last_write_offset: usize,
}
pub trait XaeroMerkleStorageOps<T>
where
    T: XaeroData,
{
    fn write_pages(&self, tree: Arc<XaeroMerkleTree>) -> anyhow::Result<()>;
    fn read_pages(&self) -> anyhow::Result<XaeroMerkleTree>;
}

impl XaeroMerkleStorage {
    pub fn new(fp: &str) -> Self {
        let poll = Arc::new(Poll::new().expect("Failed to create poll instance"));
        let events = Events::with_capacity(1024);
        let config = XaeroMerkleStorageConfig {
            page_size: get_page_size(),
            max_pages: 1024,
            file_path: fp.to_string(),
            nodes_per_page: 512,
        };
        mm(fp);
        let merkle_mmap_buffer = mm(fp);
        Self {
            poll,
            events,
            pages: {
                let (sender, receiver) = crossbeam::channel::bounded::<XaeroMerklePage>(1024);
                (receiver, sender)
            },
            config: Arc::new(config),
            merkle_mmap_buffer,
            last_read_offset: 0,
            last_write_offset: 0,
        }
    }

    pub fn start(
        &mut self,
        last_read_offset: usize,
        _last_write_offset: usize,
    ) -> anyhow::Result<()> {
        let file = File::open(&self.config.file_path).expect("Failed to open file");
        let fd = file.as_raw_fd();
        let mut source = SourceFd(&fd);
        self.poll.registry().register(
            &mut source,
            Token(0),
            Interest::READABLE | Interest::WRITABLE,
        )?;
        loop {
            let mut poll = Arc::clone(&self.poll);
            Arc::get_mut(&mut poll)
                .expect("Failed to get mutable reference to Poll")
                .poll(&mut self.events, None)?;
            for event in &self.events {
                if let Token(0) = event.token() {
                    // read the whole file and accumulate pages
                    let read_size = min(
                        self.config.page_size,
                        self.last_read_offset + self.config.page_size,
                    );
                    let _read_page = self.merkle_mmap_buffer
                        [last_read_offset..last_read_offset + read_size]
                        .to_vec();
                    let _page = XaeroMerklePage {
                        page: self.merkle_mmap_buffer
                            [last_read_offset..last_read_offset + read_size]
                            .try_into()
                            .expect("Failed to convert slice to array"),
                        version: 0,
                        is_dirty: false,
                        header: XaeroPageHeader {
                            marker: *XAERO_MERKLE_PAGE_MARKER,
                            event_type: 0, // Replace with the appropriate event type
                        },
                    };
                    let res = self.pages.1.send(_page);
                    match res {
                        Ok(_) => {
                            println!("Page sent successfully");
                            self.last_read_offset += read_size;
                            if self.last_read_offset >= self.merkle_mmap_buffer.len() {
                                self.last_read_offset = 0;
                                self.merkle_mmap_buffer
                                    .flush()
                                    .expect("Failed to flush mmap buffer");
                            }
                        }
                        Err(err) => {
                            // Handle the error, e.g., log it
                            println!("Failed to send page: {}", err);
                        }
                    }
                }
            }
        }
    }
}
