use std::time;

pub enum ProbeType {
    Performance,
    Security,
    Diagnostics,
}

pub struct ProbeData {
    pub data: Vec<u8>, // page_size aligned
    pub version: u64,
}

impl Default for ProbeData {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeData {
    pub fn new() -> Self {
        let mut _data = vec![0; 1024 * 16]; // 16KB pages for write
        let version: u64 = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        Self {
            data: _data,
            version,
        }
    }
}
pub struct XaeroProbe {
    pub probe_type: ProbeType,
    pub probe_id: u32,
    pub probe_data: Vec<ProbeData>,
}
