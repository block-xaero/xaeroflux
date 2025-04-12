pub struct Peer {
    pub id: [u8; 32],
}

impl Peer {
    pub fn new(id: [u8; 32]) -> Self {
        Peer { id }
    }
}