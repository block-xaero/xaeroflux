pub struct XaeroTopic {
    pub id: [u8; 32],
    pub name: String,
}

impl XaeroTopic {
    pub fn new(id: [u8; 32], name: String) -> XaeroTopic {
        Self { id, name }
    }
}
