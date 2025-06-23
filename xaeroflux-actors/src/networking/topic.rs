use bytemuck::{Pod, Zeroable};
use crate::subject::SubjectHash;

#[derive(Debug, Clone, Copy)]
pub enum TopicKind {
    Control,
    Data,
    Signal,
}

#[derive(Debug, Clone, Copy)]
pub struct XaeroTopic {
    pub group: SubjectHash,
    pub workspace: SubjectHash,
    pub object: SubjectHash,
    pub kind: TopicKind,
}

unsafe impl Pod for XaeroTopic {}
unsafe impl Zeroable for XaeroTopic {}