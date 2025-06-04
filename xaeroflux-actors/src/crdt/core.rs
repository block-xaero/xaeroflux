use bytemuck::{Pod, Zeroable};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpType {
    Insert = 0,
    Delete = 1,
}

unsafe impl Zeroable for OpType {}
unsafe impl Pod for OpType {}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrdtOp {
    pub op_type: OpType,
    pub actor: u64,
    pub seq: u64,
    pub index: u32,
    pub count: u32,
}

unsafe impl Pod for CrdtOp {}
unsafe impl Zeroable for CrdtOp {}

