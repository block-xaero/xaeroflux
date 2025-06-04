/// CRDT Bus for sending and receiving CRDT operations
pub struct CRDTBus {
    pub tx: Sender<CrdtOp>,
    pub rx: Receiver<CrdtOp>,
}

