use xaeroid::{XaeroCredential, XaeroID, XaeroProof};

mod actor;
mod control_plane;
mod data_plane;
mod p2p;
mod pool;
mod state;
mod topic;

pub enum BufferStatus {
    Init,
    Red,
    Green,
}

pub enum SyncContext {
    NewEvent,
    RebootSync,
    SYNC,
}

pub fn xaero_id_zero() -> XaeroID {
    let xaero_proofs = [
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
    ];
    XaeroID {
        did_peer: [0u8; 897],
        did_peer_len: 0,
        secret_key: [0u8; 1281],
        _pad: [0u8; 3],
        credential: XaeroCredential {
            vc: [0u8; 256],
            vc_len: 256,
            proofs: xaero_proofs,
            proof_count: 0,
            _pad: [0u8; 1],
        },
    }
}
