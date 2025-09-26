use rkyv::{Archive, Deserialize, Serialize};
use xaeroflux_core::{
    hash::sha_256_concat_hash,
    merkle_tree::{XaeroMerkleProof, XaeroMerkleTree, XaeroMerkleTreeOps},
};

/// Peak represents a peak in the Merkle Mountain Range (MMR).
#[repr(C)]
#[derive(Clone, Copy, Archive, Serialize, Debug)]
#[rkyv(derive(Debug))]
pub struct Peak {
    pub height: usize,
    pub root: [u8; 32],
}

/// XaeroMMR is a Merkle Mountain Range (MMR) implementation used in the Xaero Sync protocol.
#[repr(C)]
#[derive(Clone, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct XaeroMmr {
    pub root: [u8; 32],
    pub peaks: Vec<Peak>,
    pub leaf_count: usize,
    pub leaf_hashes: Vec<[u8; 32]>,
}



pub trait XaeroMmrOps {
    /// Append one new leaf hash.
    /// ```markdown
    /// Say you have a list of leaves [A, B, C, D, E]
    /// - START:
    /// - step 1: append(a) -> peaks [h(A)]
    /// - step 2: append(b) -> pop peak[h(A)] -> peak[h(h(A) || h(B))] -> peaks [h(h(A) || h(B))]
    /// - step 3: append(c) -> peaks [h(hA || hB), h(C)]
    /// - step 4: append(d) -> pop peak[h(C)] -> peak[h(h(c) || h(d))] -> peaks [h(hA || hB), h(h(c)
    /// || h(d))]
    /// - step 5: append(e) -> peaks [h(hA || hB), h(h(c) || h(d)), h(E)]
    /// - step 6: bag peaks -> `XaeroMerkleTree::neo(peaks)` -> tree.root() -> root
    /// - RETURN changed_peaks. Changed_peaks is a list of peaks that were created or merged in the
    /// process.
    /// - END
    /// ```
    /// Returns the list of Peaks that were created or merged in the process
    /// (so you can persist or broadcast them).
    fn append(&mut self, leaf_hash: [u8; 32]) -> Vec<Peak>;

    /// The current overall root
    fn root(&self) -> [u8; 32];

    /// All of the current peaks, sorted e.g. by descending height.
    fn peaks(&self) -> &[Peak];

    /// Number of leaves appended so far.
    fn leaf_count(&self) -> usize;

    /// Build an inclusion proof for the leaf at `leaf_index`.
    /// Returns `None` if that index is out of bounds.
    fn proof(&self, leaf_index: usize) -> Option<XaeroMerkleProof>;

    /// Given a leaf's hash, its proof, and an expected root, verify
    /// that this leaf really is included under that root.
    fn verify(&self, leaf_hash: [u8; 32], proof: &XaeroMerkleProof, expected_root: [u8; 32]) -> bool;

    /// Convenience: gets the leaf hash by its index.
    fn get_leaf_hash_by_index(&self, index: usize) -> Option<&[u8; 32]>;
}

impl Default for XaeroMmr {
    fn default() -> Self {
        Self::new()
    }
}

impl XaeroMmr {
    pub fn new() -> Self {
        XaeroMmr {
            root: [0u8; 32],
            peaks: Vec::new(),
            leaf_count: 0,
            leaf_hashes: Vec::new(),
        }
    }
}
impl XaeroMmrOps for XaeroMmr {
    fn append(&mut self, leaf_hash: [u8; 32]) -> Vec<Peak> {
        let mut carry = Peak { height: 0, root: leaf_hash };
        self.leaf_hashes.push(leaf_hash);
        let mut changed = vec![];
        changed.push(carry);
        while let Some(p) = self.peaks.last_mut() {
            if p.height != carry.height {
                break;
            }
            if p.height == carry.height {
                let popped = self.peaks.pop();
                match popped {
                    Some(popped_peak) => {
                        let merged_root = sha_256_concat_hash(&popped_peak.root, &carry.root);
                        carry = Peak {
                            height: popped_peak.height + 1,
                            root: merged_root,
                        };
                        changed.push(carry);
                    }
                    None => {
                        // This case should not happen, but if it does, we just break
                        tracing::warn!("~~ !!! Popped peak is None? This should not happen !!! ~~");
                        break;
                    }
                }
            }
        }
        self.peaks.push(carry);
        self.leaf_count += 1;
        // bag peaks and recalculate root
        let t = XaeroMerkleTree::neo(self.peaks.iter().map(|p| p.root).collect());
        self.root = t.root();
        changed
    }

    fn root(&self) -> [u8; 32] {
        self.root
    }

    fn peaks(&self) -> &[Peak] {
        self.peaks.as_slice()
    }

    fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    fn proof(&self, leaf_index: usize) -> Option<XaeroMerkleProof> {
        if leaf_index >= self.leaf_count {
            return None;
        }

        // 1) Find which peak & the local offset
        let mut target = leaf_index;
        let mut peak_idx = 0;
        while peak_idx < self.peaks.len() {
            let size = 1 << self.peaks[peak_idx].height;
            if target < size {
                break;
            }
            target -= size;
            peak_idx += 1;
        }
        if peak_idx >= self.peaks.len() {
            return None;
        }
        let peak = &self.peaks[peak_idx];

        // 2) Compute the start index of that peak in leaf_hashes
        let start = self.peaks[..peak_idx].iter().map(|p| 1 << p.height).sum::<usize>();

        let mut proof = XaeroMerkleProof { leaf_index, value: Vec::new() };

        // 3) **Only** build a per‐peak Merkle proof if height > 0
        if peak.height > 0 {
            let slice = &self.leaf_hashes[start..start + (1 << peak.height)];
            let mut subtree = XaeroMerkleTree::neo(slice.to_vec());
            let leaf_val = slice[target];
            let mut sub_proof = subtree.generate_proof(leaf_val).expect("leaf must exist in its peak");
            proof.value.append(&mut sub_proof.value);
        }

        // 4) Bag‐of‐peaks proof (always)
        let roots = self.peaks.iter().map(|p| p.root).collect::<Vec<_>>();
        let mut bag_proof = XaeroMerkleTree::neo(roots).generate_proof(peak.root).expect("peak root must exist in bag");
        proof.value.append(&mut bag_proof.value);

        Some(proof)
    }

    fn verify(&self, leaf_hash: [u8; 32], proof: &XaeroMerkleProof, expected_root: [u8; 32]) -> bool {
        let mut running = leaf_hash;
        for seg in &proof.value {
            if seg.is_left {
                running = sha_256_concat_hash(&seg.node_hash, &running);
            } else {
                running = sha_256_concat_hash(&running, &seg.node_hash);
            }
        }
        running == expected_root
    }

    fn get_leaf_hash_by_index(&self, index: usize) -> Option<&[u8; 32]> {
        self.leaf_hashes.get(index)
    }
}

#[cfg(test)]
mod tests {
    use xaeroflux_core::hash::sha_256_concat_hash;

    use super::*;

    /// Convenience: create a 32‐byte array filled with `b`.
    fn fill(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn test_empty_mmr() {
        let mmr = XaeroMmr::new();
        // no leaves, no peaks
        assert_eq!(mmr.leaf_count(), 0);
        assert!(mmr.peaks().is_empty());
        // by convention root of empty is zero
        assert_eq!(mmr.root(), [0u8; 32]);
    }

    #[test]
    fn test_append_single_leaf() {
        let mut mmr = XaeroMmr::new();
        let a = fill(0x01);
        let changed = mmr.append(a);
        tracing::debug!("changed: {:?}", changed.iter().map(|e| e.root).collect::<Vec<_>>());
        // leaf_count and peaks
        assert_eq!(mmr.leaf_count(), 1);
        assert_eq!(mmr.peaks().len(), 1);
        assert_eq!(mmr.peaks()[0].height, 0);
        assert_eq!(mmr.peaks()[0].root, a);

        // changed list should contain exactly that one height-0 peak
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].height, 0);
        assert_eq!(changed[0].root, a);

        // root of MMR now equals the leaf
        // the MMR-root is H(a ∥ a), not a
        let expected = sha_256_concat_hash(&a, &a);
        assert_eq!(mmr.root(), expected);
    }

    #[test]
    fn test_append_two_leaves_merges() {
        let mut mmr = XaeroMmr::new();
        let a = fill(0xAA);
        let b = fill(0xBB);

        mmr.append(a);
        let changed2 = mmr.append(b);

        // now 2 leaves → single peak of height 1
        assert_eq!(mmr.leaf_count(), 2);
        assert_eq!(mmr.peaks().len(), 1);
        assert_eq!(mmr.peaks()[0].height, 1);

        // its root = H(A ∥ B)
        let r = sha_256_concat_hash(&a, &b);
        let root_exp = sha_256_concat_hash(&r, &r);
        assert_eq!(mmr.peaks()[0].root, r);
        assert_eq!(mmr.root(), root_exp);

        // changed2 contains first the raw B leaf, then the merged AB peak
        assert_eq!(changed2.len(), 2);
        assert_eq!(changed2[0].height, 0);
        assert_eq!(changed2[0].root, b);
        assert_eq!(changed2[1].height, 1);
        assert_eq!(changed2[1].root, r);
    }

    #[test]
    fn test_append_three_leaves_two_peaks_and_root() {
        let mut mmr = XaeroMmr::new();
        let a = fill(0x11);
        let b = fill(0x22);
        let c = fill(0x33);

        mmr.append(a);
        mmr.append(b);
        mmr.append(c);

        // 3 leaves → peaks [h1(AB), h0(C)]
        assert_eq!(mmr.leaf_count(), 3);
        assert_eq!(mmr.peaks().len(), 2);
        assert_eq!(mmr.peaks()[0].height, 1);
        assert_eq!(mmr.peaks()[1].height, 0);

        let root_ab = sha_256_concat_hash(&a, &b);
        assert_eq!(mmr.peaks()[0].root, root_ab);
        assert_eq!(mmr.peaks()[1].root, c);

        // global root = H( root_ab ∥ C )
        let overall = sha_256_concat_hash(&root_ab, &c);
        assert_eq!(mmr.root(), overall);
    }

    #[test]
    fn test_proof_and_verify_all_leaves() {
        let mut mmr = XaeroMmr::new();
        // make 5 distinct leaves
        let leaves = [fill(1), fill(2), fill(3), fill(4), fill(5)];
        for leaf in &leaves {
            mmr.append(*leaf);
        }

        // generate+verify a proof for each leaf index
        let root = mmr.root();
        for (idx, _) in leaves.iter().enumerate() {
            let proof = mmr.proof(idx).expect("proof must exist");
            assert!(mmr.verify(leaves[idx], &proof, root), "proof should verify for leaf {}", idx);
        }
        // out‐of‐bounds proof request returns None
        assert!(mmr.proof(leaves.len()).is_none());
    }
}
