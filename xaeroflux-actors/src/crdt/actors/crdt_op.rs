pub trait CrdtIdentifier: Eq + Hash + Clone {
    type Site: Eq + Hash + Clone;
    type Counter: Ord + Copy;
    fn site(&self) -> Self::Site;
    fn counter(&self) -> Self::Counter;
}

pub trait CrdtApply<Id, Op> {
    fn apply(&mut self, id: &Id, op: &Op);
}
pub struct VersionVector<Site, Counter> {
    pub site: Site,
    pub counter: Counter,
}
pub struct CrdtOpActor<State, Id, Op> {
    pub actor: u64,
    pub seq: u64,
    pub index: u32,
    pub count: u32,
}
