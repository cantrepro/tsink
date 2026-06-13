#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalHighWatermark {
    pub segment: u64,
    pub frame: u64,
}
