use super::SeriesRegistry;

mod memory;
mod persistence;
mod postings;
mod registration;
mod value_family;

pub(super) fn assert_memory_usage_reconciled(registry: &SeriesRegistry) {
    assert_eq!(
        registry.memory_usage_bytes(),
        registry.recompute_memory_usage_bytes(),
        "incremental registry memory accounting drifted from a full recompute",
    );
}
