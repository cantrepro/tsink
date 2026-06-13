//! Partition list implementation.

use crate::{Result, TsinkError};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::partition::SharedPartition;

/// A linked list of partitions ordered from newest to oldest.
pub struct PartitionList {
    head: RwLock<Option<Arc<PartitionNode>>>,
    mutation_lock: Mutex<()>,
    num_partitions: AtomicUsize,
}

impl PartitionList {
    /// Creates a new empty partition list.
    pub fn new() -> Self {
        Self {
            head: RwLock::new(None),
            mutation_lock: Mutex::new(()),
            num_partitions: AtomicUsize::new(0),
        }
    }

    /// Inserts a new partition at the head of the list.
    pub fn insert(&self, partition: SharedPartition) {
        let _mutation_guard = self.mutation_lock.lock();
        let new_node = Arc::new(PartitionNode {
            partition,
            next: RwLock::new(self.head.read().clone()),
        });

        *self.head.write() = Some(new_node);
        self.num_partitions.fetch_add(1, Ordering::SeqCst);
    }

    /// Inserts a new partition at the tail of the list.
    pub fn insert_tail(&self, partition: SharedPartition) {
        let _mutation_guard = self.mutation_lock.lock();
        let new_node = Arc::new(PartitionNode {
            partition,
            next: RwLock::new(None),
        });

        let mut head = self.head.write();
        if head.is_none() {
            *head = Some(new_node);
            self.num_partitions.fetch_add(1, Ordering::SeqCst);
            return;
        }

        let mut current = head.as_ref().cloned();
        drop(head);

        while let Some(node) = current {
            let next = node.next.read().clone();
            if next.is_none() {
                *node.next.write() = Some(new_node.clone());
                self.num_partitions.fetch_add(1, Ordering::SeqCst);
                return;
            }
            current = next;
        }
    }

    /// Removes a partition from the list.
    pub fn remove(&self, target: &SharedPartition) -> Result<()> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut head = self.head.write();

        // Check if removing head
        if let Some(head_node) = head.clone()
            && Self::same_partitions(&head_node.partition, target)
        {
            *head = head_node.next.read().clone();
            self.num_partitions.fetch_sub(1, Ordering::SeqCst);
            target.clean()?;
            return Ok(());
        }

        // Search for the node to remove
        let current = head.clone();
        drop(head); // Release the write lock

        let mut current = current;
        while let Some(node) = current {
            let next_opt = node.next.read().clone();

            if let Some(ref next_node) = next_opt
                && Self::same_partitions(&next_node.partition, target)
            {
                // Remove next node
                let new_next = next_node.next.read().clone();
                *node.next.write() = new_next;
                self.num_partitions.fetch_sub(1, Ordering::SeqCst);
                target.clean()?;
                return Ok(());
            }

            current = next_opt;
        }

        Err(TsinkError::PartitionNotFound {
            timestamp: target.min_timestamp(),
        })
    }

    /// Swaps an old partition with a new one.
    pub fn swap(&self, old: &SharedPartition, new: SharedPartition) -> Result<()> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut head = self.head.write();

        // Check if swapping head
        if let Some(head_node) = head.clone()
            && Self::same_partitions(&head_node.partition, old)
        {
            let new_node = Arc::new(PartitionNode {
                partition: new,
                next: RwLock::new(head_node.next.read().clone()),
            });
            *head = Some(new_node);
            return Ok(());
        }

        // Search for the node to swap
        let current = head.clone();
        drop(head); // Release the write lock

        let mut current = current;
        while let Some(node) = current {
            let next_opt = node.next.read().clone();

            if let Some(ref next_node) = next_opt
                && Self::same_partitions(&next_node.partition, old)
            {
                // Swap next node
                let new_node = Arc::new(PartitionNode {
                    partition: new,
                    next: RwLock::new(next_node.next.read().clone()),
                });
                *node.next.write() = Some(new_node);
                return Ok(());
            }

            current = next_opt;
        }

        Err(TsinkError::PartitionNotFound {
            timestamp: old.min_timestamp(),
        })
    }

    /// Gets the head partition.
    pub fn get_head(&self) -> Option<SharedPartition> {
        self.head.read().as_ref().map(|node| node.partition.clone())
    }

    /// Returns the number of partitions.
    pub fn size(&self) -> usize {
        self.num_partitions.load(Ordering::SeqCst)
    }

    /// Creates an iterator over the partitions.
    pub fn iter(&self) -> PartitionIterator {
        PartitionIterator {
            current: self.head.read().clone(),
        }
    }

    /// Checks if two partition references point to the same underlying partition.
    fn same_partitions(a: &SharedPartition, b: &SharedPartition) -> bool {
        Arc::ptr_eq(a, b)
    }
}

impl Default for PartitionList {
    fn default() -> Self {
        Self::new()
    }
}

/// A node in the partition list.
struct PartitionNode {
    partition: SharedPartition,
    next: RwLock<Option<Arc<PartitionNode>>>,
}

/// Iterator over partitions in the list.
pub struct PartitionIterator {
    current: Option<Arc<PartitionNode>>,
}

impl Iterator for PartitionIterator {
    type Item = SharedPartition;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.current.take()?;
        let partition = node.partition.clone();
        self.current = node.next.read().clone();
        Some(partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::PartitionMeta;
    use crate::partition::Partition;
    use crate::{DataPoint, Label, Row};
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::thread;

    struct TestPartition {
        min: i64,
        cleaned: AtomicBool,
    }

    impl TestPartition {
        fn new(min: i64) -> Self {
            Self {
                min,
                cleaned: AtomicBool::new(false),
            }
        }
    }

    impl Partition for TestPartition {
        fn insert_rows(&self, _rows: &[Row]) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }

        fn select_data_points(
            &self,
            _metric: &str,
            _labels: &[Label],
            _start: i64,
            _end: i64,
        ) -> Result<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_all_labels(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            Ok(Vec::new())
        }

        fn min_timestamp(&self) -> i64 {
            self.min
        }

        fn max_timestamp(&self) -> i64 {
            self.min
        }

        fn size(&self) -> usize {
            1
        }

        fn active(&self) -> bool {
            false
        }

        fn expired(&self) -> bool {
            false
        }

        fn clean(&self) -> Result<()> {
            self.cleaned.store(true, AtomicOrdering::SeqCst);
            Ok(())
        }

        fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, PartitionMeta)>> {
            Ok(None)
        }
    }

    #[test]
    fn remove_targets_exact_partition_when_min_timestamps_collide() {
        let list = PartitionList::new();

        let first_impl = Arc::new(TestPartition::new(100));
        let second_impl = Arc::new(TestPartition::new(100));

        let first: SharedPartition = first_impl.clone();
        let second: SharedPartition = second_impl.clone();

        list.insert(first.clone());
        list.insert(second.clone());

        list.remove(&first).unwrap();

        let remaining: Vec<_> = list.iter().collect();
        assert_eq!(remaining.len(), 1);
        assert!(Arc::ptr_eq(&remaining[0], &second));
        assert!(first_impl.cleaned.load(AtomicOrdering::SeqCst));
        assert!(!second_impl.cleaned.load(AtomicOrdering::SeqCst));
    }

    #[test]
    fn insert_tail_appends_after_existing_nodes() {
        let list = PartitionList::new();

        let newest_impl = Arc::new(TestPartition::new(300));
        let middle_impl = Arc::new(TestPartition::new(200));
        let oldest_impl = Arc::new(TestPartition::new(100));

        let newest: SharedPartition = newest_impl;
        let middle: SharedPartition = middle_impl;
        let oldest: SharedPartition = oldest_impl;

        list.insert(newest.clone());
        list.insert_tail(middle.clone());
        list.insert_tail(oldest.clone());

        let ordered: Vec<_> = list.iter().collect();
        assert_eq!(ordered.len(), 3);
        assert!(Arc::ptr_eq(&ordered[0], &newest));
        assert!(Arc::ptr_eq(&ordered[1], &middle));
        assert!(Arc::ptr_eq(&ordered[2], &oldest));
    }

    #[test]
    fn concurrent_insert_tail_keeps_structure_consistent() {
        for _ in 0..200 {
            let list = Arc::new(PartitionList::new());
            let head: SharedPartition = Arc::new(TestPartition::new(0));
            list.insert(head);

            let threads = 16usize;
            let mut handles = Vec::with_capacity(threads);
            for i in 0..threads {
                let list_clone = Arc::clone(&list);
                handles.push(thread::spawn(move || {
                    let partition: SharedPartition = Arc::new(TestPartition::new(i as i64 + 1));
                    list_clone.insert_tail(partition);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let expected = threads + 1;
            let iter_count = list.iter().count();
            let size = list.size();
            assert_eq!(iter_count, expected);
            assert_eq!(size, expected);
            assert_eq!(iter_count, size);
        }
    }
}
