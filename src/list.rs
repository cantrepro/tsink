//! Partition list implementation.

use crate::{Result, TsinkError};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::partition::SharedPartition;

/// A linked list of partitions ordered from newest to oldest.
pub struct PartitionList {
    head: RwLock<Option<Arc<PartitionNode>>>,
    num_partitions: AtomicUsize,
}

impl PartitionList {
    /// Creates a new empty partition list.
    pub fn new() -> Self {
        Self {
            head: RwLock::new(None),
            num_partitions: AtomicUsize::new(0),
        }
    }

    /// Inserts a new partition at the head of the list.
    pub fn insert(&self, partition: SharedPartition) {
        let new_node = Arc::new(PartitionNode {
            partition,
            next: RwLock::new(self.head.read().clone()),
        });

        *self.head.write() = Some(new_node);
        self.num_partitions.fetch_add(1, Ordering::SeqCst);
    }

    /// Removes a partition from the list.
    pub fn remove(&self, target: &SharedPartition) -> Result<()> {
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
            timestamp: 0, // We don't have timestamp context here
        })
    }

    /// Swaps an old partition with a new one.
    pub fn swap(&self, old: &SharedPartition, new: SharedPartition) -> Result<()> {
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
            timestamp: 0, // We don't have timestamp context here
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

    /// Checks if two partitions are the same based on min timestamp.
    fn same_partitions(a: &SharedPartition, b: &SharedPartition) -> bool {
        a.min_timestamp() == b.min_timestamp()
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
