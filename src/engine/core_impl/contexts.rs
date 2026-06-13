use super::*;
#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct CatalogContext<'a> {
    pub(in crate::engine::storage_engine) metadata_shard_index: Option<&'a MetadataShardIndex>,
    pub(in crate::engine::storage_engine) write_txn_shards:
        &'a [Mutex<()>; REGISTRY_TXN_SHARD_COUNT],
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct ChunkContext<'a> {
    pub(in crate::engine::storage_engine) active_builders:
        &'a [ActiveBuilderShard; IN_MEMORY_SHARD_COUNT],
    pub(in crate::engine::storage_engine) sealed_chunks:
        &'a [SealedChunkShard; IN_MEMORY_SHARD_COUNT],
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct CoordinationContext<'a> {
    pub(in crate::engine::storage_engine) lifecycle: &'a AtomicU8,
    pub(in crate::engine::storage_engine) background_fail_fast: bool,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct PersistedRefreshContext<'a> {
    pub(in crate::engine::storage_engine) persisted_refresh_in_progress: &'a AtomicBool,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteSeriesValidationContext<'a> {
    pub(in crate::engine::storage_engine) chunks: ChunkContext<'a>,
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) persisted_index: &'a RwLock<PersistedIndexState>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteResolveContext<'a> {
    pub(in crate::engine::storage_engine) catalog: CatalogContext<'a>,
    pub(in crate::engine::storage_engine) support: WriteResolveSupportContext<'a>,
    pub(in crate::engine::storage_engine) cardinality_limit: usize,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) budget_bytes: &'a AtomicU64,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WritePrepareContext<'a> {
    pub(in crate::engine::storage_engine) series_validation: WriteSeriesValidationContext<'a>,
    pub(in crate::engine::storage_engine) config: WritePrepareConfigContext,
    pub(in crate::engine::storage_engine) visibility: WritePrepareVisibilityContext<'a>,
    pub(in crate::engine::storage_engine) wal: WritePrepareWalContext<'a>,
    pub(in crate::engine::storage_engine) memory_budget: WritePrepareMemoryBudgetContext<'a>,
    pub(in crate::engine::storage_engine) admission: WriteAdmissionControlContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteApplyContext<'a> {
    pub(in crate::engine::storage_engine) series_validation: WriteSeriesValidationContext<'a>,
    pub(in crate::engine::storage_engine) registry: WriteApplyRegistryContext<'a>,
    pub(in crate::engine::storage_engine) wal: WriteApplyWalContext<'a>,
    pub(in crate::engine::storage_engine) shard_mutation: WriteApplyShardMutationContext<'a>,
    pub(in crate::engine::storage_engine) memory: WriteApplyMemoryAccountingContext<'a>,
    pub(in crate::engine::storage_engine) publication: WriteApplyPublicationContext<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct WriteCommitContext<'a> {
    pub(in crate::engine::storage_engine) stage: WriteCommitStageContext<'a>,
    pub(in crate::engine::storage_engine) wal_completion: WriteCommitWalCompletionContext<'a>,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn catalog_context(&self) -> CatalogContext<'_> {
        CatalogContext {
            metadata_shard_index: self.catalog.metadata_shard_index.as_ref(),
            write_txn_shards: &self.catalog.write_txn_shards,
            registry: &self.catalog.registry,
        }
    }

    pub(in crate::engine::storage_engine) fn chunk_context(&self) -> ChunkContext<'_> {
        ChunkContext {
            active_builders: &self.chunks.active_builders,
            sealed_chunks: &self.chunks.sealed_chunks,
        }
    }

    pub(in crate::engine::storage_engine) fn coordination_context(
        &self,
    ) -> CoordinationContext<'_> {
        CoordinationContext {
            lifecycle: self.coordination.lifecycle.as_ref(),
            background_fail_fast: self.background.fail_fast_enabled,
        }
    }

    pub(in crate::engine::storage_engine) fn persisted_refresh_context(
        &self,
    ) -> PersistedRefreshContext<'_> {
        PersistedRefreshContext {
            persisted_refresh_in_progress: &self.persisted.persisted_refresh_in_progress,
        }
    }

    pub(in crate::engine::storage_engine) fn write_series_validation_context(
        &self,
    ) -> WriteSeriesValidationContext<'_> {
        WriteSeriesValidationContext {
            chunks: self.chunk_context(),
            registry: &self.catalog.registry,
            persisted_index: &self.persisted.persisted_index,
        }
    }

    pub(in crate::engine::storage_engine) fn registry_memory_context(
        &self,
    ) -> RegistryMemoryContext<'_> {
        RegistryMemoryContext {
            accounting_enabled: self.memory.accounting_enabled,
            registry: &self.catalog.registry,
            registry_used_bytes: &self.memory.registry_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn clock_context(&self) -> ClockContext<'_> {
        ClockContext {
            timestamp_precision: self.runtime.timestamp_precision,
            future_skew_window: self.runtime.future_skew_window,
            #[cfg(test)]
            current_time_override: &self.current_time_override,
            #[cfg(not(test))]
            marker: std::marker::PhantomData,
        }
    }

    pub(in crate::engine::storage_engine) fn materialized_series_read_context(
        &self,
    ) -> MaterializedSeriesReadContext<'_> {
        MaterializedSeriesReadContext {
            materialized_series: &self.visibility.materialized_series,
        }
    }

    pub(in crate::engine::storage_engine) fn visibility_cache_read_context(
        &self,
    ) -> VisibilityCacheReadContext<'_> {
        VisibilityCacheReadContext {
            series_visibility_summaries: &self.visibility.series_visibility_summaries,
            series_visible_max_timestamps: &self.visibility.series_visible_max_timestamps,
            series_visible_bounded_max_timestamps: &self
                .visibility
                .series_visible_bounded_max_timestamps,
        }
    }

    pub(in crate::engine::storage_engine) fn worker_wake_context(&self) -> WorkerWakeContext<'_> {
        WorkerWakeContext {
            background: &self.background,
        }
    }

    pub(in crate::engine::storage_engine) fn registry_bookkeeping_context(
        &self,
    ) -> RegistryBookkeepingContext<'_> {
        RegistryBookkeepingContext {
            accounting_enabled: self.memory.accounting_enabled,
            pending_series_ids: &self.catalog.pending_series_ids,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn shard_memory_accounting_context(
        &self,
    ) -> ShardMemoryAccountingContext<'_> {
        ShardMemoryAccountingContext {
            accounting_enabled: self.memory.accounting_enabled,
            used_bytes_by_shard: &self.memory.used_bytes_by_shard,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn persisted_sealed_budget_context(
        &self,
    ) -> PersistedSealedBudgetContext<'_> {
        PersistedSealedBudgetContext {
            memory: self.shard_memory_accounting_context(),
            backpressure_lock: &self.memory.backpressure_lock,
            has_persisted_lanes: self.persisted.numeric_lane_path.is_some()
                || self.persisted.blob_lane_path.is_some(),
            persisted_chunk_watermarks: &self.chunks.persisted_chunk_watermarks,
            persisted_index: &self.persisted.persisted_index,
            sealed_chunks: &self.chunks.sealed_chunks,
            flush_metrics: &self.observability.flush,
        }
    }

    pub(in crate::engine::storage_engine) fn series_timestamp_write_context(
        &self,
    ) -> SeriesTimestampWriteContext<'_> {
        SeriesTimestampWriteContext {
            clock: self.clock_context(),
            retention_metrics: &self.observability.retention,
            tombstones: &self.visibility.tombstones,
            series_visibility_summaries: &self.visibility.series_visibility_summaries,
            series_visible_max_timestamps: &self.visibility.series_visible_max_timestamps,
            series_visible_bounded_max_timestamps: &self
                .visibility
                .series_visible_bounded_max_timestamps,
            recency_state_lock: &self.visibility.recency_state_lock,
            max_observed_timestamp: &self.visibility.max_observed_timestamp,
            max_bounded_observed_timestamp: &self.visibility.max_bounded_observed_timestamp,
            live_series_pruning_generation: &self.visibility.live_series_pruning_generation,
            accounting_enabled: self.memory.accounting_enabled,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn materialized_series_write_context(
        &self,
    ) -> MaterializedSeriesWriteContext<'_> {
        MaterializedSeriesWriteContext {
            accounting_enabled: self.memory.accounting_enabled,
            materialized_series: &self.visibility.materialized_series,
            live_series_pruning_generation: &self.visibility.live_series_pruning_generation,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn sealed_chunk_publish_context(
        &self,
    ) -> SealedChunkPublishContext<'_> {
        SealedChunkPublishContext {
            sealed_chunks: &self.chunks.sealed_chunks,
            next_chunk_sequence: &self.chunks.next_chunk_sequence,
            memory: self.shard_memory_accounting_context(),
            #[cfg(test)]
            pre_publish_hook: &self.persist_test_hooks.pre_sealed_chunk_publish_hook,
        }
    }

    pub(in crate::engine::storage_engine) fn wal_metrics_context(&self) -> WalMetricsContext<'_> {
        WalMetricsContext {
            metrics: &self.observability.wal,
        }
    }

    #[cfg(test)]
    fn write_apply_test_hooks_context(&self) -> WriteApplyTestHooksContext<'_> {
        WriteApplyTestHooksContext {
            hooks: &self.persist_test_hooks,
        }
    }

    #[cfg(test)]
    fn write_commit_test_hooks_context(&self) -> WriteCommitTestHooksContext<'_> {
        WriteCommitTestHooksContext {
            hooks: &self.persist_test_hooks,
        }
    }

    pub(in crate::engine::storage_engine) fn write_resolve_context(
        &self,
    ) -> WriteResolveContext<'_> {
        WriteResolveContext {
            catalog: self.catalog_context(),
            support: WriteResolveSupportContext {
                registry_memory: self.registry_memory_context(),
            },
            cardinality_limit: self.runtime.cardinality_limit,
            used_bytes: &self.memory.used_bytes,
            budget_bytes: &self.memory.budget_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn write_prepare_context(
        &self,
    ) -> WritePrepareContext<'_> {
        WritePrepareContext {
            series_validation: self.write_series_validation_context(),
            config: WritePrepareConfigContext {
                chunk_point_cap: self.chunks.chunk_point_cap,
                partition_window: self.runtime.partition_window,
                max_active_partition_heads_per_series: self
                    .runtime
                    .max_active_partition_heads_per_series,
            },
            visibility: WritePrepareVisibilityContext {
                clock: self.clock_context(),
                pending_series_ids: &self.catalog.pending_series_ids,
                has_metadata_shards: self.catalog.metadata_shard_index.is_some(),
                materialized_series: self.materialized_series_read_context(),
                visibility_cache: self.visibility_cache_read_context(),
                max_bounded_observed_timestamp: &self.visibility.max_bounded_observed_timestamp,
                retention_window: self.runtime.retention_window,
                future_skew_window: self.runtime.future_skew_window,
                retention_enforced: self.runtime.retention_enforced,
            },
            wal: WritePrepareWalContext {
                wal: self.persisted.wal.as_ref(),
                wal_size_limit_bytes: self.runtime.wal_size_limit_bytes,
            },
            memory_budget: WritePrepareMemoryBudgetContext {
                used_bytes: &self.memory.used_bytes,
                budget_bytes: &self.memory.budget_bytes,
            },
            admission: WriteAdmissionControlContext {
                budget: self.persisted_sealed_budget_context(),
                workers: self.worker_wake_context(),
                observability: self.observability.as_ref(),
                write_timeout: self.runtime.write_timeout,
                admission_poll_interval: self.runtime.admission_poll_interval,
                admission_backpressure_lock: &self.memory.admission_backpressure_lock,
            },
        }
    }

    pub(in crate::engine::storage_engine) fn write_apply_context(&self) -> WriteApplyContext<'_> {
        let series_validation = self.write_series_validation_context();
        WriteApplyContext {
            series_validation,
            registry: WriteApplyRegistryContext {
                series_validation,
                registry_memory: self.registry_memory_context(),
            },
            wal: WriteApplyWalContext {
                #[cfg(test)]
                test_hooks: self.write_apply_test_hooks_context(),
                #[cfg(test)]
                crash_after_samples_persisted: &self
                    .persist_test_hooks
                    .crash_after_samples_persisted,
                #[cfg(not(test))]
                marker: std::marker::PhantomData,
            },
            shard_mutation: WriteApplyShardMutationContext {
                chunks: self.chunk_context(),
                timestamps: self.series_timestamp_write_context(),
                chunk_point_cap: self.chunks.chunk_point_cap,
                partition_window: self.runtime.partition_window,
                max_active_partition_heads_per_series: self
                    .runtime
                    .max_active_partition_heads_per_series,
            },
            memory: WriteApplyMemoryAccountingContext {
                shards: self.shard_memory_accounting_context(),
            },
            publication: WriteApplyPublicationContext {
                registry_bookkeeping: self.registry_bookkeeping_context(),
                materialized_series: self.materialized_series_write_context(),
                runtime_metadata_delta: self.runtime_metadata_delta_write_context(),
                metadata_shards: self.metadata_shard_publication_context(),
                sealed_chunks: self.sealed_chunk_publish_context(),
            },
        }
    }

    pub(in crate::engine::storage_engine) fn write_commit_context(&self) -> WriteCommitContext<'_> {
        WriteCommitContext {
            stage: WriteCommitStageContext {
                chunks: self.chunk_context(),
                wal: self.persisted.wal.as_ref(),
                wal_metrics: self.wal_metrics_context(),
                #[cfg(test)]
                test_hooks: self.write_commit_test_hooks_context(),
            },
            wal_completion: WriteCommitWalCompletionContext {
                observability: self.observability.as_ref(),
                wal: self.persisted.wal.as_ref(),
                wal_metrics: self.wal_metrics_context(),
                #[cfg(test)]
                crash_before_publish_persisted: &self
                    .persist_test_hooks
                    .crash_before_publish_persisted,
            },
        }
    }

    pub(in crate::engine::storage_engine) fn lifecycle_publication_context(
        &self,
    ) -> LifecyclePublicationContext<'_> {
        LifecyclePublicationContext {
            registry: &self.catalog.registry,
            persisted_index: &self.persisted.persisted_index,
            accounting_enabled: self.memory.accounting_enabled,
            registry_used_bytes: &self.memory.registry_used_bytes,
            persisted_index_used_bytes: &self.memory.persisted_index_used_bytes,
            persisted_mmap_used_bytes: &self.memory.persisted_mmap_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
            registry_bookkeeping: self.registry_bookkeeping_context(),
            materialized_series: self.materialized_series_write_context(),
            runtime_metadata_delta: self.runtime_metadata_delta_write_context(),
            metadata_shards: self.metadata_shard_publication_context(),
        }
    }
}
