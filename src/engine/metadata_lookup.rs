use super::*;

impl<'a> CatalogContext<'a> {
    fn metadata_series_ids_for_scope(
        self,
        scope: &crate::storage::MetadataShardScope,
    ) -> MetadataScopeSeriesLookup {
        if scope.shards.is_empty() {
            return MetadataScopeSeriesLookup::Indexed(Vec::new());
        }

        let Some(index) = self.metadata_shard_index else {
            return MetadataScopeSeriesLookup::Unavailable(
                MetadataScopeSeriesLookupUnavailable::Disabled,
            );
        };
        if scope.shard_count != index.shard_count {
            return MetadataScopeSeriesLookup::Unavailable(
                MetadataScopeSeriesLookupUnavailable::ShardGeometryMismatch {
                    indexed_shard_count: index.shard_count,
                    requested_shard_count: scope.shard_count,
                },
            );
        }

        let shard_buckets = index.series_ids_by_shard.read();
        let mut series_ids = Vec::new();
        for shard in &scope.shards {
            let Some(bucket) = shard_buckets.get(*shard as usize) else {
                return MetadataScopeSeriesLookup::Unavailable(
                    MetadataScopeSeriesLookupUnavailable::Stale,
                );
            };
            series_ids.extend(bucket.iter().copied());
        }
        series_ids.sort_unstable();
        series_ids.dedup();
        MetadataScopeSeriesLookup::Indexed(series_ids)
    }
}

impl ChunkStorage {
    pub(super) fn validate_bounded_metadata_shard_scope(
        &self,
        scope: &crate::storage::MetadataShardScope,
        operation: &'static str,
    ) -> Result<()> {
        if scope.shards.is_empty() {
            return Ok(());
        }
        let context = self.catalog_context();
        let Some(index) = context.metadata_shard_index else {
            return Err(
                MetadataScopeSeriesLookupUnavailable::Disabled.unsupported_operation(operation)
            );
        };
        if scope.shard_count != index.shard_count {
            return Err(
                MetadataScopeSeriesLookupUnavailable::ShardGeometryMismatch {
                    indexed_shard_count: index.shard_count,
                    requested_shard_count: scope.shard_count,
                }
                .unsupported_operation(operation),
            );
        }
        let shard_buckets = index.series_ids_by_shard.read();
        if scope
            .shards
            .iter()
            .any(|shard| shard_buckets.get(*shard as usize).is_none())
        {
            return Err(
                MetadataScopeSeriesLookupUnavailable::Stale.unsupported_operation(operation)
            );
        }
        Ok(())
    }

    pub(super) fn metadata_series_ids_for_scope(
        &self,
        scope: &crate::storage::MetadataShardScope,
    ) -> MetadataScopeSeriesLookup {
        self.catalog_context().metadata_series_ids_for_scope(scope)
    }

    pub(super) fn bounded_metadata_series_ids_for_scope(
        &self,
        scope: &crate::storage::MetadataShardScope,
        operation: &'static str,
    ) -> Result<Vec<SeriesId>> {
        match self.metadata_series_ids_for_scope(scope) {
            MetadataScopeSeriesLookup::Indexed(series_ids) => Ok(series_ids),
            MetadataScopeSeriesLookup::Unavailable(reason) => {
                Err(reason.unsupported_operation(operation))
            }
        }
    }

    pub(super) fn bounded_metadata_series_ids_for_scope_with_preflight<R>(
        &self,
        scope: &crate::storage::MetadataShardScope,
        operation: &'static str,
        preflight: impl FnOnce(usize) -> Result<R>,
    ) -> Result<(Vec<SeriesId>, R)> {
        if scope.shards.is_empty() {
            return preflight(0).map(|reservation| (Vec::new(), reservation));
        }

        self.validate_bounded_metadata_shard_scope(scope, operation)?;
        let context = self.catalog_context();
        let index = context
            .metadata_shard_index
            .expect("validated metadata shard index remains configured");

        let shard_buckets = index.series_ids_by_shard.read();
        let mut series_count = 0usize;
        for shard in &scope.shards {
            let Some(bucket) = shard_buckets.get(*shard as usize) else {
                return Err(
                    MetadataScopeSeriesLookupUnavailable::Stale.unsupported_operation(operation)
                );
            };
            series_count = series_count.checked_add(bucket.len()).ok_or_else(|| {
                TsinkError::Other(
                    "shard-scoped metadata candidate count exceeds the supported range".to_string(),
                )
            })?;
        }

        let reservation = preflight(series_count)?;
        let mut series_ids = Vec::new();
        series_ids.try_reserve(series_count).map_err(|err| {
            TsinkError::Other(format!(
                "failed to reserve shard-scoped metadata candidates: {err}"
            ))
        })?;
        for shard in &scope.shards {
            let bucket = shard_buckets
                .get(*shard as usize)
                .expect("validated shard bucket remains available under the read lock");
            series_ids.extend(bucket.iter().copied());
        }
        series_ids.sort_unstable();
        Ok((series_ids, reservation))
    }
}
