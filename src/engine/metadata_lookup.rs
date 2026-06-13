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
}
