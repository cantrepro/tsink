use super::*;
use crate::{
    QueryBudgetLimits, ResourceLimitOverride, ResourceLimits, ResourceProfile, ResourceProfileName,
};

#[test]
fn standard_profiles_are_finite_and_self_consistent() {
    for (profile, name) in [
        (ResourceProfile::Test, ResourceProfileName::Test),
        (ResourceProfile::Embedded, ResourceProfileName::Embedded),
        (ResourceProfile::Edge, ResourceProfileName::Edge),
        (ResourceProfile::Server, ResourceProfileName::Server),
    ] {
        let limits = profile
            .finite_limits()
            .expect("standard profile must be finite");
        limits.validate().expect("standard profile must validate");
        assert!(
            limits.background.maintenance_max_bytes_per_pass >= limits.accounted_memory_bytes,
            "one admitted sealed chunk must fit through a maintenance pass"
        );
        assert!(
            limits.background.maintenance_max_items_per_pass
                >= limits.write_batch.max_rows.expect("finite write rows"),
            "one admitted batch must fit through a maintenance pass"
        );
        assert!(
            u64::try_from(
                limits
                    .write_batch
                    .max_modeled_input_bytes
                    .expect("finite write input")
            )
            .unwrap_or(u64::MAX)
                <= limits.background.maintenance_max_bytes_per_pass,
            "one admitted write must fit through a maintenance pass"
        );

        let snapshot = StorageBuilder::new()
            .with_resource_profile(profile)
            .with_data_path("profile-data")
            .with_object_store_path("profile-object")
            .resource_configuration_snapshot();
        let as_u64 = |value: usize| u64::try_from(value).expect("profile value fits u64");
        let duration_nanos = |value: Duration| {
            u64::try_from(value.as_nanos()).expect("profile duration fits nanoseconds")
        };
        assert_eq!(snapshot.selected_profile, name);
        assert!(snapshot.reported_by_backend);
        let storage = snapshot.resolved_limits.storage;
        assert!(storage.persistent);
        assert!(storage.wal_enabled);
        assert_eq!(
            storage.accounted_memory_bytes,
            Some(limits.accounted_memory_bytes)
        );
        assert_eq!(storage.cardinality, Some(limits.cardinality));
        assert_eq!(
            storage.max_labels_per_series,
            Some(limits.max_labels_per_series)
        );
        assert_eq!(
            storage.max_series_identity_bytes,
            Some(limits.max_series_identity_bytes)
        );
        assert_eq!(
            storage.max_new_series_per_window,
            Some(limits.max_new_series_per_window)
        );
        assert_eq!(
            storage.new_series_window_nanos,
            Some(duration_nanos(limits.new_series_window))
        );
        assert_eq!(
            storage.max_write_batch_rows,
            Some(as_u64(
                limits.write_batch.max_rows.expect("finite write rows")
            ))
        );
        assert_eq!(
            storage.max_write_batch_input_bytes,
            Some(as_u64(
                limits
                    .write_batch
                    .max_modeled_input_bytes
                    .expect("finite write input")
            ))
        );
        assert_eq!(storage.wal_bytes, Some(limits.wal_bytes));
        assert_eq!(
            storage.wal_write_buffer_bytes,
            Some(limits.wal_write_buffer_bytes)
        );
        assert_eq!(storage.local_disk_bytes, Some(limits.local_disk_bytes));
        assert_eq!(
            storage.filesystem_free_headroom_bytes,
            Some(limits.filesystem_free_headroom_bytes)
        );
        assert_eq!(
            storage.maintenance_temp_reserve_bytes,
            Some(limits.maintenance_temp_reserve_bytes)
        );
        assert_eq!(
            storage.max_concurrent_writers,
            Some(limits.max_concurrent_writers)
        );
        assert_eq!(
            storage.write_timeout_nanos,
            Some(duration_nanos(limits.write_timeout))
        );
        assert_eq!(
            storage.max_remote_tier_fetch_concurrency,
            limits.query.max_concurrent_queries
        );
        assert_eq!(
            storage.flush_interval_nanos,
            Some(duration_nanos(limits.background.flush_interval))
        );
        assert_eq!(
            storage.compaction_interval_nanos,
            Some(duration_nanos(limits.background.compaction_interval))
        );
        assert_eq!(
            storage.persisted_refresh_poll_interval_nanos,
            Some(duration_nanos(limits.background.persisted_refresh_interval))
        );
        assert_eq!(
            storage.rollup_interval_nanos,
            Some(duration_nanos(limits.background.rollup_interval))
        );
        assert_eq!(
            storage.max_active_partition_heads_per_series,
            Some(limits.max_active_partition_heads_per_series)
        );
        assert_eq!(snapshot.resolved_limits.query, limits.query);
        assert_eq!(
            snapshot.resolved_limits.maintenance_max_items_per_pass,
            Some(as_u64(limits.background.maintenance_max_items_per_pass))
        );
        assert_eq!(
            snapshot.resolved_limits.maintenance_max_bytes_per_pass,
            Some(limits.background.maintenance_max_bytes_per_pass)
        );
    }
}

#[test]
fn default_builder_uses_embedded_profile_and_built_snapshot_propagates() {
    let storage = StorageBuilder::new().build().expect("embedded storage");
    let snapshot = storage.resource_configuration_snapshot();
    assert_eq!(snapshot.selected_profile, ResourceProfileName::Embedded);
    assert_eq!(
        snapshot.schema_version,
        crate::RESOURCE_CONFIGURATION_SCHEMA_VERSION
    );
    assert_eq!(
        snapshot.resolved_limits.storage.accounted_memory_bytes,
        Some(ResourceLimits::embedded().accounted_memory_bytes)
    );
    assert_eq!(
        storage.observability_snapshot().resource_configuration,
        snapshot
    );
    let encoded = serde_json::to_vec(&snapshot).expect("serialize resource snapshot");
    let decoded: crate::ResourceConfigurationSnapshot =
        serde_json::from_slice(&encoded).expect("deserialize resource snapshot");
    assert_eq!(decoded, snapshot);
    storage.close().expect("close");
}

#[test]
fn sparse_overrides_win_independently_of_call_order_and_can_be_cleared() {
    let before_profile = StorageBuilder::new()
        .with_memory_limit(123_456)
        .with_resource_profile(ResourceProfile::Edge)
        .resource_configuration_snapshot();
    let after_profile = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Edge)
        .with_memory_limit(123_456)
        .resource_configuration_snapshot();

    assert_eq!(before_profile, after_profile);
    assert_eq!(
        before_profile
            .resolved_limits
            .storage
            .accounted_memory_bytes,
        Some(123_456)
    );
    assert_eq!(
        before_profile.overrides,
        vec![ResourceLimitOverride::AccountedMemory]
    );

    let cleared = StorageBuilder::new()
        .with_memory_limit(123_456)
        .with_resource_profile(ResourceProfile::Edge)
        .clear_resource_limit_override(ResourceLimitOverride::AccountedMemory)
        .resource_configuration_snapshot();
    assert_eq!(
        cleared.resolved_limits.storage.accounted_memory_bytes,
        Some(ResourceLimits::edge().accounted_memory_bytes)
    );
    assert!(cleared.overrides.is_empty());
}

#[test]
fn invalid_custom_profile_relationships_are_rejected() {
    let mut disk = ResourceLimits::test();
    disk.wal_bytes = disk.local_disk_bytes;
    assert!(StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(disk))
        .build()
        .is_err());

    let mut query = ResourceLimits::test();
    query.query.max_shared_memory_bytes = Some(1024);
    query.query.per_query.max_memory_bytes = Some(1025);
    assert!(StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(query))
        .build()
        .is_err());

    let mut identity = ResourceLimits::test();
    identity.max_labels_per_series = crate::MAX_SUPPORTED_LABELS_PER_SERIES as u64 + 1;
    assert!(StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(identity))
        .build()
        .is_err());

    let mut async_capacity = ResourceLimits::test();
    async_capacity.async_runtime.read_workers = 0;
    assert!(StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(async_capacity))
        .build()
        .is_err());

    let mut ignored_cadence = ResourceLimits::test();
    ignored_cadence.background.flush_interval = Duration::from_secs(1);
    let error = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(ignored_cadence))
        .build()
        .err()
        .expect("a custom cadence that the runtime cannot honor must fail");
    assert!(error.to_string().contains("fixed background cadences"));

    let mut stranded_chunk = ResourceLimits::test();
    stranded_chunk.background.maintenance_max_bytes_per_pass =
        stranded_chunk.accounted_memory_bytes - 1;
    let error = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(stranded_chunk))
        .build()
        .err()
        .expect("a custom profile must not strand an admitted sealed chunk");
    assert!(error.to_string().contains("one admitted sealed chunk"));

    let error = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::ExpertUnlimited)
        .with_maintenance_max_bytes_per_pass(1024)
        .build()
        .err()
        .expect("finite maintenance work requires a finite chunk envelope");
    assert!(error.to_string().contains("finite accounted-memory limit"));

    let mut stranded_batch = ResourceLimits::test();
    stranded_batch.background.maintenance_max_items_per_pass =
        stranded_batch.write_batch.max_rows.unwrap() - 1;
    let error = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Custom(stranded_batch))
        .build()
        .err()
        .expect("a custom profile must not strand an admitted batch");
    assert!(error.to_string().contains("one admitted batch"));

    let error = StorageBuilder::new()
        .with_write_batch_limits(crate::WriteBatchLimits {
            max_rows: Some(101),
            max_modeled_input_bytes: Some(1024),
        })
        .with_maintenance_max_items_per_pass(100)
        .build()
        .err()
        .expect("effective finite overrides must not strand an admitted batch");
    assert!(error.to_string().contains("one admitted batch"));
}

#[test]
fn custom_profiles_reject_internal_unlimited_sentinels() {
    let mut memory = ResourceLimits::test();
    memory.accounted_memory_bytes = usize::MAX as u64;
    memory.background.maintenance_max_bytes_per_pass = u64::MAX;
    let error = memory
        .validate()
        .expect_err("a finite custom profile must not resolve to an unlimited memory sentinel");
    assert!(
        error.to_string().contains("accounted_memory_bytes")
            && error.to_string().contains("ExpertUnlimited"),
        "{error}"
    );

    let mut maintenance_items = ResourceLimits::test();
    maintenance_items.background.maintenance_max_items_per_pass = usize::MAX;
    let error = maintenance_items.validate().expect_err(
        "a finite custom profile must not resolve to the unlimited maintenance-item sentinel",
    );
    assert!(
        error
            .to_string()
            .contains("background.maintenance_max_items_per_pass")
            && error.to_string().contains("ExpertUnlimited"),
        "{error}"
    );
}

#[test]
fn expert_unlimited_preserves_legacy_unbounded_storage_and_query_controls() {
    let storage = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::ExpertUnlimited)
        .build()
        .expect("expert storage");
    let snapshot = storage.resource_configuration_snapshot();
    assert_eq!(
        snapshot.selected_profile,
        ResourceProfileName::ExpertUnlimited
    );
    assert_eq!(
        snapshot.resolved_limits.storage.accounted_memory_bytes,
        None
    );
    assert_eq!(snapshot.resolved_limits.storage.cardinality, None);
    assert_eq!(snapshot.resolved_limits.storage.wal_bytes, None);
    assert_eq!(snapshot.resolved_limits.query, QueryBudgetLimits::default());
    assert_eq!(
        snapshot.resolved_limits.maintenance_max_items_per_pass,
        None
    );
    storage.close().expect("close");
}

#[test]
fn tiny_combined_memory_and_disk_envelope_accepts_small_work_then_rejects_growth() {
    let temp = TempDir::new().unwrap();
    let mut query = ResourceLimits::test().query;
    query.max_shared_memory_bytes = Some(512 * 1024);
    query.per_query.max_memory_bytes = Some(256 * 1024);
    let storage = StorageBuilder::new()
        .with_resource_profile(ResourceProfile::Test)
        .with_data_path(temp.path())
        .with_wal_enabled(false)
        .with_memory_limit(1024 * 1024)
        .with_write_timeout(Duration::ZERO)
        .with_local_disk_limit(4 * 1024 * 1024)
        .with_maintenance_temp_reserve(512 * 1024)
        .with_query_budget_limits(query)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .expect("tiny bounded storage should open");

    storage
        .insert_rows(&[Row::with_labels(
            "tiny_profile",
            Vec::new(),
            DataPoint::new(1, 1.0),
        )])
        .expect("small operation should fit");

    let error = storage
        .insert_rows(&[Row::with_labels(
            "tiny_profile_blob",
            Vec::new(),
            DataPoint::new(2, Value::Bytes(vec![0; 2 * 1024 * 1024])),
        )])
        .expect_err("large transient growth must be rejected");
    assert!(matches!(error, TsinkError::MemoryBudgetExceeded { .. }));

    let limits = storage.effective_storage_limits();
    assert_eq!(limits.accounted_memory_bytes, Some(1024 * 1024));
    assert_eq!(limits.local_disk_bytes, Some(4 * 1024 * 1024));
    storage
        .abandon_without_close_for_tests()
        .expect("test shutdown");
}

#[test]
fn async_profile_and_facade_overrides_propagate_to_snapshot() {
    let async_storage = crate::AsyncStorageBuilder::new()
        .with_resource_profile(ResourceProfile::Edge)
        .with_read_workers(3)
        .build()
        .expect("async edge storage");
    let snapshot = async_storage.resource_configuration_snapshot();
    assert_eq!(snapshot.selected_profile, ResourceProfileName::Edge);
    let async_limits = snapshot
        .resolved_limits
        .async_runtime
        .expect("async facade limits");
    assert_eq!(async_limits.read_workers, 3);
    assert_eq!(
        async_limits.queue_command_capacity,
        ResourceLimits::edge().async_runtime.queue_command_capacity
    );
    assert!(snapshot
        .overrides
        .contains(&ResourceLimitOverride::AsyncReadWorkers));
    drop(async_storage);
}
