use tempfile::TempDir;

use super::super::{
    SeriesRegistry, SeriesValueFamily, REGISTRY_INDEX_MAGIC, REGISTRY_INDEX_VERSION,
    REGISTRY_SECTION_VALUE_FAMILY,
};
use crate::engine::binio::{decode_optional_zstd_framed_file, read_u16, read_u64};
use crate::Label;

#[test]
fn persist_to_path_round_trips_series_value_families() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("series_index.bin");

    let registry = SeriesRegistry::new();
    let numeric = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let blob = registry
        .resolve_or_insert("events", &[Label::new("host", "b")])
        .unwrap()
        .series_id;
    registry
        .record_series_value_family(numeric, SeriesValueFamily::F64)
        .unwrap();
    registry
        .record_series_value_family(blob, SeriesValueFamily::Blob)
        .unwrap();
    assert_eq!(
        registry.series_value_family(numeric),
        Some(SeriesValueFamily::F64)
    );
    assert_eq!(
        registry.series_value_family(blob),
        Some(SeriesValueFamily::Blob)
    );

    registry.persist_to_path(&snapshot_path).unwrap();
    let raw_bytes = std::fs::read(&snapshot_path).unwrap();
    let bytes = decode_optional_zstd_framed_file(
        &raw_bytes,
        REGISTRY_INDEX_MAGIC,
        REGISTRY_INDEX_VERSION,
        "series index",
    )
    .unwrap();
    let mut pos = 4usize;
    assert_eq!(read_u16(&bytes, &mut pos).unwrap(), REGISTRY_INDEX_VERSION);
    assert_eq!(read_u16(&bytes, &mut pos).unwrap(), 0);
    pos = pos.saturating_add(8 + 4 + 4 + 4 + 8);
    assert_eq!(
        read_u64(&bytes, &mut pos).unwrap() & REGISTRY_SECTION_VALUE_FAMILY,
        REGISTRY_SECTION_VALUE_FAMILY
    );
    let loaded = SeriesRegistry::load_from_path(&snapshot_path).unwrap();

    assert_eq!(
        loaded.series_value_family(numeric),
        Some(SeriesValueFamily::F64)
    );
    assert_eq!(
        loaded.series_value_family(blob),
        Some(SeriesValueFamily::Blob)
    );
}
