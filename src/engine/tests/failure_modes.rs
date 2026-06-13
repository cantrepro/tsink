use super::*;

fn next_u64(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn copy_dir_recursive(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path);
        } else if file_type.is_file() {
            std::fs::copy(source_path, destination_path).unwrap();
        }
    }
}

fn collect_bin_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && path.extension().and_then(|ext| ext.to_str()) == Some("bin")
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

#[test]
fn segment_loading_tolerates_random_corruption_without_panics() {
    let base_dir = TempDir::new().unwrap();
    let lane_path = base_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "fuzz")];
    let mut registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("segment_fuzz", &labels)
        .unwrap()
        .series_id;
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &chunks_by_series)
        .unwrap();

    for seed in 0u64..48 {
        let run_dir = TempDir::new().unwrap();
        let run_lane = run_dir.path().join(NUMERIC_LANE_ROOT);
        copy_dir_recursive(&lane_path, &run_lane);

        let mut files = collect_bin_files(&run_lane);
        assert!(!files.is_empty());

        let mut state = seed.wrapping_mul(31).wrapping_add(7);
        let target_index = (next_u64(&mut state) as usize) % files.len();
        let target = files.swap_remove(target_index);
        let mut bytes = std::fs::read(&target).unwrap();
        match (next_u64(&mut state) % 3) as u8 {
            0 => {
                if !bytes.is_empty() {
                    let offset = (next_u64(&mut state) as usize) % bytes.len();
                    bytes[offset] ^= ((next_u64(&mut state) as u8) | 1).max(1);
                } else {
                    bytes.push(0xff);
                }
            }
            1 => {
                if !bytes.is_empty() {
                    let cut = ((next_u64(&mut state) as usize) % bytes.len()).max(1);
                    bytes.truncate(cut);
                }
            }
            _ => {
                let extra = ((next_u64(&mut state) % 16) + 1) as usize;
                for _ in 0..extra {
                    bytes.push((next_u64(&mut state) & 0xff) as u8);
                }
            }
        }
        std::fs::write(&target, bytes).unwrap();

        let result = load_segment_indexes(&run_lane);
        match result {
            Ok(_) => {}
            Err(TsinkError::DataCorruption(_)) => {}
            Err(TsinkError::Io(_)) => {}
            Err(TsinkError::IoWithPath { .. }) => {}
            Err(TsinkError::InvalidConfiguration(_)) => {}
            Err(TsinkError::MemoryMap { .. }) => {}
            Err(other) => panic!("unexpected error variant during corruption fuzz: {other}"),
        }
    }
}
