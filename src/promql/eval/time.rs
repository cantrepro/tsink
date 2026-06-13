/// Converts duration in milliseconds to timestamp units according to units-per-second.
pub fn duration_to_units(duration_ms: i64, units_per_second: i64) -> i64 {
    if duration_ms <= 0 || units_per_second <= 0 {
        return 0;
    }
    let scaled = (duration_ms as i128) * (units_per_second as i128);
    let units = scaled / 1_000;
    units.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub fn step_times(start: i64, end: i64, step: i64) -> impl Iterator<Item = i64> {
    let mut current = start;
    std::iter::from_fn(move || {
        if current > end {
            return None;
        }
        let out = current;
        current = current.saturating_add(step);
        Some(out)
    })
}

#[cfg(test)]
mod tests {
    use super::duration_to_units;

    #[test]
    fn converts_ms_to_seconds_units() {
        assert_eq!(duration_to_units(5_000, 1), 5);
    }

    #[test]
    fn converts_ms_to_nanoseconds_units() {
        assert_eq!(duration_to_units(1_000, 1_000_000_000), 1_000_000_000);
    }
}
