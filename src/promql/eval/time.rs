/// Converts duration in milliseconds to timestamp units according to units-per-second.
pub fn duration_to_units(duration_ms: i64, units_per_second: i64) -> i64 {
    if units_per_second <= 0 {
        return 0;
    }
    let scaled = (duration_ms as i128) * (units_per_second as i128);
    let units = scaled / 1_000;
    units.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub fn step_times(start: i64, end: i64, step: i64) -> impl Iterator<Item = i64> {
    let mut current = (step > 0).then_some(start);
    std::iter::from_fn(move || {
        let out = current?;
        if out > end {
            current = None;
            return None;
        }
        current = out.checked_add(step).filter(|next| *next > out);
        Some(out)
    })
}

#[cfg(test)]
mod tests {
    use super::{duration_to_units, step_times};

    #[test]
    fn converts_ms_to_seconds_units() {
        assert_eq!(duration_to_units(5_000, 1), 5);
    }

    #[test]
    fn converts_ms_to_nanoseconds_units() {
        assert_eq!(duration_to_units(1_000, 1_000_000_000), 1_000_000_000);
    }

    #[test]
    fn preserves_negative_durations_for_signed_offsets() {
        assert_eq!(duration_to_units(-60_000, 1), -60);
    }

    #[test]
    fn step_times_stops_after_end() {
        let times = step_times(10, 15, 2).collect::<Vec<_>>();
        assert_eq!(times, vec![10, 12, 14]);
    }

    #[test]
    fn step_times_yields_max_timestamp_once() {
        let times = step_times(i64::MAX, i64::MAX, 1)
            .take(2)
            .collect::<Vec<_>>();
        assert_eq!(times, vec![i64::MAX]);
    }

    #[test]
    fn step_times_rejects_non_positive_steps() {
        assert_eq!(step_times(0, 10, 0).next(), None);
        assert_eq!(step_times(0, 10, -1).next(), None);
    }
}
