use crate::TimestampPrecision;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert the current wall clock time to the configured timestamp precision.
pub(crate) fn now_in_precision(precision: TimestampPrecision) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));

    duration_to_units(now, precision)
}

/// Convert a duration into the configured timestamp precision, saturating on overflow.
pub(crate) fn duration_to_units(duration: Duration, precision: TimestampPrecision) -> i64 {
    let units = match precision {
        TimestampPrecision::Nanoseconds => duration.as_nanos(),
        TimestampPrecision::Microseconds => duration.as_micros(),
        TimestampPrecision::Milliseconds => duration.as_millis(),
        TimestampPrecision::Seconds => duration.as_secs() as u128,
    };

    if units > i64::MAX as u128 {
        i64::MAX
    } else {
        units as i64
    }
}
