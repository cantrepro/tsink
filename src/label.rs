//! Label handling for metrics.

use crate::TsinkError;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub const MAX_LABEL_NAME_LEN: usize = 256;

pub const MAX_LABEL_VALUE_LEN: usize = 16 * 1024;

/// Maximum metric-name length that can be marshaled losslessly by the current binary format.
pub const MAX_METRIC_NAME_LEN: usize = u16::MAX as usize;

/// Metric label. Empty names are invalid; empty values are valid.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    /// Validation of emptiness and length limits happens at ingest/query boundaries.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.name.is_empty()
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.name.cmp(&other.name) {
            Ordering::Equal => self.value.cmp(&other.value),
            other => other,
        }
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Computes a collision-safe canonical binary identity for a metric series.
pub fn canonical_series_identity(metric: &str, labels: &[Label]) -> Vec<u8> {
    marshal_metric_name(metric, labels)
}

/// Computes a collision-safe canonical string key for a metric series.
pub fn canonical_series_identity_key(metric: &str, labels: &[Label]) -> String {
    fn push_hex_byte(out: &mut String, byte: u8) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        out.push(char::from(HEX[(byte >> 4) as usize]));
        out.push(char::from(HEX[(byte & 0x0f) as usize]));
    }

    let canonical = canonical_series_identity(metric, labels);
    let mut encoded = String::with_capacity(canonical.len() * 2);
    for byte in canonical {
        push_hex_byte(&mut encoded, byte);
    }
    encoded
}

/// Marshals a metric name and labels into a unique binary identifier.
pub fn marshal_metric_name(metric: &str, labels: &[Label]) -> Vec<u8> {
    let mut sorted_labels = labels.to_vec();
    sorted_labels.sort();

    let metric_bytes = metric.as_bytes();
    let metric_len = metric_bytes.len().min(MAX_METRIC_NAME_LEN);
    let mut size = metric_len + 2;
    for label in &sorted_labels {
        if label.is_valid() {
            size += label.name.len().min(u16::MAX as usize);
            size += label.value.len().min(u16::MAX as usize);
            size += 4;
        }
    }

    let mut out = Vec::with_capacity(size);

    out.extend_from_slice(&(metric_len as u16).to_le_bytes());
    out.extend_from_slice(&metric_bytes[..metric_len]);

    for label in &sorted_labels {
        if label.is_valid() {
            let name_bytes = label.name.as_bytes();
            let name_len = name_bytes.len().min(u16::MAX as usize);
            out.extend_from_slice(&(name_len as u16).to_le_bytes());
            out.extend_from_slice(&name_bytes[..name_len]);

            let value_bytes = label.value.as_bytes();
            let value_len = value_bytes.len().min(u16::MAX as usize);
            out.extend_from_slice(&(value_len as u16).to_le_bytes());
            out.extend_from_slice(&value_bytes[..value_len]);
        }
    }

    out
}

/// Computes a stable 64-bit hash for a metric series identity.
pub fn stable_series_identity_hash(metric: &str, labels: &[Label]) -> u64 {
    let canonical = canonical_series_identity(metric, labels);
    let mut hash = 0xcbf29ce484222325u64;
    for byte in canonical {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Unmarshals a metric name back into metric and labels.
pub fn unmarshal_metric_name(marshaled: &[u8]) -> crate::Result<(String, Vec<Label>)> {
    let bytes = marshaled;
    if bytes.len() < 2 {
        return Err(TsinkError::DataCorruption(
            "invalid metric key encoding: missing metric length".to_string(),
        ));
    }

    let mut pos = 0;

    let metric_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;

    if pos + metric_len > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "invalid metric key encoding: metric length {} exceeds payload size {}",
            metric_len,
            bytes.len().saturating_sub(pos)
        )));
    }

    let metric =
        String::from_utf8(bytes[pos..pos + metric_len].to_vec()).map_err(TsinkError::Utf8)?;
    pos += metric_len;

    let mut labels = Vec::new();
    while pos < bytes.len() {
        if pos + 2 > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "invalid metric key encoding: truncated label name length".to_string(),
            ));
        }
        let name_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        if name_len == 0 {
            return Err(TsinkError::DataCorruption(
                "invalid metric key encoding: empty label name".to_string(),
            ));
        }

        if pos + name_len > bytes.len() {
            return Err(TsinkError::DataCorruption(format!(
                "invalid metric key encoding: label name length {} exceeds payload size {}",
                name_len,
                bytes.len().saturating_sub(pos)
            )));
        }
        let name =
            String::from_utf8(bytes[pos..pos + name_len].to_vec()).map_err(TsinkError::Utf8)?;
        pos += name_len;

        if pos + 2 > bytes.len() {
            return Err(TsinkError::DataCorruption(
                "invalid metric key encoding: truncated label value length".to_string(),
            ));
        }
        let value_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        if pos + value_len > bytes.len() {
            return Err(TsinkError::DataCorruption(format!(
                "invalid metric key encoding: label value length {} exceeds payload size {}",
                value_len,
                bytes.len().saturating_sub(pos)
            )));
        }
        let value =
            String::from_utf8(bytes[pos..pos + value_len].to_vec()).map_err(TsinkError::Utf8)?;
        pos += value_len;

        // Decode preserves encoded label bytes; max-length validation happens on write.
        labels.push(Label { name, value });
    }

    Ok((metric, labels))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_creation() {
        let label = Label::new("host", "server1");
        assert_eq!(label.name, "host");
        assert_eq!(label.value, "server1");
        assert!(label.is_valid());
    }

    #[test]
    fn test_label_new_preserves_oversized_input() {
        let long_name = "a".repeat(MAX_LABEL_NAME_LEN + 100);
        let long_value = "b".repeat(MAX_LABEL_VALUE_LEN + 100);

        let label = Label::new(long_name.clone(), long_value.clone());
        assert_eq!(label.name, long_name);
        assert_eq!(label.value, long_value);
    }

    #[test]
    fn test_invalid_label() {
        let label1 = Label::new("", "value");
        assert!(!label1.is_valid());

        let label2 = Label::new("name", "");
        assert!(label2.is_valid());
    }

    #[test]
    fn test_marshal_metric_name() {
        let metric = "cpu_usage";

        let marshaled = marshal_metric_name(metric, &[]);
        let decoded = unmarshal_metric_name(&marshaled).unwrap();
        assert_eq!(decoded, ("cpu_usage".to_string(), Vec::new()));

        let labels = vec![
            Label::new("host", "server1"),
            Label::new("region", "us-west"),
        ];
        let marshaled = marshal_metric_name(metric, &labels);
        let (decoded_metric, decoded_labels) = unmarshal_metric_name(&marshaled).unwrap();
        assert_eq!(decoded_metric, "cpu_usage");
        assert_eq!(decoded_labels.len(), 2);
    }

    #[test]
    fn test_marshal_with_long_labels() {
        let label = Label::new("a".repeat(0x80), "b");
        let marshaled = marshal_metric_name("hello", &[label]);
        assert!(!marshaled.is_empty());

        let label2 = Label::new("key", "b".repeat(0x80));
        let marshaled2 = marshal_metric_name("world", &[label2]);
        assert!(!marshaled2.is_empty());
    }

    #[test]
    fn test_marshal_preserves_empty_label_values() {
        let marshaled = marshal_metric_name("metric", &[Label::new("optional", "")]);
        let (metric, labels) = unmarshal_metric_name(&marshaled).unwrap();

        assert_eq!(metric, "metric");
        assert_eq!(labels, vec![Label::new("optional", "")]);
    }

    #[test]
    fn test_label_new_preserves_utf8_input() {
        let long_name = "é".repeat(MAX_LABEL_NAME_LEN + 1);
        let long_value = "😀".repeat((MAX_LABEL_VALUE_LEN / 4) + 10);

        let label = Label::new(long_name.clone(), long_value.clone());
        assert!(label.name.is_char_boundary(label.name.len()));
        assert!(label.value.is_char_boundary(label.value.len()));
        assert_eq!(label.name, long_name);
        assert_eq!(label.value, long_value);
    }

    #[test]
    fn test_oversized_values_with_common_prefix_remain_distinct() {
        let common_prefix = "x".repeat(MAX_LABEL_VALUE_LEN);
        let label_a = Label::new("k", format!("{common_prefix}a"));
        let label_b = Label::new("k", format!("{common_prefix}b"));

        assert_ne!(label_a, label_b);
        assert_eq!(label_a.value.len(), MAX_LABEL_VALUE_LEN + 1);
        assert_eq!(label_b.value.len(), MAX_LABEL_VALUE_LEN + 1);
    }

    #[test]
    fn test_marshal_metric_name_long_metric_does_not_overflow() {
        let metric = "m".repeat(MAX_METRIC_NAME_LEN + 10);
        let marshaled = marshal_metric_name(&metric, &[Label::new("k", "v")]);

        let encoded_len = u16::from_le_bytes([marshaled[0], marshaled[1]]) as usize;
        assert_eq!(encoded_len, MAX_METRIC_NAME_LEN);
    }

    #[test]
    fn test_unmarshal_rejects_legacy_raw_metric_bytes() {
        let err = unmarshal_metric_name(b"cpu_usage").unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));
    }

    #[test]
    fn test_unlabeled_roundtrip_with_length_prefixed_like_name() {
        let metric = "\u{3}\u{0}abc";
        let marshaled = marshal_metric_name(metric, &[]);
        let (decoded_metric, decoded_labels) = unmarshal_metric_name(&marshaled).unwrap();
        assert_eq!(decoded_metric, metric);
        assert!(decoded_labels.is_empty());
    }

    #[test]
    fn test_label_ordering() {
        let label1 = Label::new("a", "1");
        let label2 = Label::new("a", "2");
        let label3 = Label::new("b", "1");

        assert!(label1 < label2);
        assert!(label2 < label3);
    }

    #[test]
    fn test_unmarshal_preserves_oversized_label_name_roundtrip() {
        let label = Label {
            name: "a".repeat(1000),
            value: "v".to_string(),
        };

        let marshaled = marshal_metric_name("metric", &[label]);
        let (_, labels) = unmarshal_metric_name(&marshaled).unwrap();

        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].name.len(), 1000);
        assert_eq!(labels[0].value, "v");
    }

    #[test]
    fn stable_series_hash_is_order_invariant_for_labels() {
        let left =
            stable_series_identity_hash("metric", &[Label::new("b", "2"), Label::new("a", "1")]);
        let right =
            stable_series_identity_hash("metric", &[Label::new("a", "1"), Label::new("b", "2")]);

        assert_eq!(left, right);
    }

    #[test]
    fn canonical_series_identity_key_is_order_invariant_for_labels() {
        let left =
            canonical_series_identity_key("metric", &[Label::new("b", "2"), Label::new("a", "1")]);
        let right =
            canonical_series_identity_key("metric", &[Label::new("a", "1"), Label::new("b", "2")]);

        assert_eq!(left, right);
    }

    #[test]
    fn canonical_series_identity_key_distinguishes_delimiter_collision_cases() {
        let left = canonical_series_identity_key(
            "metric",
            &[Label::new("job", "api,zone=west|prod\u{1f}blue")],
        );
        let right = canonical_series_identity_key(
            "metric",
            &[
                Label::new("job", "api"),
                Label::new("zone", "west|prod\u{1f}blue"),
            ],
        );

        assert_ne!(left, right);
    }
}
