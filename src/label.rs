//! Label handling for metrics.

use crate::TsinkError;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Maximum length of label name.
pub const MAX_LABEL_NAME_LEN: usize = 256;

/// Maximum length of label value.
pub const MAX_LABEL_VALUE_LEN: usize = 16 * 1024;

/// Maximum metric-name length that can be marshaled losslessly by the v1 format.
pub const MAX_METRIC_NAME_LEN: usize = u16::MAX as usize;

/// A time-series label.
/// A label with missing name or value is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    fn truncate_utf8(s: &mut String, max_len: usize) {
        if s.len() <= max_len {
            return;
        }

        let mut trunc_at = max_len;
        while trunc_at > 0 && !s.is_char_boundary(trunc_at) {
            trunc_at -= 1;
        }
        s.truncate(trunc_at);
    }

    /// Creates a new label.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        let mut name = name.into();
        let mut value = value.into();

        // Truncate if necessary
        if name.len() > MAX_LABEL_NAME_LEN {
            Self::truncate_utf8(&mut name, MAX_LABEL_NAME_LEN);
        }
        if value.len() > MAX_LABEL_VALUE_LEN {
            Self::truncate_utf8(&mut value, MAX_LABEL_VALUE_LEN);
        }

        Self { name, value }
    }

    /// Checks if the label is valid (both name and value are non-empty).
    pub fn is_valid(&self) -> bool {
        !self.name.is_empty() && !self.value.is_empty()
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

/// Marshals a metric name and labels into a unique binary identifier.
pub fn marshal_metric_name(metric: &str, labels: &[Label]) -> Vec<u8> {
    if labels.is_empty() {
        return metric.as_bytes().to_vec();
    }

    // Sort labels by name for consistent marshaling
    let mut sorted_labels = labels.to_vec();
    sorted_labels.sort();

    // Calculate size
    let metric_bytes = metric.as_bytes();
    let metric_len = metric_bytes.len().min(MAX_METRIC_NAME_LEN);
    let mut size = metric_len + 2; // 2 bytes for metric length
    for label in &sorted_labels {
        if label.is_valid() {
            size += label.name.len().min(u16::MAX as usize);
            size += label.value.len().min(u16::MAX as usize);
            size += 4; // 4 bytes for lengths
        }
    }

    // Build the unique identifier
    let mut out = Vec::with_capacity(size);

    // Write metric length and metric
    out.extend_from_slice(&(metric_len as u16).to_le_bytes());
    out.extend_from_slice(&metric_bytes[..metric_len]);

    // Write labels
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

/// Unmarshals a metric name back into metric and labels.
pub fn unmarshal_metric_name(marshaled: &[u8]) -> crate::Result<(String, Vec<Label>)> {
    let bytes = marshaled;
    if bytes.len() < 2 {
        return Ok((String::from_utf8_lossy(marshaled).into_owned(), Vec::new()));
    }

    // Try to parse as marshaled format
    let mut pos = 0;

    // Read metric length
    if pos + 2 > bytes.len() {
        // Not marshaled format, return as plain metric name
        return Ok((String::from_utf8_lossy(marshaled).into_owned(), Vec::new()));
    }

    let metric_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;

    // Read metric
    if pos + metric_len > bytes.len() {
        // Not marshaled format, return as plain metric name
        return Ok((String::from_utf8_lossy(marshaled).into_owned(), Vec::new()));
    }

    let metric =
        String::from_utf8(bytes[pos..pos + metric_len].to_vec()).map_err(TsinkError::Utf8)?;
    pos += metric_len;

    // Read labels
    let mut labels = Vec::new();
    while pos < bytes.len() {
        // Read label name length
        if pos + 2 > bytes.len() {
            break;
        }
        let name_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;

        // Read label name
        if pos + name_len > bytes.len() {
            break;
        }
        let name =
            String::from_utf8(bytes[pos..pos + name_len].to_vec()).map_err(TsinkError::Utf8)?;
        pos += name_len;

        // Read label value length
        if pos + 2 > bytes.len() {
            break;
        }
        let value_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;

        // Read label value
        if pos + value_len > bytes.len() {
            break;
        }
        let value =
            String::from_utf8(bytes[pos..pos + value_len].to_vec()).map_err(TsinkError::Utf8)?;
        pos += value_len;

        labels.push(Label::new(name, value));
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
    fn test_label_truncation() {
        let long_name = "a".repeat(MAX_LABEL_NAME_LEN + 100);
        let long_value = "b".repeat(MAX_LABEL_VALUE_LEN + 100);

        let label = Label::new(long_name, long_value);
        assert_eq!(label.name.len(), MAX_LABEL_NAME_LEN);
        assert_eq!(label.value.len(), MAX_LABEL_VALUE_LEN);
    }

    #[test]
    fn test_invalid_label() {
        let label1 = Label::new("", "value");
        assert!(!label1.is_valid());

        let label2 = Label::new("name", "");
        assert!(!label2.is_valid());
    }

    #[test]
    fn test_marshal_metric_name() {
        let metric = "cpu_usage";

        // Without labels
        assert_eq!(marshal_metric_name(metric, &[]), metric.as_bytes());

        // With labels
        let labels = vec![
            Label::new("host", "server1"),
            Label::new("region", "us-west"),
        ];
        let marshaled = marshal_metric_name(metric, &labels);
        // Check that the marshaled data contains the metric name bytes
        assert!(
            marshaled
                .windows(metric.len())
                .any(|w| w == metric.as_bytes())
        );
    }

    #[test]
    fn test_marshal_with_long_labels() {
        // Test case that previously caused undefined behavior
        let label = Label::new("a".repeat(0x80), "b");
        let marshaled = marshal_metric_name("hello", &[label]);
        // This should not panic or cause undefined behavior
        assert!(!marshaled.is_empty());

        // Test with label value > 127 chars
        let label2 = Label::new("key", "b".repeat(0x80));
        let marshaled2 = marshal_metric_name("world", &[label2]);
        assert!(!marshaled2.is_empty());
    }

    #[test]
    fn test_label_truncation_preserves_utf8_boundaries() {
        let long_name = "é".repeat(MAX_LABEL_NAME_LEN);
        let long_value = "😀".repeat((MAX_LABEL_VALUE_LEN / 4) + 10);

        let label = Label::new(long_name, long_value);
        assert!(label.name.is_char_boundary(label.name.len()));
        assert!(label.value.is_char_boundary(label.value.len()));
        assert!(label.name.len() <= MAX_LABEL_NAME_LEN);
        assert!(label.value.len() <= MAX_LABEL_VALUE_LEN);
    }

    #[test]
    fn test_marshal_metric_name_long_metric_does_not_overflow() {
        let metric = "m".repeat(MAX_METRIC_NAME_LEN + 10);
        let marshaled = marshal_metric_name(&metric, &[Label::new("k", "v")]);

        // First two bytes encode u16::MAX and not a wrapped remainder.
        let encoded_len = u16::from_le_bytes([marshaled[0], marshaled[1]]) as usize;
        assert_eq!(encoded_len, MAX_METRIC_NAME_LEN);
    }

    #[test]
    fn test_label_ordering() {
        let label1 = Label::new("a", "1");
        let label2 = Label::new("a", "2");
        let label3 = Label::new("b", "1");

        assert!(label1 < label2);
        assert!(label2 < label3);
    }
}
