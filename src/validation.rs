use std::collections::HashSet;

use crate::{Label, Result, TsinkError};

pub(crate) fn validate_metric(metric: &str) -> Result<()> {
    if metric.is_empty() {
        return Err(TsinkError::MetricRequired);
    }
    if metric.len() > u16::MAX as usize {
        return Err(TsinkError::InvalidMetricName(format!(
            "metric name too long: {} bytes (max {})",
            metric.len(),
            u16::MAX as usize
        )));
    }
    Ok(())
}

pub(crate) fn validate_labels(labels: &[Label]) -> Result<()> {
    if labels.len() > crate::label::MAX_SUPPORTED_LABELS_PER_SERIES {
        return Err(TsinkError::InvalidLabel(format!(
            "series has {} labels, exceeding the storage-format limit {}",
            labels.len(),
            crate::label::MAX_SUPPORTED_LABELS_PER_SERIES
        )));
    }
    let mut seen_names = HashSet::with_capacity(labels.len());
    for label in labels {
        if !seen_names.insert(label.name.as_str()) {
            return Err(TsinkError::InvalidLabel(format!(
                "duplicate label '{}'",
                label.name
            )));
        }
        if !label.is_valid() {
            return Err(TsinkError::InvalidLabel(
                "label name must be non-empty".to_string(),
            ));
        }
        if label.name.len() > crate::label::MAX_LABEL_NAME_LEN
            || label.value.len() > crate::label::MAX_LABEL_VALUE_LEN
        {
            return Err(TsinkError::InvalidLabel(format!(
                "label name/value must be within limits (name <= {}, value <= {})",
                crate::label::MAX_LABEL_NAME_LEN,
                crate::label::MAX_LABEL_VALUE_LEN
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_series_identity(
    metric: &str,
    labels: &[Label],
    max_labels_per_series: usize,
    max_identity_bytes: usize,
) -> Result<()> {
    validate_metric(metric)?;
    if labels.len() > max_labels_per_series {
        return Err(TsinkError::InvalidLabel(format!(
            "series has {} labels, exceeding the configured limit {max_labels_per_series}",
            labels.len()
        )));
    }

    let identity_bytes = labels.iter().fold(metric.len(), |bytes, label| {
        bytes
            .saturating_add(label.name.len())
            .saturating_add(label.value.len())
    });
    if identity_bytes > max_identity_bytes {
        return Err(TsinkError::InvalidLabel(format!(
            "series identity is {identity_bytes} bytes, exceeding the configured limit {max_identity_bytes}"
        )));
    }

    validate_labels(labels)
}

pub(crate) fn canonicalize_labels(labels: &[Label]) -> Result<Vec<Label>> {
    validate_labels(labels)?;

    let mut normalized = labels.to_vec();
    normalized.sort();
    Ok(normalized)
}
