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
                "label name and value must be non-empty".to_string(),
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

pub(crate) fn canonicalize_labels(labels: &[Label]) -> Result<Vec<Label>> {
    validate_labels(labels)?;

    let mut normalized = labels.to_vec();
    normalized.sort();
    Ok(normalized)
}
