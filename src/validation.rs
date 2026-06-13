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
    for label in labels {
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
