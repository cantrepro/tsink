use crate::{Label, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub metric: String,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub metric: String,
    pub labels: Vec<Label>,
    pub samples: Vec<(i64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PromqlValue {
    Scalar(f64, i64),
    InstantVector(Vec<Sample>),
    RangeVector(Vec<Series>),
    String(String, i64),
}

impl PromqlValue {
    pub fn as_scalar(&self) -> Option<(f64, i64)> {
        match self {
            Self::Scalar(v, t) => Some((*v, *t)),
            _ => None,
        }
    }

    pub fn as_instant_vector(&self) -> Option<&[Sample]> {
        match self {
            Self::InstantVector(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_range_vector(&self) -> Option<&[Series]> {
        match self {
            Self::RangeVector(v) => Some(v),
            _ => None,
        }
    }
}

pub fn value_to_f64(value: &Value) -> f64 {
    match value {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        Value::U64(v) => *v as f64,
        Value::Bool(v) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        Value::Bytes(_) | Value::String(_) => f64::NAN,
    }
}
