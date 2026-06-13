use std::collections::hash_map::Entry;

use super::*;
use crate::engine::binio::{read_bytes, read_u32};
use crate::engine::chunk::{ValueCodecId, ValueLane};
use crate::{Result, TsinkError, Value};

impl SeriesValueFamily {
    pub fn from_value(value: &Value, lane: ValueLane) -> Result<Self> {
        match (value, lane) {
            (Value::F64(_), ValueLane::Numeric) => Ok(Self::F64),
            (Value::I64(_), ValueLane::Numeric) => Ok(Self::I64),
            (Value::U64(_), ValueLane::Numeric) => Ok(Self::U64),
            (Value::Bool(_), ValueLane::Numeric) => Ok(Self::Bool),
            (Value::Bytes(_) | Value::String(_), ValueLane::Blob) => Ok(Self::Blob),
            (Value::Histogram(_), ValueLane::Blob) => Ok(Self::Histogram),
            (_, ValueLane::Numeric) => Err(TsinkError::ValueTypeMismatch {
                expected: "numeric lane value".to_string(),
                actual: value.kind().to_string(),
            }),
            (_, ValueLane::Blob) => Err(TsinkError::ValueTypeMismatch {
                expected: "blob lane value".to_string(),
                actual: value.kind().to_string(),
            }),
        }
    }

    pub fn from_encoded(
        lane: ValueLane,
        value_codec: ValueCodecId,
        payload: &[u8],
    ) -> Result<Self> {
        match value_codec {
            ValueCodecId::GorillaXorF64 => {
                if lane != ValueLane::Numeric {
                    return Err(TsinkError::DataCorruption(
                        "f64 codec stored in blob lane".to_string(),
                    ));
                }
                Ok(Self::F64)
            }
            ValueCodecId::ZigZagDeltaBitpackI64 => {
                if lane != ValueLane::Numeric {
                    return Err(TsinkError::DataCorruption(
                        "i64 codec stored in blob lane".to_string(),
                    ));
                }
                Ok(Self::I64)
            }
            ValueCodecId::DeltaBitpackU64 => {
                if lane != ValueLane::Numeric {
                    return Err(TsinkError::DataCorruption(
                        "u64 codec stored in blob lane".to_string(),
                    ));
                }
                Ok(Self::U64)
            }
            ValueCodecId::BoolBitpack => {
                if lane != ValueLane::Numeric {
                    return Err(TsinkError::DataCorruption(
                        "bool codec stored in blob lane".to_string(),
                    ));
                }
                Ok(Self::Bool)
            }
            ValueCodecId::BytesDeltaBlock => {
                let value_payload = Self::value_payload_from_encoded_chunk(payload)?;
                let Some(&tag) = value_payload.first() else {
                    return Err(TsinkError::DataCorruption(
                        "blob delta-block payload is empty".to_string(),
                    ));
                };
                Self::from_encoded_tag(tag, lane, "blob delta-block")
            }
            ValueCodecId::ConstantRle => {
                let value_payload = Self::value_payload_from_encoded_chunk(payload)?;
                let Some(&tag) = value_payload.first() else {
                    return Err(TsinkError::DataCorruption(
                        "constant-rle payload is empty".to_string(),
                    ));
                };
                Self::from_encoded_tag(tag, lane, "constant-rle")
            }
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::F64 => "f64",
            Self::I64 => "i64",
            Self::U64 => "u64",
            Self::Bool => "bool",
            Self::Blob => "bytes/string",
            Self::Histogram => "histogram",
        }
    }

    fn from_encoded_tag(tag: u8, lane: ValueLane, context: &str) -> Result<Self> {
        match lane {
            ValueLane::Numeric => match tag {
                1 => Ok(Self::F64),
                2 => Ok(Self::I64),
                3 => Ok(Self::U64),
                4 => Ok(Self::Bool),
                5..=7 => Err(TsinkError::DataCorruption(format!(
                    "{context} encoded blob tag {tag} in numeric lane"
                ))),
                _ => Err(TsinkError::DataCorruption(format!(
                    "unknown {context} value tag {tag}"
                ))),
            },
            ValueLane::Blob => match tag {
                5 | 6 => Ok(Self::Blob),
                7 => Ok(Self::Histogram),
                1..=4 => Err(TsinkError::DataCorruption(format!(
                    "{context} encoded numeric tag {tag} in blob lane"
                ))),
                _ => Err(TsinkError::DataCorruption(format!(
                    "unknown {context} value tag {tag}"
                ))),
            },
        }
    }

    fn value_payload_from_encoded_chunk(payload: &[u8]) -> Result<&[u8]> {
        let mut pos = 0usize;
        let ts_len = read_u32(payload, &mut pos)? as usize;
        let _ts_payload = read_bytes(payload, &mut pos, ts_len)?;
        let value_len = read_u32(payload, &mut pos)? as usize;
        let value_payload = read_bytes(payload, &mut pos, value_len)?;
        if pos != payload.len() {
            return Err(TsinkError::DataCorruption(
                "encoded chunk payload has trailing bytes".to_string(),
            ));
        }
        Ok(value_payload)
    }
}

impl SeriesRegistry {
    pub fn series_value_family(&self, series_id: SeriesId) -> Option<SeriesValueFamily> {
        let shard_idx = self.load_series_registry_shard_idx(series_id)?;
        self.series_shards[shard_idx]
            .read()
            .value_families
            .get(&series_id)
            .copied()
    }

    pub fn assign_series_value_family_if_missing(
        &self,
        series_id: SeriesId,
        family: SeriesValueFamily,
    ) -> Result<bool> {
        let Some(shard_idx) = self.load_series_registry_shard_idx(series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "cannot assign value family to unknown series id {}",
                series_id
            )));
        };
        let mut shard = self.series_shards[shard_idx].write();
        if !shard.by_id.contains_key(&series_id) {
            return Err(TsinkError::DataCorruption(format!(
                "cannot assign value family to unknown series id {}",
                series_id
            )));
        }

        match shard.value_families.entry(series_id) {
            Entry::Occupied(existing) => {
                if *existing.get() != family {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: existing.get().name().to_string(),
                        actual: family.name().to_string(),
                    });
                }
                Ok(false)
            }
            Entry::Vacant(vacant) => {
                vacant.insert(family);
                self.add_estimated_memory_bytes(Self::value_family_entry_bytes());
                shard.estimated_series_bytes = shard
                    .estimated_series_bytes
                    .saturating_add(Self::value_family_entry_bytes());
                Ok(true)
            }
        }
    }

    pub fn record_series_value_family(
        &self,
        series_id: SeriesId,
        family: SeriesValueFamily,
    ) -> Result<()> {
        let Some(shard_idx) = self.load_series_registry_shard_idx(series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "cannot record value family for unknown series id {}",
                series_id
            )));
        };
        let mut shard = self.series_shards[shard_idx].write();
        if !shard.by_id.contains_key(&series_id) {
            return Err(TsinkError::DataCorruption(format!(
                "cannot record value family for unknown series id {}",
                series_id
            )));
        }

        match shard.value_families.entry(series_id) {
            Entry::Occupied(existing) => {
                if *existing.get() != family {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} mixes value families across history: expected {}, got {}",
                        series_id,
                        existing.get().name(),
                        family.name(),
                    )));
                }
                Ok(())
            }
            Entry::Vacant(vacant) => {
                vacant.insert(family);
                self.add_estimated_memory_bytes(Self::value_family_entry_bytes());
                shard.estimated_series_bytes = shard
                    .estimated_series_bytes
                    .saturating_add(Self::value_family_entry_bytes());
                Ok(())
            }
        }
    }

    pub fn clear_series_value_family(&self, series_id: SeriesId) {
        let Some(shard_idx) = self.load_series_registry_shard_idx(series_id) else {
            return;
        };
        let mut shard = self.series_shards[shard_idx].write();
        if shard.value_families.remove(&series_id).is_some() {
            self.sub_estimated_memory_bytes(Self::value_family_entry_bytes());
            shard.estimated_series_bytes = shard
                .estimated_series_bytes
                .saturating_sub(Self::value_family_entry_bytes());
        }
    }
}

pub(super) fn encode_optional_series_value_family(family: Option<SeriesValueFamily>) -> u16 {
    match family {
        None => 0,
        Some(SeriesValueFamily::F64) => 1,
        Some(SeriesValueFamily::I64) => 2,
        Some(SeriesValueFamily::U64) => 3,
        Some(SeriesValueFamily::Bool) => 4,
        Some(SeriesValueFamily::Blob) => 5,
        Some(SeriesValueFamily::Histogram) => 6,
    }
}

pub(super) fn decode_optional_series_value_family(raw: u16) -> Result<Option<SeriesValueFamily>> {
    match raw {
        0 => Ok(None),
        1 => Ok(Some(SeriesValueFamily::F64)),
        2 => Ok(Some(SeriesValueFamily::I64)),
        3 => Ok(Some(SeriesValueFamily::U64)),
        4 => Ok(Some(SeriesValueFamily::Bool)),
        5 => Ok(Some(SeriesValueFamily::Blob)),
        6 => Ok(Some(SeriesValueFamily::Histogram)),
        _ => Err(TsinkError::DataCorruption(format!(
            "unknown registry value family tag {raw}"
        ))),
    }
}
