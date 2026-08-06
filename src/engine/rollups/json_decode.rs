//! Bounded, schema-equivalent JSON preflight for rollup policy and state snapshots.

use std::io::Read;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

use super::*;

const JSON_ALLOCATION_ALLOWANCE_BYTES: usize = 64;
const JSON_DECODE_FIXED_BYTES: usize = 512;
const JSON_READ_BUFFER_BYTES: usize = 16 * 1024;

#[cfg(test)]
thread_local! {
    static TYPED_JSON_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) fn note_typed_json_materialization() {
    #[cfg(test)]
    TYPED_JSON_MATERIALIZATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
fn reset_typed_json_materializations() {
    TYPED_JSON_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(test)]
fn typed_json_materializations() -> u64 {
    TYPED_JSON_MATERIALIZATIONS.with(std::cell::Cell::get)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RollupJsonLimits {
    pub(super) max_items: usize,
    pub(super) max_modeled_bytes: usize,
}

impl Default for RollupJsonLimits {
    fn default() -> Self {
        Self {
            max_items: ROLLUP_STATE_SNAPSHOT_MAX_ITEMS,
            max_modeled_bytes: ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RollupJsonDecodePlan {
    pub(super) usage: RollupStateEnvelopeUsage,
    added_modeled_bytes: usize,
}

impl RollupJsonDecodePlan {
    /// Admits the two typed-decode phases. During serde materialization the raw input, its
    /// escaped-string scratch, and the DTO coexist. During state conversion the raw input has
    /// been dropped, but the DTO vector buffers coexist with the destination maps. The logical
    /// envelope's fixed charges cover DTO slots, destination nodes, string capacities, allocator
    /// slack, and (for policies) the persistent `policy_stats` key clone.
    pub(super) fn admit_typed_decode(
        self,
        raw_capacity: usize,
        raw_len: usize,
        retained_modeled_bytes: usize,
        memory_limit_bytes: usize,
        destination_maps: bool,
    ) -> Result<()> {
        let raw_bytes = raw_capacity
            .checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES)
            .ok_or_else(json_memory_model_overflow)?;
        let serde_scratch = raw_len
            .checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES)
            .ok_or_else(json_memory_model_overflow)?;
        let dto_bytes = self
            .added_modeled_bytes
            .checked_add(JSON_DECODE_FIXED_BYTES)
            .ok_or_else(json_memory_model_overflow)?;
        // serde may grow an outer or nested Vec by allocating its successor before releasing the
        // predecessor. One additional complete candidate is therefore a conservative upper bound
        // for every predecessor buffer at the typed-materialization peak.
        let dto_materialization_bytes = dto_bytes
            .checked_mul(2)
            .ok_or_else(json_memory_model_overflow)?;
        let decode_peak = retained_modeled_bytes
            .checked_add(raw_bytes)
            .and_then(|bytes| bytes.checked_add(serde_scratch))
            .and_then(|bytes| bytes.checked_add(dto_materialization_bytes))
            .ok_or_else(json_memory_model_overflow)?;
        crate::disk_budget::admit_startup_memory(memory_limit_bytes, decode_peak)?;
        if destination_maps {
            // Hash-map growth can likewise retain a predecessor table until the successor is
            // allocated. Destination strings are moved from the DTO, but doubling the physical
            // added envelope conservatively covers both tables and their allocator slack.
            let destination_bytes = self
                .added_modeled_bytes
                .checked_mul(2)
                .ok_or_else(json_memory_model_overflow)?;
            let conversion_peak = retained_modeled_bytes
                .checked_add(dto_bytes)
                .and_then(|bytes| bytes.checked_add(destination_bytes))
                .ok_or_else(json_memory_model_overflow)?;
            crate::disk_budget::admit_startup_memory(memory_limit_bytes, conversion_peak)?;
        }
        Ok(())
    }
}

fn json_memory_model_overflow() -> TsinkError {
    TsinkError::Other("rollup JSON decode memory model overflow".to_string())
}

fn json_allocation_failed(
    context: &str,
    bytes: usize,
    error: impl std::fmt::Display,
) -> TsinkError {
    TsinkError::Other(format!(
        "failed to reserve {bytes} bytes while decoding {context}: {error}"
    ))
}

fn reserve_json_read_growth(
    output: &mut Vec<u8>,
    read: usize,
    next_len: usize,
    context: &str,
) -> Result<()> {
    // `Vec::try_reserve_exact` takes an amount relative to the current *length*, not its
    // capacity. Reserving only the missing spare capacity would allow the subsequent
    // `extend_from_slice` to reallocate outside admission when some spare remained.
    let additional = next_len.checked_sub(output.len()).ok_or_else(|| {
        TsinkError::DataCorruption(format!("{context} length moved backwards while reading"))
    })?;
    output
        .try_reserve_exact(additional)
        .map_err(|error| json_allocation_failed(context, additional, error))?;
    debug_assert!(output.capacity().saturating_sub(output.len()) >= read);
    Ok(())
}

/// Reads a rollup JSON snapshot while admitting every prospective raw-buffer growth before the
/// allocation. `try_reserve_exact` is modeled with a fixed allocator allowance, matching the
/// project's other startup allocation models.
pub(super) fn read_json_to_end_budgeted(
    reader: &mut impl Read,
    max_bytes: usize,
    initial_size_hint: usize,
    context: &str,
    memory_limit_bytes: usize,
    retained_modeled_bytes: usize,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let initial_capacity = initial_size_hint.min(max_bytes).min(JSON_READ_BUFFER_BYTES);
    if initial_capacity > 0 {
        let required = retained_modeled_bytes
            .checked_add(initial_capacity)
            .and_then(|bytes| bytes.checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES))
            .ok_or_else(json_memory_model_overflow)?;
        crate::disk_budget::admit_startup_memory(memory_limit_bytes, required)?;
        output
            .try_reserve_exact(initial_capacity)
            .map_err(|error| json_allocation_failed(context, initial_capacity, error))?;
    }

    let mut buffer = [0u8; JSON_READ_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(output);
        }
        let next_len = output.len().checked_add(read).ok_or_else(|| {
            TsinkError::DataCorruption(format!("{context} length overflow while reading"))
        })?;
        if next_len > max_bytes {
            return Err(TsinkError::DataCorruption(format!(
                "{context} exceeds the format safety limit {max_bytes} while reading"
            )));
        }
        if output.capacity().saturating_sub(output.len()) < read {
            let old_allocation = output
                .capacity()
                .checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES)
                .ok_or_else(json_memory_model_overflow)?;
            let new_allocation = next_len
                .checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES)
                .ok_or_else(json_memory_model_overflow)?;
            let required = retained_modeled_bytes
                .checked_add(old_allocation)
                .and_then(|bytes| bytes.checked_add(new_allocation))
                .ok_or_else(json_memory_model_overflow)?;
            crate::disk_budget::admit_startup_memory(memory_limit_bytes, required)?;
            reserve_json_read_growth(&mut output, read, next_len, context)?;
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

#[derive(Default)]
struct ChargeTrace {
    charges: Vec<usize>,
}

impl ChargeTrace {
    fn push<E: de::Error>(
        &mut self,
        charge: usize,
        memory: &mut PreflightMemory,
    ) -> std::result::Result<Option<usize>, E> {
        if self.charges.len() >= memory.trace_item_cap {
            return Ok(None);
        }
        if !memory.reserve_trace::<E>(&mut self.charges, 1)? {
            return Ok(None);
        }
        let index = self.charges.len();
        self.charges.push(charge);
        Ok(Some(index))
    }

    fn insert_repeated<E: de::Error>(
        &mut self,
        index: usize,
        count: usize,
        charge: usize,
        memory: &mut PreflightMemory,
    ) -> std::result::Result<(), E> {
        let retained = count.min(memory.trace_item_cap.saturating_sub(self.charges.len()));
        if retained == 0 || !memory.reserve_trace::<E>(&mut self.charges, retained)? {
            return Ok(());
        }
        let old_len = self.charges.len();
        self.charges.resize(old_len + retained, charge);
        self.charges.copy_within(index..old_len, index + retained);
        self.charges[index..index + retained].fill(charge);
        Ok(())
    }
}

struct PreflightMemory {
    memory_limit_bytes: usize,
    retained_modeled_bytes: usize,
    raw_capacity: usize,
    raw_len: usize,
    trace_item_cap: usize,
    trace_slots: usize,
    trace_allocations: usize,
    deferred_required: Option<usize>,
}

impl PreflightMemory {
    fn new(
        memory_limit_bytes: usize,
        retained_modeled_bytes: usize,
        raw_capacity: usize,
        raw_len: usize,
        limits: RollupJsonLimits,
    ) -> Result<Self> {
        let memory = Self {
            memory_limit_bytes,
            retained_modeled_bytes,
            raw_capacity,
            raw_len,
            trace_item_cap: limits.max_items.saturating_add(1),
            trace_slots: 0,
            trace_allocations: 0,
            deferred_required: None,
        };
        crate::disk_budget::admit_startup_memory(memory_limit_bytes, memory.required(0, 0)?)?;
        Ok(memory)
    }

    fn required(&self, trace_slots: usize, trace_allocations: usize) -> Result<usize> {
        let raw = self
            .raw_capacity
            .checked_add(JSON_ALLOCATION_ALLOWANCE_BYTES)
            .ok_or_else(json_memory_model_overflow)?;
        // serde_json retains at most one escaped-string scratch buffer. A second raw-length term
        // covers the top-level magic string and the largest decoded scalar handed to a visitor.
        let string_scratch = self
            .raw_len
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(2 * JSON_ALLOCATION_ALLOWANCE_BYTES))
            .ok_or_else(json_memory_model_overflow)?;
        let traces = trace_slots
            .checked_mul(std::mem::size_of::<usize>())
            .and_then(|bytes| {
                trace_allocations
                    .checked_mul(JSON_ALLOCATION_ALLOWANCE_BYTES)
                    .and_then(|allowance| bytes.checked_add(allowance))
            })
            .ok_or_else(json_memory_model_overflow)?;
        self.retained_modeled_bytes
            .checked_add(raw)
            .and_then(|bytes| bytes.checked_add(string_scratch))
            .and_then(|bytes| bytes.checked_add(traces))
            .and_then(|bytes| bytes.checked_add(JSON_DECODE_FIXED_BYTES))
            .ok_or_else(json_memory_model_overflow)
    }

    fn reserve_trace<E: de::Error>(
        &mut self,
        trace: &mut Vec<usize>,
        additional: usize,
    ) -> std::result::Result<bool, E> {
        if self.deferred_required.is_some() {
            return Ok(false);
        }
        let needed = trace
            .len()
            .checked_add(additional)
            .ok_or_else(|| E::custom("rollup JSON charge trace length overflow"))?;
        if needed <= trace.capacity() {
            return Ok(true);
        }
        let target = needed
            .max(trace.capacity().saturating_mul(2))
            .max(4)
            .min(self.trace_item_cap);
        let retained_slots = self
            .trace_slots
            .checked_sub(trace.capacity())
            .and_then(|slots| slots.checked_add(target))
            .ok_or_else(|| E::custom("rollup JSON charge trace capacity overflow"))?;
        let retained_allocations = self
            .trace_allocations
            .checked_add(usize::from(trace.capacity() == 0))
            .ok_or_else(|| E::custom("rollup JSON charge trace allocation overflow"))?;
        // A Vec reallocation may retain the predecessor buffer until the successor has been
        // allocated. Admit that transient sum, then retain only the allocator-reported capacity.
        let peak_slots = self
            .trace_slots
            .checked_add(target)
            .ok_or_else(|| E::custom("rollup JSON charge trace reallocation overflow"))?;
        let peak_allocations = self
            .trace_allocations
            .checked_add(1)
            .ok_or_else(|| E::custom("rollup JSON charge trace allocation overflow"))?;
        let required = self
            .required(peak_slots, peak_allocations)
            .map_err(E::custom)?;
        if required > self.memory_limit_bytes {
            self.deferred_required = Some(required);
            return Ok(false);
        }
        let old_capacity = trace.capacity();
        // `required` includes one fixed allocator allowance for this exact reserve. Record the
        // allocator-reported capacity afterward so every later admission uses the actual value.
        trace
            .try_reserve_exact(target.saturating_sub(trace.len()))
            .map_err(E::custom)?;
        self.trace_slots = self
            .trace_slots
            .checked_sub(old_capacity)
            .and_then(|slots| slots.checked_add(trace.capacity()))
            .unwrap_or(retained_slots);
        self.trace_allocations = retained_allocations;
        Ok(true)
    }

    fn finish(self) -> Result<()> {
        if let Some(required) = self.deferred_required {
            return Err(TsinkError::MemoryBudgetExceeded {
                budget: self.memory_limit_bytes,
                required,
            });
        }
        Ok(())
    }
}

fn replay_traces(
    initial: RollupStateEnvelopeUsage,
    traces: &[&ChargeTrace],
    limits: RollupJsonLimits,
    operation: &'static str,
) -> Result<RollupJsonDecodePlan> {
    let mut usage = initial;
    let mut added_modeled_bytes = 0usize;
    for trace in traces {
        for charge in &trace.charges {
            usage.items = usage
                .items
                .checked_add(1)
                .ok_or_else(json_memory_model_overflow)?;
            usage.modeled_bytes = usage
                .modeled_bytes
                .checked_add(*charge)
                .ok_or_else(json_memory_model_overflow)?;
            added_modeled_bytes = added_modeled_bytes
                .checked_add(*charge)
                .ok_or_else(json_memory_model_overflow)?;
            if usage.items > limits.max_items || usage.modeled_bytes > limits.max_modeled_bytes {
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation,
                    item_limit: limits.max_items,
                    byte_limit: u64::try_from(limits.max_modeled_bytes).unwrap_or(u64::MAX),
                    selected_items: usage.items,
                    selected_bytes: u64::try_from(usage.modeled_bytes).unwrap_or(u64::MAX),
                });
            }
        }
    }
    Ok(RollupJsonDecodePlan {
        usage,
        added_modeled_bytes,
    })
}

struct StringLength(usize);

impl<'de> Deserialize<'de> for StringLength {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StringLengthVisitor;

        impl Visitor<'_> for StringLengthVisitor {
            type Value = StringLength;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_borrowed_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(StringLength(value.len()))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(StringLength(value.len()))
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(StringLength(value.len()))
            }
        }

        deserializer.deserialize_string(StringLengthVisitor)
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum PolicyFileField {
    #[serde(rename = "magic")]
    Magic,
    #[serde(rename = "version")]
    Version,
    #[serde(rename = "policies")]
    Policies,
    #[serde(other)]
    Ignore,
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum PolicyField {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "metric")]
    Metric,
    #[serde(rename = "matchLabels")]
    MatchLabels,
    #[serde(rename = "interval")]
    Interval,
    #[serde(rename = "aggregation")]
    Aggregation,
    #[serde(rename = "bucketOrigin")]
    BucketOrigin,
    #[serde(other)]
    Ignore,
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum LabelField {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "value")]
    Value,
    #[serde(other)]
    Ignore,
}

struct PolicyPreflightSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for PolicyPreflightSeed<'_> {
    type Value = (String, u16);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "PersistedRollupPoliciesFile",
            &["magic", "version", "policies"],
            PolicyFileVisitor {
                trace: self.trace,
                memory: self.memory,
            },
        )
    }
}

struct PolicyFileVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> Visitor<'de> for PolicyFileVisitor<'_> {
    type Value = (String, u16);

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a rollup policies object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut magic = None;
        let mut version = None;
        let mut policies = false;
        while let Some(field) = map.next_key::<PolicyFileField>()? {
            match field {
                PolicyFileField::Magic => {
                    if magic.is_some() {
                        return Err(de::Error::duplicate_field("magic"));
                    }
                    magic = Some(map.next_value()?);
                }
                PolicyFileField::Version => {
                    if version.is_some() {
                        return Err(de::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                PolicyFileField::Policies => {
                    if policies {
                        return Err(de::Error::duplicate_field("policies"));
                    }
                    policies = true;
                    map.next_value_seed(PoliciesSeed {
                        trace: self.trace,
                        memory: self.memory,
                    })?;
                }
                PolicyFileField::Ignore => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !policies {
            return Err(de::Error::missing_field("policies"));
        }
        Ok((
            magic.ok_or_else(|| de::Error::missing_field("magic"))?,
            version.ok_or_else(|| de::Error::missing_field("version"))?,
        ))
    }
}

struct PoliciesSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for PoliciesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(PoliciesVisitor {
            trace: self.trace,
            memory: self.memory,
        })
    }
}

struct PoliciesVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> Visitor<'de> for PoliciesVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of rollup policies")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence
            .next_element_seed(PolicySeed {
                trace: self.trace,
                memory: self.memory,
            })?
            .is_some()
        {}
        Ok(())
    }
}

struct PolicySeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for PolicySeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let policy_index = self.trace.push::<D::Error>(0, self.memory)?;
        deserializer.deserialize_struct(
            "RollupPolicy",
            &[
                "id",
                "metric",
                "matchLabels",
                "interval",
                "aggregation",
                "bucketOrigin",
            ],
            PolicyVisitor {
                trace: self.trace,
                memory: self.memory,
                policy_index,
            },
        )
    }
}

struct PolicyVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
    policy_index: Option<usize>,
}

impl<'de> Visitor<'de> for PolicyVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a rollup policy object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut id = None;
        let mut metric = None;
        let mut labels = false;
        let mut interval = false;
        let mut aggregation = false;
        let mut bucket_origin = false;
        while let Some(field) = map.next_key::<PolicyField>()? {
            match field {
                PolicyField::Id => {
                    if id.is_some() {
                        return Err(de::Error::duplicate_field("id"));
                    }
                    id = Some(map.next_value::<StringLength>()?.0);
                }
                PolicyField::Metric => {
                    if metric.is_some() {
                        return Err(de::Error::duplicate_field("metric"));
                    }
                    metric = Some(map.next_value::<StringLength>()?.0);
                }
                PolicyField::MatchLabels => {
                    if labels {
                        return Err(de::Error::duplicate_field("matchLabels"));
                    }
                    labels = true;
                    map.next_value_seed(LabelsSeed {
                        trace: self.trace,
                        memory: self.memory,
                    })?;
                }
                PolicyField::Interval => {
                    if interval {
                        return Err(de::Error::duplicate_field("interval"));
                    }
                    interval = true;
                    map.next_value::<i64>()?;
                }
                PolicyField::Aggregation => {
                    if aggregation {
                        return Err(de::Error::duplicate_field("aggregation"));
                    }
                    aggregation = true;
                    map.next_value::<Aggregation>()?;
                }
                PolicyField::BucketOrigin => {
                    if bucket_origin {
                        return Err(de::Error::duplicate_field("bucketOrigin"));
                    }
                    bucket_origin = true;
                    map.next_value::<i64>()?;
                }
                PolicyField::Ignore => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
        let metric = metric.ok_or_else(|| de::Error::missing_field("metric"))?;
        if !interval {
            return Err(de::Error::missing_field("interval"));
        }
        if !aggregation {
            return Err(de::Error::missing_field("aggregation"));
        }
        if let Some(index) = self.policy_index {
            self.trace.charges[index] = id
                .checked_add(metric)
                .and_then(|bytes| bytes.checked_mul(6))
                .and_then(|bytes| bytes.checked_add(192))
                .ok_or_else(|| de::Error::custom("rollup policy charge overflow"))?;
        }
        Ok(())
    }
}

struct LabelsSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for LabelsSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(LabelsVisitor {
            trace: self.trace,
            memory: self.memory,
        })
    }
}

struct LabelsVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> Visitor<'de> for LabelsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of labels")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some((name, value)) = sequence.next_element_seed(LabelSeed)? {
            let charge = name
                .checked_add(value)
                .and_then(|bytes| bytes.checked_mul(6))
                .and_then(|bytes| bytes.checked_add(64))
                .ok_or_else(|| de::Error::custom("rollup label charge overflow"))?;
            self.trace.push::<A::Error>(charge, self.memory)?;
        }
        Ok(())
    }
}

struct LabelSeed;

impl<'de> DeserializeSeed<'de> for LabelSeed {
    type Value = (usize, usize);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LabelVisitor;

        impl<'de> Visitor<'de> for LabelVisitor {
            type Value = (usize, usize);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a label object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut name = None;
                let mut value = None;
                while let Some(field) = map.next_key::<LabelField>()? {
                    match field {
                        LabelField::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value::<StringLength>()?.0);
                        }
                        LabelField::Value => {
                            if value.is_some() {
                                return Err(de::Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value::<StringLength>()?.0);
                        }
                        LabelField::Ignore => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok((
                    name.ok_or_else(|| de::Error::missing_field("name"))?,
                    value.ok_or_else(|| de::Error::missing_field("value"))?,
                ))
            }
        }

        deserializer.deserialize_struct("Label", &["name", "value"], LabelVisitor)
    }
}

pub(super) fn preflight_rollup_policies(
    bytes: &Vec<u8>,
    path: &Path,
    initial: RollupStateEnvelopeUsage,
    memory_limit_bytes: usize,
    limits: RollupJsonLimits,
) -> Result<(String, u16, RollupJsonDecodePlan)> {
    let mut trace = ChargeTrace::default();
    let mut memory = PreflightMemory::new(
        memory_limit_bytes,
        initial.modeled_bytes,
        bytes.capacity(),
        bytes.len(),
        limits,
    )?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let (magic, version) = PolicyPreflightSeed {
        trace: &mut trace,
        memory: &mut memory,
    }
    .deserialize(&mut deserializer)?;
    deserializer.end()?;
    if magic != ROLLUP_POLICIES_MAGIC || version != ROLLUP_SCHEMA_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported rollup policies file {}",
            path.display()
        )));
    }
    let plan = replay_traces(initial, &[&trace], limits, "rollup state snapshot encoding")?;
    memory.finish()?;
    Ok((magic, version, plan))
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum StateFileField {
    #[serde(rename = "magic")]
    Magic,
    #[serde(rename = "version")]
    Version,
    #[serde(rename = "journal_epoch")]
    JournalEpoch,
    #[serde(rename = "checkpoints")]
    Checkpoints,
    #[serde(rename = "pending_materializations")]
    PendingMaterializations,
    #[serde(rename = "pending_delete_invalidations")]
    PendingDeleteInvalidations,
    #[serde(rename = "generations")]
    Generations,
    #[serde(other)]
    Ignore,
}

#[derive(Default)]
struct StateTraces {
    checkpoints: ChargeTrace,
    generations: ChargeTrace,
    pending_materializations: ChargeTrace,
    pending_delete_invalidations: ChargeTrace,
}

struct StatePreflightSeed<'a> {
    traces: &'a mut StateTraces,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for StatePreflightSeed<'_> {
    type Value = (String, u16, u64);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "PersistedRollupStateFile",
            &[
                "magic",
                "version",
                "journal_epoch",
                "checkpoints",
                "pending_materializations",
                "pending_delete_invalidations",
                "generations",
            ],
            StateFileVisitor {
                traces: self.traces,
                memory: self.memory,
            },
        )
    }
}

struct StateFileVisitor<'a> {
    traces: &'a mut StateTraces,
    memory: &'a mut PreflightMemory,
}

impl<'de> Visitor<'de> for StateFileVisitor<'_> {
    type Value = (String, u16, u64);

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a rollup state object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut magic = None;
        let mut version = None;
        let mut journal_epoch = None;
        let mut checkpoints = false;
        let mut pending_materializations = false;
        let mut pending_delete_invalidations = false;
        let mut generations = false;
        while let Some(field) = map.next_key::<StateFileField>()? {
            match field {
                StateFileField::Magic => {
                    if magic.is_some() {
                        return Err(de::Error::duplicate_field("magic"));
                    }
                    magic = Some(map.next_value()?);
                }
                StateFileField::Version => {
                    if version.is_some() {
                        return Err(de::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                StateFileField::JournalEpoch => {
                    if journal_epoch.is_some() {
                        return Err(de::Error::duplicate_field("journal_epoch"));
                    }
                    journal_epoch = Some(map.next_value()?);
                }
                StateFileField::Checkpoints => {
                    if checkpoints {
                        return Err(de::Error::duplicate_field("checkpoints"));
                    }
                    checkpoints = true;
                    map.next_value_seed(StateRecordsSeed::<CheckpointRecord> {
                        trace: &mut self.traces.checkpoints,
                        memory: self.memory,
                        marker: std::marker::PhantomData,
                    })?;
                }
                StateFileField::PendingMaterializations => {
                    if pending_materializations {
                        return Err(de::Error::duplicate_field("pending_materializations"));
                    }
                    pending_materializations = true;
                    map.next_value_seed(StateRecordsSeed::<PendingMaterializationRecord> {
                        trace: &mut self.traces.pending_materializations,
                        memory: self.memory,
                        marker: std::marker::PhantomData,
                    })?;
                }
                StateFileField::PendingDeleteInvalidations => {
                    if pending_delete_invalidations {
                        return Err(de::Error::duplicate_field("pending_delete_invalidations"));
                    }
                    pending_delete_invalidations = true;
                    map.next_value_seed(InvalidationsSeed {
                        trace: &mut self.traces.pending_delete_invalidations,
                        memory: self.memory,
                    })?;
                }
                StateFileField::Generations => {
                    if generations {
                        return Err(de::Error::duplicate_field("generations"));
                    }
                    generations = true;
                    map.next_value_seed(StateRecordsSeed::<GenerationRecord> {
                        trace: &mut self.traces.generations,
                        memory: self.memory,
                        marker: std::marker::PhantomData,
                    })?;
                }
                StateFileField::Ignore => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !checkpoints {
            return Err(de::Error::missing_field("checkpoints"));
        }
        Ok((
            magic.ok_or_else(|| de::Error::missing_field("magic"))?,
            version.ok_or_else(|| de::Error::missing_field("version"))?,
            journal_epoch.unwrap_or_default(),
        ))
    }
}

trait StateRecord {
    fn deserialize_record<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>;
}

struct StateRecordsSeed<'a, R> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
    marker: std::marker::PhantomData<R>,
}

impl<'de, R: StateRecord> DeserializeSeed<'de> for StateRecordsSeed<'_, R> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(StateRecordsVisitor::<R> {
            trace: self.trace,
            memory: self.memory,
            marker: std::marker::PhantomData,
        })
    }
}

struct StateRecordsVisitor<'a, R> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
    marker: std::marker::PhantomData<R>,
}

impl<'de, R: StateRecord> Visitor<'de> for StateRecordsVisitor<'_, R> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of rollup state records")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(charge) = sequence.next_element_seed(StateRecordSeed::<R> {
            marker: std::marker::PhantomData,
        })? {
            self.trace.push::<A::Error>(charge, self.memory)?;
        }
        Ok(())
    }
}

struct StateRecordSeed<R> {
    marker: std::marker::PhantomData<R>,
}

impl<'de, R: StateRecord> DeserializeSeed<'de> for StateRecordSeed<R> {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        R::deserialize_record(deserializer)
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum CheckpointField {
    #[serde(rename = "policy_id")]
    PolicyId,
    #[serde(rename = "source_key")]
    SourceKey,
    #[serde(rename = "materialized_through")]
    MaterializedThrough,
    #[serde(other)]
    Ignore,
}

struct CheckpointRecord;

impl StateRecord for CheckpointRecord {
    fn deserialize_record<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RecordVisitor;
        impl<'de> Visitor<'de> for RecordVisitor {
            type Value = usize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a rollup checkpoint object")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<usize, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut policy_id = None;
                let mut source_key = None;
                let mut materialized_through = false;
                while let Some(field) = map.next_key::<CheckpointField>()? {
                    match field {
                        CheckpointField::PolicyId => {
                            if policy_id.is_some() {
                                return Err(de::Error::duplicate_field("policy_id"));
                            }
                            policy_id = Some(map.next_value::<StringLength>()?.0);
                        }
                        CheckpointField::SourceKey => {
                            if source_key.is_some() {
                                return Err(de::Error::duplicate_field("source_key"));
                            }
                            source_key = Some(map.next_value::<StringLength>()?.0);
                        }
                        CheckpointField::MaterializedThrough => {
                            if materialized_through {
                                return Err(de::Error::duplicate_field("materialized_through"));
                            }
                            materialized_through = true;
                            map.next_value::<i64>()?;
                        }
                        CheckpointField::Ignore => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                let policy_id = policy_id.ok_or_else(|| de::Error::missing_field("policy_id"))?;
                let source_key =
                    source_key.ok_or_else(|| de::Error::missing_field("source_key"))?;
                if !materialized_through {
                    return Err(de::Error::missing_field("materialized_through"));
                }
                policy_id
                    .checked_add(source_key)
                    .and_then(|bytes| bytes.checked_mul(6))
                    .and_then(|bytes| bytes.checked_add(128))
                    .ok_or_else(|| de::Error::custom("rollup checkpoint charge overflow"))
            }
        }
        deserializer.deserialize_struct(
            "PersistedRollupCheckpoint",
            &["policy_id", "source_key", "materialized_through"],
            RecordVisitor,
        )
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum GenerationField {
    #[serde(rename = "policy_id")]
    PolicyId,
    #[serde(rename = "generation")]
    Generation,
    #[serde(other)]
    Ignore,
}

struct GenerationRecord;

impl StateRecord for GenerationRecord {
    fn deserialize_record<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RecordVisitor;
        impl<'de> Visitor<'de> for RecordVisitor {
            type Value = usize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a rollup generation object")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<usize, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut policy_id = None;
                let mut generation = false;
                while let Some(field) = map.next_key::<GenerationField>()? {
                    match field {
                        GenerationField::PolicyId => {
                            if policy_id.is_some() {
                                return Err(de::Error::duplicate_field("policy_id"));
                            }
                            policy_id = Some(map.next_value::<StringLength>()?.0);
                        }
                        GenerationField::Generation => {
                            if generation {
                                return Err(de::Error::duplicate_field("generation"));
                            }
                            generation = true;
                            map.next_value::<u64>()?;
                        }
                        GenerationField::Ignore => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                let policy_id = policy_id.ok_or_else(|| de::Error::missing_field("policy_id"))?;
                if !generation {
                    return Err(de::Error::missing_field("generation"));
                }
                policy_id
                    .checked_mul(6)
                    .and_then(|bytes| bytes.checked_add(96))
                    .ok_or_else(|| de::Error::custom("rollup generation charge overflow"))
            }
        }
        deserializer.deserialize_struct(
            "PersistedRollupGeneration",
            &["policy_id", "generation"],
            RecordVisitor,
        )
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum PendingMaterializationField {
    #[serde(rename = "policy_id")]
    PolicyId,
    #[serde(rename = "source_key")]
    SourceKey,
    #[serde(rename = "checkpoint")]
    Checkpoint,
    #[serde(rename = "materialized_through")]
    MaterializedThrough,
    #[serde(rename = "generation")]
    Generation,
    #[serde(other)]
    Ignore,
}

struct PendingMaterializationRecord;

impl StateRecord for PendingMaterializationRecord {
    fn deserialize_record<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RecordVisitor;
        impl<'de> Visitor<'de> for RecordVisitor {
            type Value = usize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a pending rollup materialization object")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<usize, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut policy_id = None;
                let mut source_key = None;
                let mut checkpoint = false;
                let mut materialized_through = false;
                let mut generation = false;
                while let Some(field) = map.next_key::<PendingMaterializationField>()? {
                    match field {
                        PendingMaterializationField::PolicyId => {
                            if policy_id.is_some() {
                                return Err(de::Error::duplicate_field("policy_id"));
                            }
                            policy_id = Some(map.next_value::<StringLength>()?.0);
                        }
                        PendingMaterializationField::SourceKey => {
                            if source_key.is_some() {
                                return Err(de::Error::duplicate_field("source_key"));
                            }
                            source_key = Some(map.next_value::<StringLength>()?.0);
                        }
                        PendingMaterializationField::Checkpoint => {
                            if checkpoint {
                                return Err(de::Error::duplicate_field("checkpoint"));
                            }
                            checkpoint = true;
                            map.next_value::<i64>()?;
                        }
                        PendingMaterializationField::MaterializedThrough => {
                            if materialized_through {
                                return Err(de::Error::duplicate_field("materialized_through"));
                            }
                            materialized_through = true;
                            map.next_value::<i64>()?;
                        }
                        PendingMaterializationField::Generation => {
                            if generation {
                                return Err(de::Error::duplicate_field("generation"));
                            }
                            generation = true;
                            map.next_value::<u64>()?;
                        }
                        PendingMaterializationField::Ignore => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                let policy_id = policy_id.ok_or_else(|| de::Error::missing_field("policy_id"))?;
                let source_key =
                    source_key.ok_or_else(|| de::Error::missing_field("source_key"))?;
                for (present, field) in [
                    (checkpoint, "checkpoint"),
                    (materialized_through, "materialized_through"),
                    (generation, "generation"),
                ] {
                    if !present {
                        return Err(de::Error::missing_field(field));
                    }
                }
                policy_id
                    .checked_add(source_key)
                    .and_then(|bytes| bytes.checked_mul(6))
                    .and_then(|bytes| bytes.checked_add(192))
                    .ok_or_else(|| {
                        de::Error::custom("pending rollup materialization charge overflow")
                    })
            }
        }
        deserializer.deserialize_struct(
            "PersistedPendingRollupMaterialization",
            &[
                "policy_id",
                "source_key",
                "checkpoint",
                "materialized_through",
                "generation",
            ],
            RecordVisitor,
        )
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum InvalidationField {
    #[serde(rename = "tombstone")]
    Tombstone,
    #[serde(rename = "series_ids")]
    SeriesIds,
    #[serde(rename = "affected_policy_ids")]
    AffectedPolicyIds,
    #[serde(other)]
    Ignore,
}

struct InvalidationsSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for InvalidationsSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(InvalidationsVisitor {
            trace: self.trace,
            memory: self.memory,
        })
    }
}

struct InvalidationsVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> Visitor<'de> for InvalidationsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of pending delete invalidations")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence
            .next_element_seed(InvalidationSeed {
                trace: self.trace,
                memory: self.memory,
            })?
            .is_some()
        {}
        Ok(())
    }
}

struct InvalidationSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for InvalidationSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let start = self.trace.charges.len();
        self.trace.push::<D::Error>(192, self.memory)?;
        deserializer.deserialize_struct(
            "PersistedPendingRollupDeleteInvalidation",
            &["tombstone", "series_ids", "affected_policy_ids"],
            InvalidationVisitor {
                trace: self.trace,
                memory: self.memory,
                start,
            },
        )
    }
}

struct InvalidationVisitor<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
    start: usize,
}

impl<'de> Visitor<'de> for InvalidationVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a pending delete invalidation object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut tombstone = false;
        let mut series_ids = false;
        let mut affected_policy_ids = false;
        while let Some(field) = map.next_key::<InvalidationField>()? {
            match field {
                InvalidationField::Tombstone => {
                    if tombstone {
                        return Err(de::Error::duplicate_field("tombstone"));
                    }
                    tombstone = true;
                    map.next_value::<TombstoneRange>()?;
                }
                InvalidationField::SeriesIds => {
                    if series_ids {
                        return Err(de::Error::duplicate_field("series_ids"));
                    }
                    series_ids = true;
                    let count = map.next_value_seed(SeriesIdsSeed)?;
                    self.trace.insert_repeated::<A::Error>(
                        self.start.saturating_add(1),
                        count,
                        32,
                        self.memory,
                    )?;
                }
                InvalidationField::AffectedPolicyIds => {
                    if affected_policy_ids {
                        return Err(de::Error::duplicate_field("affected_policy_ids"));
                    }
                    affected_policy_ids = true;
                    map.next_value_seed(AffectedPolicyIdsSeed {
                        trace: self.trace,
                        memory: self.memory,
                    })?;
                }
                InvalidationField::Ignore => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        for (present, field) in [
            (tombstone, "tombstone"),
            (series_ids, "series_ids"),
            (affected_policy_ids, "affected_policy_ids"),
        ] {
            if !present {
                return Err(de::Error::missing_field(field));
            }
        }
        Ok(())
    }
}

struct SeriesIdsSeed;

impl<'de> DeserializeSeed<'de> for SeriesIdsSeed {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SeriesIdsVisitor;
        impl<'de> Visitor<'de> for SeriesIdsVisitor {
            type Value = usize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an array of series ids")
            }
            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<usize, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut count = 0usize;
                while sequence.next_element::<SeriesId>()?.is_some() {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("series id count overflow"))?;
                }
                Ok(count)
            }
        }
        deserializer.deserialize_seq(SeriesIdsVisitor)
    }
}

struct AffectedPolicyIdsSeed<'a> {
    trace: &'a mut ChargeTrace,
    memory: &'a mut PreflightMemory,
}

impl<'de> DeserializeSeed<'de> for AffectedPolicyIdsSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AffectedPolicyIdsVisitor<'a> {
            trace: &'a mut ChargeTrace,
            memory: &'a mut PreflightMemory,
        }
        impl<'de> Visitor<'de> for AffectedPolicyIdsVisitor<'_> {
            type Value = ();
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an array of policy ids")
            }
            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
            where
                A: SeqAccess<'de>,
            {
                while let Some(StringLength(length)) = sequence.next_element()? {
                    let charge = length
                        .checked_mul(6)
                        .and_then(|bytes| bytes.checked_add(32))
                        .ok_or_else(|| de::Error::custom("affected policy id charge overflow"))?;
                    self.trace.push::<A::Error>(charge, self.memory)?;
                }
                Ok(())
            }
        }
        deserializer.deserialize_seq(AffectedPolicyIdsVisitor {
            trace: self.trace,
            memory: self.memory,
        })
    }
}

pub(super) fn preflight_rollup_state(
    bytes: &Vec<u8>,
    path: &Path,
    initial: RollupStateEnvelopeUsage,
    memory_limit_bytes: usize,
    limits: RollupJsonLimits,
) -> Result<(String, u16, u64, RollupJsonDecodePlan)> {
    let mut traces = StateTraces::default();
    let mut memory = PreflightMemory::new(
        memory_limit_bytes,
        initial.modeled_bytes,
        bytes.capacity(),
        bytes.len(),
        limits,
    )?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let (magic, version, journal_epoch) = StatePreflightSeed {
        traces: &mut traces,
        memory: &mut memory,
    }
    .deserialize(&mut deserializer)?;
    deserializer.end()?;
    if magic != ROLLUP_STATE_MAGIC || version != ROLLUP_SCHEMA_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported rollup state file {}",
            path.display()
        )));
    }
    let plan = replay_traces(
        initial,
        &[
            &traces.checkpoints,
            &traces.generations,
            &traces.pending_materializations,
            &traces.pending_delete_invalidations,
        ],
        limits,
        "rollup state snapshot encoding",
    )?;
    memory.finish()?;
    Ok((magic, version, journal_epoch, plan))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn reserve_for_read_is_relative_to_length_when_spare_is_insufficient() {
        let mut bytes = Vec::<u8>::new();
        bytes.try_reserve_exact(5).unwrap();
        bytes.extend_from_slice(&[1, 2, 3]);
        assert_eq!(bytes.len(), 3);
        assert!(bytes.capacity().saturating_sub(bytes.len()) < 4);

        let next_len = bytes.len().checked_add(4).unwrap();
        reserve_json_read_growth(&mut bytes, 4, next_len, "test JSON").unwrap();

        assert!(bytes.capacity().saturating_sub(bytes.len()) >= 4);
        bytes.extend_from_slice(&[4, 5, 6, 7]);
        assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn raw_read_growth_admits_predecessor_and_successor_at_exact_boundary() {
        let retained = 512usize;
        let payload = vec![7u8; 10];
        let exact = retained
            + (5 + JSON_ALLOCATION_ALLOWANCE_BYTES)
            + (10 + JSON_ALLOCATION_ALLOWANCE_BYTES);
        let decoded = read_json_to_end_budgeted(
            &mut std::io::Cursor::new(&payload),
            payload.len(),
            5,
            "test JSON",
            exact,
            retained,
        )
        .unwrap();
        assert_eq!(decoded, payload);

        let error = read_json_to_end_budgeted(
            &mut std::io::Cursor::new(&payload),
            payload.len(),
            5,
            "test JSON",
            exact - 1,
            retained,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == exact - 1 && required == exact
        ));
    }

    fn policy_json(magic: &str, labels: &str) -> Vec<u8> {
        format!(
            r#"{{"magic":"{magic}","version":1,"policies":[{{"id":"p","metric":"cpu","matchLabels":[{labels}],"interval":1,"aggregation":"Avg"}}]}}"#
        )
        .into_bytes()
    }

    #[test]
    fn policy_preflight_counts_policy_and_labels_at_exact_tiny_boundary() {
        let bytes = policy_json(ROLLUP_POLICIES_MAGIC, r#"{"name":"host","value":"a"}"#);
        let path = Path::new("policies.json");
        let initial = RollupStateEnvelopeUsage::empty();
        let exact = preflight_rollup_policies(
            &bytes,
            path,
            initial,
            usize::MAX,
            RollupJsonLimits {
                max_items: 2,
                max_modeled_bytes: usize::MAX,
            },
        )
        .unwrap()
        .2;
        assert_eq!(exact.usage.items, 2);

        let error = preflight_rollup_policies(
            &bytes,
            path,
            initial,
            usize::MAX,
            RollupJsonLimits {
                max_items: 1,
                max_modeled_bytes: usize::MAX,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                selected_items: 2,
                ..
            }
        ));
    }

    #[test]
    fn escaped_policy_strings_are_charged_by_decoded_length() {
        let bytes = br#"{"magic":"tsink-rollup-policies","version":1,"policies":[{"id":"\u0070","metric":"cpu","interval":1,"aggregation":"Avg"}]}"#.to_vec();
        let initial = RollupStateEnvelopeUsage::empty();
        let plan = preflight_rollup_policies(
            &bytes,
            Path::new("policies.json"),
            initial,
            usize::MAX,
            RollupJsonLimits::default(),
        )
        .unwrap()
        .2;
        assert_eq!(plan.usage.modeled_bytes, 512 + 192 + 6 * (1 + 3));
    }

    #[test]
    fn policy_preflight_memory_has_exact_n_and_n_minus_one_boundary() {
        let bytes = policy_json(ROLLUP_POLICIES_MAGIC, "");
        let initial = RollupStateEnvelopeUsage::empty();
        let exact = initial.modeled_bytes
            + bytes.capacity()
            + JSON_ALLOCATION_ALLOWANCE_BYTES
            + 2 * bytes.len()
            + 2 * JSON_ALLOCATION_ALLOWANCE_BYTES
            + JSON_DECODE_FIXED_BYTES
            + 4 * std::mem::size_of::<usize>()
            + JSON_ALLOCATION_ALLOWANCE_BYTES;
        preflight_rollup_policies(
            &bytes,
            Path::new("policies.json"),
            initial,
            exact,
            RollupJsonLimits::default(),
        )
        .unwrap();
        let error = preflight_rollup_policies(
            &bytes,
            Path::new("policies.json"),
            initial,
            exact - 1,
            RollupJsonLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == exact - 1 && required == exact
        ));
    }

    #[test]
    fn state_preflight_retains_the_policy_envelope_baseline() {
        let policy_bytes = policy_json(ROLLUP_POLICIES_MAGIC, "");
        let policy_plan = preflight_rollup_policies(
            &policy_bytes,
            Path::new("policies.json"),
            RollupStateEnvelopeUsage::empty(),
            usize::MAX,
            RollupJsonLimits::default(),
        )
        .unwrap()
        .2;
        let state_bytes = br#"{"magic":"tsink-rollup-state","version":1,"checkpoints":[{"policy_id":"p","source_key":"s","materialized_through":1}]}"#.to_vec();
        let state_plan = preflight_rollup_state(
            &state_bytes,
            Path::new("state.json"),
            policy_plan.usage,
            usize::MAX,
            RollupJsonLimits::default(),
        )
        .unwrap()
        .3;
        assert_eq!(state_plan.usage.items, 2);
        assert_eq!(
            state_plan.usage.modeled_bytes,
            policy_plan.usage.modeled_bytes + 128 + 6 * 2
        );
    }

    #[test]
    fn missing_policies_schema_error_precedes_deferred_tight_memory() {
        let bytes = br#"{"magic":"tsink-rollup-policies","version":1}"#.to_vec();
        let initial = RollupStateEnvelopeUsage::empty();
        let initial_preflight_peak = initial.modeled_bytes
            + bytes.capacity()
            + JSON_ALLOCATION_ALLOWANCE_BYTES
            + 2 * bytes.len()
            + 2 * JSON_ALLOCATION_ALLOWANCE_BYTES
            + JSON_DECODE_FIXED_BYTES;
        let error = preflight_rollup_policies(
            &bytes,
            Path::new("policies.json"),
            initial,
            initial_preflight_peak,
            RollupJsonLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(error, TsinkError::Json(_)));
        assert!(error.to_string().contains("missing field `policies`"));
    }

    #[test]
    fn bad_magic_precedes_a_physical_item_overflow() {
        let bytes = policy_json("wrong-magic", "");
        let error = preflight_rollup_policies(
            &bytes,
            Path::new("policies.json"),
            RollupStateEnvelopeUsage::empty(),
            usize::MAX,
            RollupJsonLimits {
                max_items: 0,
                max_modeled_bytes: 0,
            },
        )
        .unwrap_err();
        assert!(matches!(error, TsinkError::DataCorruption(_)));
    }

    #[test]
    fn duplicate_state_records_are_bounded_before_logical_overwrite() {
        let bytes = br#"{"magic":"tsink-rollup-state","version":1,"checkpoints":[{"policy_id":"p","source_key":"s","materialized_through":1},{"policy_id":"p","source_key":"s","materialized_through":2}]}"#.to_vec();
        let error = preflight_rollup_state(
            &bytes,
            Path::new("state.json"),
            RollupStateEnvelopeUsage::empty(),
            usize::MAX,
            RollupJsonLimits {
                max_items: 1,
                max_modeled_bytes: usize::MAX,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                selected_items: 2,
                ..
            }
        ));
    }

    #[test]
    fn state_category_replay_order_is_independent_of_json_field_order() {
        let bytes = br#"{"magic":"tsink-rollup-state","version":1,"generations":[{"policy_id":"generation","generation":0}],"checkpoints":[{"policy_id":"p","source_key":"s","materialized_through":1}]}"#.to_vec();
        let initial = RollupStateEnvelopeUsage::empty();
        let checkpoint_charge = 128 + 6 * 2;
        let error = preflight_rollup_state(
            &bytes,
            Path::new("state.json"),
            initial,
            usize::MAX,
            RollupJsonLimits {
                max_items: usize::MAX,
                max_modeled_bytes: initial.modeled_bytes + checkpoint_charge,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                selected_items: 2,
                selected_bytes,
                ..
            } if selected_bytes == (initial.modeled_bytes + checkpoint_charge + 96 + 6 * "generation".len()) as u64
        ));
    }

    #[test]
    fn preflight_rejection_does_not_start_typed_materialization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("policies.json");
        fs::write(&path, br#"{"magic":"tsink-rollup-policies","version":1}"#).unwrap();
        reset_typed_json_materializations();
        let error = super::super::policy::load_rollup_policies_budgeted(Some(&path), usize::MAX)
            .unwrap_err();
        assert!(matches!(error, TsinkError::Json(_)));
        assert_eq!(typed_json_materializations(), 0);
    }
}
