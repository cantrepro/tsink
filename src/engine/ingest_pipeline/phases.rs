use std::collections::BTreeMap;

use super::super::super::{
    SeriesDefinitionFrame, SeriesId, SeriesResolution, SeriesValueFamily, TsinkError, Value,
    ValueLane, WalHighWatermark,
};
use crate::engine::wal::LogicalWalWrite;
use crate::WriteAcknowledgement;

#[derive(Debug)]
pub(super) struct PendingPoint {
    pub(super) series_id: SeriesId,
    pub(super) lane: ValueLane,
    pub(super) ts: i64,
    pub(super) value: Value,
    pub(super) wal_highwater: WalHighWatermark,
}

#[derive(Debug)]
pub(super) struct PreparedWalWrite {
    pub(super) encoded_series_definition_payloads: Vec<Vec<u8>>,
    pub(super) encoded_samples_payload: Option<Vec<u8>>,
    pub(super) encoded_bytes: u64,
    pub(super) sample_batch_count: usize,
    pub(super) sample_point_count: usize,
}

#[derive(Debug)]
pub(super) struct ResolvedWrite {
    pub(super) pending_points: Vec<PendingPoint>,
    pub(super) new_series_defs: Vec<SeriesDefinitionFrame>,
    pub(super) created_series: Vec<SeriesResolution>,
}

#[derive(Debug)]
pub(super) struct PreparedWrite {
    pub(super) resolved: ResolvedWrite,
    pub(super) prepared_wal: Option<PreparedWalWrite>,
    pub(super) pending_series_families: BTreeMap<SeriesId, SeriesValueFamily>,
}

pub(super) struct StagedWalWrite<'a> {
    pub(super) wal_write: LogicalWalWrite<'a>,
    pub(super) committed_highwater: WalHighWatermark,
}

pub(super) struct StagedWrite<'a> {
    pub(super) prepared: PreparedWrite,
    pub(super) staged_wal: Option<StagedWalWrite<'a>>,
    pub(super) reserved_series: Vec<SeriesId>,
    pub(super) assigned_series_families: Vec<SeriesId>,
}

pub(super) struct AppliedWrite<'a> {
    pub(super) prepared_wal: Option<PreparedWalWrite>,
    pub(super) staged_wal: Option<StagedWalWrite<'a>>,
}

pub(super) struct CommittedWrite {
    pub(super) acknowledgement: WriteAcknowledgement,
}

pub(super) type PrepareResolvedWriteError = Box<(ResolvedWrite, TsinkError)>;
pub(super) type StagePreparedWriteError = Box<(PreparedWrite, TsinkError)>;
pub(super) type ApplyStagedWriteError<'a> = Box<(StagedWrite<'a>, TsinkError)>;
