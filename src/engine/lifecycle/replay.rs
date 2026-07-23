use super::*;

#[derive(Default)]
struct ReplayRunStats {
    frames: u64,
    series_definitions: u64,
    sample_batches: u64,
    points: u64,
}

#[derive(Clone, Copy)]
struct LifecycleReplayContext<'a> {
    wal: Option<&'a crate::engine::wal::FramedWal>,
    observability: &'a StorageObservabilityCounters,
    publication: LifecyclePublicationContext<'a>,
}

impl<'a> LifecycleReplayContext<'a> {
    fn wal(self) -> Option<&'a crate::engine::wal::FramedWal> {
        self.wal
    }

    fn record_started(self) {
        self.observability
            .wal
            .replay_runs_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn register_series_definitions(
        self,
        definitions: &[crate::engine::wal::SeriesDefinitionFrame],
    ) -> Result<()> {
        let replayed_series_ids =
            self.publication
                .mutate_registry(|registry| -> Result<Vec<SeriesId>> {
                    let mut replayed_series_ids = Vec::with_capacity(definitions.len());
                    for definition in definitions {
                        let resolution = registry.register_series_with_id(
                            definition.series_id,
                            &definition.metric,
                            &definition.labels,
                        )?;
                        if resolution.created {
                            replayed_series_ids.push(definition.series_id);
                        }
                    }
                    Ok(replayed_series_ids)
                })?;
        self.publication
            .registry_bookkeeping
            .mark_series_pending(replayed_series_ids);
        Ok(())
    }

    fn record_replayed_series_definition(
        self,
        definition: crate::engine::wal::SeriesDefinitionFrame,
    ) {
        if let Some(wal) = self.wal {
            wal.record_replayed_series_definition_if_initialized(definition);
            // Replay processes one admitted frame at a time. Publish its retained cache charge
            // while that frame's transient lease is still alive so the next frame sees it.
            self.publication
                .sync_wal_series_definition_cache_memory_usage(Some(wal));
        }
    }

    fn record_replayed_samples(self, sample_batches: &[crate::engine::wal::SamplesBatchFrame]) {
        if let Some(wal) = self.wal {
            wal.record_replayed_samples_if_initialized(
                sample_batches.iter().map(|batch| batch.series_id),
            );
            self.publication
                .sync_wal_series_definition_cache_memory_usage(Some(wal));
        }
    }

    fn finish(
        self,
        started: Instant,
        stats: ReplayRunStats,
        replay_result: Result<()>,
    ) -> Result<()> {
        self.observability
            .wal
            .replay_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match replay_result {
            Ok(()) => {
                self.observability
                    .wal
                    .replay_frames_total
                    .fetch_add(stats.frames, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_series_definitions_total
                    .fetch_add(stats.series_definitions, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_sample_batches_total
                    .fetch_add(stats.sample_batches, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_points_total
                    .fetch_add(stats.points, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .wal
                    .replay_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }
}

impl ChunkStorage {
    fn lifecycle_replay_context(&self) -> LifecycleReplayContext<'_> {
        LifecycleReplayContext {
            wal: self.persisted.wal.as_ref(),
            observability: self.observability.as_ref(),
            publication: self.lifecycle_publication_context(),
        }
    }

    pub(in super::super) fn replay_from_wal(
        &self,
        replay_highwater: WalHighWatermark,
        replay_mode: crate::wal::WalReplayMode,
    ) -> Result<()> {
        let replay = self.lifecycle_replay_context();
        let Some(wal) = replay.wal() else {
            return Ok(());
        };

        wal.set_configured_replay_mode(replay_mode);
        replay.record_started();
        let started = Instant::now();
        let mut replay_stats = ReplayRunStats::default();

        let replay_result = (|| -> Result<()> {
            let mut stream =
                wal.replay_committed_frame_stream_after_with_mode(replay_highwater, replay_mode)?;
            while let Some(admitted) = stream
                .next_frame_with_admission(|bytes| self.reserve_write_transient_memory(bytes))?
            {
                replay_stats.frames = replay_stats.frames.saturating_add(1);
                let highwater = admitted.highwater;
                let transient_memory = admitted.reservation;
                match admitted.frame {
                    crate::engine::wal::ReplayFrame::SeriesDefinition(definition) => {
                        replay_stats.series_definitions =
                            replay_stats.series_definitions.saturating_add(1);
                        replay.register_series_definitions(std::slice::from_ref(&definition))?;
                        replay.record_replayed_series_definition(definition);
                    }
                    crate::engine::wal::ReplayFrame::Samples(sample_batches) => {
                        replay_stats.sample_batches = replay_stats
                            .sample_batches
                            .saturating_add(saturating_u64_from_usize(sample_batches.len()));
                        replay.record_replayed_samples(&sample_batches);
                        replay_stats.points =
                            replay_stats
                                .points
                                .saturating_add(self.replay_wal_sample_batches(
                                    sample_batches,
                                    highwater,
                                    &transient_memory,
                                )?);
                    }
                }
            }

            Ok(())
        })();

        replay.finish(started, replay_stats, replay_result)
    }
}
