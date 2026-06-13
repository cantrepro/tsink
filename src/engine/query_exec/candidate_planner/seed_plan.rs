use roaring::RoaringTreemap;

use crate::query_matcher::CompiledSeriesMatcher;
use crate::storage::SeriesMatcherOp;
use crate::SeriesSelection;

use super::bitmap_ops::bitmap_cardinality;
use super::{CandidateSeed, MatcherSeedMode, MetadataCandidatePlanner};

struct MatcherOrderingEntry<'a> {
    idx: usize,
    matcher: &'a CompiledSeriesMatcher,
    uses_fallback_cost: bool,
    priority: usize,
}

struct SeedChoice {
    seed: CandidateSeed,
    bitmap: RoaringTreemap,
}

impl SeedChoice {
    fn new(seed: CandidateSeed, bitmap: RoaringTreemap) -> Self {
        Self { seed, bitmap }
    }

    fn cardinality(&self) -> usize {
        bitmap_cardinality(&self.bitmap)
    }
}

impl<'a, P: super::MetadataPostingsProvider + ?Sized> MetadataCandidatePlanner<'a, P> {
    fn matcher_has_cheap_seed(matcher: &CompiledSeriesMatcher) -> bool {
        if matcher.name == "__name__" {
            return matcher.op == SeriesMatcherOp::Equal
                || (matcher.op == SeriesMatcherOp::RegexMatch
                    && matcher.finite_literal_values.is_some());
        }

        match matcher.op {
            SeriesMatcherOp::Equal => true,
            SeriesMatcherOp::NotEqual => matcher.value.is_empty(),
            SeriesMatcherOp::RegexMatch => {
                matcher.value == ".*"
                    || matcher.value == ".+"
                    || matcher.finite_literal_values.is_some()
            }
            SeriesMatcherOp::RegexNoMatch => matcher.value == ".*" || matcher.value == ".+",
        }
    }

    fn coarse_seed_bitmap_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> Option<RoaringTreemap> {
        if matcher.name == "__name__" || matcher.matches_empty {
            return None;
        }

        Some(
            self.postings
                .series_id_postings_for_label_name(&matcher.name)
                .unwrap_or_default(),
        )
    }

    fn coarse_seed_estimate_for_matcher(&self, matcher: &CompiledSeriesMatcher) -> Option<usize> {
        let coarse = self.coarse_seed_bitmap_for_matcher(matcher)?;
        Some(bitmap_cardinality(&coarse))
    }

    fn cheap_seed_estimate_for_matcher(&self, matcher: &CompiledSeriesMatcher) -> Option<usize> {
        if matcher.name == "__name__" {
            return match matcher.op {
                SeriesMatcherOp::Equal => Some(
                    self.postings
                        .series_id_postings_for_metric(&matcher.value)
                        .map(|bitmap| bitmap_cardinality(&bitmap))
                        .unwrap_or(0),
                ),
                SeriesMatcherOp::RegexMatch => {
                    matcher.finite_literal_values.as_ref().map(|values| {
                        values.iter().fold(0usize, |total, value| {
                            total.saturating_add(
                                self.postings
                                    .series_id_postings_for_metric(value)
                                    .map(|bitmap| bitmap_cardinality(&bitmap))
                                    .unwrap_or(0),
                            )
                        })
                    })
                }
                SeriesMatcherOp::NotEqual | SeriesMatcherOp::RegexNoMatch => None,
            };
        }

        match matcher.op {
            SeriesMatcherOp::Equal if !matcher.value.is_empty() => Some(
                self.postings
                    .postings_for_label(&matcher.name, &matcher.value)
                    .map(|bitmap| bitmap_cardinality(&bitmap))
                    .unwrap_or(0),
            ),
            SeriesMatcherOp::Equal => Some(
                self.missing_label_partition_for_matcher(matcher)
                    .map(|bitmap| bitmap_cardinality(&bitmap))
                    .unwrap_or(0),
            ),
            SeriesMatcherOp::NotEqual if matcher.value.is_empty() => Some(
                self.postings
                    .series_id_postings_for_label_name(&matcher.name)
                    .map(|bitmap| bitmap_cardinality(&bitmap))
                    .unwrap_or(0),
            ),
            SeriesMatcherOp::RegexMatch if matcher.value == ".*" => {
                Some(self.postings.series_count())
            }
            SeriesMatcherOp::RegexMatch if matcher.value == ".+" => Some(
                self.postings
                    .series_id_postings_for_label_name(&matcher.name)
                    .map(|bitmap| bitmap_cardinality(&bitmap))
                    .unwrap_or(0),
            ),
            SeriesMatcherOp::RegexMatch => matcher.finite_literal_values.as_ref().map(|values| {
                let mut total = 0usize;
                for value in values {
                    if value.is_empty() {
                        continue;
                    }
                    total = total.saturating_add(
                        self.postings
                            .postings_for_label(&matcher.name, value)
                            .map(|bitmap| bitmap_cardinality(&bitmap))
                            .unwrap_or(0),
                    );
                }
                if matcher.matches_empty {
                    total = total.saturating_add(
                        self.missing_label_partition_for_matcher(matcher)
                            .map(|bitmap| bitmap_cardinality(&bitmap))
                            .unwrap_or(0),
                    );
                }
                total
            }),
            SeriesMatcherOp::RegexNoMatch if matcher.value == ".*" => Some(0),
            SeriesMatcherOp::RegexNoMatch if matcher.value == ".+" => Some(
                self.missing_label_partition_for_matcher(matcher)
                    .map(|bitmap| bitmap_cardinality(&bitmap))
                    .unwrap_or(0),
            ),
            SeriesMatcherOp::NotEqual | SeriesMatcherOp::RegexNoMatch => None,
        }
    }

    pub(super) fn matcher_global_scan_cost(&self, matcher: &CompiledSeriesMatcher) -> usize {
        if matcher.name == "__name__" {
            return self.postings.metric_postings_count();
        }

        self.postings
            .series_id_postings_for_label_name(&matcher.name)
            .map(|bitmap| bitmap_cardinality(&bitmap))
            .unwrap_or_else(|| {
                if matcher.matches_empty {
                    self.postings.series_count()
                } else {
                    0
                }
            })
    }

    pub(super) fn indexed_execution_cost_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> Option<usize> {
        if matcher.name == "__name__" {
            return match matcher.op {
                SeriesMatcherOp::Equal => Some(1),
                SeriesMatcherOp::RegexMatch => matcher
                    .finite_literal_values
                    .as_ref()
                    .map(|values| values.iter().filter(|value| !value.is_empty()).count()),
                SeriesMatcherOp::NotEqual | SeriesMatcherOp::RegexNoMatch => None,
            };
        }

        match matcher.op {
            SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual if matcher.value.is_empty() => {
                Some(1)
            }
            SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => Some(1),
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
                if matcher.value == ".*" =>
            {
                Some(0)
            }
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
                if matcher.value == ".+" =>
            {
                Some(1)
            }
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                matcher.finite_literal_values.as_ref().map(|values| {
                    let literal_cost = values.iter().filter(|value| !value.is_empty()).count();
                    literal_cost + usize::from(matcher.matches_empty)
                })
            }
        }
    }

    pub(super) fn ordered_compiled_matchers<'b>(
        &self,
        compiled_matchers: &'b [CompiledSeriesMatcher],
    ) -> Vec<(usize, &'b CompiledSeriesMatcher)> {
        let mut ordered = compiled_matchers
            .iter()
            .enumerate()
            .map(|(idx, matcher)| {
                let seed_estimate = self
                    .cheap_seed_estimate_for_matcher(matcher)
                    .or_else(|| self.coarse_seed_estimate_for_matcher(matcher));

                MatcherOrderingEntry {
                    idx,
                    matcher,
                    uses_fallback_cost: seed_estimate.is_none(),
                    priority: seed_estimate
                        .unwrap_or_else(|| self.matcher_global_scan_cost(matcher)),
                }
            })
            .collect::<Vec<_>>();

        ordered.sort_by_key(|entry| (entry.uses_fallback_cost, entry.priority, entry.idx));
        ordered
            .into_iter()
            .map(|entry| (entry.idx, entry.matcher))
            .collect()
    }

    pub(super) fn initial_series_candidates(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
    ) -> (RoaringTreemap, CandidateSeed) {
        let mut seeded_matcher = None::<SeedChoice>;
        let mut use_metric_seed = false;
        let mut use_scope_seed = scope_filter.is_some();
        let mut best_count = scope_filter.map(bitmap_cardinality).unwrap_or(usize::MAX);

        if let Some(metric) = selection.metric.as_ref() {
            let count = self
                .postings
                .series_id_postings_for_metric(metric)
                .map(|bitmap| bitmap_cardinality(&bitmap))
                .unwrap_or(0);
            if count < best_count || scope_filter.is_none() {
                best_count = count;
                use_metric_seed = true;
                use_scope_seed = false;
                seeded_matcher = None;
            }
        }

        for (idx, matcher) in compiled_matchers.iter().enumerate() {
            let Some(exact_estimate) = self.cheap_seed_estimate_for_matcher(matcher) else {
                continue;
            };
            let have_seed = use_scope_seed || use_metric_seed || seeded_matcher.is_some();
            if exact_estimate < best_count || !have_seed {
                let exact = SeedChoice::new(
                    CandidateSeed::Matcher {
                        idx,
                        mode: MatcherSeedMode::Exact,
                    },
                    self.exact_result_bitmap_for_matcher(matcher),
                );
                best_count = exact.cardinality();
                use_metric_seed = false;
                use_scope_seed = false;
                seeded_matcher = Some(exact);
                if best_count == 0 {
                    break;
                }
            }
        }

        for (idx, matcher) in compiled_matchers.iter().enumerate() {
            if Self::matcher_has_cheap_seed(matcher) {
                continue;
            }

            let Some(coarse_estimate) = self.coarse_seed_estimate_for_matcher(matcher) else {
                continue;
            };

            let have_seed = use_scope_seed || use_metric_seed || seeded_matcher.is_some();
            if have_seed && coarse_estimate >= best_count {
                continue;
            }

            let coarse = SeedChoice::new(
                CandidateSeed::Matcher {
                    idx,
                    mode: MatcherSeedMode::Coarse,
                },
                self.coarse_seed_bitmap_for_matcher(matcher)
                    .unwrap_or_default(),
            );
            best_count = coarse.cardinality();
            use_metric_seed = false;
            use_scope_seed = false;
            seeded_matcher = Some(coarse);
            if best_count == 0 {
                break;
            }
        }

        if use_scope_seed {
            return (
                scope_filter.cloned().unwrap_or_default(),
                CandidateSeed::Scope,
            );
        }
        if use_metric_seed {
            return (
                selection
                    .metric
                    .as_ref()
                    .and_then(|metric| self.postings.series_id_postings_for_metric(metric))
                    .unwrap_or_default(),
                CandidateSeed::Metric,
            );
        }
        if let Some(seed_choice) = seeded_matcher {
            return (seed_choice.bitmap, seed_choice.seed);
        }

        if let Some((idx, matcher)) = self.best_indexed_scan_seed_matcher(compiled_matchers) {
            return (
                self.exact_result_bitmap_for_matcher(matcher),
                CandidateSeed::Matcher {
                    idx,
                    mode: MatcherSeedMode::Exact,
                },
            );
        }

        (
            self.postings.all_series_postings(),
            CandidateSeed::AllSeries,
        )
    }

    fn best_indexed_scan_seed_matcher<'b>(
        &self,
        compiled_matchers: &'b [CompiledSeriesMatcher],
    ) -> Option<(usize, &'b CompiledSeriesMatcher)> {
        compiled_matchers
            .iter()
            .enumerate()
            .min_by_key(|(idx, matcher)| {
                (
                    self.indexed_execution_cost_for_matcher(matcher)
                        .unwrap_or_else(|| self.matcher_global_scan_cost(matcher)),
                    *idx,
                )
            })
    }
}
