use roaring::RoaringTreemap;

use crate::query_matcher::CompiledSeriesMatcher;
use crate::SeriesSelection;

#[path = "candidate_planner/bitmap_ops.rs"]
mod bitmap_ops;
#[path = "candidate_planner/matcher_eval.rs"]
mod matcher_eval;
#[path = "candidate_planner/seed_plan.rs"]
mod seed_plan;

use self::bitmap_ops::intersect_candidates;
use super::metadata_postings::MetadataPostingsProvider;
#[cfg(test)]
use super::SeriesId;
use super::SeriesRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatcherSeedMode {
    Exact,
    Coarse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateSeed {
    Scope,
    Metric,
    Matcher { idx: usize, mode: MatcherSeedMode },
    AllSeries,
}

impl CandidateSeed {
    fn seeded_exact_matcher_idx(self) -> Option<usize> {
        match self {
            Self::Matcher {
                idx,
                mode: MatcherSeedMode::Exact,
            } => Some(idx),
            Self::Scope
            | Self::Metric
            | Self::AllSeries
            | Self::Matcher {
                mode: MatcherSeedMode::Coarse,
                ..
            } => None,
        }
    }
}

pub(super) struct CandidatePlanningResult {
    pub(super) candidate_series_ids: RoaringTreemap,
    #[cfg(test)]
    pub(super) used_all_series_seed: bool,
}

pub(super) struct MetadataCandidatePlanner<'a, P: MetadataPostingsProvider + ?Sized> {
    postings: &'a P,
    registry: &'a SeriesRegistry,
    on_direct_scan: Option<&'a dyn Fn()>,
}

impl<'a, P: MetadataPostingsProvider + ?Sized> MetadataCandidatePlanner<'a, P> {
    pub(super) fn new(
        postings: &'a P,
        registry: &'a SeriesRegistry,
        on_direct_scan: Option<&'a dyn Fn()>,
    ) -> Self {
        Self {
            postings,
            registry,
            on_direct_scan,
        }
    }

    pub(super) fn plan_series_candidates(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
        scope_filter: Option<&RoaringTreemap>,
    ) -> CandidatePlanningResult {
        let ordered_matchers = self.ordered_compiled_matchers(compiled_matchers);
        let (mut candidates, candidate_seed) =
            self.initial_series_candidates(selection, compiled_matchers, scope_filter);
        #[cfg(test)]
        let used_all_series_seed = matches!(candidate_seed, CandidateSeed::AllSeries);
        let seeded_matcher_idx = candidate_seed.seeded_exact_matcher_idx();
        self.apply_selection_filters(&mut candidates, selection, scope_filter, candidate_seed);

        for (idx, matcher) in ordered_matchers {
            if seeded_matcher_idx == Some(idx) {
                continue;
            }
            self.apply_postings_matcher_to_candidates(&mut candidates, matcher);
            if candidates.is_empty() {
                break;
            }
        }

        CandidatePlanningResult {
            candidate_series_ids: candidates,
            #[cfg(test)]
            used_all_series_seed,
        }
    }

    fn apply_selection_filters(
        &self,
        candidates: &mut RoaringTreemap,
        selection: &SeriesSelection,
        scope_filter: Option<&RoaringTreemap>,
        candidate_seed: CandidateSeed,
    ) {
        if let Some(metric) = selection.metric.as_ref() {
            if !matches!(candidate_seed, CandidateSeed::Metric) {
                let metric_bitmap = self.postings.series_id_postings_for_metric(metric);
                intersect_candidates(candidates, metric_bitmap.as_ref());
            }
        }

        if !matches!(candidate_seed, CandidateSeed::Scope) {
            if let Some(scope_filter) = scope_filter {
                intersect_candidates(candidates, Some(scope_filter));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use roaring::RoaringTreemap;

    use super::*;
    use crate::query_matcher::compile_series_matchers;
    use crate::{Label, SeriesMatcher};

    struct FakeMetadataPostingsProvider {
        all_series: RoaringTreemap,
        metric_postings: HashMap<String, RoaringTreemap>,
        label_name_postings: HashMap<String, RoaringTreemap>,
        missing_label_postings: HashMap<String, RoaringTreemap>,
        label_postings: HashMap<(String, String), RoaringTreemap>,
    }

    impl FakeMetadataPostingsProvider {
        fn new(all_series: RoaringTreemap) -> Self {
            Self {
                all_series,
                metric_postings: HashMap::new(),
                label_name_postings: HashMap::new(),
                missing_label_postings: HashMap::new(),
                label_postings: HashMap::new(),
            }
        }

        fn with_metric(mut self, metric: &str, series_ids: &[SeriesId]) -> Self {
            self.metric_postings
                .insert(metric.to_string(), bitmap(series_ids));
            self
        }

        fn with_label_name(mut self, label_name: &str, series_ids: &[SeriesId]) -> Self {
            self.label_name_postings
                .insert(label_name.to_string(), bitmap(series_ids));
            self
        }

        fn with_missing_label(mut self, label_name: &str, series_ids: &[SeriesId]) -> Self {
            self.missing_label_postings
                .insert(label_name.to_string(), bitmap(series_ids));
            self
        }

        fn with_label(
            mut self,
            label_name: &str,
            label_value: &str,
            series_ids: &[SeriesId],
        ) -> Self {
            self.label_postings.insert(
                (label_name.to_string(), label_value.to_string()),
                bitmap(series_ids),
            );
            self
        }
    }

    impl MetadataPostingsProvider for FakeMetadataPostingsProvider {
        fn all_series_postings(&self) -> RoaringTreemap {
            self.all_series.clone()
        }

        fn series_count(&self) -> usize {
            usize::try_from(self.all_series.len()).unwrap_or(usize::MAX)
        }

        fn metric_postings_count(&self) -> usize {
            self.metric_postings.len()
        }

        fn for_each_metric_postings(&self, mut visit: impl FnMut(&str, &RoaringTreemap)) {
            for (metric, series_ids) in &self.metric_postings {
                visit(metric, series_ids);
            }
        }

        fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
            self.metric_postings.get(metric).cloned()
        }

        fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
            self.label_name_postings.get(label_name).cloned()
        }

        fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap> {
            self.missing_label_postings.get(label_name).cloned()
        }

        fn postings_for_label(
            &self,
            label_name: &str,
            label_value: &str,
        ) -> Option<RoaringTreemap> {
            self.label_postings
                .get(&(label_name.to_string(), label_value.to_string()))
                .cloned()
        }

        fn for_each_postings_for_label_name(
            &self,
            label_name: &str,
            mut visit: impl FnMut(&str, &RoaringTreemap),
        ) {
            for ((candidate_name, label_value), series_ids) in &self.label_postings {
                if candidate_name == label_name {
                    visit(label_value, series_ids);
                }
            }
        }
    }

    fn bitmap(series_ids: &[SeriesId]) -> RoaringTreemap {
        series_ids.iter().copied().collect()
    }

    fn registry_with_series() -> (SeriesRegistry, SeriesId, SeriesId, SeriesId) {
        let registry = SeriesRegistry::new();
        let api = registry
            .resolve_or_insert("cpu", &[Label::new("job", "api"), Label::new("host", "a")])
            .unwrap()
            .series_id;
        let batch = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("job", "batch"), Label::new("host", "b")],
            )
            .unwrap()
            .series_id;
        let other = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("job", "job-10"), Label::new("host", "c")],
            )
            .unwrap()
            .series_id;
        (registry, api, batch, other)
    }

    #[test]
    fn planner_prefers_exact_matcher_seed_over_scope_seed() {
        let (registry, api, batch, other) = registry_with_series();
        let postings = FakeMetadataPostingsProvider::new(bitmap(&[api, batch, other]))
            .with_metric("cpu", &[api, batch, other])
            .with_label_name("job", &[api, batch, other])
            .with_label("job", "api", &[api])
            .with_label("job", "batch", &[batch])
            .with_label("job", "job-10", &[other]);
        let planner = MetadataCandidatePlanner::new(&postings, &registry, None);
        let selection = SeriesSelection::new().with_matcher(SeriesMatcher::equal("job", "api"));
        let compiled = compile_series_matchers(&selection.matchers).unwrap();
        let scope_filter = bitmap(&[api, batch, other]);

        let result = planner.plan_series_candidates(&selection, &compiled, Some(&scope_filter));

        assert!(!result.used_all_series_seed);
        assert_eq!(
            result.candidate_series_ids.iter().collect::<Vec<_>>(),
            vec![api],
        );
    }

    #[test]
    fn planner_direct_scans_small_candidate_sets_for_unbounded_regexes() {
        let registry = SeriesRegistry::new();
        let api = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("job", "job-08"), Label::new("host", "a")],
            )
            .unwrap()
            .series_id;
        let batch = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("job", "job-09"), Label::new("host", "b")],
            )
            .unwrap()
            .series_id;
        let other = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("job", "job-10"), Label::new("host", "c")],
            )
            .unwrap()
            .series_id;
        let postings = FakeMetadataPostingsProvider::new(bitmap(&[api, batch, other]))
            .with_metric("cpu", &[api, batch, other])
            .with_label_name("job", &[api, batch, other])
            .with_label("job", "job-08", &[api])
            .with_label("job", "job-09", &[batch])
            .with_label("job", "job-10", &[other])
            .with_missing_label("zone", &[]);
        let direct_scans = Cell::new(0usize);
        let on_direct_scan = || direct_scans.set(direct_scans.get().saturating_add(1));
        let planner = MetadataCandidatePlanner::new(&postings, &registry, Some(&on_direct_scan));
        let selection =
            SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("job", "job-08[a-z]*"));
        let compiled = compile_series_matchers(&selection.matchers).unwrap();
        let scope_filter = bitmap(&[api, batch]);

        let result = planner.plan_series_candidates(&selection, &compiled, Some(&scope_filter));

        assert_eq!(direct_scans.get(), 1);
        assert_eq!(
            result.candidate_series_ids.iter().collect::<Vec<_>>(),
            vec![api],
        );
    }
}
