use roaring::RoaringTreemap;

use crate::query_matcher::CompiledSeriesMatcher;
use crate::storage::SeriesMatcherOp;

use super::bitmap_ops::{bitmap_cardinality, intersect_candidates, subtract_candidates};
use super::MetadataCandidatePlanner;

impl<'a, P: super::MetadataPostingsProvider + ?Sized> MetadataCandidatePlanner<'a, P> {
    fn invoke_direct_scan_hook(&self) {
        if let Some(hook) = self.on_direct_scan {
            hook();
        }
    }

    pub(super) fn missing_label_partition_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> Option<RoaringTreemap> {
        if matcher.name == "__name__" || !matcher.matches_empty {
            return None;
        }

        self.postings
            .missing_label_postings_for_name(&matcher.name)
            .or_else(|| {
                let mut missing = self.postings.all_series_postings();
                if let Some(present) = self
                    .postings
                    .series_id_postings_for_label_name(&matcher.name)
                {
                    missing -= &present;
                }
                Some(missing)
            })
    }

    fn exact_lookup_bitmap_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> Option<RoaringTreemap> {
        if matcher.name == "__name__" {
            return match matcher.op {
                SeriesMatcherOp::Equal => Some(
                    self.postings
                        .series_id_postings_for_metric(&matcher.value)
                        .unwrap_or_default(),
                ),
                SeriesMatcherOp::RegexNoMatch if matcher.value == ".*" || matcher.value == ".+" => {
                    Some(RoaringTreemap::new())
                }
                SeriesMatcherOp::RegexMatch => {
                    let values = matcher.finite_literal_values.as_ref()?;
                    let mut matched = RoaringTreemap::new();
                    for value in values {
                        if value.is_empty() {
                            continue;
                        }
                        if let Some(bitmap) = self.postings.series_id_postings_for_metric(value) {
                            matched |= bitmap;
                        }
                    }
                    Some(matched)
                }
                SeriesMatcherOp::NotEqual | SeriesMatcherOp::RegexNoMatch => None,
            };
        }

        match matcher.op {
            SeriesMatcherOp::Equal if !matcher.value.is_empty() => Some(
                self.postings
                    .postings_for_label(&matcher.name, &matcher.value)
                    .unwrap_or_default(),
            ),
            SeriesMatcherOp::Equal => Some(
                self.missing_label_partition_for_matcher(matcher)
                    .unwrap_or_default(),
            ),
            SeriesMatcherOp::NotEqual if matcher.value.is_empty() => Some(
                self.postings
                    .series_id_postings_for_label_name(&matcher.name)
                    .unwrap_or_default(),
            ),
            SeriesMatcherOp::RegexMatch if matcher.value == ".+" => Some(
                self.postings
                    .series_id_postings_for_label_name(&matcher.name)
                    .unwrap_or_default(),
            ),
            SeriesMatcherOp::RegexMatch => {
                let values = matcher.finite_literal_values.as_ref()?;
                let mut matched = RoaringTreemap::new();
                for value in values {
                    if value.is_empty() {
                        continue;
                    }
                    if let Some(bitmap) = self.postings.postings_for_label(&matcher.name, value) {
                        matched |= bitmap;
                    }
                }
                if let Some(missing) = self.missing_label_partition_for_matcher(matcher) {
                    matched |= missing;
                }
                Some(matched)
            }
            SeriesMatcherOp::RegexNoMatch if matcher.value == ".*" => Some(RoaringTreemap::new()),
            SeriesMatcherOp::RegexNoMatch if matcher.value == ".+" => Some(
                self.missing_label_partition_for_matcher(matcher)
                    .unwrap_or_default(),
            ),
            SeriesMatcherOp::NotEqual | SeriesMatcherOp::RegexNoMatch => None,
        }
    }

    pub(super) fn exact_result_bitmap_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> RoaringTreemap {
        if let Some(exact) = self.exact_lookup_bitmap_for_matcher(matcher) {
            return exact;
        }

        if matcher.name == "__name__" {
            return self
                .postings
                .metric_postings_where(|metric| matcher.matches_value(metric));
        }

        let mut matched = if matcher.matches_empty {
            if let Some(rejection_bitmap) = self.rejection_bitmap_for_matcher(matcher) {
                let mut matched = self
                    .postings
                    .series_id_postings_for_label_name(&matcher.name)
                    .unwrap_or_default();
                matched -= &rejection_bitmap;
                matched
            } else {
                self.postings
                    .label_postings_where_for_name(&matcher.name, |value| {
                        matcher.matches_value(value)
                    })
            }
        } else {
            self.postings
                .label_postings_where_for_name(&matcher.name, |value| matcher.matches_value(value))
        };

        if matcher.matches_empty {
            if let Some(missing) = self.missing_label_partition_for_matcher(matcher) {
                matched |= missing;
            }
        }

        matched
    }

    fn matcher_requires_label_value_scan(matcher: &CompiledSeriesMatcher) -> bool {
        matcher.name != "__name__"
            && matches!(
                matcher.op,
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
            )
            && matcher.value != ".*"
            && matcher.value != ".+"
            && matcher.finite_literal_values.is_none()
    }

    fn rejection_bitmap_for_matcher(
        &self,
        matcher: &CompiledSeriesMatcher,
    ) -> Option<RoaringTreemap> {
        if matcher.name == "__name__" || !matcher.matches_empty {
            return None;
        }

        match matcher.op {
            SeriesMatcherOp::Equal => self
                .postings
                .series_id_postings_for_label_name(&matcher.name),
            SeriesMatcherOp::NotEqual if !matcher.value.is_empty() => self
                .postings
                .postings_for_label(&matcher.name, &matcher.value),
            SeriesMatcherOp::RegexMatch if matcher.value == ".*" => Some(RoaringTreemap::new()),
            SeriesMatcherOp::RegexNoMatch => {
                if let Some(values) = matcher.finite_literal_values.as_ref() {
                    let mut rejected = RoaringTreemap::new();
                    for value in values {
                        if value.is_empty() {
                            continue;
                        }
                        if let Some(bitmap) = self.postings.postings_for_label(&matcher.name, value)
                        {
                            rejected |= bitmap;
                        }
                    }
                    return Some(rejected);
                }

                Some(
                    self.postings
                        .label_postings_where_for_name(&matcher.name, |value| {
                            !matcher.matches_value(value)
                        }),
                )
            }
            SeriesMatcherOp::RegexMatch => Some(
                self.postings
                    .label_postings_where_for_name(&matcher.name, |value| {
                        !matcher.matches_value(value)
                    }),
            ),
            SeriesMatcherOp::NotEqual => None,
        }
    }

    pub(super) fn apply_postings_matcher_to_candidates(
        &self,
        candidates: &mut RoaringTreemap,
        matcher: &CompiledSeriesMatcher,
    ) {
        if candidates.is_empty() {
            return;
        }

        if matcher.op == SeriesMatcherOp::RegexMatch && matcher.value == ".*" {
            return;
        }

        if matcher.name == "__name__" {
            if matcher.op == SeriesMatcherOp::Equal {
                let metric_bitmap = self.postings.series_id_postings_for_metric(&matcher.value);
                intersect_candidates(candidates, metric_bitmap.as_ref());
                return;
            }

            if let Some(exact) = self.exact_lookup_bitmap_for_matcher(matcher) {
                intersect_candidates(candidates, Some(&exact));
                return;
            }

            let scan_cost = self
                .indexed_execution_cost_for_matcher(matcher)
                .unwrap_or_else(|| self.matcher_global_scan_cost(matcher));
            if scan_cost == 0 || bitmap_cardinality(candidates) < scan_cost {
                self.direct_scan_candidates(candidates, matcher);
                return;
            }

            let exact = self.exact_result_bitmap_for_matcher(matcher);
            intersect_candidates(candidates, Some(&exact));
            return;
        }

        if matcher.op == SeriesMatcherOp::Equal && !matcher.value.is_empty() {
            let label_bitmap = self
                .postings
                .postings_for_label(&matcher.name, &matcher.value);
            intersect_candidates(candidates, label_bitmap.as_ref());
            return;
        }

        if matcher.op == SeriesMatcherOp::NotEqual && !matcher.value.is_empty() {
            let label_bitmap = self
                .postings
                .postings_for_label(&matcher.name, &matcher.value);
            subtract_candidates(candidates, label_bitmap.as_ref());
            return;
        }

        if let Some(exact) = self.exact_lookup_bitmap_for_matcher(matcher) {
            intersect_candidates(candidates, Some(&exact));
            return;
        }

        if Self::matcher_requires_label_value_scan(matcher) {
            if !self.prefilter_candidates_by_label_presence(candidates, matcher) {
                return;
            }
            self.direct_scan_candidates(candidates, matcher);
            return;
        }

        let scan_cost = self
            .indexed_execution_cost_for_matcher(matcher)
            .unwrap_or_else(|| self.matcher_global_scan_cost(matcher));
        if scan_cost == 0 || bitmap_cardinality(candidates) < scan_cost {
            if !self.prefilter_candidates_by_label_presence(candidates, matcher) {
                return;
            }
            self.direct_scan_candidates(candidates, matcher);
            return;
        }

        if let Some(rejection_bitmap) = self.rejection_bitmap_for_matcher(matcher) {
            subtract_candidates(candidates, Some(&rejection_bitmap));
            return;
        }

        if !self.prefilter_candidates_by_label_presence(candidates, matcher) {
            return;
        }

        self.direct_scan_candidates(candidates, matcher);
    }

    fn direct_scan_candidates(
        &self,
        candidates: &mut RoaringTreemap,
        matcher: &CompiledSeriesMatcher,
    ) {
        self.invoke_direct_scan_hook();
        self.filter_candidates_directly(candidates, matcher);
    }

    fn prefilter_candidates_by_label_presence(
        &self,
        candidates: &mut RoaringTreemap,
        matcher: &CompiledSeriesMatcher,
    ) -> bool {
        if matcher.matches_empty {
            return !candidates.is_empty();
        }

        let label_name_bitmap = self
            .postings
            .series_id_postings_for_label_name(&matcher.name);
        intersect_candidates(candidates, label_name_bitmap.as_ref());
        !candidates.is_empty()
    }

    fn filter_candidates_directly(
        &self,
        candidates: &mut RoaringTreemap,
        matcher: &CompiledSeriesMatcher,
    ) {
        if matcher.name == "__name__" {
            *candidates = candidates
                .iter()
                .filter(|series_id| {
                    self.registry
                        .series_metric(*series_id)
                        .is_some_and(|metric| matcher.matches_value(&metric))
                })
                .collect();
            return;
        }

        let label_name_id = self.registry.label_name_id(&matcher.name);
        *candidates = candidates
            .iter()
            .filter(|series_id| {
                let actual = label_name_id
                    .and_then(|name_id| {
                        self.registry
                            .series_label_value_by_name_id(*series_id, name_id)
                    })
                    .unwrap_or_default();
                matcher.matches_value(&actual)
            })
            .collect();
    }
}
