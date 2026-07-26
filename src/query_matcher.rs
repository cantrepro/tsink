use std::fmt;

use regex::{Regex, RegexBuilder};
use regex_syntax::hir::{Hir, HirKind};
use regex_syntax::ParserBuilder as RegexParserBuilder;

use crate::storage::{SeriesMatcher, SeriesMatcherOp};
use crate::{Label, QueryBudgetError, QueryExecution, QueryMemoryReservation, Result, TsinkError};

/// Maximum number of matchers accepted by one structured series selection.
pub const MAX_SERIES_SELECTION_MATCHERS: usize = 128;

/// Maximum UTF-8 byte length of one structured matcher label name.
pub const MAX_SERIES_MATCHER_NAME_BYTES: usize = crate::label::MAX_LABEL_NAME_LEN;

/// Maximum UTF-8 byte length of one structured matcher value or regex pattern.
pub const MAX_SERIES_MATCHER_VALUE_BYTES: usize = crate::label::MAX_LABEL_VALUE_LEN;

/// Maximum cumulative name-and-value bytes in one structured series selection.
pub const MAX_SERIES_SELECTION_MATCHER_BYTES: usize =
    crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES;

/// Approximate compiled-program limit passed to every query-controlled regex builder.
pub const QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES: usize = 256 * 1024;

/// Lazy-DFA cache limit passed to every query-controlled regex builder.
pub const QUERY_REGEX_DFA_SIZE_LIMIT_BYTES: usize = 64 * 1024;

/// Regex parser nesting limit used for structured matchers and core PromQL regexes.
pub const QUERY_REGEX_NEST_LIMIT: u32 = 64;

/// Maximum dynamic diagnostic bytes reserved for a query-controlled regex error.
///
/// Current diagnostics deliberately do not echo pattern text and remain below this ceiling.
pub const MAX_QUERY_REGEX_DIAGNOSTIC_BYTES: usize = 256;

const MATCHER_LITERAL_SET_LIMIT: usize = 64;
const MATCHER_LITERAL_SET_TOTAL_BYTES_LIMIT: usize = 64 * 1024;
const MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
// The regex crate's configured compiled-program limit is not a precise peak-allocation meter.
// Reserve a separate conservative compiler/parser scratch envelope in addition to every retained
// compiled program and lazy-DFA cache.
const QUERY_REGEX_COMPILER_SCRATCH_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegexAnchoring {
    Unanchored,
    Anchored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MatcherShapeError {
    TooManyMatchers { actual: usize },
    EmptyName { index: usize },
    NameTooLong { index: usize, actual: usize },
    ValueTooLong { index: usize, actual: usize },
    TotalTooLong { actual: usize },
}

impl fmt::Display for MatcherShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyMatchers { actual } => write!(
                formatter,
                "series selection has {actual} matchers, exceeding the hard limit {MAX_SERIES_SELECTION_MATCHERS}"
            ),
            Self::EmptyName { index } => write!(
                formatter,
                "series matcher at index {index} has an empty label name"
            ),
            Self::NameTooLong { index, actual } => write!(
                formatter,
                "series matcher at index {index} has a {actual}-byte label name, exceeding the hard limit {MAX_SERIES_MATCHER_NAME_BYTES}"
            ),
            Self::ValueTooLong { index, actual } => write!(
                formatter,
                "series matcher at index {index} has a {actual}-byte value, exceeding the hard limit {MAX_SERIES_MATCHER_VALUE_BYTES}"
            ),
            Self::TotalTooLong { actual } => write!(
                formatter,
                "series selection matcher names and values total {actual} bytes, exceeding the hard limit {MAX_SERIES_SELECTION_MATCHER_BYTES}"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedRegexError {
    PatternTooLong { actual: usize },
    InvalidSyntax,
    CompiledSizeLimit,
}

impl fmt::Display for BoundedRegexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PatternTooLong { actual } => write!(
                formatter,
                "regex pattern is {actual} bytes, exceeding the hard limit {MAX_SERIES_MATCHER_VALUE_BYTES}"
            ),
            Self::InvalidSyntax => formatter.write_str("regex syntax is invalid"),
            Self::CompiledSizeLimit => write!(
                formatter,
                "compiled regex exceeds the {QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES}-byte complexity limit"
            ),
        }
    }
}

#[derive(Debug)]
pub(crate) enum BoundedRegexPreparationError {
    Regex(BoundedRegexError),
    Query(QueryBudgetError),
}

pub(crate) struct ExecutionBoundedRegex {
    regex: Regex,
    _reservation: QueryMemoryReservation,
}

impl ExecutionBoundedRegex {
    pub(crate) fn regex(&self) -> &Regex {
        &self.regex
    }
}

pub(crate) fn modeled_execution_regex_slots_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        0
    } else {
        u64::try_from(capacity)
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(std::mem::size_of::<ExecutionBoundedRegex>()).unwrap_or(u64::MAX),
            )
            .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledSeriesMatcher {
    pub(crate) name: String,
    pub(crate) op: SeriesMatcherOp,
    pub(crate) value: String,
    pub(crate) regex: Option<Regex>,
    pub(crate) matches_empty: bool,
    pub(crate) finite_literal_values: Option<Vec<String>>,
}

impl CompiledSeriesMatcher {
    pub(crate) fn matches_value(&self, actual: &str) -> bool {
        match self.op {
            SeriesMatcherOp::Equal => actual == self.value,
            SeriesMatcherOp::NotEqual => actual != self.value,
            SeriesMatcherOp::RegexMatch => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(actual)),
            SeriesMatcherOp::RegexNoMatch => !self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(actual)),
        }
    }

    pub(crate) fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        let actual = if self.name == "__name__" {
            Some(metric)
        } else {
            labels
                .iter()
                .find(|label| label.name == self.name)
                .map(|label| label.value.as_str())
        };

        self.matches_value(actual.unwrap_or(""))
    }
}

pub(crate) fn validate_matcher_shapes<'a>(
    matcher_count: usize,
    matchers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> std::result::Result<(), MatcherShapeError> {
    if matcher_count > MAX_SERIES_SELECTION_MATCHERS {
        return Err(MatcherShapeError::TooManyMatchers {
            actual: matcher_count,
        });
    }

    let mut total_bytes = 0usize;
    for (index, (name, value)) in matchers.into_iter().enumerate() {
        if name.is_empty() {
            return Err(MatcherShapeError::EmptyName { index });
        }
        if name.len() > MAX_SERIES_MATCHER_NAME_BYTES {
            return Err(MatcherShapeError::NameTooLong {
                index,
                actual: name.len(),
            });
        }
        if value.len() > MAX_SERIES_MATCHER_VALUE_BYTES {
            return Err(MatcherShapeError::ValueTooLong {
                index,
                actual: value.len(),
            });
        }
        total_bytes = total_bytes
            .saturating_add(name.len())
            .saturating_add(value.len());
        if total_bytes > MAX_SERIES_SELECTION_MATCHER_BYTES {
            return Err(MatcherShapeError::TotalTooLong {
                actual: total_bytes,
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_series_matcher_shapes(matchers: &[SeriesMatcher]) -> Result<()> {
    validate_matcher_shapes(
        matchers.len(),
        matchers
            .iter()
            .map(|matcher| (matcher.name.as_str(), matcher.value.as_str())),
    )
    .map_err(|error| TsinkError::InvalidConfiguration(error.to_string()))
}

pub(crate) fn build_bounded_regex(
    pattern: &str,
    anchoring: RegexAnchoring,
) -> std::result::Result<Regex, BoundedRegexError> {
    if pattern.len() > MAX_SERIES_MATCHER_VALUE_BYTES {
        return Err(BoundedRegexError::PatternTooLong {
            actual: pattern.len(),
        });
    }

    let mut anchored_pattern = String::new();
    let build_pattern = match anchoring {
        RegexAnchoring::Unanchored => pattern,
        RegexAnchoring::Anchored => {
            anchored_pattern.reserve(pattern.len().saturating_add(6));
            anchored_pattern.push_str("^(?:");
            anchored_pattern.push_str(pattern);
            anchored_pattern.push_str(")$");
            anchored_pattern.as_str()
        }
    };

    let mut builder = RegexBuilder::new(build_pattern);
    builder
        .size_limit(QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES)
        .dfa_size_limit(QUERY_REGEX_DFA_SIZE_LIMIT_BYTES)
        .nest_limit(QUERY_REGEX_NEST_LIMIT);
    builder.build().map_err(|error| match error {
        regex::Error::CompiledTooBig(_) => BoundedRegexError::CompiledSizeLimit,
        regex::Error::Syntax(_) => BoundedRegexError::InvalidSyntax,
        _ => BoundedRegexError::InvalidSyntax,
    })
}

fn modeled_bounded_regex_retained_bytes() -> u64 {
    u64::try_from(
        QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES.saturating_add(QUERY_REGEX_DFA_SIZE_LIMIT_BYTES),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES)
}

/// Models the reusable capture-location workspace used by capture-aware regex operations.
///
/// `Regex::captures_len` includes the whole-match slot. Four machine words per slot
/// conservatively cover the start/end pair plus iterator/replacement bookkeeping. Callers only
/// need one such workspace because regex replacement reuses it across non-overlapping matches.
pub(crate) fn modeled_bounded_regex_capture_scratch_bytes(regex: &Regex) -> u64 {
    let slots = u64::try_from(regex.captures_len()).unwrap_or(u64::MAX);
    let word_bytes = u64::try_from(std::mem::size_of::<usize>()).unwrap_or(u64::MAX);
    slots
        .saturating_mul(4)
        .saturating_mul(word_bytes)
        .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_bounded_regex_preparation_bytes(pattern: &str, anchoring: RegexAnchoring) -> u64 {
    let anchored_scratch = match anchoring {
        RegexAnchoring::Unanchored => 0,
        RegexAnchoring::Anchored => {
            modeled_string_allocation_bytes(pattern.len().saturating_add(6))
        }
    };
    modeled_bounded_regex_retained_bytes()
        .saturating_add(QUERY_REGEX_COMPILER_SCRATCH_BYTES)
        .saturating_add(anchored_scratch)
}

pub(crate) fn prepare_bounded_regex_with_execution(
    pattern: &str,
    anchoring: RegexAnchoring,
    execution: &QueryExecution,
) -> std::result::Result<ExecutionBoundedRegex, BoundedRegexPreparationError> {
    if pattern.len() > MAX_SERIES_MATCHER_VALUE_BYTES {
        return Err(BoundedRegexPreparationError::Regex(
            BoundedRegexError::PatternTooLong {
                actual: pattern.len(),
            },
        ));
    }

    execution
        .checkpoint()
        .map_err(BoundedRegexPreparationError::Query)?;
    let mut reservation = execution
        .reserve_memory(modeled_bounded_regex_preparation_bytes(pattern, anchoring))
        .map_err(BoundedRegexPreparationError::Query)?;
    execution
        .checkpoint()
        .map_err(BoundedRegexPreparationError::Query)?;
    let regex =
        build_bounded_regex(pattern, anchoring).map_err(BoundedRegexPreparationError::Regex)?;
    // Compiler and anchored-pattern scratch are dead after build. Keep the bounded program and
    // its possible lazy-DFA cache charged for as long as the regex can be used.
    reservation
        .resize(modeled_bounded_regex_retained_bytes())
        .map_err(BoundedRegexPreparationError::Query)?;
    Ok(ExecutionBoundedRegex {
        regex,
        _reservation: reservation,
    })
}

fn literal_cross_product(
    prefixes: Vec<String>,
    suffixes: &[String],
    count_limit: usize,
    byte_limit: usize,
) -> Option<Vec<String>> {
    if prefixes.is_empty() || suffixes.is_empty() {
        return Some(Vec::new());
    }
    if prefixes.len().saturating_mul(suffixes.len()) > count_limit {
        return None;
    }

    let mut output_bytes = 0usize;
    for prefix in &prefixes {
        for suffix in suffixes {
            output_bytes = output_bytes.checked_add(prefix.len().checked_add(suffix.len())?)?;
            if output_bytes > byte_limit {
                return None;
            }
        }
    }

    let mut combined = Vec::with_capacity(prefixes.len().saturating_mul(suffixes.len()));
    for prefix in prefixes {
        for suffix in suffixes {
            let mut value = prefix.clone();
            value.push_str(suffix);
            combined.push(value);
        }
    }
    Some(combined)
}

fn finite_literal_values_from_hir(
    hir: &Hir,
    count_limit: usize,
    byte_limit: usize,
) -> Option<Vec<String>> {
    match hir.kind() {
        HirKind::Empty => Some(vec![String::new()]),
        HirKind::Literal(literal) => {
            if literal.0.len() > byte_limit {
                return None;
            }
            Some(vec![String::from_utf8(literal.0.to_vec()).ok()?])
        }
        HirKind::Class(_) | HirKind::Look(_) => None,
        HirKind::Capture(capture) => {
            finite_literal_values_from_hir(&capture.sub, count_limit, byte_limit)
        }
        HirKind::Concat(parts) => {
            let mut combined = vec![String::new()];
            for part in parts {
                let values = finite_literal_values_from_hir(part, count_limit, byte_limit)?;
                combined = literal_cross_product(combined, &values, count_limit, byte_limit)?;
            }
            Some(combined)
        }
        HirKind::Alternation(parts) => {
            let mut combined = Vec::new();
            let mut combined_bytes = 0usize;
            for part in parts {
                let values = finite_literal_values_from_hir(part, count_limit, byte_limit)?;
                if combined.len().saturating_add(values.len()) > count_limit {
                    return None;
                }
                combined_bytes = combined_bytes.checked_add(
                    values
                        .iter()
                        .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?,
                )?;
                if combined_bytes > byte_limit {
                    return None;
                }
                combined.extend(values);
            }
            combined.sort();
            combined.dedup();
            (combined.len() <= count_limit).then_some(combined)
        }
        HirKind::Repetition(repetition) => {
            let max = repetition.max?;
            if max > 8 {
                return None;
            }

            let repeated_values =
                finite_literal_values_from_hir(&repetition.sub, count_limit, byte_limit)?;
            let mut combined = Vec::new();
            let mut combined_bytes = 0usize;
            for repeat_count in repetition.min..=max {
                let mut repeated = vec![String::new()];
                for _ in 0..repeat_count {
                    repeated =
                        literal_cross_product(repeated, &repeated_values, count_limit, byte_limit)?;
                }
                if combined.len().saturating_add(repeated.len()) > count_limit {
                    return None;
                }
                combined_bytes = combined_bytes.checked_add(
                    repeated
                        .iter()
                        .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?,
                )?;
                if combined_bytes > byte_limit {
                    return None;
                }
                combined.extend(repeated);
            }
            combined.sort();
            combined.dedup();
            (combined.len() <= count_limit).then_some(combined)
        }
    }
}

fn extract_finite_literal_values(pattern: &str) -> Option<Vec<String>> {
    let hir = RegexParserBuilder::new()
        .nest_limit(QUERY_REGEX_NEST_LIMIT)
        .build()
        .parse(pattern)
        .ok()?;
    let mut values = finite_literal_values_from_hir(
        &hir,
        MATCHER_LITERAL_SET_LIMIT,
        MATCHER_LITERAL_SET_TOTAL_BYTES_LIMIT,
    )?;
    values.sort();
    values.dedup();
    let total_bytes = values
        .iter()
        .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?;
    (!values.is_empty()
        && values.len() <= MATCHER_LITERAL_SET_LIMIT
        && total_bytes <= MATCHER_LITERAL_SET_TOTAL_BYTES_LIMIT)
        .then_some(values)
}

fn modeled_string_allocation_bytes(len: usize) -> u64 {
    if len == 0 {
        0
    } else {
        u64::try_from(len)
            .unwrap_or(u64::MAX)
            .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

pub(crate) fn modeled_series_matcher_preparation_bytes(matchers: &[SeriesMatcher]) -> u64 {
    let mut bytes = if matchers.is_empty() {
        0
    } else {
        u64::try_from(matchers.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(std::mem::size_of::<CompiledSeriesMatcher>()).unwrap_or(u64::MAX),
            )
            .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES)
    };
    let mut regex_count = 0u64;
    let mut largest_pattern_bytes = 0usize;

    for matcher in matchers {
        bytes = bytes
            .saturating_add(modeled_string_allocation_bytes(matcher.name.len()))
            .saturating_add(modeled_string_allocation_bytes(matcher.value.len()));
        if matches!(
            matcher.op,
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
        ) {
            regex_count = regex_count.saturating_add(1);
            largest_pattern_bytes = largest_pattern_bytes.max(matcher.value.len());
        }
    }

    if regex_count == 0 {
        return bytes;
    }

    let retained_regex_bytes = u64::try_from(
        QUERY_REGEX_COMPILED_SIZE_LIMIT_BYTES.saturating_add(QUERY_REGEX_DFA_SIZE_LIMIT_BYTES),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES);
    let retained_literal_set_bytes = u64::try_from(
        MATCHER_LITERAL_SET_LIMIT
            .saturating_mul(std::mem::size_of::<String>())
            .saturating_add(MATCHER_LITERAL_SET_TOTAL_BYTES_LIMIT),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(
        u64::try_from(MATCHER_LITERAL_SET_LIMIT)
            .unwrap_or(u64::MAX)
            .saturating_add(1)
            .saturating_mul(MATCHER_PREPARATION_ALLOCATION_ALLOWANCE_BYTES),
    );
    let anchored_scratch_bytes =
        modeled_string_allocation_bytes(largest_pattern_bytes.saturating_add(6));

    bytes
        .saturating_add(
            regex_count
                .saturating_mul(retained_regex_bytes.saturating_add(retained_literal_set_bytes)),
        )
        .saturating_add(QUERY_REGEX_COMPILER_SCRATCH_BYTES)
        .saturating_add(anchored_scratch_bytes)
}

fn compile_validated_series_matchers(
    matchers: &[SeriesMatcher],
    execution: Option<&QueryExecution>,
    before_regex_compile: Option<&dyn Fn()>,
) -> Result<Vec<CompiledSeriesMatcher>> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        if let Some(execution) = execution {
            execution.checkpoint()?;
        }

        let regex = match matcher.op {
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                if let Some(hook) = before_regex_compile {
                    hook();
                }
                if let Some(execution) = execution {
                    // A cancellation or deadline raised by the pre-compile hook must win before
                    // entering the regex parser/compiler.
                    execution.checkpoint()?;
                }
                Some(
                    build_bounded_regex(&matcher.value, RegexAnchoring::Anchored).map_err(
                        |error| {
                            TsinkError::InvalidConfiguration(format!(
                                "invalid regex for series matcher at index {}: {error}",
                                compiled.len()
                            ))
                        },
                    )?,
                )
            }
            SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
        };

        let matches_empty = match matcher.op {
            SeriesMatcherOp::Equal => matcher.value.is_empty(),
            SeriesMatcherOp::NotEqual => !matcher.value.is_empty(),
            SeriesMatcherOp::RegexMatch => regex.as_ref().is_some_and(|regex| regex.is_match("")),
            SeriesMatcherOp::RegexNoMatch => {
                !regex.as_ref().is_some_and(|regex| regex.is_match(""))
            }
        };
        let finite_literal_values = matches!(
            matcher.op,
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
        )
        .then(|| extract_finite_literal_values(&matcher.value))
        .flatten();

        compiled.push(CompiledSeriesMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
            matches_empty,
            finite_literal_values,
        });
    }
    Ok(compiled)
}

pub(crate) fn compile_series_matchers(
    matchers: &[SeriesMatcher],
) -> Result<Vec<CompiledSeriesMatcher>> {
    validate_series_matcher_shapes(matchers)?;
    compile_validated_series_matchers(matchers, None, None)
}

pub(crate) fn compile_series_matchers_with_execution(
    matchers: &[SeriesMatcher],
    execution: &QueryExecution,
    before_regex_compile: Option<&dyn Fn()>,
) -> Result<(Vec<CompiledSeriesMatcher>, QueryMemoryReservation)> {
    validate_series_matcher_shapes(matchers)?;
    execution.checkpoint()?;
    let reservation =
        execution.reserve_memory(modeled_series_matcher_preparation_bytes(matchers))?;
    execution.checkpoint()?;
    let compiled =
        compile_validated_series_matchers(matchers, Some(execution), before_regex_compile)?;
    Ok((compiled, reservation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher_with_total_bytes(total: usize) -> SeriesMatcher {
        SeriesMatcher::equal("n", "v".repeat(total.saturating_sub(1)))
    }

    #[test]
    fn matcher_shape_limits_accept_exact_boundaries_and_reject_one_over() {
        let exact_count = vec![SeriesMatcher::equal("n", ""); MAX_SERIES_SELECTION_MATCHERS];
        assert!(compile_series_matchers(&exact_count).is_ok());
        let one_over_count = vec![SeriesMatcher::equal("n", ""); MAX_SERIES_SELECTION_MATCHERS + 1];
        assert!(compile_series_matchers(&one_over_count).is_err());

        assert!(compile_series_matchers(&[SeriesMatcher::equal(
            "n".repeat(MAX_SERIES_MATCHER_NAME_BYTES),
            ""
        )])
        .is_ok());
        assert!(compile_series_matchers(&[SeriesMatcher::equal(
            "n".repeat(MAX_SERIES_MATCHER_NAME_BYTES + 1),
            ""
        )])
        .is_err());

        assert!(compile_series_matchers(&[SeriesMatcher::equal(
            "n",
            "v".repeat(MAX_SERIES_MATCHER_VALUE_BYTES)
        )])
        .is_ok());
        assert!(compile_series_matchers(&[SeriesMatcher::equal(
            "n",
            "v".repeat(MAX_SERIES_MATCHER_VALUE_BYTES + 1)
        )])
        .is_err());

        let exact_total = (0..4)
            .map(|_| matcher_with_total_bytes(MAX_SERIES_SELECTION_MATCHER_BYTES / 4))
            .collect::<Vec<_>>();
        assert!(compile_series_matchers(&exact_total).is_ok());
        let mut one_over_total = exact_total;
        one_over_total.push(SeriesMatcher::equal("n", ""));
        assert!(compile_series_matchers(&one_over_total).is_err());
    }

    #[test]
    fn bounded_regex_rejects_excessive_nesting_without_echoing_pattern() {
        let pattern = format!(
            "{}private-marker{}",
            "(".repeat(QUERY_REGEX_NEST_LIMIT as usize + 1),
            ")".repeat(QUERY_REGEX_NEST_LIMIT as usize + 1)
        );
        let error = compile_series_matchers(&[SeriesMatcher::regex_match("host", &pattern)])
            .expect_err("over-nested regex must fail");
        let diagnostic = error.to_string();
        assert!(diagnostic.len() <= MAX_QUERY_REGEX_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains("private-marker"));
    }

    #[test]
    fn finite_literal_extraction_has_a_cumulative_byte_bound() {
        let pattern = format!("({}){{8}}", "x".repeat(9_000));
        assert!(extract_finite_literal_values(&pattern).is_none());
    }

    #[test]
    fn capture_scratch_model_scales_with_every_capture_slot() {
        let one_capture = build_bounded_regex("(a)", RegexAnchoring::Unanchored).unwrap();
        let many_captures =
            build_bounded_regex(&"()".repeat(64), RegexAnchoring::Unanchored).unwrap();
        let one_bytes = modeled_bounded_regex_capture_scratch_bytes(&one_capture);
        let many_bytes = modeled_bounded_regex_capture_scratch_bytes(&many_captures);
        assert!(many_bytes > one_bytes);
        assert_eq!(
            many_bytes - one_bytes,
            u64::try_from(many_captures.captures_len() - one_capture.captures_len())
                .unwrap()
                .saturating_mul(4)
                .saturating_mul(u64::try_from(std::mem::size_of::<usize>()).unwrap())
        );
    }
}
