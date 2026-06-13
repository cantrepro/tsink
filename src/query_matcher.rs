use regex::Regex;
use regex_syntax::hir::{Hir, HirKind};
use regex_syntax::Parser as RegexParser;

use crate::storage::{SeriesMatcher, SeriesMatcherOp};
use crate::{Label, Result, TsinkError};

const MATCHER_LITERAL_SET_LIMIT: usize = 64;

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

fn literal_cross_product(
    prefixes: Vec<String>,
    suffixes: &[String],
    limit: usize,
) -> Option<Vec<String>> {
    if prefixes.is_empty() || suffixes.is_empty() {
        return Some(Vec::new());
    }
    if prefixes.len().saturating_mul(suffixes.len()) > limit {
        return None;
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

fn finite_literal_values_from_hir(hir: &Hir, limit: usize) -> Option<Vec<String>> {
    match hir.kind() {
        HirKind::Empty => Some(vec![String::new()]),
        HirKind::Literal(literal) => Some(vec![String::from_utf8(literal.0.to_vec()).ok()?]),
        HirKind::Class(_) | HirKind::Look(_) => None,
        HirKind::Capture(capture) => finite_literal_values_from_hir(&capture.sub, limit),
        HirKind::Concat(parts) => {
            let mut combined = vec![String::new()];
            for part in parts {
                let values = finite_literal_values_from_hir(part, limit)?;
                combined = literal_cross_product(combined, &values, limit)?;
            }
            Some(combined)
        }
        HirKind::Alternation(parts) => {
            let mut combined = Vec::new();
            for part in parts {
                let values = finite_literal_values_from_hir(part, limit)?;
                if combined.len().saturating_add(values.len()) > limit {
                    return None;
                }
                combined.extend(values);
            }
            combined.sort();
            combined.dedup();
            (combined.len() <= limit).then_some(combined)
        }
        HirKind::Repetition(repetition) => {
            let max = repetition.max?;
            if max > 8 {
                return None;
            }

            let repeated_values = finite_literal_values_from_hir(&repetition.sub, limit)?;
            let mut combined = Vec::new();
            for repeat_count in repetition.min..=max {
                let mut repeated = vec![String::new()];
                for _ in 0..repeat_count {
                    repeated = literal_cross_product(repeated, &repeated_values, limit)?;
                }
                if combined.len().saturating_add(repeated.len()) > limit {
                    return None;
                }
                combined.extend(repeated);
            }
            combined.sort();
            combined.dedup();
            (combined.len() <= limit).then_some(combined)
        }
    }
}

fn extract_finite_literal_values(pattern: &str) -> Option<Vec<String>> {
    let mut parser = RegexParser::new();
    let hir = parser.parse(pattern).ok()?;
    let mut values = finite_literal_values_from_hir(&hir, MATCHER_LITERAL_SET_LIMIT)?;
    values.sort();
    values.dedup();
    (!values.is_empty() && values.len() <= MATCHER_LITERAL_SET_LIMIT).then_some(values)
}

pub(crate) fn compile_series_matchers(
    matchers: &[SeriesMatcher],
) -> Result<Vec<CompiledSeriesMatcher>> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        if matcher.name.is_empty() {
            return Err(TsinkError::InvalidLabel(
                "series matcher label name cannot be empty".to_string(),
            ));
        }

        let regex = match matcher.op {
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                let anchored = format!("^(?:{})$", matcher.value);
                Some(Regex::new(&anchored).map_err(|err| {
                    TsinkError::InvalidConfiguration(format!(
                        "invalid matcher regex '{}': {err}",
                        matcher.value
                    ))
                })?)
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
