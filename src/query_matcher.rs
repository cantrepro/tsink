use regex::Regex;

use crate::storage::{SeriesMatcher, SeriesMatcherOp};
use crate::{Label, Result, TsinkError};

#[derive(Debug, Clone)]
pub(crate) struct CompiledSeriesMatcher {
    pub(crate) name: String,
    pub(crate) op: SeriesMatcherOp,
    pub(crate) value: String,
    pub(crate) regex: Option<Regex>,
}

impl CompiledSeriesMatcher {
    pub(crate) fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        let actual = if self.name == "__name__" {
            Some(metric)
        } else {
            labels
                .iter()
                .find(|label| label.name == self.name)
                .map(|label| label.value.as_str())
        };

        match self.op {
            SeriesMatcherOp::Equal => actual.is_some_and(|value| value == self.value),
            SeriesMatcherOp::NotEqual => actual.is_none_or(|value| value != self.value),
            SeriesMatcherOp::RegexMatch => actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
            SeriesMatcherOp::RegexNoMatch => !actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
        }
    }
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

        compiled.push(CompiledSeriesMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        });
    }
    Ok(compiled)
}
