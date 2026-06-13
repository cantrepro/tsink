use thiserror::Error;

pub type Result<T> = std::result::Result<T, PromqlError>;

#[derive(Debug, Error)]
pub enum PromqlError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("unexpected token: expected {expected}, found {found}")]
    UnexpectedToken { expected: String, found: String },

    #[error("unknown function '{0}'")]
    UnknownFunction(String),

    #[error("invalid argument count for '{func}': expected {expected}, got {got}")]
    ArgumentCount {
        func: String,
        expected: String,
        got: usize,
    },

    #[error("type error: {0}")]
    Type(String),

    #[error("evaluation error: {0}")]
    Eval(String),

    #[error("regex error: {0}")]
    Regex(String),

    #[error("storage error: {0}")]
    Storage(#[from] tsink::TsinkError),
}

impl From<regex::Error> for PromqlError {
    fn from(value: regex::Error) -> Self {
        PromqlError::Regex(value.to_string())
    }
}
