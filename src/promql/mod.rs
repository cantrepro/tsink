pub mod ast;
pub mod error;
pub mod eval;
pub mod lexer;
pub mod parser;
pub mod types;

pub use error::{PromqlError, Result};
pub use eval::{Engine, PromqlExecutionResult};
pub use types::{PromqlValue, Sample, Series};

/// Maximum UTF-8 byte length accepted by the PromQL lexer and parser.
pub const MAX_PARSE_INPUT_BYTES: usize = 64 * 1024;

/// Maximum number of non-EOF tokens accepted by the PromQL lexer and parser.
pub const MAX_PARSE_TOKENS: usize = 16 * 1024;

/// Maximum nested expression depth accepted by the PromQL parser.
pub const MAX_PARSE_DEPTH: usize = 64;

pub fn parse(input: &str) -> Result<ast::Expr> {
    parser::parse(input)
}
