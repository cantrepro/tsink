pub mod ast;
pub mod error;
pub mod eval;
pub mod lexer;
pub mod parser;
pub mod types;

pub use error::{PromqlError, Result};
pub use eval::Engine;
pub use types::{PromqlValue, Sample, Series};

/// Parses a PromQL expression into an AST.
pub fn parse(input: &str) -> Result<ast::Expr> {
    parser::parse(input)
}
