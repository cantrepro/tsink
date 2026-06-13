use crate::promql::error::{PromqlError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
    pub literal: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Number(f64),
    String(String),
    Duration(i64),
    Ident(String),
    By,
    Without,
    Offset,
    Bool,
    And,
    Or,
    Unless,
    On,
    Ignoring,
    Inf,
    Nan,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Eq,
    NotEq,
    Lt,
    Gt,
    Lte,
    Gte,
    RegexEq,
    RegexNe,
    Assign,
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Eof,
}

impl TokenKind {
    pub fn display(&self) -> String {
        match self {
            TokenKind::Number(_) => "number".to_string(),
            TokenKind::String(_) => "string".to_string(),
            TokenKind::Duration(_) => "duration".to_string(),
            TokenKind::Ident(name) => format!("identifier({name})"),
            TokenKind::By => "by".to_string(),
            TokenKind::Without => "without".to_string(),
            TokenKind::Offset => "offset".to_string(),
            TokenKind::Bool => "bool".to_string(),
            TokenKind::And => "and".to_string(),
            TokenKind::Or => "or".to_string(),
            TokenKind::Unless => "unless".to_string(),
            TokenKind::On => "on".to_string(),
            TokenKind::Ignoring => "ignoring".to_string(),
            TokenKind::Inf => "inf".to_string(),
            TokenKind::Nan => "nan".to_string(),
            TokenKind::Plus => "+".to_string(),
            TokenKind::Minus => "-".to_string(),
            TokenKind::Star => "*".to_string(),
            TokenKind::Slash => "/".to_string(),
            TokenKind::Percent => "%".to_string(),
            TokenKind::Caret => "^".to_string(),
            TokenKind::Eq => "==".to_string(),
            TokenKind::NotEq => "!=".to_string(),
            TokenKind::Lt => "<".to_string(),
            TokenKind::Gt => ">".to_string(),
            TokenKind::Lte => "<=".to_string(),
            TokenKind::Gte => ">=".to_string(),
            TokenKind::RegexEq => "=~".to_string(),
            TokenKind::RegexNe => "!~".to_string(),
            TokenKind::Assign => "=".to_string(),
            TokenKind::LParen => "(".to_string(),
            TokenKind::RParen => ")".to_string(),
            TokenKind::LBrace => "{".to_string(),
            TokenKind::RBrace => "}".to_string(),
            TokenKind::LBracket => "[".to_string(),
            TokenKind::RBracket => "]".to_string(),
            TokenKind::Comma => ",".to_string(),
            TokenKind::Eof => "EOF".to_string(),
        }
    }
}

pub struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace();
            let start = self.pos;
            if self.is_eof() {
                tokens.push(Token {
                    kind: TokenKind::Eof,
                    span: Span {
                        start: self.pos,
                        end: self.pos,
                    },
                    literal: String::new(),
                });
                break;
            }

            let kind = match self.peek_byte() {
                b'0'..=b'9' => self.lex_number_or_duration()?,
                b'a'..=b'z' | b'A'..=b'Z' | b'_' | b':' => self.lex_ident_or_keyword(),
                b'"' => self.lex_string()?,
                b'+' => {
                    self.pos += 1;
                    TokenKind::Plus
                }
                b'-' => {
                    self.pos += 1;
                    TokenKind::Minus
                }
                b'*' => {
                    self.pos += 1;
                    TokenKind::Star
                }
                b'/' => {
                    self.pos += 1;
                    TokenKind::Slash
                }
                b'%' => {
                    self.pos += 1;
                    TokenKind::Percent
                }
                b'^' => {
                    self.pos += 1;
                    TokenKind::Caret
                }
                b'=' => {
                    self.pos += 1;
                    if self.match_byte(b'=') {
                        TokenKind::Eq
                    } else if self.match_byte(b'~') {
                        TokenKind::RegexEq
                    } else {
                        TokenKind::Assign
                    }
                }
                b'!' => {
                    self.pos += 1;
                    if self.match_byte(b'=') {
                        TokenKind::NotEq
                    } else if self.match_byte(b'~') {
                        TokenKind::RegexNe
                    } else {
                        return Err(PromqlError::Parse(format!(
                            "unexpected token '!' at byte {}",
                            start
                        )));
                    }
                }
                b'<' => {
                    self.pos += 1;
                    if self.match_byte(b'=') {
                        TokenKind::Lte
                    } else {
                        TokenKind::Lt
                    }
                }
                b'>' => {
                    self.pos += 1;
                    if self.match_byte(b'=') {
                        TokenKind::Gte
                    } else {
                        TokenKind::Gt
                    }
                }
                b'(' => {
                    self.pos += 1;
                    TokenKind::LParen
                }
                b')' => {
                    self.pos += 1;
                    TokenKind::RParen
                }
                b'{' => {
                    self.pos += 1;
                    TokenKind::LBrace
                }
                b'}' => {
                    self.pos += 1;
                    TokenKind::RBrace
                }
                b'[' => {
                    self.pos += 1;
                    TokenKind::LBracket
                }
                b']' => {
                    self.pos += 1;
                    TokenKind::RBracket
                }
                b',' => {
                    self.pos += 1;
                    TokenKind::Comma
                }
                other => {
                    return Err(PromqlError::Parse(format!(
                        "unexpected byte '{}' at {}",
                        other as char, start
                    )));
                }
            };

            let end = self.pos;
            tokens.push(Token {
                literal: self.input[start..end].to_string(),
                kind,
                span: Span { start, end },
            });
        }

        Ok(tokens)
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek_byte(&self) -> u8 {
        self.input.as_bytes()[self.pos]
    }

    fn match_byte(&mut self, expected: u8) -> bool {
        if !self.is_eof() && self.peek_byte() == expected {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn skip_whitespace(&mut self) {
        while !self.is_eof() && self.peek_byte().is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn lex_ident_or_keyword(&mut self) -> TokenKind {
        let start = self.pos;
        while !self.is_eof() {
            let b = self.peek_byte();
            if b.is_ascii_alphanumeric() || b == b'_' || b == b':' {
                self.pos += 1;
            } else {
                break;
            }
        }

        let raw = &self.input[start..self.pos];
        let lower = raw.to_ascii_lowercase();
        match lower.as_str() {
            "by" => TokenKind::By,
            "without" => TokenKind::Without,
            "offset" => TokenKind::Offset,
            "bool" => TokenKind::Bool,
            "and" => TokenKind::And,
            "or" => TokenKind::Or,
            "unless" => TokenKind::Unless,
            "on" => TokenKind::On,
            "ignoring" => TokenKind::Ignoring,
            "inf" => TokenKind::Inf,
            "nan" => TokenKind::Nan,
            _ => TokenKind::Ident(raw.to_string()),
        }
    }

    fn lex_string(&mut self) -> Result<TokenKind> {
        self.pos += 1; // opening quote
        let mut out = String::new();

        while !self.is_eof() {
            let b = self.peek_byte();
            self.pos += 1;
            match b {
                b'"' => return Ok(TokenKind::String(out)),
                b'\\' => {
                    if self.is_eof() {
                        return Err(PromqlError::Parse(
                            "unterminated escape sequence".to_string(),
                        ));
                    }
                    let escaped = self.peek_byte();
                    self.pos += 1;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        other => out.push(other as char),
                    }
                }
                other => out.push(other as char),
            }
        }

        Err(PromqlError::Parse(
            "unterminated string literal".to_string(),
        ))
    }

    fn lex_number_or_duration(&mut self) -> Result<TokenKind> {
        let start = self.pos;
        let mut has_dot = false;

        while !self.is_eof() {
            let b = self.peek_byte();
            if b.is_ascii_digit() {
                self.pos += 1;
                continue;
            }
            if b == b'.' {
                has_dot = true;
                self.pos += 1;
                continue;
            }
            break;
        }

        if !has_dot && !self.is_eof() && self.peek_byte().is_ascii_alphabetic() {
            if let Some((total_ms, end)) = Self::parse_duration(self.input, start) {
                self.pos = end;
                return Ok(TokenKind::Duration(total_ms));
            }
        }

        let number = self.input[start..self.pos].parse::<f64>().map_err(|e| {
            PromqlError::Parse(format!(
                "invalid number '{}': {e}",
                &self.input[start..self.pos]
            ))
        })?;
        Ok(TokenKind::Number(number))
    }

    fn parse_duration(input: &str, start: usize) -> Option<(i64, usize)> {
        let bytes = input.as_bytes();
        let len = bytes.len();
        let mut pos = start;
        let mut total: i128 = 0;
        let mut saw_segment = false;

        while pos < len {
            let num_start = pos;
            while pos < len && bytes[pos].is_ascii_digit() {
                pos += 1;
            }
            if pos == num_start {
                break;
            }

            let value = input[num_start..pos].parse::<i128>().ok()?;
            let unit_start = pos;
            while pos < len && bytes[pos].is_ascii_alphabetic() {
                pos += 1;
            }
            if pos == unit_start {
                return None;
            }

            let unit = &input[unit_start..pos];
            let factor: i128 = match unit {
                "ms" => 1,
                "s" => 1_000,
                "m" => 60_000,
                "h" => 3_600_000,
                "d" => 86_400_000,
                "w" => 604_800_000,
                "y" => 31_536_000_000,
                _ => return None,
            };

            total = total.checked_add(value.checked_mul(factor)?)?;
            saw_segment = true;

            if pos >= len || !bytes[pos].is_ascii_digit() {
                break;
            }
        }

        if !saw_segment || total > i64::MAX as i128 {
            return None;
        }

        Some((total as i64, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::{Lexer, TokenKind};

    #[test]
    fn tokenizes_selector_with_matchers() {
        let tokens = Lexer::new(r#"http_requests_total{method="GET",status=~"2.."}"#)
            .tokenize()
            .unwrap();
        assert!(matches!(&tokens[0].kind, TokenKind::Ident(name) if name == "http_requests_total"));
        assert!(matches!(tokens[1].kind, TokenKind::LBrace));
        assert!(matches!(tokens[3].kind, TokenKind::Assign));
        assert!(tokens
            .iter()
            .any(|token| matches!(token.kind, TokenKind::RegexEq)));
    }

    #[test]
    fn tokenizes_duration_and_number() {
        let tokens = Lexer::new("rate(metric[5m30s]) + 1.5").tokenize().unwrap();
        assert!(tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::Duration(330_000))));
        assert!(tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::Number(v) if (v - 1.5).abs() < 1e-9)));
    }
}
