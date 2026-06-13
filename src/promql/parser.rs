use crate::promql::ast::{
    AggregationExpr, AggregationOp, AtModifier, BinaryExpr, BinaryOp, CallExpr, Expr, Grouping,
    LabelMatcher, MatchOp, MatrixSelector, SubqueryExpr, UnaryExpr, UnaryOp,
    VectorMatchCardinality, VectorMatching, VectorSelector,
};
use crate::promql::error::{PromqlError, Result};
use crate::promql::lexer::{Lexer, Token, TokenKind};

pub fn parse(input: &str) -> Result<Expr> {
    let tokens = Lexer::new(input).tokenize()?;
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_expr(0)?;
    parser.expect(TokenExpect::Eof)?;
    Ok(expr)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

#[derive(Clone, Copy)]
enum TokenExpect {
    Eof,
    LParen,
    RParen,
    LBrace,
    RBrace,
    RBracket,
    Comma,
    Duration,
    Ident,
    String,
}

impl TokenExpect {
    fn display(self) -> &'static str {
        match self {
            Self::Eof => "EOF",
            Self::LParen => "'('",
            Self::RParen => "')'",
            Self::LBrace => "'{'",
            Self::RBrace => "'}'",
            Self::RBracket => "']'",
            Self::Comma => "','",
            Self::Duration => "duration",
            Self::Ident => "identifier",
            Self::String => "string",
        }
    }
}

impl Parser {
    fn parse_expr(&mut self, min_prec: u8) -> Result<Expr> {
        let mut lhs = self.parse_unary()?;

        loop {
            let Some((op, prec, right_assoc)) = self.peek_binary_op() else {
                break;
            };
            if prec < min_prec {
                break;
            }

            self.advance();
            let (return_bool, matching) = self.parse_binary_modifiers(op)?;
            let next_min_prec = if right_assoc { prec } else { prec + 1 };
            let rhs = self.parse_expr(next_min_prec)?;

            lhs = Expr::Binary(BinaryExpr {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                return_bool,
                matching,
            });
        }

        Ok(lhs)
    }

    fn parse_binary_modifiers(&mut self, op: BinaryOp) -> Result<(bool, Option<VectorMatching>)> {
        let mut return_bool = false;
        let mut matching = None;

        loop {
            if matches!(self.peek().kind, TokenKind::Bool) {
                self.advance();
                return_bool = true;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::On) {
                self.advance();
                let parsed = self.parse_vector_matching(true)?;
                merge_vector_matching(&mut matching, parsed)?;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::Ignoring) {
                self.advance();
                let parsed = self.parse_vector_matching(false)?;
                merge_vector_matching(&mut matching, parsed)?;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::GroupLeft) {
                self.advance();
                let parsed = self.parse_group_modifier(VectorMatchCardinality::ManyToOne)?;
                merge_vector_matching(&mut matching, parsed)?;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::GroupRight) {
                self.advance();
                let parsed = self.parse_group_modifier(VectorMatchCardinality::OneToMany)?;
                merge_vector_matching(&mut matching, parsed)?;
                continue;
            }

            break;
        }

        if return_bool && !op.is_comparison() {
            return Err(PromqlError::Parse(
                "bool modifier can only be used with comparison operators".to_string(),
            ));
        }

        if matching
            .as_ref()
            .is_some_and(|m| m.cardinality != VectorMatchCardinality::OneToOne)
            && op.is_set()
        {
            return Err(PromqlError::Parse(
                "group_left/group_right modifiers cannot be used with set operators".to_string(),
            ));
        }

        Ok((return_bool, matching))
    }

    fn parse_vector_matching(&mut self, on: bool) -> Result<VectorMatching> {
        let labels = self.parse_label_list()?;
        Ok(VectorMatching {
            on,
            labels,
            cardinality: VectorMatchCardinality::OneToOne,
            include_labels: Vec::new(),
        })
    }

    fn parse_group_modifier(
        &mut self,
        cardinality: VectorMatchCardinality,
    ) -> Result<VectorMatching> {
        let include_labels = if matches!(self.peek().kind, TokenKind::LParen) {
            self.parse_label_list()?
        } else {
            Vec::new()
        };

        Ok(VectorMatching {
            on: false,
            labels: Vec::new(),
            cardinality,
            include_labels,
        })
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if matches!(self.peek().kind, TokenKind::Plus) {
            self.advance();
            let expr = self.parse_unary()?;
            return Ok(Expr::Unary(UnaryExpr {
                op: UnaryOp::Pos,
                expr: Box::new(expr),
            }));
        }

        if matches!(self.peek().kind, TokenKind::Minus) {
            self.advance();
            let expr = self.parse_unary()?;
            return Ok(Expr::Unary(UnaryExpr {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            }));
        }

        let expr = self.parse_primary()?;
        self.parse_postfix(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.peek().kind.clone() {
            TokenKind::Number(v) => {
                self.advance();
                Ok(Expr::NumberLiteral(v))
            }
            TokenKind::String(v) => {
                self.advance();
                Ok(Expr::StringLiteral(v))
            }
            TokenKind::Inf => {
                self.advance();
                Ok(Expr::NumberLiteral(f64::INFINITY))
            }
            TokenKind::Nan => {
                self.advance();
                Ok(Expr::NumberLiteral(f64::NAN))
            }
            TokenKind::LParen => {
                self.advance();
                let expr = self.parse_expr(0)?;
                self.expect(TokenExpect::RParen)?;
                Ok(Expr::Paren(Box::new(expr)))
            }
            TokenKind::LBrace => self.parse_vector_selector(None),
            TokenKind::Ident(name) => {
                self.advance();
                if let Some(op) = AggregationOp::from_ident(&name) {
                    if self.maybe_aggregation_start() {
                        return self.parse_aggregation(op);
                    }
                }

                if matches!(self.peek().kind, TokenKind::LParen) {
                    self.parse_call(name)
                } else {
                    self.parse_vector_selector(Some(name))
                }
            }
            _ => Err(PromqlError::UnexpectedToken {
                expected: "expression".to_string(),
                found: self.peek().kind.display(),
            }),
        }
    }

    fn maybe_aggregation_start(&self) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::By | TokenKind::Without | TokenKind::LParen
        )
    }

    fn parse_aggregation(&mut self, op: AggregationOp) -> Result<Expr> {
        let mut grouping = self.parse_grouping()?;
        self.expect(TokenExpect::LParen)?;

        let (param, expr) = match op {
            AggregationOp::TopK
            | AggregationOp::BottomK
            | AggregationOp::Quantile
            | AggregationOp::CountValues
            | AggregationOp::LimitK
            | AggregationOp::LimitRatio => {
                let p = self.parse_expr(0)?;
                self.expect(TokenExpect::Comma)?;
                let e = self.parse_expr(0)?;
                (Some(Box::new(p)), Box::new(e))
            }
            _ => {
                let e = self.parse_expr(0)?;
                (None, Box::new(e))
            }
        };

        self.expect(TokenExpect::RParen)?;
        if grouping.is_none() {
            grouping = self.parse_grouping()?;
        }

        Ok(Expr::Aggregation(AggregationExpr {
            op,
            expr,
            param,
            grouping,
        }))
    }

    fn parse_grouping(&mut self) -> Result<Option<Grouping>> {
        let without = if matches!(self.peek().kind, TokenKind::By) {
            self.advance();
            false
        } else if matches!(self.peek().kind, TokenKind::Without) {
            self.advance();
            true
        } else {
            return Ok(None);
        };

        self.expect(TokenExpect::LParen)?;
        let mut labels = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                labels.push(self.expect_ident()?);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect(TokenExpect::RParen)?;

        Ok(Some(Grouping { without, labels }))
    }

    fn parse_label_list(&mut self) -> Result<Vec<String>> {
        self.expect(TokenExpect::LParen)?;
        let mut labels = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                labels.push(self.expect_ident()?);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect(TokenExpect::RParen)?;
        Ok(labels)
    }

    fn parse_call(&mut self, func: String) -> Result<Expr> {
        self.expect(TokenExpect::LParen)?;
        let mut args = Vec::new();
        if !matches!(self.peek().kind, TokenKind::RParen) {
            loop {
                args.push(self.parse_expr(0)?);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect(TokenExpect::RParen)?;

        Ok(Expr::Call(CallExpr { func, args }))
    }

    fn parse_vector_selector(&mut self, metric_name: Option<String>) -> Result<Expr> {
        let mut matchers = Vec::new();
        if matches!(self.peek().kind, TokenKind::LBrace) {
            matchers = self.parse_label_matchers()?;
        }

        let vector = VectorSelector {
            metric_name,
            matchers,
            offset: 0,
            at: None,
        };
        Ok(Expr::VectorSelector(vector))
    }

    fn parse_postfix(&mut self, mut expr: Expr) -> Result<Expr> {
        loop {
            if matches!(self.peek().kind, TokenKind::LBracket) {
                expr = self.parse_bracket_postfix(expr)?;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::Offset) {
                self.advance();
                let offset = self.expect_signed_duration()?;
                apply_offset_modifier(&mut expr, offset)?;
                continue;
            }

            if matches!(self.peek().kind, TokenKind::At) {
                self.advance();
                let at = self.parse_at_modifier()?;
                apply_at_modifier(&mut expr, at)?;
                continue;
            }

            break;
        }

        Ok(expr)
    }

    fn parse_bracket_postfix(&mut self, expr: Expr) -> Result<Expr> {
        self.advance();
        let range = self.expect_duration()?;

        if matches!(self.peek().kind, TokenKind::Colon) {
            self.advance();
            let step = if matches!(self.peek().kind, TokenKind::RBracket) {
                None
            } else {
                Some(self.expect_duration()?)
            };
            self.expect(TokenExpect::RBracket)?;

            return Ok(Expr::Subquery(SubqueryExpr {
                expr: Box::new(expr),
                range,
                step,
                offset: 0,
                at: None,
            }));
        }

        self.expect(TokenExpect::RBracket)?;
        match expr {
            Expr::VectorSelector(vector) => {
                Ok(Expr::MatrixSelector(MatrixSelector { vector, range }))
            }
            other => Err(PromqlError::Type(format!(
                "range selectors can only apply to vectors, got {other:?}"
            ))),
        }
    }

    fn parse_at_modifier(&mut self) -> Result<AtModifier> {
        let sign = if matches!(self.peek().kind, TokenKind::Minus) {
            self.advance();
            -1.0
        } else if matches!(self.peek().kind, TokenKind::Plus) {
            self.advance();
            1.0
        } else {
            1.0
        };

        match self.peek().kind.clone() {
            TokenKind::Number(value) => {
                self.advance();
                Ok(AtModifier::Timestamp(sign * value))
            }
            TokenKind::Inf => {
                self.advance();
                Ok(AtModifier::Timestamp(sign * f64::INFINITY))
            }
            TokenKind::Nan => {
                self.advance();
                Ok(AtModifier::Timestamp(f64::NAN))
            }
            TokenKind::Ident(name) if sign == 1.0 && (name == "start" || name == "end") => {
                self.advance();
                self.expect(TokenExpect::LParen)?;
                self.expect(TokenExpect::RParen)?;
                if name == "start" {
                    Ok(AtModifier::Start)
                } else {
                    Ok(AtModifier::End)
                }
            }
            _ => Err(PromqlError::UnexpectedToken {
                expected: "timestamp, start(), or end()".to_string(),
                found: self.peek().kind.display(),
            }),
        }
    }

    fn parse_label_matchers(&mut self) -> Result<Vec<LabelMatcher>> {
        self.expect(TokenExpect::LBrace)?;
        let mut out = Vec::new();
        if matches!(self.peek().kind, TokenKind::RBrace) {
            self.advance();
            return Ok(out);
        }

        loop {
            let name = self.expect_ident()?;
            let op = match self.peek().kind {
                TokenKind::Assign => {
                    self.advance();
                    MatchOp::Equal
                }
                TokenKind::NotEq => {
                    self.advance();
                    MatchOp::NotEqual
                }
                TokenKind::RegexEq => {
                    self.advance();
                    MatchOp::RegexMatch
                }
                TokenKind::RegexNe => {
                    self.advance();
                    MatchOp::RegexNoMatch
                }
                _ => {
                    return Err(PromqlError::UnexpectedToken {
                        expected: "label matcher operator".to_string(),
                        found: self.peek().kind.display(),
                    });
                }
            };

            let value = self.expect_string()?;
            out.push(LabelMatcher { name, op, value });

            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                if matches!(self.peek().kind, TokenKind::RBrace) {
                    break;
                }
                continue;
            }
            break;
        }

        self.expect(TokenExpect::RBrace)?;
        Ok(out)
    }

    fn peek_binary_op(&self) -> Option<(BinaryOp, u8, bool)> {
        match self.peek().kind {
            TokenKind::Or => Some((BinaryOp::Or, 1, false)),
            TokenKind::And => Some((BinaryOp::And, 2, false)),
            TokenKind::Unless => Some((BinaryOp::Unless, 2, false)),
            TokenKind::Eq => Some((BinaryOp::Eq, 3, false)),
            TokenKind::NotEq => Some((BinaryOp::NotEq, 3, false)),
            TokenKind::Lt => Some((BinaryOp::Lt, 3, false)),
            TokenKind::Gt => Some((BinaryOp::Gt, 3, false)),
            TokenKind::Lte => Some((BinaryOp::Lte, 3, false)),
            TokenKind::Gte => Some((BinaryOp::Gte, 3, false)),
            TokenKind::Plus => Some((BinaryOp::Add, 4, false)),
            TokenKind::Minus => Some((BinaryOp::Sub, 4, false)),
            TokenKind::Star => Some((BinaryOp::Mul, 5, false)),
            TokenKind::Slash => Some((BinaryOp::Div, 5, false)),
            TokenKind::Percent => Some((BinaryOp::Mod, 5, false)),
            TokenKind::Atan2 => Some((BinaryOp::Atan2, 5, false)),
            TokenKind::Caret => Some((BinaryOp::Pow, 6, true)),
            _ => None,
        }
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .or_else(|| self.tokens.last())
            .expect("parser token stream is never empty")
    }

    fn advance(&mut self) -> &Token {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        self.tokens
            .get(self.pos.saturating_sub(1))
            .expect("parser advance is always called with available token")
    }

    fn expect(&mut self, expect: TokenExpect) -> Result<()> {
        let ok = matches!(
            (expect, &self.peek().kind),
            (TokenExpect::Eof, TokenKind::Eof)
                | (TokenExpect::LParen, TokenKind::LParen)
                | (TokenExpect::RParen, TokenKind::RParen)
                | (TokenExpect::LBrace, TokenKind::LBrace)
                | (TokenExpect::RBrace, TokenKind::RBrace)
                | (TokenExpect::RBracket, TokenKind::RBracket)
                | (TokenExpect::Comma, TokenKind::Comma)
                | (TokenExpect::Duration, TokenKind::Duration(_))
                | (TokenExpect::Ident, TokenKind::Ident(_))
                | (TokenExpect::String, TokenKind::String(_))
        );

        if ok {
            self.advance();
            return Ok(());
        }

        Err(PromqlError::UnexpectedToken {
            expected: expect.display().to_string(),
            found: self.peek().kind.display(),
        })
    }

    fn expect_ident(&mut self) -> Result<String> {
        let tok = self.peek().kind.clone();
        self.expect(TokenExpect::Ident)?;
        if let TokenKind::Ident(v) = tok {
            Ok(v)
        } else {
            unreachable!()
        }
    }

    fn expect_string(&mut self) -> Result<String> {
        let tok = self.peek().kind.clone();
        self.expect(TokenExpect::String)?;
        if let TokenKind::String(v) = tok {
            Ok(v)
        } else {
            unreachable!()
        }
    }

    fn expect_duration(&mut self) -> Result<i64> {
        let tok = self.peek().kind.clone();
        self.expect(TokenExpect::Duration)?;
        if let TokenKind::Duration(v) = tok {
            Ok(v)
        } else {
            unreachable!()
        }
    }

    fn expect_signed_duration(&mut self) -> Result<i64> {
        let sign = if matches!(self.peek().kind, TokenKind::Minus) {
            self.advance();
            -1
        } else if matches!(self.peek().kind, TokenKind::Plus) {
            self.advance();
            1
        } else {
            1
        };

        Ok(self.expect_duration()?.saturating_mul(sign))
    }
}

fn apply_offset_modifier(expr: &mut Expr, offset: i64) -> Result<()> {
    match expr {
        Expr::VectorSelector(vector) => {
            if vector.offset != 0 {
                return Err(PromqlError::Parse(
                    "offset modifier can only be specified once".to_string(),
                ));
            }
            vector.offset = offset;
            Ok(())
        }
        Expr::MatrixSelector(matrix) => {
            if matrix.vector.offset != 0 {
                return Err(PromqlError::Parse(
                    "offset modifier can only be specified once".to_string(),
                ));
            }
            matrix.vector.offset = offset;
            Ok(())
        }
        Expr::Subquery(subquery) => {
            if subquery.offset != 0 {
                return Err(PromqlError::Parse(
                    "offset modifier can only be specified once".to_string(),
                ));
            }
            subquery.offset = offset;
            Ok(())
        }
        _ => Err(PromqlError::Type(
            "offset modifier can only apply to vector/matrix selectors or subqueries".to_string(),
        )),
    }
}

fn apply_at_modifier(expr: &mut Expr, at: AtModifier) -> Result<()> {
    match expr {
        Expr::VectorSelector(vector) => {
            if vector.at.is_some() {
                return Err(PromqlError::Parse(
                    "@ modifier can only be specified once".to_string(),
                ));
            }
            vector.at = Some(at);
            Ok(())
        }
        Expr::MatrixSelector(matrix) => {
            if matrix.vector.at.is_some() {
                return Err(PromqlError::Parse(
                    "@ modifier can only be specified once".to_string(),
                ));
            }
            matrix.vector.at = Some(at);
            Ok(())
        }
        Expr::Subquery(subquery) => {
            if subquery.at.is_some() {
                return Err(PromqlError::Parse(
                    "@ modifier can only be specified once".to_string(),
                ));
            }
            subquery.at = Some(at);
            Ok(())
        }
        _ => Err(PromqlError::Type(
            "@ modifier can only apply to vector/matrix selectors or subqueries".to_string(),
        )),
    }
}

fn merge_vector_matching(
    matching: &mut Option<VectorMatching>,
    parsed: VectorMatching,
) -> Result<()> {
    if matching.is_none() {
        *matching = Some(parsed);
        return Ok(());
    }

    let current = matching
        .as_mut()
        .expect("matching was initialized in the branch above");

    if parsed.cardinality != VectorMatchCardinality::OneToOne {
        if current.cardinality != VectorMatchCardinality::OneToOne {
            return Err(PromqlError::Parse(
                "vector matching can only include one group_left/group_right modifier".to_string(),
            ));
        }
        current.cardinality = parsed.cardinality;
        current.include_labels = parsed.include_labels;
    } else {
        if !current.labels.is_empty() || current.on {
            return Err(PromqlError::Parse(
                "vector matching can only include one on()/ignoring() modifier".to_string(),
            ));
        }
        current.on = parsed.on;
        current.labels = parsed.labels;
    }

    let overlap = current
        .labels
        .iter()
        .find(|label| current.include_labels.iter().any(|extra| extra == *label));
    if let Some(label) = overlap {
        return Err(PromqlError::Parse(format!(
            "label '{label}' cannot appear in both vector matching and group modifier lists"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::promql::ast::{AggregationOp, BinaryOp, Expr, MatchOp};

    use super::parse;

    #[test]
    fn parses_selector_and_matrix_with_offset() {
        let expr = parse("http_requests_total{method=\"GET\"}[5m] offset 30s").unwrap();
        match expr {
            Expr::MatrixSelector(sel) => {
                assert_eq!(sel.range, 300_000);
                assert_eq!(sel.vector.offset, 30_000);
                assert_eq!(
                    sel.vector.metric_name.as_deref(),
                    Some("http_requests_total")
                );
                assert_eq!(sel.vector.matchers.len(), 1);
                assert_eq!(sel.vector.matchers[0].op, MatchOp::Equal);
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }

    #[test]
    fn parses_binary_precedence() {
        let expr = parse("a + b * c").unwrap();
        match expr {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Add);
                assert!(matches!(*b.rhs, Expr::Binary(_)));
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }

    #[test]
    fn parses_aggregation_with_grouping() {
        let expr = parse("sum by (method) (rate(http_requests_total[5m]))").unwrap();
        match expr {
            Expr::Aggregation(agg) => {
                assert_eq!(agg.op, AggregationOp::Sum);
                assert_eq!(agg.grouping.unwrap().labels, vec!["method"]);
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }

    #[test]
    fn parses_topk() {
        let expr = parse("topk(3, up)").unwrap();
        match expr {
            Expr::Aggregation(agg) => {
                assert_eq!(agg.op, AggregationOp::TopK);
                assert!(agg.param.is_some());
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }
}
