use tsink::promql::ast::{
    AggregationOp, AtModifier, BinaryOp, Expr, MatchOp, VectorMatchCardinality,
};
use tsink::promql::lexer::{Lexer, Token, TokenKind};
use tsink::promql::{parse, PromqlError};

fn lex(input: &str) -> Vec<Token> {
    Lexer::new(input).tokenize().unwrap()
}

#[test]
fn lexer_01_empty_input_emits_only_eof() {
    let tokens = lex("");
    assert_eq!(tokens.len(), 1);
    assert!(matches!(tokens[0].kind, TokenKind::Eof));
}

#[test]
fn lexer_02_whitespace_only_emits_eof_at_end() {
    let tokens = lex("   \n\t ");
    assert_eq!(tokens.len(), 1);
    assert!(matches!(tokens[0].kind, TokenKind::Eof));
    assert_eq!(tokens[0].span.start, 6);
    assert_eq!(tokens[0].span.end, 6);
}

#[test]
fn lexer_03_identifier_allows_colon_underscore_and_digits() {
    let tokens = lex("metric_name:sum_42");
    assert!(matches!(
        &tokens[0].kind,
        TokenKind::Ident(name) if name == "metric_name:sum_42"
    ));
}

#[test]
fn lexer_04_keywords_are_case_insensitive() {
    let tokens = lex("By WITHOUT OffSet BoOl AnD Or UNLESS On Ignoring Group_Left GROUP_RIGHT");
    assert!(matches!(tokens[0].kind, TokenKind::By));
    assert!(matches!(tokens[1].kind, TokenKind::Without));
    assert!(matches!(tokens[2].kind, TokenKind::Offset));
    assert!(matches!(tokens[3].kind, TokenKind::Bool));
    assert!(matches!(tokens[4].kind, TokenKind::And));
    assert!(matches!(tokens[5].kind, TokenKind::Or));
    assert!(matches!(tokens[6].kind, TokenKind::Unless));
    assert!(matches!(tokens[7].kind, TokenKind::On));
    assert!(matches!(tokens[8].kind, TokenKind::Ignoring));
    assert!(matches!(tokens[9].kind, TokenKind::GroupLeft));
    assert!(matches!(tokens[10].kind, TokenKind::GroupRight));
}

#[test]
fn lexer_05_inf_and_nan_are_case_insensitive_keywords() {
    let tokens = lex("INF NaN");
    assert!(matches!(tokens[0].kind, TokenKind::Inf));
    assert!(matches!(tokens[1].kind, TokenKind::Nan));
}

#[test]
fn lexer_06_string_escape_sequences_are_decoded() {
    let tokens = lex(r#""line\n\t\"ok\"\\done""#);
    assert!(matches!(
        &tokens[0].kind,
        TokenKind::String(s) if s == "line\n\t\"ok\"\\done"
    ));
}

#[test]
fn lexer_07_unknown_escape_keeps_character() {
    let tokens = lex(r#""a\qb""#);
    assert!(matches!(&tokens[0].kind, TokenKind::String(s) if s == "aqb"));
}

#[test]
fn lexer_08_unterminated_string_returns_parse_error() {
    let err = Lexer::new(r#""unterminated"#).tokenize().unwrap_err();
    assert!(matches!(err, PromqlError::Parse(msg) if msg.contains("unterminated string literal")));
}

#[test]
fn lexer_09_unterminated_escape_returns_parse_error() {
    let err = Lexer::new("\"abc\\").tokenize().unwrap_err();
    assert!(matches!(err, PromqlError::Parse(msg) if msg.contains("unterminated escape sequence")));
}

#[test]
fn lexer_10_integer_number_tokenizes_as_number() {
    let tokens = lex("12345");
    assert!(matches!(tokens[0].kind, TokenKind::Number(v) if (v - 12345.0).abs() < 1e-12));
}

#[test]
fn lexer_11_float_number_tokenizes_as_number() {
    let tokens = lex("12.75");
    assert!(matches!(tokens[0].kind, TokenKind::Number(v) if (v - 12.75).abs() < 1e-12));
}

#[test]
fn lexer_12_duration_seconds_tokenizes_in_millis() {
    let tokens = lex("15s");
    assert!(matches!(tokens[0].kind, TokenKind::Duration(15_000)));
}

#[test]
fn lexer_13_duration_milliseconds_tokenizes_in_millis() {
    let tokens = lex("250ms");
    assert!(matches!(tokens[0].kind, TokenKind::Duration(250)));
}

#[test]
fn lexer_14_duration_multi_segment_is_accumulated() {
    let tokens = lex("1h30m5s");
    assert!(matches!(tokens[0].kind, TokenKind::Duration(5_405_000)));
}

#[test]
fn lexer_15_invalid_duration_unit_falls_back_to_number_and_ident() {
    let tokens = lex("5q");
    assert!(matches!(tokens[0].kind, TokenKind::Number(v) if (v - 5.0).abs() < 1e-12));
    assert!(matches!(&tokens[1].kind, TokenKind::Ident(s) if s == "q"));
}

#[test]
fn lexer_16_decimal_followed_by_ident_is_not_duration() {
    let tokens = lex("1.5m");
    assert!(matches!(tokens[0].kind, TokenKind::Number(v) if (v - 1.5).abs() < 1e-12));
    assert!(matches!(&tokens[1].kind, TokenKind::Ident(s) if s == "m"));
}

#[test]
fn lexer_17_arithmetic_operators_tokenize_correctly() {
    let tokens = lex("+ - * / % ^");
    assert!(matches!(tokens[0].kind, TokenKind::Plus));
    assert!(matches!(tokens[1].kind, TokenKind::Minus));
    assert!(matches!(tokens[2].kind, TokenKind::Star));
    assert!(matches!(tokens[3].kind, TokenKind::Slash));
    assert!(matches!(tokens[4].kind, TokenKind::Percent));
    assert!(matches!(tokens[5].kind, TokenKind::Caret));
}

#[test]
fn lexer_18_comparison_and_regex_operators_tokenize_correctly() {
    let tokens = lex("== != <= >= =~ !~ < > =");
    assert!(matches!(tokens[0].kind, TokenKind::Eq));
    assert!(matches!(tokens[1].kind, TokenKind::NotEq));
    assert!(matches!(tokens[2].kind, TokenKind::Lte));
    assert!(matches!(tokens[3].kind, TokenKind::Gte));
    assert!(matches!(tokens[4].kind, TokenKind::RegexEq));
    assert!(matches!(tokens[5].kind, TokenKind::RegexNe));
    assert!(matches!(tokens[6].kind, TokenKind::Lt));
    assert!(matches!(tokens[7].kind, TokenKind::Gt));
    assert!(matches!(tokens[8].kind, TokenKind::Assign));
}

#[test]
fn lexer_19_standalone_exclamation_is_error() {
    let err = Lexer::new("!").tokenize().unwrap_err();
    assert!(matches!(err, PromqlError::Parse(msg) if msg.contains("unexpected token '!'")));
}

#[test]
fn lexer_20_punctuation_tokens_are_recognized() {
    let tokens = lex("(){}[],:@");
    assert!(matches!(tokens[0].kind, TokenKind::LParen));
    assert!(matches!(tokens[1].kind, TokenKind::RParen));
    assert!(matches!(tokens[2].kind, TokenKind::LBrace));
    assert!(matches!(tokens[3].kind, TokenKind::RBrace));
    assert!(matches!(tokens[4].kind, TokenKind::LBracket));
    assert!(matches!(tokens[5].kind, TokenKind::RBracket));
    assert!(matches!(tokens[6].kind, TokenKind::Comma));
    assert!(matches!(tokens[7].kind, TokenKind::Colon));
    assert!(matches!(tokens[8].kind, TokenKind::At));
}

#[test]
fn lexer_21_span_tracks_positions_after_whitespace() {
    let tokens = lex("  up + 1");
    assert_eq!(tokens[0].span.start, 2);
    assert_eq!(tokens[0].span.end, 4);
    assert_eq!(tokens[1].span.start, 5);
    assert_eq!(tokens[1].span.end, 6);
    assert_eq!(tokens[2].span.start, 7);
    assert_eq!(tokens[2].span.end, 8);
}

#[test]
fn lexer_22_unexpected_byte_error_mentions_character() {
    let err = Lexer::new("`").tokenize().unwrap_err();
    assert!(matches!(err, PromqlError::Parse(msg) if msg.contains("unexpected byte '`'")));
}

#[test]
fn lexer_23_duration_supports_all_units_in_sequence() {
    let tokens = lex("1y2w3d4h5m6s7ms");
    assert!(matches!(
        tokens[0].kind,
        TokenKind::Duration(33_019_506_007)
    ));
}

#[test]
fn lexer_24_duration_stops_before_non_duration_token() {
    let tokens = lex("15m+foo");
    assert!(matches!(tokens[0].kind, TokenKind::Duration(900_000)));
    assert!(matches!(tokens[1].kind, TokenKind::Plus));
    assert!(matches!(&tokens[2].kind, TokenKind::Ident(s) if s == "foo"));
}

#[test]
fn lexer_25_stream_always_ends_with_eof() {
    let tokens = lex(r#"rate(http_requests_total{method="GET"}[5m])"#);
    assert!(matches!(
        tokens.last().map(|t| &t.kind),
        Some(TokenKind::Eof)
    ));
}

#[test]
fn lexer_26_skips_hash_comments() {
    let tokens = lex("up # trailing comment\n+ 1");
    assert!(matches!(&tokens[0].kind, TokenKind::Ident(name) if name == "up"));
    assert!(matches!(tokens[1].kind, TokenKind::Plus));
    assert!(matches!(tokens[2].kind, TokenKind::Number(v) if (v - 1.0).abs() < 1e-12));
}

#[test]
fn parser_01_parses_number_literal() {
    let expr = parse("42").unwrap();
    assert!(matches!(expr, Expr::NumberLiteral(v) if (v - 42.0).abs() < 1e-12));
}

#[test]
fn parser_02_parses_inf_literal() {
    let expr = parse("inf").unwrap();
    assert!(matches!(expr, Expr::NumberLiteral(v) if v.is_infinite() && v.is_sign_positive()));
}

#[test]
fn parser_03_parses_nan_literal() {
    let expr = parse("NaN").unwrap();
    assert!(matches!(expr, Expr::NumberLiteral(v) if v.is_nan()));
}

#[test]
fn parser_04_parses_string_literal() {
    let expr = parse(r#""abc""#).unwrap();
    assert!(matches!(expr, Expr::StringLiteral(ref s) if s == "abc"));
}

#[test]
fn parser_05_parses_unary_minus_expression() {
    let expr = parse("-5").unwrap();
    match expr {
        Expr::Unary(unary) => {
            assert_eq!(unary.op, tsink::promql::ast::UnaryOp::Neg);
            assert!(matches!(*unary.expr, Expr::NumberLiteral(v) if (v - 5.0).abs() < 1e-12));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_05b_parses_unary_plus_expression() {
    let expr = parse("+5").unwrap();
    match expr {
        Expr::Unary(unary) => {
            assert_eq!(unary.op, tsink::promql::ast::UnaryOp::Pos);
            assert!(matches!(*unary.expr, Expr::NumberLiteral(v) if (v - 5.0).abs() < 1e-12));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_06_parses_parenthesized_binary_expression() {
    let expr = parse("(1 + 2)").unwrap();
    match expr {
        Expr::Paren(inner) => {
            assert!(matches!(
                *inner,
                Expr::Binary(ref b)
                if b.op == BinaryOp::Add
            ));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_07_parses_vector_selector_with_metric_only() {
    let expr = parse("up").unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert_eq!(sel.metric_name.as_deref(), Some("up"));
            assert!(sel.matchers.is_empty());
            assert_eq!(sel.offset, 0);
            assert!(sel.at.is_none());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_08_parses_vector_selector_without_metric_name() {
    let expr = parse(r#"{job="api"}"#).unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert_eq!(sel.metric_name, None);
            assert_eq!(sel.matchers.len(), 1);
            assert_eq!(sel.matchers[0].name, "job");
            assert_eq!(sel.matchers[0].op, MatchOp::Equal);
            assert_eq!(sel.matchers[0].value, "api");
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_08b_rejects_metricless_selectors_that_can_match_empty() {
    for query in [
        "{}",
        r#"{job=""}"#,
        r#"{job!="api"}"#,
        r#"{job=~".*"}"#,
        r#"{job!~".+"}"#,
    ] {
        let err = parse(query).expect_err("metricless selector should be rejected");
        assert!(
            err.to_string().contains("does not match the empty string"),
            "unexpected error for {query}: {err}"
        );
    }

    assert!(parse(r#"{job!=""}"#).is_ok());
    assert!(parse(r#"{job=~".+"}"#).is_ok());
}

#[test]
fn parser_09_parses_all_label_matcher_operators() {
    let expr = parse(r#"up{a="1",b!="2",c=~"re",d!~"no"}"#).unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert_eq!(sel.matchers.len(), 4);
            assert_eq!(sel.matchers[0].op, MatchOp::Equal);
            assert_eq!(sel.matchers[1].op, MatchOp::NotEqual);
            assert_eq!(sel.matchers[2].op, MatchOp::RegexMatch);
            assert_eq!(sel.matchers[3].op, MatchOp::RegexNoMatch);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_10_allows_trailing_comma_in_label_matchers() {
    let expr = parse(r#"up{job="api",}"#).unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert_eq!(sel.matchers.len(), 1);
            assert_eq!(sel.matchers[0].name, "job");
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_11_parses_matrix_selector_with_range() {
    let expr = parse("up[5m]").unwrap();
    match expr {
        Expr::MatrixSelector(sel) => {
            assert_eq!(sel.range, 300_000);
            assert_eq!(sel.vector.metric_name.as_deref(), Some("up"));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_12_parses_vector_selector_with_offset() {
    let expr = parse("up offset 30s").unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert_eq!(sel.metric_name.as_deref(), Some("up"));
            assert_eq!(sel.offset, 30_000);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_13_parses_matrix_selector_with_offset() {
    let expr = parse("up[5m] offset 1m").unwrap();
    match expr {
        Expr::MatrixSelector(sel) => {
            assert_eq!(sel.range, 300_000);
            assert_eq!(sel.vector.offset, 60_000);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_14_rejects_range_selector_on_scalar() {
    let err = parse("1[5m]").unwrap_err();
    assert!(
        matches!(err, PromqlError::Type(msg) if msg.contains("range selectors can only apply to vectors"))
    );
}

#[test]
fn parser_15_rejects_offset_modifier_on_scalar() {
    let err = parse("1 offset 5m").unwrap_err();
    assert!(
        matches!(err, PromqlError::Type(msg) if msg.contains("offset modifier can only apply to vector/matrix selectors or subqueries"))
    );
}

#[test]
fn parser_16_binary_precedence_mul_binds_tighter_than_add() {
    let expr = parse("a + b * c").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Add);
            assert!(matches!(
                *b.rhs,
                Expr::Binary(ref rhs) if rhs.op == BinaryOp::Mul
            ));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_17_binary_precedence_comparison_binds_tighter_than_or() {
    let expr = parse("a == b or c == d").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Or);
            assert!(matches!(
                *b.lhs,
                Expr::Binary(ref lhs) if lhs.op == BinaryOp::Eq
            ));
            assert!(matches!(
                *b.rhs,
                Expr::Binary(ref rhs) if rhs.op == BinaryOp::Eq
            ));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_18_power_operator_is_right_associative() {
    let expr = parse("a ^ b ^ c").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Pow);
            assert!(matches!(
                *b.rhs,
                Expr::Binary(ref rhs) if rhs.op == BinaryOp::Pow
            ));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_19_comparison_supports_bool_modifier() {
    let expr = parse("up == bool 1").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Eq);
            assert!(b.return_bool);
            assert!(b.matching.is_none());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_20_bool_modifier_on_arithmetic_operator_is_rejected() {
    let err = parse("up + bool 1").unwrap_err();
    assert!(
        matches!(err, PromqlError::Parse(msg) if msg.contains("bool modifier can only be used with comparison operators"))
    );
}

#[test]
fn parser_21_parses_on_vector_matching_modifier() {
    let expr = parse("a / on(job,instance) b").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Div);
            let matching = b.matching.expect("matching should be present");
            assert!(matching.on);
            assert_eq!(matching.labels, vec!["job", "instance"]);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_22_parses_ignoring_vector_matching_with_empty_label_set() {
    let expr = parse("a and ignoring() b").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::And);
            let matching = b.matching.expect("matching should be present");
            assert!(!matching.on);
            assert!(matching.labels.is_empty());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_23_parses_aggregation_with_prefix_grouping() {
    let expr = parse("sum by (job,instance) (up)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            let grouping = agg.grouping.expect("grouping should be present");
            assert!(!grouping.without);
            assert_eq!(grouping.labels, vec!["job", "instance"]);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_24_parses_aggregation_with_postfix_grouping() {
    let expr = parse("sum(up) without (instance)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            let grouping = agg.grouping.expect("grouping should be present");
            assert!(grouping.without);
            assert_eq!(grouping.labels, vec!["instance"]);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_25_parses_topk_with_parameter_expression() {
    let expr = parse("topk(3, up)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::TopK);
            match agg.param {
                Some(param) => {
                    assert!(matches!(*param, Expr::NumberLiteral(v) if (v - 3.0).abs() < 1e-12));
                }
                None => panic!("topk should have a parameter"),
            }
            assert!(matches!(*agg.expr, Expr::VectorSelector(_)));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_26_parses_group_left_modifier_with_included_labels() {
    let expr = parse("a / on(job) group_left(team) b").unwrap();
    match expr {
        Expr::Binary(b) => {
            let matching = b.matching.expect("matching should be present");
            assert!(matching.on);
            assert_eq!(matching.labels, vec!["job"]);
            assert_eq!(matching.cardinality, VectorMatchCardinality::ManyToOne);
            assert_eq!(matching.include_labels, vec!["team"]);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_27_parses_group_right_modifier_without_matching_clause() {
    let expr = parse("a == group_right(region) b").unwrap();
    match expr {
        Expr::Binary(b) => {
            let matching = b.matching.expect("matching should be present");
            assert!(!matching.on);
            assert!(matching.labels.is_empty());
            assert_eq!(matching.cardinality, VectorMatchCardinality::OneToMany);
            assert_eq!(matching.include_labels, vec!["region"]);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_28_rejects_group_modifier_for_set_operator() {
    let err = parse("a and on(job) group_left(team) b").unwrap_err();
    assert!(
        matches!(err, PromqlError::Parse(msg) if msg.contains("group_left/group_right modifiers cannot be used with set operators"))
    );
}

#[test]
fn parser_29_parses_quantile_aggregation_with_scalar_parameter() {
    let expr = parse("quantile(0.9, up)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Quantile);
            match agg.param {
                Some(param) => {
                    assert!(matches!(*param, Expr::NumberLiteral(v) if (v - 0.9).abs() < 1e-12));
                }
                None => panic!("quantile should have a parameter"),
            }
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_30_parses_count_values_aggregation_with_string_parameter() {
    let expr = parse(r#"count_values("version", up)"#).unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::CountValues);
            match agg.param {
                Some(param) => {
                    assert!(matches!(*param, Expr::StringLiteral(ref s) if s == "version"));
                }
                None => panic!("count_values should have a parameter"),
            }
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_31_parses_limitk_with_scalar_parameter() {
    let expr = parse("limitk(2, up)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::LimitK);
            assert!(agg.param.is_some());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_32_parses_limit_ratio_with_scalar_parameter() {
    let expr = parse("limit_ratio(-0.5, up)").unwrap();
    match expr {
        Expr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::LimitRatio);
            assert!(agg.param.is_some());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_33_parses_at_modifier_with_numeric_timestamp() {
    let expr = parse("up @ 123.5").unwrap();
    match expr {
        Expr::VectorSelector(sel) => {
            assert!(matches!(sel.at, Some(AtModifier::Timestamp(v)) if (v - 123.5).abs() < 1e-12));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_34_parses_at_modifier_with_start_call_and_signed_offset() {
    let expr = parse("up[5m] @ start() offset -1m").unwrap();
    match expr {
        Expr::MatrixSelector(sel) => {
            assert!(matches!(sel.vector.at, Some(AtModifier::Start)));
            assert_eq!(sel.vector.offset, -60_000);
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_35_parses_subquery_with_explicit_step() {
    let expr = parse("rate(up[1m])[5m:30s]").unwrap();
    match expr {
        Expr::Subquery(subquery) => {
            assert_eq!(subquery.range, 300_000);
            assert_eq!(subquery.step, Some(30_000));
            assert_eq!(subquery.offset, 0);
            assert!(subquery.at.is_none());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_36_parses_subquery_with_default_step_and_end_modifier() {
    let expr = parse("up[5m:] @ end()").unwrap();
    match expr {
        Expr::Subquery(subquery) => {
            assert_eq!(subquery.range, 300_000);
            assert_eq!(subquery.step, None);
            assert!(matches!(subquery.at, Some(AtModifier::End)));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn parser_37_rejects_duplicate_at_modifier() {
    let err = parse("up @ 1 @ 2").unwrap_err();
    assert!(
        matches!(err, PromqlError::Parse(msg) if msg.contains("@ modifier can only be specified once"))
    );
}

#[test]
fn parser_38_parses_atan2_as_binary_operator() {
    let expr = parse("a atan2 b + c").unwrap();
    match expr {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Add);
            assert!(matches!(*b.lhs, Expr::Binary(ref lhs) if lhs.op == BinaryOp::Atan2));
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}
