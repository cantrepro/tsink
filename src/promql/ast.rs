#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    NumberLiteral(f64),
    StringLiteral(String),
    VectorSelector(VectorSelector),
    MatrixSelector(MatrixSelector),
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Aggregation(AggregationExpr),
    Call(CallExpr),
    Paren(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSelector {
    pub metric_name: Option<String>,
    pub matchers: Vec<LabelMatcher>,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSelector {
    pub vector: VectorSelector,
    pub range: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexMatch,
    RegexNoMatch,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub op: UnaryOp,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: BinaryOp,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
    pub return_bool: bool,
    pub matching: Option<VectorMatching>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    NotEq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Unless,
}

impl BinaryOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::Gt | Self::Lte | Self::Gte
        )
    }

    pub fn is_set(self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorMatching {
    pub on: bool,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregationExpr {
    pub op: AggregationOp,
    pub expr: Box<Expr>,
    pub param: Option<Box<Expr>>,
    pub grouping: Option<Grouping>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    TopK,
    BottomK,
}

impl AggregationOp {
    pub fn from_ident(ident: &str) -> Option<Self> {
        match ident {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            "topk" => Some(Self::TopK),
            "bottomk" => Some(Self::BottomK),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Grouping {
    pub without: bool,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallExpr {
    pub func: String,
    pub args: Vec<Expr>,
}
