use serde::Serialize;

/// データベース変更に対するフィルター条件
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseFilter {
    /// フィルター対象のカラム名
    pub column: String,
    /// 比較演算子
    pub operator: FilterOperator,
    /// 比較する値
    pub value: serde_json::Value,
}

/// フィルター演算子
#[derive(Debug, Clone, PartialEq, Eq, Serialize)] // Added Eq
pub enum FilterOperator {
    /// 等しい
    Eq,
    /// 等しくない
    Neq,
    /// より大きい
    Gt,
    /// より大きいか等しい
    Gte,
    /// より小さい
    Lt,
    /// より小さいか等しい
    Lte,
    /// 含む
    In,
    // Note: Removed duplicates and less common/clear operators for now
    // NotIn,
    // ContainedBy,
    // Contains,
    // ContainedByArray,
    // Like,
    // ILike,
}

impl std::fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FilterOperator::Eq => "eq",
            FilterOperator::Neq => "neq",
            FilterOperator::Gt => "gt",
            FilterOperator::Gte => "gte",
            FilterOperator::Lt => "lt",
            FilterOperator::Lte => "lte",
            FilterOperator::In => "in",
            // FilterOperator::NotIn => "not.in",
            // FilterOperator::ContainedBy => "contained_by",
            // FilterOperator::Contains => "contains",
            // FilterOperator::ContainedByArray => "contained_by_array",
            // FilterOperator::Like => "like",
            // FilterOperator::ILike => "ilike",
        };
        write!(f, "{s}")
    }
}
