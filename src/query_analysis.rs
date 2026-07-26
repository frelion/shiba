//! Stable, PostgreSQL-independent representation of an analyzed CTAS query.
//!
//! These types are also Shiba's registration wire protocol.  Serde attributes and
//! field names therefore intentionally preserve the version-1 JSON shape.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct AnalysisVersion(pub u32);

impl AnalysisVersion {
    pub const CURRENT: Self = Self(1);
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct RelationOid(pub u32);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ColumnNumber(pub i16);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Full,
    Right,
    Semi,
    Anti,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubqueryKind {
    Semi,
    Anti,
    NullAnti,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TargetExpression {
    Column,
    Aggregate,
    Window,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryAnalysis {
    pub version: AnalysisVersion,
    pub has_aggregates: bool,
    pub has_window_functions: bool,
    pub has_sublinks: bool,
    pub has_distinct: bool,
    pub has_distinct_on: bool,
    pub has_having: bool,
    pub has_set_operations: bool,
    pub has_ordering: bool,
    pub has_limit: bool,
    pub has_aggregate_filters: bool,
    pub has_window_filters: bool,
    pub limit_with_ties: bool,
    pub limit_count: Option<i64>,
    pub limit_offset: Option<i64>,
    pub group_keys: usize,
    pub sources: Vec<Source>,
    pub joins: Vec<Join>,
    pub subqueries: Vec<Subquery>,
    pub windows: Vec<WindowSpec>,
    pub ordering: Vec<OrderSpec>,
    pub targets: Vec<Target>,
    pub where_predicate: Option<PredicateAnalysis>,
    pub having_predicate: Option<PredicateAnalysis>,
    pub having_distinct_inputs: Vec<ColumnInput>,
    pub having_sum_inputs: Vec<ColumnInput>,
}

impl QueryAnalysis {
    /// Serialize the stable registration protocol consumed by SQL.
    pub fn to_wire_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Decode a persisted registration protocol value.
    pub fn from_wire_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }

    /// Classify the analyzed tree into the closed set of execution families.
    pub fn validate(self) -> Result<ValidatedQuery, ValidationError> {
        if self.version != AnalysisVersion::CURRENT {
            return Err(ValidationError::new(
                ValidationErrorKind::UnsupportedVersion,
                format!("unsupported query analysis version {}", self.version.0),
            ));
        }
        if self.has_aggregate_filters || self.has_window_filters {
            return Err(ValidationError::new(
                ValidationErrorKind::UnsupportedFeature,
                "aggregate and window FILTER clauses are not executable",
            ));
        }
        if self.limit_with_ties {
            return Err(ValidationError::new(
                ValidationErrorKind::UnsupportedFeature,
                "FETCH ... WITH TIES is not executable",
            ));
        }
        if self.has_set_operations {
            return Err(ValidationError::unsupported(
                "set operations are not executable",
            ));
        }

        if let Some(predicate) = [&self.where_predicate, &self.having_predicate]
            .into_iter()
            .flatten()
            .find(|predicate| predicate.error.is_some())
        {
            return Err(ValidationError::unsupported(
                predicate
                    .error
                    .as_deref()
                    .unwrap_or("unsupported predicate"),
            ));
        }

        if self.has_window_functions {
            if self.has_sublinks
                || self.has_aggregates
                || self.has_distinct
                || self.has_set_operations
                || self.has_limit
                || self.group_keys != 0
                || self.has_having
            {
                return Err(ValidationError::unsupported(
                    "window queries cannot combine windows with aggregates, DISTINCT, subqueries, set operations, LIMIT, GROUP BY, or HAVING",
                ));
            }
            let source = self.single_unjoined_source("window")?;
            let [window] = self.windows.as_slice() else {
                return Err(ValidationError::invalid(
                    "window queries require exactly one window specification",
                ));
            };
            if window.partition_keys != 1 || window.order_keys != 1 || window.frame_error.is_some()
            {
                return Err(ValidationError::invalid(
                    "window queries require one partition key, one order key, and a valid frame",
                ));
            }
            let partition_key =
                ColumnRef::ordinary(window.partition_table_oid, window.partition_column)?;
            let order_key = ColumnRef::ordinary(window.order_table_oid, window.order_column)?;
            if partition_key.relation != source || order_key.relation != source {
                return Err(ValidationError::invalid(
                    "window keys must reference the query source",
                ));
            }
            let mut outputs = Vec::new();
            let mut projects_partition_key = false;
            for target in self.targets.iter().filter(|target| !target.resjunk) {
                let name = target.name.clone().ok_or_else(|| {
                    ValidationError::invalid("every window output requires a name")
                })?;
                let expression = match target.expression {
                    TargetExpression::Column => {
                        let column =
                            ColumnRef::ordinary(target.origin_table_oid, target.origin_column)?;
                        if column.relation != source {
                            return Err(ValidationError::invalid(
                                "window projections must be ordinary source columns",
                            ));
                        }
                        projects_partition_key |= column == partition_key;
                        WindowOutput::Column(NamedColumnRef { name, column })
                    }
                    TargetExpression::Window => {
                        if target.window_ref != window.window_ref {
                            return Err(ValidationError::invalid(
                                "all window functions must use the same window specification",
                            ));
                        }
                        let function = target
                            .window_function
                            .as_deref()
                            .unwrap_or_default()
                            .to_ascii_lowercase();
                        let argument =
                            if matches!(function.as_str(), "row_number" | "rank" | "dense_rank")
                                && target.input_table_oid.0 == 0
                            {
                                WindowArgument::None
                            } else if function == "count" && target.window_star {
                                WindowArgument::Star
                            } else if matches!(
                                function.as_str(),
                                "count" | "sum" | "avg" | "min" | "max"
                            ) {
                                let input = ColumnRef::ordinary(
                                    target.input_table_oid,
                                    target.input_column,
                                )?;
                                if input.relation != source {
                                    return Err(ValidationError::invalid(
                                        "window function inputs must be ordinary source columns",
                                    ));
                                }
                                WindowArgument::Column(input)
                            } else {
                                return Err(ValidationError::unsupported(format!(
                                    "unsupported window function {function}"
                                )));
                            };
                        WindowOutput::Function {
                            name,
                            function,
                            argument,
                        }
                    }
                    _ => {
                        return Err(ValidationError::unsupported(
                            "window outputs must be source columns or supported window functions",
                        ));
                    }
                };
                outputs.push(expression);
            }
            if !projects_partition_key {
                return Err(ValidationError::invalid(
                    "the window PARTITION BY column must be projected",
                ));
            }
            self.validate_filter_sources(&[source], "window")?;
            Ok(ValidatedQuery::Window(ValidatedWindowQuery {
                analysis: self,
                source,
                partition_key,
                order_key,
                outputs,
            }))
        } else if self.has_distinct {
            if self.has_distinct_on
                || self.has_sublinks
                || self.has_aggregates
                || self.has_limit
                || self.group_keys != 0
                || self.has_having
            {
                return Err(ValidationError::unsupported(
                    "DISTINCT queries require plain DISTINCT columns without aggregates, subqueries, set operations, LIMIT, or GROUP BY",
                ));
            }
            let source = self.single_unjoined_source("DISTINCT")?;
            let outputs = self.ordinary_outputs(source, "DISTINCT")?;
            self.validate_filter_sources(&[source], "DISTINCT")?;
            Ok(ValidatedQuery::Distinct(DistinctQuery {
                analysis: self,
                source,
                outputs,
            }))
        } else if self.has_limit {
            if self.has_window_functions
                || self.has_sublinks
                || self.has_aggregates
                || self.has_distinct
                || self.group_keys != 0
                || self.has_having
            {
                return Err(ValidationError::unsupported(
                    "TopN queries cannot combine LIMIT with windows, subqueries, aggregates, DISTINCT, set operations, or GROUP BY",
                ));
            }
            let source = self.single_unjoined_source("TopN")?;
            let [ordering] = self.ordering.as_slice() else {
                return Err(ValidationError::invalid(
                    "TopN requires exactly one ordering key",
                ));
            };
            let order_key = ColumnRef::ordinary(ordering.table_oid, ordering.column)?;
            if order_key.relation != source {
                return Err(ValidationError::invalid(
                    "TopN ordering key must reference the query source",
                ));
            }
            let limit = self
                .limit_count
                .filter(|limit| *limit > 0)
                .ok_or_else(|| ValidationError::invalid("TopN requires a positive LIMIT"))?;
            let offset = self.limit_offset.unwrap_or(0);
            if offset < 0 {
                return Err(ValidationError::invalid(
                    "TopN requires a nonnegative OFFSET",
                ));
            }
            let outputs = self.ordinary_outputs(source, "TopN")?;
            self.validate_filter_sources(&[source], "TopN")?;
            Ok(ValidatedQuery::TopN(TopNQuery {
                analysis: self,
                source,
                order_key,
                limit,
                offset,
                outputs,
            }))
        } else if self.has_sublinks {
            let decorrelation = DecorrelatedSubquery::try_from_analysis(&self)?;
            let aggregate = self.validate_aggregate_shape(&[
                decorrelation.outer.relation,
                decorrelation.inner.relation,
            ])?;
            if aggregate.sum.relation != decorrelation.outer.relation {
                return Err(ValidationError::invalid(
                    "subquery SUM input must belong to the outer source",
                ));
            }
            if self
                .where_predicate
                .as_ref()
                .is_some_and(|predicate| !predicate.source_oids.is_empty())
            {
                self.validate_filter_sources(
                    &[decorrelation.outer.relation, decorrelation.inner.relation],
                    "subquery",
                )?;
            }
            Ok(ValidatedQuery::DecorrelatedSubquery {
                analysis: self,
                decorrelation,
                aggregate,
            })
        } else if !self.joins.is_empty() {
            if self.has_set_operations || self.has_ordering || self.has_limit {
                return Err(ValidationError::unsupported(
                    "aggregate joins cannot contain set operations, ORDER BY, or LIMIT",
                ));
            }
            if self.sources.len() != 2 || self.joins.len() != 1 {
                return Err(ValidationError::invalid(
                    "joins require exactly two relation sources and one join edge",
                ));
            }
            let edge = &self.joins[0];
            if edge.operator.as_deref() != Some("=") {
                return Err(ValidationError::unsupported(
                    "only equality joins are executable",
                ));
            }
            let left = ColumnRef::ordinary(edge.left_table_oid, edge.left_column)?;
            let right = ColumnRef::ordinary(edge.right_table_oid, edge.right_column)?;
            let kind = edge.kind;
            if kind == JoinKind::Other {
                return Err(ValidationError::unsupported(
                    "unsupported PostgreSQL join kind",
                ));
            }
            let source_oids = [self.sources[0].oid, self.sources[1].oid];
            if source_oids[0] == source_oids[1] {
                return Err(ValidationError::unsupported(
                    "self-joins are not executable",
                ));
            }
            if left.relation == right.relation
                || !source_oids.contains(&left.relation)
                || !source_oids.contains(&right.relation)
            {
                return Err(ValidationError::invalid(
                    "join edge must reference both relation sources",
                ));
            }
            let aggregate = self.validate_aggregate_shape(&source_oids)?;
            if aggregate.sum.relation != source_oids[0] {
                return Err(ValidationError::invalid(
                    "join SUM input must belong to the first (left) source",
                ));
            }
            self.validate_filter_sources(&source_oids, "join")?;
            Ok(ValidatedQuery::Join(ValidatedJoinQuery {
                analysis: self,
                kind,
                left,
                right,
                aggregate,
            }))
        } else {
            if self.has_set_operations || self.has_ordering {
                return Err(ValidationError::unsupported(
                    "aggregate queries cannot contain set operations or ORDER BY",
                ));
            }
            let source = self.single_unjoined_source("aggregate")?;
            let aggregate = self.validate_aggregate_shape(&[source])?;
            self.validate_filter_sources(&[source], "aggregate")?;
            Ok(ValidatedQuery::Aggregate(AggregateQuery {
                analysis: self,
                source,
                aggregate,
            }))
        }
    }

    fn single_unjoined_source(&self, family: &str) -> Result<RelationOid, ValidationError> {
        let [source] = self.sources.as_slice() else {
            return Err(ValidationError::invalid(format!(
                "{family} queries require exactly one relation source"
            )));
        };
        if !self.joins.is_empty() {
            return Err(ValidationError::invalid(format!(
                "{family} queries cannot contain a join edge"
            )));
        }
        Ok(source.oid)
    }

    fn ordinary_outputs(
        &self,
        source: RelationOid,
        family: &str,
    ) -> Result<Vec<NamedColumnRef>, ValidationError> {
        let outputs: Result<Vec<_>, _> = self
            .targets
            .iter()
            .filter(|target| !target.resjunk)
            .map(|target| {
                if target.expression != TargetExpression::Column {
                    return Err(ValidationError::invalid(format!(
                        "{family} outputs must be ordinary source columns"
                    )));
                }
                let column = ColumnRef::ordinary(target.origin_table_oid, target.origin_column)?;
                if column.relation != source {
                    return Err(ValidationError::invalid(format!(
                        "{family} output must reference its source"
                    )));
                }
                let name = target.name.clone().ok_or_else(|| {
                    ValidationError::invalid(format!("{family} output must have a name"))
                })?;
                Ok(NamedColumnRef { name, column })
            })
            .collect();
        let outputs = outputs?;
        if outputs.is_empty() {
            return Err(ValidationError::invalid(format!(
                "{family} requires at least one output"
            )));
        }
        Ok(outputs)
    }

    fn validate_aggregate_shape(
        &self,
        sources: &[RelationOid],
    ) -> Result<AggregateSpec, ValidationError> {
        if self.group_keys != 1 || self.targets.len() != 3 {
            return Err(ValidationError::invalid(
                "aggregate queries require one group key and exactly three targets",
            ));
        }
        let group_target = &self.targets[0];
        if group_target.expression != TargetExpression::Column
            || group_target.grouping_reference == 0
        {
            return Err(ValidationError::invalid(
                "the first aggregate target must be the projected group column",
            ));
        }
        let group = ColumnRef::ordinary(group_target.origin_table_oid, group_target.origin_column)?;
        let group_name = required_name(group_target, "group")?;
        if !sources.contains(&group.relation) {
            return Err(ValidationError::invalid(
                "the group column must belong to an input source",
            ));
        }

        let count_target = &self.targets[1];
        if count_target.expression != TargetExpression::Aggregate
            || !count_target
                .aggregate
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("count"))
        {
            return Err(ValidationError::invalid(
                "the second aggregate target must be COUNT",
            ));
        }
        let count = if count_target.aggregate_star && !count_target.aggregate_distinct {
            CountSpec::Star
        } else if !count_target.aggregate_star && count_target.aggregate_distinct {
            let input =
                ColumnRef::ordinary(count_target.input_table_oid, count_target.input_column)?;
            if !sources.contains(&input.relation) {
                return Err(ValidationError::invalid(
                    "COUNT(DISTINCT) input must belong to an input source",
                ));
            }
            CountSpec::Distinct(input)
        } else {
            return Err(ValidationError::unsupported(
                "COUNT must be COUNT(*) or COUNT(DISTINCT ordinary_column)",
            ));
        };
        let count_name = required_name(count_target, "COUNT")?;

        let sum_target = &self.targets[2];
        if sum_target.expression != TargetExpression::Aggregate
            || !sum_target
                .aggregate
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("sum"))
            || sum_target.aggregate_distinct
        {
            return Err(ValidationError::unsupported(
                "the third aggregate target must be SUM(column) without DISTINCT",
            ));
        }
        let sum = ColumnRef::ordinary(sum_target.input_table_oid, sum_target.input_column)?;
        if !sources.contains(&sum.relation) {
            return Err(ValidationError::invalid(
                "SUM input must belong to an input source",
            ));
        }
        let sum_name = required_name(sum_target, "SUM")?;

        validate_having_input(
            &self.having_distinct_inputs,
            match count {
                CountSpec::Distinct(column) => Some(column),
                CountSpec::Star => None,
            },
            "COUNT(DISTINCT)",
        )?;
        validate_having_input(&self.having_sum_inputs, Some(sum), "SUM")?;

        Ok(AggregateSpec {
            group: NamedColumnRef {
                name: group_name,
                column: group,
            },
            count_name,
            count,
            sum_name,
            sum,
        })
    }

    fn validate_filter_sources(
        &self,
        sources: &[RelationOid],
        family: &str,
    ) -> Result<(), ValidationError> {
        let Some(predicate) = &self.where_predicate else {
            return Ok(());
        };
        if predicate.sql.is_none() {
            return Ok(());
        }
        if predicate.source_oids.is_empty()
            || predicate.source_oids.len() > sources.len()
            || predicate
                .source_oids
                .iter()
                .any(|source| !sources.contains(source))
        {
            return Err(ValidationError::invalid(format!(
                "{family} filter must reference its input source(s)"
            )));
        }
        Ok(())
    }
}

fn required_name(target: &Target, role: &str) -> Result<String, ValidationError> {
    target
        .name
        .clone()
        .ok_or_else(|| ValidationError::invalid(format!("{role} output requires a name")))
}

fn validate_having_input(
    inputs: &[ColumnInput],
    expected: Option<ColumnRef>,
    role: &str,
) -> Result<(), ValidationError> {
    if inputs.is_empty() {
        return Ok(());
    }
    let [input] = inputs else {
        return Err(ValidationError::invalid(format!(
            "HAVING {role} must match the maintained SELECT aggregate"
        )));
    };
    let actual = ColumnRef::ordinary(input.table_oid, input.column)?;
    if Some(actual) != expected {
        return Err(ValidationError::invalid(format!(
            "HAVING {role} must match the maintained SELECT aggregate"
        )));
    }
    Ok(())
}

/// A query family accepted by the typed compiler boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedQuery {
    Aggregate(AggregateQuery),
    Join(ValidatedJoinQuery),
    DecorrelatedSubquery {
        analysis: QueryAnalysis,
        decorrelation: DecorrelatedSubquery,
        aggregate: AggregateSpec,
    },
    Window(ValidatedWindowQuery),
    Distinct(DistinctQuery),
    TopN(TopNQuery),
}

impl ValidatedQuery {
    pub fn analysis(&self) -> &QueryAnalysis {
        match self {
            Self::Aggregate(spec) => &spec.analysis,
            Self::Join(spec) => &spec.analysis,
            Self::Window(spec) => &spec.analysis,
            Self::Distinct(spec) => &spec.analysis,
            Self::TopN(spec) => &spec.analysis,
            Self::DecorrelatedSubquery { analysis, .. } => analysis,
        }
    }

    /// The complete, deduplicated set of base relations that must be locked
    /// before PostgreSQL materializes the CTAS result.
    pub fn sources(&self) -> Vec<RelationOid> {
        let mut sources: Vec<_> = match self {
            Self::DecorrelatedSubquery { decorrelation, .. } => {
                vec![decorrelation.outer.relation, decorrelation.inner.relation]
            }
            _ => self
                .analysis()
                .sources
                .iter()
                .map(|source| source.oid)
                .collect(),
        };
        sources.sort_unstable();
        sources.dedup();
        sources
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrdinaryColumnNumber(i16);

impl OrdinaryColumnNumber {
    pub fn get(self) -> i16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnRef {
    pub relation: RelationOid,
    pub column: OrdinaryColumnNumber,
}

impl ColumnRef {
    fn ordinary(relation: RelationOid, column: ColumnNumber) -> Result<Self, ValidationError> {
        if relation.0 == 0 || column.0 <= 0 {
            return Err(ValidationError::invalid(
                "ordinary columns require a relation OID and positive attribute number",
            ));
        }
        Ok(Self {
            relation,
            column: OrdinaryColumnNumber(column.0),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedColumnRef {
    pub name: String,
    pub column: ColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateQuery {
    pub analysis: QueryAnalysis,
    pub source: RelationOid,
    pub aggregate: AggregateSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedJoinQuery {
    pub analysis: QueryAnalysis,
    pub kind: JoinKind,
    pub left: ColumnRef,
    pub right: ColumnRef,
    pub aggregate: AggregateSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedWindowQuery {
    pub analysis: QueryAnalysis,
    pub source: RelationOid,
    pub partition_key: ColumnRef,
    pub order_key: ColumnRef,
    pub outputs: Vec<WindowOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctQuery {
    pub analysis: QueryAnalysis,
    pub source: RelationOid,
    pub outputs: Vec<NamedColumnRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopNQuery {
    pub analysis: QueryAnalysis,
    pub source: RelationOid,
    pub order_key: ColumnRef,
    pub limit: i64,
    pub offset: i64,
    pub outputs: Vec<NamedColumnRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountSpec {
    Star,
    Distinct(ColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    pub group: NamedColumnRef,
    pub count_name: String,
    pub count: CountSpec,
    pub sum_name: String,
    pub sum: ColumnRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowArgument {
    None,
    Star,
    Column(ColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowOutput {
    Column(NamedColumnRef),
    Function {
        name: String,
        function: String,
        argument: WindowArgument,
    },
}

/// The explicit join edge produced when a supported subquery is decorrelated.
///
/// Keeping this separate from `Join` records that the edge came from a
/// semi/anti subquery and prevents later stages from reconstructing it from JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecorrelatedSubquery {
    pub kind: SubqueryKind,
    pub outer: ColumnRef,
    pub inner: ColumnRef,
}

impl DecorrelatedSubquery {
    fn try_from_analysis(analysis: &QueryAnalysis) -> Result<Self, ValidationError> {
        let [outer_source] = analysis.sources.as_slice() else {
            return Err(ValidationError::invalid(
                "a decorrelatable query requires exactly one outer relation source",
            ));
        };
        let [subquery] = analysis.subqueries.as_slice() else {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidShape,
                "a decorrelatable query must contain exactly one subquery edge",
            ));
        };
        let outer = ColumnRef::ordinary(subquery.left_table_oid, subquery.left_column)?;
        let inner = ColumnRef::ordinary(subquery.right_table_oid, subquery.right_column)?;
        if inner.relation != subquery.source_oid {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidShape,
                "subquery inner edge does not reference its source",
            ));
        }
        if outer.relation != outer_source.oid || outer.relation == inner.relation {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidShape,
                "subquery outer edge does not reference an outer source",
            ));
        }
        Ok(Self {
            kind: subquery.kind,
            outer,
            inner,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationErrorKind {
    UnsupportedVersion,
    UnsupportedFeature,
    InvalidShape,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub kind: ValidationErrorKind,
    pub message: String,
}

impl ValidationError {
    fn new(kind: ValidationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ValidationErrorKind::UnsupportedFeature, message)
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(ValidationErrorKind::InvalidShape, message)
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ValidationError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PredicateAnalysis {
    pub sql: Option<String>,
    pub source_oids: Vec<RelationOid>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnInput {
    pub table_oid: RelationOid,
    pub column: ColumnNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Source {
    pub oid: RelationOid,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Join {
    pub kind: JoinKind,
    pub operator: Option<String>,
    pub left_table_oid: RelationOid,
    pub left_column: ColumnNumber,
    pub right_table_oid: RelationOid,
    pub right_column: ColumnNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subquery {
    pub kind: SubqueryKind,
    pub source_oid: RelationOid,
    pub left_table_oid: RelationOid,
    pub left_column: ColumnNumber,
    pub right_table_oid: RelationOid,
    pub right_column: ColumnNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Target {
    pub name: Option<String>,
    pub expression: TargetExpression,
    pub type_oid: RelationOid,
    pub origin_table_oid: RelationOid,
    pub origin_column: ColumnNumber,
    pub grouping_reference: u32,
    pub aggregate: Option<String>,
    pub aggregate_star: bool,
    pub aggregate_distinct: bool,
    pub input_table_oid: RelationOid,
    pub input_column: ColumnNumber,
    pub resjunk: bool,
    pub window_function: Option<String>,
    pub window_star: bool,
    pub window_ref: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WindowSpec {
    pub window_ref: u32,
    pub partition_keys: usize,
    pub order_keys: usize,
    pub partition_table_oid: RelationOid,
    pub partition_column: ColumnNumber,
    pub order_table_oid: RelationOid,
    pub order_column: ColumnNumber,
    pub order_direction: SortDirection,
    pub nulls_first: bool,
    pub frame_options: i32,
    pub frame_clause: Option<String>,
    pub frame_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderSpec {
    pub table_oid: RelationOid,
    pub column: ColumnNumber,
    pub direction: SortDirection,
    pub nulls_first: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1_FIXTURE: &str = r#"{"version":1,"has_aggregates":false,"has_window_functions":false,"has_sublinks":false,"has_distinct":false,"has_distinct_on":false,"has_having":false,"has_set_operations":false,"has_ordering":true,"has_limit":true,"has_aggregate_filters":false,"has_window_filters":false,"limit_with_ties":false,"limit_count":10,"limit_offset":0,"group_keys":0,"sources":[{"oid":42,"alias":"events"}],"joins":[{"kind":"left","operator":"=","left_table_oid":42,"left_column":1,"right_table_oid":43,"right_column":2}],"subqueries":[{"kind":"null_anti","source_oid":44,"left_table_oid":42,"left_column":1,"right_table_oid":44,"right_column":1}],"windows":[],"ordering":[{"table_oid":42,"column":1,"direction":"desc","nulls_first":true}],"targets":[{"name":"id","expression":"column","type_oid":23,"origin_table_oid":42,"origin_column":1,"grouping_reference":1,"aggregate":null,"aggregate_star":false,"aggregate_distinct":false,"input_table_oid":0,"input_column":0,"resjunk":false,"window_function":null,"window_star":false,"window_ref":0}],"where_predicate":null,"having_predicate":null,"having_distinct_inputs":[],"having_sum_inputs":[]}"#;

    #[test]
    fn version_one_wire_shape_is_stable() {
        let analysis = QueryAnalysis::from_wire_json(V1_FIXTURE).unwrap();
        assert_eq!(analysis.version, AnalysisVersion::CURRENT);
        assert_eq!(analysis.joins[0].kind, JoinKind::Left);
        assert_eq!(analysis.subqueries[0].kind, SubqueryKind::NullAnti);
        assert_eq!(analysis.ordering[0].direction, SortDirection::Desc);
        assert_eq!(analysis.to_wire_json().unwrap(), V1_FIXTURE);
    }

    #[test]
    fn analysis_json_round_trips_without_losing_types() {
        let analysis = QueryAnalysis::from_wire_json(V1_FIXTURE).unwrap();
        let round_trip = QueryAnalysis::from_wire_json(&analysis.to_wire_json().unwrap()).unwrap();
        assert_eq!(round_trip, analysis);
    }

    fn empty_analysis() -> QueryAnalysis {
        QueryAnalysis {
            version: AnalysisVersion::CURRENT,
            has_aggregates: false,
            has_window_functions: false,
            has_sublinks: false,
            has_distinct: false,
            has_distinct_on: false,
            has_having: false,
            has_set_operations: false,
            has_ordering: false,
            has_limit: false,
            has_aggregate_filters: false,
            has_window_filters: false,
            limit_with_ties: false,
            limit_count: None,
            limit_offset: None,
            group_keys: 0,
            sources: vec![Source {
                oid: RelationOid(42),
                alias: Some("events".into()),
            }],
            joins: vec![],
            subqueries: vec![],
            windows: vec![],
            ordering: vec![],
            targets: vec![],
            where_predicate: None,
            having_predicate: None,
            having_distinct_inputs: vec![],
            having_sum_inputs: vec![],
        }
    }

    fn column_target(name: &str, relation: u32, column: i16) -> Target {
        Target {
            name: Some(name.into()),
            expression: TargetExpression::Column,
            type_oid: RelationOid(23),
            origin_table_oid: RelationOid(relation),
            origin_column: ColumnNumber(column),
            grouping_reference: 0,
            aggregate: None,
            aggregate_star: false,
            aggregate_distinct: false,
            input_table_oid: RelationOid(0),
            input_column: ColumnNumber(0),
            resjunk: false,
            window_function: None,
            window_star: false,
            window_ref: 0,
        }
    }

    fn aggregate_analysis() -> QueryAnalysis {
        let mut analysis = empty_analysis();
        analysis.has_aggregates = true;
        analysis.group_keys = 1;
        let mut group = column_target("group_id", 42, 1);
        group.grouping_reference = 1;
        let mut count = column_target("row_count", 0, 0);
        count.expression = TargetExpression::Aggregate;
        count.aggregate = Some("count".into());
        count.aggregate_star = true;
        let mut sum = column_target("total", 0, 0);
        sum.expression = TargetExpression::Aggregate;
        sum.aggregate = Some("sum".into());
        sum.input_table_oid = RelationOid(42);
        sum.input_column = ColumnNumber(2);
        analysis.targets = vec![group, count, sum];
        analysis
    }

    #[test]
    fn validation_classifies_topn_without_string_dispatch() {
        let mut analysis = empty_analysis();
        analysis.has_ordering = true;
        analysis.has_limit = true;
        analysis.limit_count = Some(10);
        analysis.limit_offset = Some(0);
        analysis.ordering.push(OrderSpec {
            table_oid: RelationOid(42),
            column: ColumnNumber(1),
            direction: SortDirection::Desc,
            nulls_first: true,
        });
        analysis.targets.push(column_target("id", 42, 1));
        let validated = analysis.validate().unwrap();
        assert!(matches!(validated, ValidatedQuery::TopN(_)));
    }

    #[test]
    fn all_sql_registration_shapes_have_typed_variants() {
        let aggregate = aggregate_analysis();
        assert!(matches!(
            aggregate.validate().unwrap(),
            ValidatedQuery::Aggregate(_)
        ));
        let mut distinct_count = aggregate_analysis();
        distinct_count.targets[1].aggregate_star = false;
        distinct_count.targets[1].aggregate_distinct = true;
        distinct_count.targets[1].input_table_oid = RelationOid(42);
        distinct_count.targets[1].input_column = ColumnNumber(3);
        distinct_count.having_distinct_inputs.push(ColumnInput {
            table_oid: RelationOid(42),
            column: ColumnNumber(3),
        });
        assert!(matches!(
            distinct_count.validate().unwrap(),
            ValidatedQuery::Aggregate(_)
        ));

        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::Semi,
            JoinKind::Anti,
        ] {
            let mut join = aggregate_analysis();
            join.sources.push(Source {
                oid: RelationOid(43),
                alias: Some("dimensions".into()),
            });
            join.joins.push(Join {
                kind,
                operator: Some("=".into()),
                left_table_oid: RelationOid(42),
                left_column: ColumnNumber(1),
                right_table_oid: RelationOid(43),
                right_column: ColumnNumber(1),
            });
            assert!(matches!(join.validate().unwrap(), ValidatedQuery::Join(_)));
        }

        for kind in [
            SubqueryKind::Semi,
            SubqueryKind::Anti,
            SubqueryKind::NullAnti,
        ] {
            let mut subquery = aggregate_analysis();
            subquery.has_sublinks = true;
            subquery.where_predicate = Some(PredicateAnalysis {
                sql: Some("true".into()),
                source_oids: vec![],
                error: None,
            });
            subquery.subqueries.push(Subquery {
                kind,
                source_oid: RelationOid(44),
                left_table_oid: RelationOid(42),
                left_column: ColumnNumber(1),
                right_table_oid: RelationOid(44),
                right_column: ColumnNumber(1),
            });
            let validated = subquery.validate().unwrap();
            assert_eq!(validated.sources(), vec![RelationOid(42), RelationOid(44)]);
            let ValidatedQuery::DecorrelatedSubquery { decorrelation, .. } = validated else {
                panic!("expected a decorrelated subquery");
            };
            assert_eq!(decorrelation.kind, kind);
            assert_eq!(decorrelation.outer.relation, RelationOid(42));
            assert_eq!(decorrelation.inner.relation, RelationOid(44));
        }

        let mut distinct = empty_analysis();
        distinct.has_distinct = true;
        distinct.targets.push(column_target("id", 42, 1));
        assert!(matches!(
            distinct.validate().unwrap(),
            ValidatedQuery::Distinct(_)
        ));

        let mut window = empty_analysis();
        window.has_window_functions = true;
        window.windows.push(WindowSpec {
            window_ref: 1,
            partition_keys: 1,
            order_keys: 1,
            partition_table_oid: RelationOid(42),
            partition_column: ColumnNumber(1),
            order_table_oid: RelationOid(42),
            order_column: ColumnNumber(2),
            order_direction: SortDirection::Asc,
            nulls_first: false,
            frame_options: 0,
            frame_clause: None,
            frame_error: None,
        });
        window.targets.push(column_target("group_id", 42, 1));
        let mut rank = column_target("position", 0, 0);
        rank.expression = TargetExpression::Window;
        rank.window_function = Some("rank".into());
        rank.window_ref = 1;
        window.targets.push(rank);
        for function in ["row_number", "dense_rank"] {
            let mut target = column_target(function, 0, 0);
            target.expression = TargetExpression::Window;
            target.window_function = Some(function.into());
            target.window_ref = 1;
            window.targets.push(target);
        }
        let mut count_star = column_target("count_all", 0, 0);
        count_star.expression = TargetExpression::Window;
        count_star.window_function = Some("count".into());
        count_star.window_star = true;
        count_star.window_ref = 1;
        window.targets.push(count_star);
        for function in ["count", "sum", "avg", "min", "max"] {
            let mut target = column_target(&format!("{function}_value"), 0, 0);
            target.expression = TargetExpression::Window;
            target.window_function = Some(function.into());
            target.input_table_oid = RelationOid(42);
            target.input_column = ColumnNumber(2);
            target.window_ref = 1;
            window.targets.push(target);
        }
        assert!(matches!(
            window.validate().unwrap(),
            ValidatedQuery::Window(_)
        ));
    }

    #[test]
    fn invalid_shapes_are_rejected_before_registration() {
        let mut other_join = aggregate_analysis();
        other_join.sources.push(Source {
            oid: RelationOid(43),
            alias: None,
        });
        other_join.joins.push(Join {
            kind: JoinKind::Other,
            operator: Some("=".into()),
            left_table_oid: RelationOid(42),
            left_column: ColumnNumber(1),
            right_table_oid: RelationOid(43),
            right_column: ColumnNumber(1),
        });

        let mut same_side_join = other_join.clone();
        same_side_join.joins[0].kind = JoinKind::Inner;
        same_side_join.joins[0].right_table_oid = RelationOid(42);

        let mut bad_aggregate = aggregate_analysis();
        bad_aggregate.targets[2].aggregate_distinct = true;

        let mut bad_distinct = empty_analysis();
        bad_distinct.has_distinct = true;
        bad_distinct.has_distinct_on = true;
        bad_distinct.targets.push(column_target("id", 42, 1));

        let mut bad_topn = empty_analysis();
        bad_topn.has_limit = true;
        bad_topn.limit_count = Some(0);
        bad_topn.ordering.push(OrderSpec {
            table_oid: RelationOid(42),
            column: ColumnNumber(1),
            direction: SortDirection::Asc,
            nulls_first: false,
        });
        bad_topn.targets.push(column_target("id", 42, 1));

        let mut bad_window = empty_analysis();
        bad_window.has_window_functions = true;
        bad_window.windows.push(WindowSpec {
            window_ref: 1,
            partition_keys: 2,
            order_keys: 1,
            partition_table_oid: RelationOid(42),
            partition_column: ColumnNumber(1),
            order_table_oid: RelationOid(42),
            order_column: ColumnNumber(2),
            order_direction: SortDirection::Asc,
            nulls_first: false,
            frame_options: 0,
            frame_clause: None,
            frame_error: None,
        });

        for (name, analysis) in [
            ("other join", other_join),
            ("same-side join edge", same_side_join),
            ("distinct sum", bad_aggregate),
            ("distinct on", bad_distinct),
            ("zero limit", bad_topn),
            ("two partition keys", bad_window),
        ] {
            assert!(analysis.validate().is_err(), "{name} should be rejected");
        }
    }
}
