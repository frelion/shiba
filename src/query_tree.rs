//! Extraction of stable facts from PostgreSQL's analyzed CTAS `Query`.

use pgrx::pg_sys;
use std::collections::BTreeSet;
use std::ffi::CStr;

use crate::query_analysis::{
    AnalysisVersion, ColumnInput, ColumnNumber, Join, JoinKind, OrderSpec, PredicateAnalysis,
    QueryAnalysis, RelationOid, SortDirection, Source, Subquery, SubqueryKind, Target,
    TargetExpression, ValidatedQuery, ValidationError, WindowSpec,
};

fn one_based_list_index(number: i32, length: i32) -> Option<i32> {
    (number > 0 && number <= length).then(|| number - 1)
}

fn is_cast_function_format(format: pg_sys::CoercionForm::Type) -> bool {
    matches!(
        format,
        pg_sys::CoercionForm::COERCE_EXPLICIT_CAST | pg_sys::CoercionForm::COERCE_IMPLICIT_CAST
    )
}

/// # Safety
/// `pointer` must be null or point to a live, NUL-terminated PostgreSQL string.
unsafe fn optional_c_string(pointer: *const std::ffi::c_char) -> Option<String> {
    (!pointer.is_null()).then(|| CStr::from_ptr(pointer).to_string_lossy().into_owned())
}

/// # Safety
/// `node` must be null or a live PostgreSQL expression node in the current memory context.
unsafe fn integer_constant(node: *mut pg_sys::Node) -> Option<i64> {
    if node.is_null() {
        return None;
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = node.cast::<pg_sys::RelabelType>();
            return integer_constant((*relabel).arg.cast());
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            let coercion = node.cast::<pg_sys::CoerceViaIO>();
            return integer_constant((*coercion).arg.cast());
        }
        pg_sys::NodeTag::T_CoerceToDomain => {
            let coercion = node.cast::<pg_sys::CoerceToDomain>();
            return integer_constant((*coercion).arg.cast());
        }
        pg_sys::NodeTag::T_FuncExpr => {
            let coercion = node.cast::<pg_sys::FuncExpr>();
            if !is_cast_function_format((*coercion).funcformat) {
                return None;
            }
            let arguments: Vec<_> = list_items((*coercion).args).collect();
            let [argument] = arguments.as_slice() else {
                return None;
            };
            return integer_constant((*argument).cast());
        }
        pg_sys::NodeTag::T_Const => {}
        _ => return None,
    }
    let constant = node.cast::<pg_sys::Const>();
    if (*constant).constisnull {
        return None;
    }
    let mut output_function = pg_sys::InvalidOid;
    let mut is_varlena = false;
    pg_sys::getTypeOutputInfo((*constant).consttype, &mut output_function, &mut is_varlena);
    let output = pg_sys::OidOutputFunctionCall(output_function, (*constant).constvalue);
    CStr::from_ptr(output).to_string_lossy().parse().ok()
}

/// # Safety
/// `window` must point to a live analyzed PostgreSQL `WindowClause`.
unsafe fn window_frame_clause(
    window: *mut pg_sys::WindowClause,
) -> (Option<String>, Option<String>) {
    let options = (*window).frameOptions as u32;
    if options & pg_sys::FRAMEOPTION_NONDEFAULT == 0 {
        return (None, None);
    }
    let mode = if options & pg_sys::FRAMEOPTION_ROWS != 0 {
        "ROWS"
    } else if options & pg_sys::FRAMEOPTION_GROUPS != 0 {
        "GROUPS"
    } else {
        "RANGE"
    };
    let boundary = |start: bool| -> Result<String, String> {
        let unbounded_preceding = if start {
            pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING
        } else {
            pg_sys::FRAMEOPTION_END_UNBOUNDED_PRECEDING
        };
        let unbounded_following = if start {
            pg_sys::FRAMEOPTION_START_UNBOUNDED_FOLLOWING
        } else {
            pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING
        };
        let current = if start {
            pg_sys::FRAMEOPTION_START_CURRENT_ROW
        } else {
            pg_sys::FRAMEOPTION_END_CURRENT_ROW
        };
        let offset_preceding = if start {
            pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING
        } else {
            pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING
        };
        let offset_following = if start {
            pg_sys::FRAMEOPTION_START_OFFSET_FOLLOWING
        } else {
            pg_sys::FRAMEOPTION_END_OFFSET_FOLLOWING
        };
        if options & unbounded_preceding != 0 {
            Ok("UNBOUNDED PRECEDING".into())
        } else if options & unbounded_following != 0 {
            Ok("UNBOUNDED FOLLOWING".into())
        } else if options & current != 0 {
            Ok("CURRENT ROW".into())
        } else if options & (offset_preceding | offset_following) != 0 {
            let offset_node = if start {
                (*window).startOffset
            } else {
                (*window).endOffset
            };
            let offset = integer_constant(offset_node)
                .filter(|offset| *offset >= 0)
                .ok_or_else(|| {
                    "window frame offsets must be nonnegative integer constants".to_string()
                })?;
            Ok(format!(
                "{offset} {}",
                if options & offset_preceding != 0 {
                    "PRECEDING"
                } else {
                    "FOLLOWING"
                }
            ))
        } else {
            Err("unsupported PostgreSQL window frame boundary".into())
        }
    };
    let start = match boundary(true) {
        Ok(value) => value,
        Err(error) => return (None, Some(error)),
    };
    let mut clause = if options & pg_sys::FRAMEOPTION_BETWEEN != 0 {
        let end = match boundary(false) {
            Ok(value) => value,
            Err(error) => return (None, Some(error)),
        };
        format!("{mode} BETWEEN {start} AND {end}")
    } else {
        format!("{mode} {start}")
    };
    if options & pg_sys::FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        clause.push_str(" EXCLUDE CURRENT ROW");
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_GROUP != 0 {
        clause.push_str(" EXCLUDE GROUP");
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_TIES != 0 {
        clause.push_str(" EXCLUDE TIES");
    }
    (Some(clause), None)
}

/// # Safety
/// `list` must be null or a live PostgreSQL list that outlives the returned iterator.
unsafe fn list_items(list: *mut pg_sys::List) -> impl Iterator<Item = *mut std::ffi::c_void> {
    let length = pg_sys::list_length(list);
    (0..length).map(move |index| pg_sys::list_nth(list, index))
}

/// # Safety
/// `pstmt` must be null or point to a live `PlannedStmt` supplied by PostgreSQL.
unsafe fn analyzed_query(pstmt: *mut pg_sys::PlannedStmt) -> Option<*mut pg_sys::Query> {
    if pstmt.is_null() || (*pstmt).utilityStmt.is_null() {
        return None;
    }
    let utility = (*pstmt).utilityStmt;
    if (*utility).type_ != pg_sys::NodeTag::T_CreateTableAsStmt {
        return None;
    }
    let ctas = utility.cast::<pg_sys::CreateTableAsStmt>();
    if (*ctas).query.is_null() || (*(*ctas).query).type_ != pg_sys::NodeTag::T_Query {
        return None;
    }
    Some((*ctas).query.cast::<pg_sys::Query>())
}

/// # Safety
/// `query` and `node` must belong to the same live analyzed PostgreSQL query tree.
unsafe fn var_origin(query: *mut pg_sys::Query, node: *mut pg_sys::Node) -> (u32, i16) {
    if query.is_null() || node.is_null() || (*node).type_ != pg_sys::NodeTag::T_Var {
        return (0, 0);
    }
    let variable = node.cast::<pg_sys::Var>();
    let Some(rte_index) =
        one_based_list_index((*variable).varno, pg_sys::list_length((*query).rtable))
    else {
        return (0, 0);
    };
    let rte = pg_sys::list_nth((*query).rtable, rte_index).cast::<pg_sys::RangeTblEntry>();
    if rte.is_null() {
        return (0, 0);
    }
    if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
        return (0, 0);
    }
    ((*rte).relid.to_u32(), (*variable).varattno)
}

/// # Safety
/// All pointers must belong to the same live outer/inner analyzed query tree.
unsafe fn scoped_var_origin(
    query: *mut pg_sys::Query,
    outer_query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
) -> (u32, i16, bool) {
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_Var {
        return (0, 0, false);
    }
    let variable = node.cast::<pg_sys::Var>();
    let (scope, is_outer) = match (*variable).varlevelsup {
        0 => (query, false),
        1 => (outer_query, true),
        _ => return (0, 0, false),
    };
    if scope.is_null() {
        return (0, 0, false);
    }
    let Some(rte_index) =
        one_based_list_index((*variable).varno, pg_sys::list_length((*scope).rtable))
    else {
        return (0, 0, false);
    };
    let rte = pg_sys::list_nth((*scope).rtable, rte_index).cast::<pg_sys::RangeTblEntry>();
    if rte.is_null() {
        return (0, 0, false);
    }
    if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
        return (0, 0, false);
    }
    ((*rte).relid.to_u32(), (*variable).varattno, is_outer)
}

/// # Safety
/// `outer_query` and `node` must be live nodes in the same analyzed query tree,
/// and `node` must point to a `SubLink`.
unsafe fn parse_sublink(
    outer_query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
    negated: bool,
) -> Result<Subquery, String> {
    let link = node.cast::<pg_sys::SubLink>();
    if (*link).subselect.is_null() || (*(*link).subselect).type_ != pg_sys::NodeTag::T_Query {
        return Err("subquery is not an analyzed PostgreSQL Query".into());
    }
    let query = (*link).subselect.cast::<pg_sys::Query>();
    let inner_sources: Vec<_> = list_items((*query).rtable)
        .map(|item| item.cast::<pg_sys::RangeTblEntry>())
        .filter(|rte| (**rte).rtekind == pg_sys::RTEKind::RTE_RELATION)
        .collect();
    if inner_sources.len() != 1
        || (*query).hasAggs
        || (*query).hasWindowFuncs
        || (*query).hasSubLinks
        || !(*query).groupClause.is_null()
        || !(*query).havingQual.is_null()
        || !(*query).limitCount.is_null()
        || !(*query).limitOffset.is_null()
    {
        return Err("Shiba semi/anti subqueries require one unaggregated relation source".into());
    }
    let source_oid = (*inner_sources[0]).relid.to_u32();
    match (*link).subLinkType {
        pg_sys::SubLinkType::EXISTS_SUBLINK => {
            let qualification = (*(*query).jointree).quals;
            if qualification.is_null() || (*qualification).type_ != pg_sys::NodeTag::T_OpExpr {
                return Err("EXISTS requires one correlated equality predicate".into());
            }
            let operation = qualification.cast::<pg_sys::OpExpr>();
            let operator = optional_c_string(pg_sys::get_opname((*operation).opno));
            let arguments: Vec<_> = list_items((*operation).args).collect();
            if operator.as_deref() != Some("=") || arguments.len() != 2 {
                return Err("EXISTS correlation must be one equality comparison".into());
            }
            let first = scoped_var_origin(query, outer_query, arguments[0].cast());
            let second = scoped_var_origin(query, outer_query, arguments[1].cast());
            let (outer, inner) = match (first.2, second.2) {
                (true, false) => (first, second),
                (false, true) => (second, first),
                _ => return Err("EXISTS equality must correlate outer and inner columns".into()),
            };
            if inner.0 != source_oid || outer.0 == 0 || inner.1 <= 0 || outer.1 <= 0 {
                return Err("EXISTS equality does not reference ordinary source columns".into());
            }
            Ok(Subquery {
                kind: if negated {
                    SubqueryKind::Anti
                } else {
                    SubqueryKind::Semi
                },
                source_oid: RelationOid(source_oid),
                left_table_oid: RelationOid(outer.0),
                left_column: ColumnNumber(outer.1),
                right_table_oid: RelationOid(inner.0),
                right_column: ColumnNumber(inner.1),
            })
        }
        pg_sys::SubLinkType::ANY_SUBLINK => {
            if !(*query).jointree.is_null() && !(*(*query).jointree).quals.is_null() {
                return Err("IN/NOT IN subquery predicates are not yet executable by Shiba".into());
            }
            if (*link).testexpr.is_null() || (*(*link).testexpr).type_ != pg_sys::NodeTag::T_OpExpr
            {
                return Err("IN requires one equality test expression".into());
            }
            let operation = (*link).testexpr.cast::<pg_sys::OpExpr>();
            let operator = optional_c_string(pg_sys::get_opname((*operation).opno));
            let arguments: Vec<_> = list_items((*operation).args).collect();
            if operator.as_deref() != Some("=") || arguments.len() != 2 {
                return Err("IN requires one equality comparison".into());
            }
            let outer = arguments
                .iter()
                .map(|argument| var_origin(outer_query, (*argument).cast()))
                .find(|origin| origin.0 != 0)
                .ok_or_else(|| "IN left expression must be an outer source column".to_string())?;
            let target = list_items((*query).targetList)
                .map(|item| item.cast::<pg_sys::TargetEntry>())
                .find(|target| !(**target).resjunk)
                .ok_or_else(|| "IN subquery has no output column".to_string())?;
            let inner = var_origin(query, (*target).expr.cast());
            if inner.0 != source_oid || inner.1 <= 0 {
                return Err("IN output must be one ordinary inner source column".into());
            }
            Ok(Subquery {
                kind: if negated {
                    SubqueryKind::NullAnti
                } else {
                    SubqueryKind::Semi
                },
                source_oid: RelationOid(source_oid),
                left_table_oid: RelationOid(outer.0),
                left_column: ColumnNumber(outer.1),
                right_table_oid: RelationOid(inner.0),
                right_column: ColumnNumber(inner.1),
            })
        }
        _ => Err("only EXISTS, NOT EXISTS, and IN subqueries are supported".into()),
    }
}

/// # Safety
/// `outer_query` and `node` must be live nodes in the same analyzed query tree.
unsafe fn collect_sublinks(
    outer_query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
    output: &mut Vec<Subquery>,
    under_or: bool,
) -> Result<(), String> {
    if node.is_null() {
        return Ok(());
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_SubLink => {
            if under_or {
                return Err("subqueries under OR cannot be decorrelated into one semi join".into());
            }
            output.push(parse_sublink(outer_query, node, false)?);
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let expression = node.cast::<pg_sys::BoolExpr>();
            let arguments: Vec<_> = list_items((*expression).args).collect();
            if (*expression).boolop == pg_sys::BoolExprType::NOT_EXPR
                && arguments.len() == 1
                && (*arguments[0].cast::<pg_sys::Node>()).type_ == pg_sys::NodeTag::T_SubLink
            {
                if under_or {
                    return Err(
                        "subqueries under OR cannot be decorrelated into one anti join".into(),
                    );
                }
                output.push(parse_sublink(outer_query, arguments[0].cast(), true)?);
            } else {
                let child_under_or =
                    under_or || (*expression).boolop == pg_sys::BoolExprType::OR_EXPR;
                for argument in arguments {
                    collect_sublinks(outer_query, argument.cast(), output, child_under_or)?;
                }
            }
        }
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = node.cast::<pg_sys::RelabelType>();
            collect_sublinks(outer_query, (*relabel).arg.cast(), output, under_or)?;
        }
        _ => {}
    }
    Ok(())
}

/// # Safety
/// `query` and `node` must belong to the same live analyzed PostgreSQL query tree.
unsafe fn compile_predicate_node(
    query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
    sources: &mut BTreeSet<u32>,
) -> Result<String, String> {
    if node.is_null() {
        return Err("NULL expression node".into());
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_SubLink => Ok("true".into()),
        pg_sys::NodeTag::T_Var => {
            let (relation_oid, column_number) = var_origin(query, node);
            if relation_oid == 0 || column_number <= 0 {
                return Err("filter column is not an ordinary source-table column".into());
            }
            sources.insert(relation_oid);
            let column = optional_c_string(pg_sys::get_attname(
                pg_sys::Oid::from(relation_oid),
                column_number,
                false,
            ))
            .ok_or_else(|| "filter column has no PostgreSQL name".to_string())?;
            Ok(format!(
                "(input_{relation_oid}.row).\"{}\"",
                column.replace('"', "\"\"")
            ))
        }
        pg_sys::NodeTag::T_Const => {
            let constant = node.cast::<pg_sys::Const>();
            if (*constant).constisnull {
                return Ok("NULL".into());
            }
            let mut output_function = pg_sys::InvalidOid;
            let mut is_varlena = false;
            pg_sys::getTypeOutputInfo((*constant).consttype, &mut output_function, &mut is_varlena);
            let output = pg_sys::OidOutputFunctionCall(output_function, (*constant).constvalue);
            let quoted = pg_sys::quote_literal_cstr(output);
            let type_name = pg_sys::format_type_be_qualified((*constant).consttype);
            Ok(format!(
                "{}::{}",
                CStr::from_ptr(quoted).to_string_lossy(),
                CStr::from_ptr(type_name).to_string_lossy()
            ))
        }
        pg_sys::NodeTag::T_OpExpr => {
            let expression = node.cast::<pg_sys::OpExpr>();
            let operator = optional_c_string(pg_sys::get_opname((*expression).opno))
                .ok_or_else(|| "filter operator has no PostgreSQL name".to_string())?;
            if !matches!(operator.as_str(), "=" | "<>" | "<" | "<=" | ">" | ">=") {
                return Err(format!("unsupported filter operator {operator}"));
            }
            let arguments: Vec<_> = list_items((*expression).args).collect();
            if arguments.len() != 2 {
                return Err("comparison filter must have two arguments".into());
            }
            let left = compile_predicate_node(query, arguments[0].cast(), sources)?;
            let right = compile_predicate_node(query, arguments[1].cast(), sources)?;
            Ok(format!("({left} {operator} {right})"))
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let expression = node.cast::<pg_sys::BoolExpr>();
            let raw_arguments: Vec<_> = list_items((*expression).args).collect();
            if (*expression).boolop == pg_sys::BoolExprType::NOT_EXPR
                && raw_arguments.len() == 1
                && (*raw_arguments[0].cast::<pg_sys::Node>()).type_ == pg_sys::NodeTag::T_SubLink
            {
                return Ok("true".into());
            }
            let arguments: Result<Vec<_>, _> = list_items((*expression).args)
                .map(|argument| compile_predicate_node(query, argument.cast(), sources))
                .collect();
            let arguments = arguments?;
            match (*expression).boolop {
                pg_sys::BoolExprType::AND_EXPR => Ok(format!("({})", arguments.join(" AND "))),
                pg_sys::BoolExprType::OR_EXPR => Ok(format!("({})", arguments.join(" OR "))),
                pg_sys::BoolExprType::NOT_EXPR if arguments.len() == 1 => {
                    Ok(format!("(NOT {})", arguments[0]))
                }
                _ => Err("invalid boolean filter expression".into()),
            }
        }
        pg_sys::NodeTag::T_NullTest => {
            let test = node.cast::<pg_sys::NullTest>();
            if (*test).argisrow {
                return Err("row-valued NULL tests are not supported".into());
            }
            let argument = compile_predicate_node(query, (*test).arg.cast(), sources)?;
            let operation = match (*test).nulltesttype {
                pg_sys::NullTestType::IS_NULL => "IS NULL",
                pg_sys::NullTestType::IS_NOT_NULL => "IS NOT NULL",
                _ => return Err("invalid NULL test".into()),
            };
            Ok(format!("({argument} {operation})"))
        }
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = node.cast::<pg_sys::RelabelType>();
            compile_predicate_node(query, (*relabel).arg.cast(), sources)
        }
        _ => Err(format!(
            "unsupported PostgreSQL filter node {:?}",
            (*node).type_
        )),
    }
}

/// # Safety
/// `query` and `node` must belong to the same live analyzed PostgreSQL query tree.
unsafe fn compile_having_node(
    query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
    distinct_inputs: &mut BTreeSet<ColumnInput>,
    sum_inputs: &mut BTreeSet<ColumnInput>,
) -> Result<String, String> {
    if node.is_null() {
        return Err("NULL HAVING expression node".into());
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_Aggref => {
            let aggregate = node.cast::<pg_sys::Aggref>();
            if !(*aggregate).aggfilter.is_null() {
                return Err("aggregate FILTER is not supported in HAVING".into());
            }
            let name = optional_c_string(pg_sys::get_func_name((*aggregate).aggfnoid))
                .ok_or_else(|| "HAVING aggregate has no PostgreSQL name".to_string())?;
            match name.to_ascii_lowercase().as_str() {
                "count" if (*aggregate).aggstar && (*aggregate).aggdistinct.is_null() => {
                    Ok("state.row_count".into())
                }
                "count" if !(*aggregate).aggstar && !(*aggregate).aggdistinct.is_null() => {
                    let argument = list_items((*aggregate).args)
                        .next()
                        .ok_or_else(|| "COUNT(DISTINCT) has no input".to_string())?
                        .cast::<pg_sys::TargetEntry>();
                    let (table_oid, column) = var_origin(query, (*argument).expr.cast());
                    if table_oid == 0 || column <= 0 {
                        return Err(
                            "HAVING COUNT(DISTINCT) input must be an ordinary source column".into(),
                        );
                    }
                    distinct_inputs.insert(ColumnInput {
                        table_oid: RelationOid(table_oid),
                        column: ColumnNumber(column),
                    });
                    Ok("state.count_value".into())
                }
                "sum" if (*aggregate).aggdistinct.is_null() => {
                    let argument = list_items((*aggregate).args)
                        .next()
                        .ok_or_else(|| "SUM has no input".to_string())?
                        .cast::<pg_sys::TargetEntry>();
                    let (table_oid, column) = var_origin(query, (*argument).expr.cast());
                    if table_oid == 0 || column <= 0 {
                        return Err("HAVING SUM input must be an ordinary source column".into());
                    }
                    sum_inputs.insert(ColumnInput {
                        table_oid: RelationOid(table_oid),
                        column: ColumnNumber(column),
                    });
                    Ok(
                        "(CASE WHEN state.sum_nonnull_count=0 THEN NULL ELSE state.sum_value END)"
                            .into(),
                    )
                }
                _ => Err(format!("unsupported HAVING aggregate {name}")),
            }
        }
        pg_sys::NodeTag::T_Const => {
            let constant = node.cast::<pg_sys::Const>();
            if (*constant).constisnull {
                return Ok("NULL".into());
            }
            let mut output_function = pg_sys::InvalidOid;
            let mut is_varlena = false;
            pg_sys::getTypeOutputInfo((*constant).consttype, &mut output_function, &mut is_varlena);
            let output = pg_sys::OidOutputFunctionCall(output_function, (*constant).constvalue);
            let quoted = pg_sys::quote_literal_cstr(output);
            let type_name = pg_sys::format_type_be_qualified((*constant).consttype);
            Ok(format!(
                "{}::{}",
                CStr::from_ptr(quoted).to_string_lossy(),
                CStr::from_ptr(type_name).to_string_lossy()
            ))
        }
        pg_sys::NodeTag::T_OpExpr => {
            let expression = node.cast::<pg_sys::OpExpr>();
            let operator = optional_c_string(pg_sys::get_opname((*expression).opno))
                .ok_or_else(|| "HAVING operator has no PostgreSQL name".to_string())?;
            if !matches!(operator.as_str(), "=" | "<>" | "<" | "<=" | ">" | ">=") {
                return Err(format!("unsupported HAVING operator {operator}"));
            }
            let arguments: Vec<_> = list_items((*expression).args).collect();
            if arguments.len() != 2 {
                return Err("HAVING comparison must have two arguments".into());
            }
            Ok(format!(
                "({} {operator} {})",
                compile_having_node(query, arguments[0].cast(), distinct_inputs, sum_inputs)?,
                compile_having_node(query, arguments[1].cast(), distinct_inputs, sum_inputs)?
            ))
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let expression = node.cast::<pg_sys::BoolExpr>();
            let arguments: Result<Vec<_>, _> = list_items((*expression).args)
                .map(|argument| {
                    compile_having_node(query, argument.cast(), distinct_inputs, sum_inputs)
                })
                .collect();
            let arguments = arguments?;
            match (*expression).boolop {
                pg_sys::BoolExprType::AND_EXPR => Ok(format!("({})", arguments.join(" AND "))),
                pg_sys::BoolExprType::OR_EXPR => Ok(format!("({})", arguments.join(" OR "))),
                pg_sys::BoolExprType::NOT_EXPR if arguments.len() == 1 => {
                    Ok(format!("(NOT {})", arguments[0]))
                }
                _ => Err("invalid HAVING boolean expression".into()),
            }
        }
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = node.cast::<pg_sys::RelabelType>();
            compile_having_node(query, (*relabel).arg.cast(), distinct_inputs, sum_inputs)
        }
        _ => Err(format!(
            "unsupported PostgreSQL HAVING node {:?}",
            (*node).type_
        )),
    }
}

/// Extract the safe analysis model from PostgreSQL-owned query tree nodes.
///
/// # Safety
/// `pstmt` must be null or point to the live `PlannedStmt` currently supplied to
/// the ProcessUtility hook. PostgreSQL must keep its memory context alive for
/// the duration of this call.
unsafe fn analyze_ctas(pstmt: *mut pg_sys::PlannedStmt) -> Option<QueryAnalysis> {
    let query = analyzed_query(pstmt)?;
    let mut sources = Vec::new();
    let mut joins = Vec::new();
    for item in list_items((*query).rtable) {
        let rte = item.cast::<pg_sys::RangeTblEntry>();
        match (*rte).rtekind {
            pg_sys::RTEKind::RTE_RELATION => sources.push(Source {
                oid: RelationOid((*rte).relid.to_u32()),
                alias: if (*rte).eref.is_null() {
                    None
                } else {
                    optional_c_string((*(*rte).eref).aliasname)
                },
            }),
            pg_sys::RTEKind::RTE_JOIN => joins.push(Join {
                kind: match (*rte).jointype {
                    pg_sys::JoinType::JOIN_INNER => JoinKind::Inner,
                    pg_sys::JoinType::JOIN_LEFT => JoinKind::Left,
                    pg_sys::JoinType::JOIN_FULL => JoinKind::Full,
                    pg_sys::JoinType::JOIN_RIGHT => JoinKind::Right,
                    pg_sys::JoinType::JOIN_SEMI => JoinKind::Semi,
                    pg_sys::JoinType::JOIN_ANTI => JoinKind::Anti,
                    _ => JoinKind::Other,
                },
                operator: None,
                left_table_oid: RelationOid(0),
                left_column: ColumnNumber(0),
                right_table_oid: RelationOid(0),
                right_column: ColumnNumber(0),
            }),
            _ => {}
        }
    }
    if joins.len() == 1 && !(*query).jointree.is_null() {
        if let Some(from_item) = list_items((*(*query).jointree).fromlist).next() {
            let node = from_item.cast::<pg_sys::Node>();
            if (*node).type_ == pg_sys::NodeTag::T_JoinExpr {
                let join_expression = node.cast::<pg_sys::JoinExpr>();
                let qualification = (*join_expression).quals;
                if !qualification.is_null() && (*qualification).type_ == pg_sys::NodeTag::T_OpExpr {
                    let operation = qualification.cast::<pg_sys::OpExpr>();
                    let mut arguments = list_items((*operation).args);
                    if let (Some(left), Some(right)) = (arguments.next(), arguments.next()) {
                        let (left_table_oid, left_column) =
                            var_origin(query, left.cast::<pg_sys::Node>());
                        let (right_table_oid, right_column) =
                            var_origin(query, right.cast::<pg_sys::Node>());
                        joins[0].operator =
                            optional_c_string(pg_sys::get_opname((*operation).opno));
                        joins[0].left_table_oid = RelationOid(left_table_oid);
                        joins[0].left_column = ColumnNumber(left_column);
                        joins[0].right_table_oid = RelationOid(right_table_oid);
                        joins[0].right_column = ColumnNumber(right_column);
                    }
                }
            }
        }
    }
    let targets = list_items((*query).targetList)
        .map(|item| {
            let target = item.cast::<pg_sys::TargetEntry>();
            let expression = (*target).expr.cast::<pg_sys::Node>();
            let mut expression_kind = TargetExpression::Other;
            let mut aggregate = None;
            let mut aggregate_star = false;
            let mut aggregate_distinct = false;
            let mut input_table_oid = 0;
            let mut input_column = 0;
            let mut window_function = None;
            let mut window_star = false;
            let mut window_ref = 0;
            if (*expression).type_ == pg_sys::NodeTag::T_Var {
                expression_kind = TargetExpression::Column;
            } else if (*expression).type_ == pg_sys::NodeTag::T_Aggref {
                expression_kind = TargetExpression::Aggregate;
                let aggregate_ref = expression.cast::<pg_sys::Aggref>();
                aggregate = optional_c_string(pg_sys::get_func_name((*aggregate_ref).aggfnoid));
                aggregate_star = (*aggregate_ref).aggstar;
                aggregate_distinct = !(*aggregate_ref).aggdistinct.is_null();
                if let Some(argument) = list_items((*aggregate_ref).args).next() {
                    let argument = argument.cast::<pg_sys::TargetEntry>();
                    let argument_expression = (*argument).expr.cast::<pg_sys::Node>();
                    (input_table_oid, input_column) = var_origin(query, argument_expression);
                }
            } else if (*expression).type_ == pg_sys::NodeTag::T_WindowFunc {
                expression_kind = TargetExpression::Window;
                let function = expression.cast::<pg_sys::WindowFunc>();
                window_function = optional_c_string(pg_sys::get_func_name((*function).winfnoid));
                window_star = (*function).winstar;
                window_ref = (*function).winref;
                if let Some(argument) = list_items((*function).args).next() {
                    (input_table_oid, input_column) =
                        var_origin(query, argument.cast::<pg_sys::Node>());
                }
            }
            Target {
                name: optional_c_string((*target).resname),
                expression: expression_kind,
                type_oid: RelationOid(pg_sys::exprType((*target).expr.cast()).to_u32()),
                origin_table_oid: RelationOid((*target).resorigtbl.to_u32()),
                origin_column: ColumnNumber((*target).resorigcol),
                grouping_reference: (*target).ressortgroupref,
                aggregate,
                aggregate_star,
                aggregate_distinct,
                input_table_oid: RelationOid(input_table_oid),
                input_column: ColumnNumber(input_column),
                resjunk: (*target).resjunk,
                window_function,
                window_star,
                window_ref,
            }
        })
        .collect();
    let has_aggregate_filters = list_items((*query).targetList).any(|item| {
        let target = item.cast::<pg_sys::TargetEntry>();
        let expression = (*target).expr.cast::<pg_sys::Node>();
        (*expression).type_ == pg_sys::NodeTag::T_Aggref
            && !(*expression.cast::<pg_sys::Aggref>()).aggfilter.is_null()
    });
    let has_window_filters = list_items((*query).targetList).any(|item| {
        let target = item.cast::<pg_sys::TargetEntry>();
        let expression = (*target).expr.cast::<pg_sys::Node>();
        (*expression).type_ == pg_sys::NodeTag::T_WindowFunc
            && !(*expression.cast::<pg_sys::WindowFunc>())
                .aggfilter
                .is_null()
    });
    let windows = list_items((*query).windowClause)
        .filter_map(|item| {
            let window = item.cast::<pg_sys::WindowClause>();
            let partition = list_items((*window).partitionClause)
                .next()?
                .cast::<pg_sys::SortGroupClause>();
            let order = list_items((*window).orderClause)
                .next()?
                .cast::<pg_sys::SortGroupClause>();
            let partition_target = list_items((*query).targetList)
                .map(|target| target.cast::<pg_sys::TargetEntry>())
                .find(|target| (**target).ressortgroupref == (*partition).tleSortGroupRef)?;
            let order_target = list_items((*query).targetList)
                .map(|target| target.cast::<pg_sys::TargetEntry>())
                .find(|target| (**target).ressortgroupref == (*order).tleSortGroupRef)?;
            let (partition_table_oid, partition_column) =
                var_origin(query, (*partition_target).expr.cast());
            let (order_table_oid, order_column) = var_origin(query, (*order_target).expr.cast());
            let order_operator = optional_c_string(pg_sys::get_opname((*order).sortop));
            let (frame_clause, frame_error) = window_frame_clause(window);
            Some(WindowSpec {
                window_ref: (*window).winref,
                partition_keys: pg_sys::list_length((*window).partitionClause) as usize,
                order_keys: pg_sys::list_length((*window).orderClause) as usize,
                partition_table_oid: RelationOid(partition_table_oid),
                partition_column: ColumnNumber(partition_column),
                order_table_oid: RelationOid(order_table_oid),
                order_column: ColumnNumber(order_column),
                order_direction: if order_operator.as_deref() == Some(">") {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
                nulls_first: (*order).nulls_first,
                frame_options: (*window).frameOptions,
                frame_clause,
                frame_error,
            })
        })
        .collect();
    let ordering = list_items((*query).sortClause)
        .filter_map(|item| {
            let order = item.cast::<pg_sys::SortGroupClause>();
            let target = list_items((*query).targetList)
                .map(|target| target.cast::<pg_sys::TargetEntry>())
                .find(|target| (**target).ressortgroupref == (*order).tleSortGroupRef)?;
            let (table_oid, column) = var_origin(query, (*target).expr.cast());
            let operator = optional_c_string(pg_sys::get_opname((*order).sortop));
            Some(OrderSpec {
                table_oid: RelationOid(table_oid),
                column: ColumnNumber(column),
                direction: if operator.as_deref() == Some(">") {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
                nulls_first: (*order).nulls_first,
            })
        })
        .collect();
    let mut subqueries = Vec::new();
    let subquery_error = if (*query).jointree.is_null() || (*(*query).jointree).quals.is_null() {
        None
    } else {
        collect_sublinks(query, (*(*query).jointree).quals, &mut subqueries, false).err()
    };
    let where_predicate = if (*query).jointree.is_null() || (*(*query).jointree).quals.is_null() {
        None
    } else if let Some(error) = subquery_error {
        Some(PredicateAnalysis {
            sql: None,
            source_oids: Vec::new(),
            error: Some(error),
        })
    } else {
        let mut predicate_sources = BTreeSet::new();
        match compile_predicate_node(query, (*(*query).jointree).quals, &mut predicate_sources) {
            Ok(sql) => Some(PredicateAnalysis {
                sql: Some(sql),
                source_oids: predicate_sources.into_iter().map(RelationOid).collect(),
                error: None,
            }),
            Err(error) => Some(PredicateAnalysis {
                sql: None,
                source_oids: predicate_sources.into_iter().map(RelationOid).collect(),
                error: Some(error),
            }),
        }
    };
    let mut having_distinct_inputs = BTreeSet::new();
    let mut having_sum_inputs = BTreeSet::new();
    let having_predicate = if (*query).havingQual.is_null() {
        None
    } else {
        match compile_having_node(
            query,
            (*query).havingQual,
            &mut having_distinct_inputs,
            &mut having_sum_inputs,
        ) {
            Ok(sql) => Some(PredicateAnalysis {
                sql: Some(sql),
                source_oids: Vec::new(),
                error: None,
            }),
            Err(error) => Some(PredicateAnalysis {
                sql: None,
                source_oids: Vec::new(),
                error: Some(error),
            }),
        }
    };
    let analysis = QueryAnalysis {
        version: AnalysisVersion::CURRENT,
        has_aggregates: (*query).hasAggs,
        has_window_functions: (*query).hasWindowFuncs,
        has_sublinks: (*query).hasSubLinks,
        has_distinct: !(*query).distinctClause.is_null(),
        has_distinct_on: (*query).hasDistinctOn,
        has_having: !(*query).havingQual.is_null(),
        has_set_operations: !(*query).setOperations.is_null(),
        has_ordering: !(*query).sortClause.is_null(),
        has_limit: !(*query).limitOffset.is_null() || !(*query).limitCount.is_null(),
        has_aggregate_filters,
        has_window_filters,
        limit_with_ties: (*query).limitOption == pg_sys::LimitOption::LIMIT_OPTION_WITH_TIES,
        limit_count: integer_constant((*query).limitCount),
        limit_offset: integer_constant((*query).limitOffset),
        group_keys: pg_sys::list_length((*query).groupClause) as usize,
        sources,
        joins,
        subqueries,
        windows,
        ordering,
        targets,
        where_predicate,
        having_predicate,
        having_distinct_inputs: having_distinct_inputs.into_iter().collect(),
        having_sum_inputs: having_sum_inputs.into_iter().collect(),
    };
    Some(analysis)
}

/// A validated CTAS inspection and the byte-stable JSON protocol sent to SQL.
pub struct InspectedCtas {
    pub validated: ValidatedQuery,
    pub wire_json: String,
}

/// Inspect and validate a PostgreSQL CTAS before PostgreSQL executes it.
///
/// # Safety
/// The pointer requirements are identical to [`analyze_ctas`].
pub unsafe fn inspect_ctas(
    pstmt: *mut pg_sys::PlannedStmt,
) -> Option<Result<InspectedCtas, ValidationError>> {
    analyze_ctas(pstmt).map(|analysis| {
        let wire_json = analysis
            .to_wire_json()
            .expect("Shiba Query analysis is not serializable");
        let validated = analysis.validate()?;
        Ok(InspectedCtas {
            validated,
            wire_json,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_indexes_are_checked_before_postgres_access() {
        for (number, length, expected) in [
            (0, 3, None),
            (1, 3, Some(0)),
            (3, 3, Some(2)),
            (4, 3, None),
            (1, 0, None),
            (1, -1, None),
            (i32::MAX, 3, None),
        ] {
            assert_eq!(one_based_list_index(number, length), expected);
        }
    }

    #[test]
    fn only_cast_func_exprs_can_wrap_integer_constants() {
        assert!(is_cast_function_format(
            pg_sys::CoercionForm::COERCE_EXPLICIT_CAST
        ));
        assert!(is_cast_function_format(
            pg_sys::CoercionForm::COERCE_IMPLICIT_CAST
        ));
        assert!(!is_cast_function_format(
            pg_sys::CoercionForm::COERCE_EXPLICIT_CALL
        ));
        assert!(!is_cast_function_format(
            pg_sys::CoercionForm::COERCE_SQL_SYNTAX
        ));
    }
}
