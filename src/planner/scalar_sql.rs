//! Compile the persisted scalar AST to PostgreSQL SQL.
//!
//! The compiler accepts identifiers, not SQL fragments, for input bindings.
//! Every database object is resolved again from its catalog OID.  The
//! generated SQL can therefore be embedded in a kernel statement without
//! trusting names captured from a user query.

use std::collections::HashMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::planner::model::{BoolExprKind, BooleanTestKind, DatumRepr, ScalarExpr, SlotType};
use crate::postgres::quote_identifier;

#[derive(Debug)]
pub(crate) struct SqlBinding {
    pub(crate) binding_id: u32,
    pub(crate) input_alias: String,
    pub(crate) attribute_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FunctionName {
    schema: String,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OperatorName {
    schema: String,
    name: String,
    kind: char,
}

trait Catalog {
    fn function(&self, oid: u32) -> Result<FunctionName, String>;
    fn operator(&self, oid: u32) -> Result<OperatorName, String>;
    fn type_name(&self, type_: &SlotType) -> Result<String, String>;
    fn collation(&self, oid: u32) -> Result<FunctionName, String>;
}

struct PgCatalog;

pub(crate) fn compile_scalar_expression(
    expression: &ScalarExpr,
    bindings: &[SqlBinding],
) -> Result<String, String> {
    Compiler::new(&PgCatalog, bindings)?.compile(expression)
}

/// Resolve every catalog object carried by a persisted scalar expression.
///
/// Registration calls this before the plan becomes durable. Execution still
/// resolves the same objects again while compiling SQL, so catalog changes
/// cannot turn this admission check into stale authority.
pub(crate) fn validate_scalar_catalog(expression: &ScalarExpr) -> Result<(), String> {
    validate_scalar_catalog_with(&PgCatalog, expression)
}

fn validate_scalar_catalog_with<C: Catalog>(
    catalog: &C,
    expression: &ScalarExpr,
) -> Result<(), String> {
    fn validate_type<C: Catalog>(catalog: &C, type_: &SlotType) -> Result<(), String> {
        catalog.type_name(type_)?;
        if type_.collation_oid != 0 {
            catalog.collation(type_.collation_oid)?;
        }
        Ok(())
    }

    let mut error = None;
    expression.visit(&mut |part| {
        if error.is_some() {
            return;
        }
        error = match part {
            ScalarExpr::Input { .. }
            | ScalarExpr::Bool { .. }
            | ScalarExpr::BooleanTest { .. }
            | ScalarExpr::NullTest { .. } => None,
            ScalarExpr::Constant { type_, .. }
            | ScalarExpr::Coalesce { type_, .. }
            | ScalarExpr::Case { type_, .. }
            | ScalarExpr::CaseTest { type_ }
            | ScalarExpr::Relabel { type_, .. }
            | ScalarExpr::CoerceViaIo { type_, .. }
            | ScalarExpr::CoerceToDomain { type_, .. } => validate_type(catalog, type_).err(),
            ScalarExpr::Call {
                function_oid,
                type_,
                ..
            } => catalog
                .function(*function_oid)
                .and_then(|_| validate_type(catalog, type_))
                .err(),
            ScalarExpr::Operator {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::Distinct {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::NullIf {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::ScalarArrayOperator {
                operator_oid,
                type_,
                ..
            } => catalog
                .operator(*operator_oid)
                .and_then(|_| validate_type(catalog, type_))
                .err(),
            ScalarExpr::Collate {
                collation_oid,
                type_,
                ..
            } => catalog
                .collation(*collation_oid)
                .and_then(|_| validate_type(catalog, type_))
                .err(),
        };
    });
    error.map_or(Ok(()), Err)
}

impl Catalog for PgCatalog {
    fn function(&self, oid: u32) -> Result<FunctionName, String> {
        let arguments = unsafe { [DatumWithOid::new(pg_sys::Oid::from(oid), pg_sys::OIDOID)] };
        let (schema, name, properties) = Spi::get_three_with_args::<String, String, String>(
            "SELECT namespace.nspname::text,
                        function.proname::text,
                        function.provolatile::text || function.prokind::text
                   FROM pg_catalog.pg_proc AS function
                  JOIN pg_catalog.pg_namespace AS namespace
                     ON namespace.oid = function.pronamespace
                  WHERE function.oid = $1::oid",
            &arguments,
        )
        .map_err(|error| format!("could not resolve function OID {oid}: {error}"))?;
        let schema = schema.ok_or_else(|| format!("unknown function OID {oid}"))?;
        let name = name.ok_or_else(|| format!("function OID {oid} has no name"))?;
        if schema != "pg_catalog" || properties.as_deref() != Some("if") {
            return Err(format!(
                "function OID {oid} is not a trusted pg_catalog immutable scalar function"
            ));
        }
        Ok(FunctionName { schema, name })
    }

    fn operator(&self, oid: u32) -> Result<OperatorName, String> {
        let arguments = unsafe { [DatumWithOid::new(pg_sys::Oid::from(oid), pg_sys::OIDOID)] };
        let (schema, name, kind) = Spi::get_three_with_args::<String, String, String>(
            "SELECT namespace.nspname::text,
                    operator.oprname::text,
                    operator.oprkind::text
               FROM pg_catalog.pg_operator AS operator
               JOIN pg_catalog.pg_namespace AS namespace
                 ON namespace.oid = operator.oprnamespace
               JOIN pg_catalog.pg_proc AS function
                 ON function.oid = operator.oprcode
              WHERE operator.oid = $1::oid
                AND namespace.nspname = 'pg_catalog'
                AND function.provolatile = 'i'",
            &arguments,
        )
        .map_err(|error| format!("could not resolve operator OID {oid}: {error}"))?;
        let schema =
            schema.ok_or_else(|| format!("unknown or non-immutable operator OID {oid}"))?;
        let name = name.ok_or_else(|| format!("operator OID {oid} has no name"))?;
        let kind = kind
            .and_then(|value| {
                let mut characters = value.chars();
                let first = characters.next()?;
                characters.next().is_none().then_some(first)
            })
            .ok_or_else(|| format!("operator OID {oid} has an invalid kind"))?;
        Ok(OperatorName { schema, name, kind })
    }

    fn type_name(&self, type_: &SlotType) -> Result<String, String> {
        let arguments = unsafe {
            [
                DatumWithOid::new(pg_sys::Oid::from(type_.type_oid), pg_sys::OIDOID),
                DatumWithOid::new(type_.typmod, pg_sys::INT4OID),
            ]
        };
        Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.format_type(type.oid, $2::integer)
               FROM pg_catalog.pg_type AS type
              WHERE type.oid = $1::oid
                AND type.typnamespace = 'pg_catalog'::pg_catalog.regnamespace
                AND type.typtype <> 'p'",
            &arguments,
        )
        .map_err(|error| {
            format!(
                "could not resolve type OID {} with typmod {}: {error}",
                type_.type_oid, type_.typmod
            )
        })?
        .ok_or_else(|| format!("unknown or pseudo type OID {}", type_.type_oid))
    }

    fn collation(&self, oid: u32) -> Result<FunctionName, String> {
        let arguments = unsafe { [DatumWithOid::new(pg_sys::Oid::from(oid), pg_sys::OIDOID)] };
        let (schema, name) = Spi::get_two_with_args::<String, String>(
            "SELECT namespace.nspname::text, catalog_collation.collname::text
               FROM pg_catalog.pg_collation AS catalog_collation
               JOIN pg_catalog.pg_namespace AS namespace
                 ON namespace.oid = catalog_collation.collnamespace
              WHERE catalog_collation.oid = $1::oid",
            &arguments,
        )
        .map_err(|error| format!("could not resolve collation OID {oid}: {error}"))?;
        let schema = schema.ok_or_else(|| format!("unknown collation OID {oid}"))?;
        if schema != "pg_catalog" {
            return Err(format!(
                "collation OID {oid} is outside the trusted pg_catalog namespace"
            ));
        }
        Ok(FunctionName {
            schema,
            name: name.ok_or_else(|| format!("collation OID {oid} has no name"))?,
        })
    }
}

struct Compiler<'a, C> {
    catalog: &'a C,
    bindings: HashMap<u32, (&'a str, &'a str)>,
}

impl<'a, C: Catalog> Compiler<'a, C> {
    fn new(catalog: &'a C, bindings: &'a [SqlBinding]) -> Result<Self, String> {
        let mut resolved = HashMap::with_capacity(bindings.len());
        for binding in bindings {
            if binding.input_alias.is_empty() || binding.attribute_name.is_empty() {
                return Err(format!(
                    "scalar binding {} has an empty identifier",
                    binding.binding_id
                ));
            }
            if resolved
                .insert(
                    binding.binding_id,
                    (
                        binding.input_alias.as_str(),
                        binding.attribute_name.as_str(),
                    ),
                )
                .is_some()
            {
                return Err(format!("duplicate scalar binding {}", binding.binding_id));
            }
        }
        Ok(Self {
            catalog,
            bindings: resolved,
        })
    }

    fn compile(&self, expression: &ScalarExpr) -> Result<String, String> {
        validate_scalar_catalog_with(self.catalog, expression)?;
        self.compile_scoped(expression, &[])
    }

    fn compile_scoped(
        &self,
        expression: &ScalarExpr,
        case_operands: &[String],
    ) -> Result<String, String> {
        match expression {
            ScalarExpr::Input { binding } => {
                let (alias, attribute) = self
                    .bindings
                    .get(&binding.0)
                    .ok_or_else(|| format!("unknown scalar binding {}", binding.0))?;
                Ok(format!(
                    "({}.row_value).{}",
                    quote_identifier(alias),
                    quote_identifier(attribute)
                ))
            }
            ScalarExpr::Constant { type_, value } => self.constant(type_, value.as_ref()),
            ScalarExpr::Call {
                function_oid, args, ..
            } => {
                let function = self.catalog.function(*function_oid)?;
                Ok(format!(
                    "{}.{}({})",
                    quote_identifier(&function.schema),
                    quote_identifier(&function.name),
                    self.arguments(args, case_operands)?
                ))
            }
            ScalarExpr::Operator {
                operator_oid, args, ..
            } => self.operator(*operator_oid, args, case_operands),
            ScalarExpr::Distinct {
                operator_oid,
                left,
                right,
                ..
            } => {
                let operator = self.catalog.operator(*operator_oid)?;
                if operator.kind != 'b' {
                    return Err(format!(
                        "DISTINCT operator OID {operator_oid} is not binary"
                    ));
                }
                let alias = format!("__shiba_distinct_{}", case_operands.len());
                let alias_sql = quote_identifier(&alias);
                Ok(format!(
                    "(SELECT CASE WHEN {0}.left_value IS NULL \
                     THEN {0}.right_value IS NOT NULL \
                     WHEN {0}.right_value IS NULL THEN TRUE \
                     ELSE NOT ({0}.left_value {1} {0}.right_value) END \
                     FROM (SELECT {2} AS left_value, {3} AS right_value) AS {0})",
                    alias_sql,
                    qualified_operator(&operator),
                    self.compile_scoped(left, case_operands)?,
                    self.compile_scoped(right, case_operands)?
                ))
            }
            ScalarExpr::NullIf {
                operator_oid,
                left,
                right,
                type_,
            } => {
                let operator = self.catalog.operator(*operator_oid)?;
                if operator.kind != 'b' {
                    return Err(format!("NULLIF operator OID {operator_oid} is not binary"));
                }
                let alias = format!("__shiba_nullif_{}", case_operands.len());
                let alias_sql = quote_identifier(&alias);
                Ok(format!(
                    "(SELECT CASE WHEN ({0}.left_value {1} {0}.right_value) \
                     THEN CAST(NULL AS {2}) ELSE {0}.left_value END \
                     FROM (SELECT {3} AS left_value, {4} AS right_value) AS {0})",
                    alias_sql,
                    qualified_operator(&operator),
                    self.catalog.type_name(type_)?,
                    self.compile_scoped(left, case_operands)?,
                    self.compile_scoped(right, case_operands)?
                ))
            }
            ScalarExpr::ScalarArrayOperator {
                operator_oid,
                left,
                right,
                use_or,
                ..
            } => {
                let operator = self.catalog.operator(*operator_oid)?;
                if operator.kind != 'b' {
                    return Err(format!(
                        "scalar-array operator OID {operator_oid} is not binary"
                    ));
                }
                Ok(format!(
                    "({} {} {} ({}))",
                    self.compile_scoped(left, case_operands)?,
                    qualified_operator(&operator),
                    if *use_or { "ANY" } else { "ALL" },
                    self.compile_scoped(right, case_operands)?
                ))
            }
            ScalarExpr::Bool { op, args } => {
                let compiled = args
                    .iter()
                    .map(|argument| self.compile_scoped(argument, case_operands))
                    .collect::<Result<Vec<_>, _>>()?;
                match op {
                    BoolExprKind::And if !compiled.is_empty() => {
                        Ok(format!("({})", compiled.join(" AND ")))
                    }
                    BoolExprKind::Or if !compiled.is_empty() => {
                        Ok(format!("({})", compiled.join(" OR ")))
                    }
                    BoolExprKind::Not if compiled.len() == 1 => {
                        Ok(format!("(NOT ({}))", compiled[0]))
                    }
                    _ => Err("boolean expression has invalid arity".into()),
                }
            }
            ScalarExpr::BooleanTest { arg, test } => Ok(format!(
                "(({}) IS {})",
                self.compile_scoped(arg, case_operands)?,
                match test {
                    BooleanTestKind::True => "TRUE",
                    BooleanTestKind::NotTrue => "NOT TRUE",
                    BooleanTestKind::False => "FALSE",
                    BooleanTestKind::NotFalse => "NOT FALSE",
                    BooleanTestKind::Unknown => "UNKNOWN",
                    BooleanTestKind::NotUnknown => "NOT UNKNOWN",
                }
            )),
            ScalarExpr::NullTest { arg, is_not } => Ok(format!(
                "(({}) IS {}NULL)",
                self.compile_scoped(arg, case_operands)?,
                if *is_not { "NOT " } else { "" }
            )),
            ScalarExpr::Coalesce { args, .. } => {
                if args.is_empty() {
                    return Err("COALESCE has no arguments".into());
                }
                Ok(format!(
                    "COALESCE({})",
                    self.arguments(args, case_operands)?
                ))
            }
            ScalarExpr::Case {
                operand,
                arms,
                else_expr,
                ..
            } => {
                if arms.is_empty() {
                    return Err("CASE has no arms".into());
                }
                let mut sql = String::from("(CASE");
                let mut arm_case_operands = case_operands.to_vec();
                let mut case_alias = None;
                if operand.is_some() {
                    let alias = format!("__shiba_case_{}", case_operands.len());
                    arm_case_operands.push(format!("{}.value", quote_identifier(&alias)));
                    case_alias = Some(alias);
                }
                for arm in arms {
                    sql.push_str(" WHEN ");
                    sql.push_str(&self.compile_scoped(&arm.when, &arm_case_operands)?);
                    sql.push_str(" THEN ");
                    sql.push_str(&self.compile_scoped(&arm.then, case_operands)?);
                }
                sql.push_str(" ELSE ");
                sql.push_str(&self.compile_scoped(else_expr, case_operands)?);
                sql.push_str(" END)");
                if let (Some(operand), Some(alias)) = (operand, case_alias) {
                    Ok(format!(
                        "(SELECT {sql} FROM (SELECT {} AS value) AS {})",
                        self.compile_scoped(operand, case_operands)?,
                        quote_identifier(&alias)
                    ))
                } else {
                    Ok(sql)
                }
            }
            ScalarExpr::CaseTest { .. } => case_operands
                .last()
                .cloned()
                .ok_or_else(|| "CASE operand placeholder is outside a simple CASE arm".into()),
            ScalarExpr::Relabel { arg, type_ }
            | ScalarExpr::CoerceViaIo { arg, type_ }
            | ScalarExpr::CoerceToDomain { arg, type_ } => Ok(format!(
                "CAST(({}) AS {})",
                self.compile_scoped(arg, case_operands)?,
                self.catalog.type_name(type_)?
            )),
            ScalarExpr::Collate {
                arg, collation_oid, ..
            } => {
                let collation = self.catalog.collation(*collation_oid)?;
                Ok(format!(
                    "(({}) COLLATE {}.{})",
                    self.compile_scoped(arg, case_operands)?,
                    quote_identifier(&collation.schema),
                    quote_identifier(&collation.name)
                ))
            }
        }
    }

    fn arguments(&self, args: &[ScalarExpr], case_operands: &[String]) -> Result<String, String> {
        args.iter()
            .map(|argument| self.compile_scoped(argument, case_operands))
            .collect::<Result<Vec<_>, _>>()
            .map(|arguments| arguments.join(", "))
    }

    fn operator(
        &self,
        oid: u32,
        args: &[ScalarExpr],
        case_operands: &[String],
    ) -> Result<String, String> {
        let operator = self.catalog.operator(oid)?;
        let qualified = qualified_operator(&operator);
        match (operator.kind, args) {
            ('b', [left, right]) => Ok(format!(
                "({} {} {})",
                self.compile_scoped(left, case_operands)?,
                qualified,
                self.compile_scoped(right, case_operands)?
            )),
            ('l', [right]) => Ok(format!(
                "({} {})",
                qualified,
                self.compile_scoped(right, case_operands)?
            )),
            ('r', [left]) => Ok(format!(
                "({} {})",
                self.compile_scoped(left, case_operands)?,
                qualified
            )),
            _ => Err(format!("operator OID {oid} has invalid arity")),
        }
    }

    fn constant(&self, type_: &SlotType, value: Option<&DatumRepr>) -> Result<String, String> {
        let type_name = self.catalog.type_name(type_)?;
        let input = match value {
            None => "NULL".to_string(),
            Some(DatumRepr::Text(text)) => {
                let hex = text
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                format!("pg_catalog.convert_from(pg_catalog.decode('{hex}', 'hex'), 'UTF8')")
            }
        };
        Ok(format!("CAST({input} AS {type_name})"))
    }
}

fn qualified_operator(operator: &OperatorName) -> String {
    // PostgreSQL operator tokens are catalog-validated punctuation, not SQL
    // identifiers.  The surrounding OPERATOR() syntax and quoted namespace
    // keep even a user-defined operator unambiguous.
    format!(
        "OPERATOR({}.{})",
        quote_identifier(&operator.schema),
        operator.name
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::model::{BindingId, CaseWhen};

    struct TestCatalog;

    impl Catalog for TestCatalog {
        fn function(&self, oid: u32) -> Result<FunctionName, String> {
            (oid == 10)
                .then(|| FunctionName {
                    schema: "odd schema".into(),
                    name: "f\"n".into(),
                })
                .ok_or_else(|| "unknown function".into())
        }

        fn operator(&self, oid: u32) -> Result<OperatorName, String> {
            (oid == 20)
                .then(|| OperatorName {
                    schema: "pg_catalog".into(),
                    name: "=".into(),
                    kind: 'b',
                })
                .ok_or_else(|| "unknown operator".into())
        }

        fn type_name(&self, type_: &SlotType) -> Result<String, String> {
            match type_.type_oid {
                16 => Ok("boolean".into()),
                23 => Ok("integer".into()),
                _ => Err("unknown type".into()),
            }
        }

        fn collation(&self, oid: u32) -> Result<FunctionName, String> {
            (oid == 100)
                .then(|| FunctionName {
                    schema: "pg_catalog".into(),
                    name: "C".into(),
                })
                .ok_or_else(|| "unknown collation".into())
        }
    }

    fn integer() -> SlotType {
        SlotType {
            type_oid: 23,
            typmod: -1,
            collation_oid: 0,
            nullable: false,
        }
    }

    fn compiler() -> Compiler<'static, TestCatalog> {
        let bindings = Box::leak(Box::new([SqlBinding {
            binding_id: 7,
            input_alias: "in\"put".into(),
            attribute_name: "odd column".into(),
        }]));
        Compiler::new(&TestCatalog, bindings).unwrap()
    }

    #[test]
    fn input_and_catalog_names_are_always_identifiers() {
        let expression = ScalarExpr::Call {
            function_oid: 10,
            args: vec![ScalarExpr::Input {
                binding: BindingId(7),
            }],
            type_: integer(),
        };
        assert_eq!(
            compiler().compile(&expression).unwrap(),
            "\"odd schema\".\"f\"\"n\"((\"in\"\"put\".row_value).\"odd column\")"
        );
    }

    #[test]
    fn constants_do_not_embed_their_text_as_sql() {
        let expression = ScalarExpr::Constant {
            type_: integer(),
            value: Some(DatumRepr::Text("1'; DROP TABLE x; --".into())),
        };
        let sql = compiler().compile(&expression).unwrap();
        assert!(!sql.contains("DROP TABLE"));
        assert_eq!(
            sql,
            "CAST(pg_catalog.convert_from(pg_catalog.decode('31273b2044524f50205441424c4520783b202d2d', 'hex'), 'UTF8') AS integer)"
        );
    }

    #[test]
    fn nested_three_valued_logic_keeps_postgresql_semantics() {
        let input = ScalarExpr::Input {
            binding: BindingId(7),
        };
        let equality = ScalarExpr::Operator {
            operator_oid: 20,
            args: vec![
                input.clone(),
                ScalarExpr::Constant {
                    type_: integer(),
                    value: None,
                },
            ],
            type_: SlotType {
                type_oid: 16,
                typmod: -1,
                collation_oid: 0,
                nullable: true,
            },
        };
        let expression = ScalarExpr::Case {
            operand: None,
            arms: vec![CaseWhen {
                when: ScalarExpr::NullTest {
                    arg: Box::new(input),
                    is_not: false,
                },
                then: ScalarExpr::Constant {
                    type_: integer(),
                    value: Some(DatumRepr::Text("1".into())),
                },
            }],
            else_expr: Box::new(ScalarExpr::Coalesce {
                args: vec![
                    ScalarExpr::Constant {
                        type_: integer(),
                        value: None,
                    },
                    ScalarExpr::Constant {
                        type_: integer(),
                        value: Some(DatumRepr::Text("0".into())),
                    },
                ],
                type_: integer(),
            }),
            type_: integer(),
        };
        let sql = compiler().compile(&ScalarExpr::Bool {
            op: BoolExprKind::Or,
            args: vec![
                equality,
                ScalarExpr::BooleanTest {
                    arg: Box::new(ScalarExpr::NullTest {
                        arg: Box::new(expression),
                        is_not: true,
                    }),
                    test: BooleanTestKind::True,
                },
            ],
        });
        assert!(sql.unwrap().contains(" OR "));
    }

    #[test]
    fn duplicate_or_missing_bindings_are_rejected() {
        let duplicate = [
            SqlBinding {
                binding_id: 1,
                input_alias: "a".into(),
                attribute_name: "x".into(),
            },
            SqlBinding {
                binding_id: 1,
                input_alias: "b".into(),
                attribute_name: "y".into(),
            },
        ];
        assert!(Compiler::new(&TestCatalog, &duplicate).is_err());
        assert!(compiler()
            .compile(&ScalarExpr::Input {
                binding: BindingId(999)
            })
            .is_err());
    }

    #[test]
    fn catalog_validation_rejects_an_untrusted_nested_coercion_type() {
        let expression = ScalarExpr::Relabel {
            arg: Box::new(ScalarExpr::CoerceToDomain {
                arg: Box::new(ScalarExpr::Input {
                    binding: BindingId(7),
                }),
                type_: SlotType {
                    type_oid: 99,
                    typmod: -1,
                    collation_oid: 0,
                    nullable: false,
                },
            }),
            type_: integer(),
        };
        assert_eq!(
            validate_scalar_catalog_with(&TestCatalog, &expression),
            Err("unknown type".into())
        );
    }
}
