//! One catalog contract for ordered kernel scans.
//!
//! Provisioning and execution must agree on the exact B-tree family.  The
//! resolved capability therefore contains both the index opclass/direction
//! and the qualified operators used by keyset predicates.

use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::planner::model::SortGroupExpr;
use crate::postgres::quote_identifier;

use super::StepContext;

const BTREE_ORDER_CAPABILITY_SQL: &str = r#"
WITH matching AS MATERIALIZED (
  SELECT actual_type.oid AS actual_type_oid,
         opclass.opcintype AS opclass_input_type,
         family.oid AS opfamily_oid,
         opclass.oid AS opclass_oid,
         opclass.opcdefault AS opclass_is_default,
         opclass_namespace.nspname::text AS opclass_namespace,
         opclass.opcname::text AS opclass_name,
         sort_member.amopstrategy::smallint AS sort_strategy,
         pg_catalog.format(
           'OPERATOR(%I.%s)',sort_namespace.nspname,sort_operator.oprname
         ) AS sort_operator,
         pg_catalog.format(
           'OPERATOR(%I.%s)',equality_namespace.nspname,equality_operator.oprname
         ) AS equality_operator
  FROM pg_catalog.pg_amop AS sort_member
  JOIN pg_catalog.pg_opfamily AS family
    ON family.oid=sort_member.amopfamily
  JOIN pg_catalog.pg_opclass AS opclass
    ON opclass.opcfamily=family.oid
   AND opclass.opcintype=sort_member.amoplefttype
   AND opclass.opcintype=sort_member.amoprighttype
  JOIN pg_catalog.pg_type AS actual_type
    ON actual_type.oid=$3
  JOIN pg_catalog.pg_namespace AS family_namespace
    ON family_namespace.oid=family.opfnamespace
   AND family_namespace.nspname='pg_catalog'
  JOIN pg_catalog.pg_namespace AS opclass_namespace
    ON opclass_namespace.oid=opclass.opcnamespace
   AND opclass_namespace.nspname='pg_catalog'
  JOIN pg_catalog.pg_am AS access_method
    ON access_method.oid=opclass.opcmethod
   AND access_method.oid=family.opfmethod
   AND access_method.amname='btree'
  JOIN pg_catalog.pg_operator AS sort_operator
    ON sort_operator.oid=sort_member.amopopr
   AND sort_operator.oprkind='b'
   AND sort_operator.oprleft=opclass.opcintype
   AND sort_operator.oprright=opclass.opcintype
   AND sort_operator.oprresult='boolean'::pg_catalog.regtype
  JOIN pg_catalog.pg_namespace AS sort_namespace
    ON sort_namespace.oid=sort_operator.oprnamespace
   AND sort_namespace.nspname='pg_catalog'
  JOIN pg_catalog.pg_proc AS sort_function
    ON sort_function.oid=sort_operator.oprcode
   AND sort_function.provolatile='i'
  JOIN pg_catalog.pg_amop AS equality_member
    ON equality_member.amopfamily=family.oid
   AND equality_member.amopopr=$2
   AND equality_member.amoplefttype=opclass.opcintype
   AND equality_member.amoprighttype=opclass.opcintype
   AND equality_member.amoppurpose='s'
   AND equality_member.amopstrategy=3
  JOIN pg_catalog.pg_operator AS equality_operator
    ON equality_operator.oid=equality_member.amopopr
   AND equality_operator.oprkind='b'
   AND equality_operator.oprleft=opclass.opcintype
   AND equality_operator.oprright=opclass.opcintype
   AND equality_operator.oprresult='boolean'::pg_catalog.regtype
  JOIN pg_catalog.pg_namespace AS equality_namespace
    ON equality_namespace.oid=equality_operator.oprnamespace
   AND equality_namespace.nspname='pg_catalog'
  JOIN pg_catalog.pg_proc AS equality_function
    ON equality_function.oid=equality_operator.oprcode
   AND equality_function.provolatile='i'
  WHERE sort_member.amopopr=$1
    AND sort_member.amoppurpose='s'
    AND sort_member.amopstrategy IN (1,5)
),
chosen AS (
  SELECT DISTINCT ON (opfamily_oid)
         actual_type_oid,opclass_input_type,opfamily_oid,
         opclass_namespace,opclass_name,sort_strategy,
         sort_operator,equality_operator
  FROM matching
  ORDER BY opfamily_oid,opclass_is_default DESC,opclass_oid
)
SELECT actual_type_oid,opclass_input_type,
       opclass_namespace,opclass_name,sort_strategy,
       sort_operator,equality_operator
FROM chosen
ORDER BY opfamily_oid
"#;

// This metadata is immutable for the lifetime of a PostgreSQL backend: the
// resolver accepts only pg_catalog B-tree families and operators.  Keep the
// cache bounded nevertheless, because a long-lived Runtime may serve many
// separately-created dataflows.
const CAPABILITY_CACHE_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CapabilityKey {
    sort_operator_oid: u32,
    equality_operator_oid: u32,
    type_oid: u32,
    nulls_first: bool,
}

thread_local! {
    static CAPABILITY_CACHE: RefCell<HashMap<CapabilityKey, BtreeOrder>> =
        RefCell::new(HashMap::new());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BtreeOrder {
    pub(crate) opclass: String,
    pub(crate) direction: &'static str,
    pub(crate) sort_operator: String,
    pub(crate) equality_operator: String,
    pub(crate) nulls_first: bool,
}

impl BtreeOrder {
    pub(crate) fn index_column(&self, column: &str) -> String {
        format!(
            "{} {} {} NULLS {}",
            quote_identifier(column),
            self.opclass,
            self.direction,
            if self.nulls_first { "FIRST" } else { "LAST" }
        )
    }
}

pub(crate) fn resolve_client(
    client: &mut SpiClient<'_>,
    order: &SortGroupExpr,
    operator: &str,
) -> Result<BtreeOrder, String> {
    if let Some(resolved) = cached(order) {
        return Ok(resolved);
    }
    let arguments = capability_arguments(order);
    let rows = client
        .select(BTREE_ORDER_CAPABILITY_SQL, None, &arguments)
        .map_err(|error| format!("could not resolve {operator} B-tree capability: {error}"))?;
    let resolved = decode(rows, order, operator)?;
    cache(order, &resolved);
    Ok(resolved)
}

pub(crate) fn resolve_step(
    transaction: &mut StepContext<'_, '_>,
    order: &SortGroupExpr,
    operator: &str,
) -> Result<BtreeOrder, String> {
    if let Some(resolved) = cached(order) {
        return Ok(resolved);
    }
    let rows = transaction.read(BTREE_ORDER_CAPABILITY_SQL, &capability_arguments(order))?;
    let resolved = decode(rows, order, operator)?;
    cache(order, &resolved);
    Ok(resolved)
}

fn cached(order: &SortGroupExpr) -> Option<BtreeOrder> {
    CAPABILITY_CACHE.with(|cache| cache.borrow().get(&capability_key(order)).cloned())
}

fn cache(order: &SortGroupExpr, resolved: &BtreeOrder) {
    CAPABILITY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache_insert(&mut cache, capability_key(order), resolved.clone());
    });
}

fn cache_insert(
    cache: &mut HashMap<CapabilityKey, BtreeOrder>,
    key: CapabilityKey,
    resolved: BtreeOrder,
) {
    if cache.len() >= CAPABILITY_CACHE_CAPACITY && !cache.contains_key(&key) {
        cache.clear();
    }
    cache.insert(key, resolved);
}

fn capability_key(order: &SortGroupExpr) -> CapabilityKey {
    CapabilityKey {
        sort_operator_oid: order.sort_operator_oid,
        equality_operator_oid: order.equality_operator_oid,
        type_oid: order.type_.type_oid,
        nulls_first: order.nulls_first,
    }
}

fn capability_arguments(order: &SortGroupExpr) -> [DatumWithOid<'static>; 3] {
    unsafe {
        [
            DatumWithOid::new(
                pg_sys::Oid::from_u32(order.sort_operator_oid),
                pg_sys::OIDOID,
            ),
            DatumWithOid::new(
                pg_sys::Oid::from_u32(order.equality_operator_oid),
                pg_sys::OIDOID,
            ),
            DatumWithOid::new(pg_sys::Oid::from_u32(order.type_.type_oid), pg_sys::OIDOID),
        ]
    }
}

fn decode(
    rows: SpiTupleTable<'_>,
    order: &SortGroupExpr,
    operator: &str,
) -> Result<BtreeOrder, String> {
    if rows.len() != 1 {
        return Err(format!(
            "{operator} sort operator {} and equality operator {} have no unique trusted B-tree operator family",
            order.sort_operator_oid, order.equality_operator_oid
        ));
    }
    let row = rows.first();
    let actual_type: pg_sys::Oid = required(&row, 1, "B-tree actual type")?;
    let opclass_input_type: pg_sys::Oid = required(&row, 2, "B-tree opclass input type")?;
    // SAFETY: both OIDs came from pg_type-backed catalog rows in this SPI
    // snapshot.  This is PostgreSQL's own ResolveOpClass applicability test.
    if !unsafe { pg_sys::IsBinaryCoercible(actual_type, opclass_input_type) } {
        return Err(format!(
            "{operator} sort operator {} and equality operator {} do not accept type {}",
            order.sort_operator_oid, order.equality_operator_oid, order.type_.type_oid
        ));
    }
    let namespace: String = required(&row, 3, "B-tree opclass namespace")?;
    let name: String = required(&row, 4, "B-tree opclass name")?;
    let strategy: i16 = required(&row, 5, "B-tree sort strategy")?;
    Ok(BtreeOrder {
        opclass: format!(
            "{}.{}",
            quote_identifier(&namespace),
            quote_identifier(&name)
        ),
        direction: decode_direction(strategy, operator)?,
        sort_operator: required(&row, 6, "B-tree sort operator")?,
        equality_operator: required(&row, 7, "B-tree equality operator")?,
        nulls_first: order.nulls_first,
    })
}

fn decode_direction(strategy: i16, operator: &str) -> Result<&'static str, String> {
    match strategy {
        1 => Ok("ASC"),
        5 => Ok("DESC"),
        _ => Err(format!("{operator} B-tree strategy is invalid")),
    }
}

fn required<T: FromDatum + IntoDatum>(
    row: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_selects_one_opclass_per_family_deterministically() {
        assert!(BTREE_ORDER_CAPABILITY_SQL.contains("DISTINCT ON (opfamily_oid)"));
        assert!(BTREE_ORDER_CAPABILITY_SQL
            .contains("ORDER BY opfamily_oid,opclass_is_default DESC,opclass_oid"));
        assert!(BTREE_ORDER_CAPABILITY_SQL.contains("ORDER BY opfamily_oid\n"));
    }

    #[test]
    fn capability_matches_catalog_declared_types_before_actual_type_validation() {
        assert!(BTREE_ORDER_CAPABILITY_SQL.contains("opclass.opcintype=sort_member.amoplefttype"));
        assert!(BTREE_ORDER_CAPABILITY_SQL.contains("sort_operator.oprleft=opclass.opcintype"));
        assert!(
            BTREE_ORDER_CAPABILITY_SQL.contains("equality_member.amoplefttype=opclass.opcintype")
        );
        assert!(BTREE_ORDER_CAPABILITY_SQL.contains("equality_operator.oprleft=opclass.opcintype"));
        assert!(!BTREE_ORDER_CAPABILITY_SQL.contains("opcintype=$3"));
    }

    #[test]
    fn only_btree_ordering_strategies_are_decoded() {
        assert_eq!(decode_direction(1, "test").unwrap(), "ASC");
        assert_eq!(decode_direction(5, "test").unwrap(), "DESC");
        assert!(decode_direction(3, "test").is_err());
    }

    #[test]
    fn capability_cache_is_bounded_without_evicting_an_existing_key() {
        let mut cache = HashMap::new();
        let resolved = BtreeOrder {
            opclass: "pg_catalog.int4_ops".into(),
            direction: "ASC",
            sort_operator: "OPERATOR(pg_catalog.<)".into(),
            equality_operator: "OPERATOR(pg_catalog.=)".into(),
            nulls_first: false,
        };
        for type_oid in 1..=CAPABILITY_CACHE_CAPACITY as u32 {
            cache_insert(
                &mut cache,
                CapabilityKey {
                    sort_operator_oid: type_oid,
                    equality_operator_oid: type_oid,
                    type_oid,
                    nulls_first: false,
                },
                resolved.clone(),
            );
        }
        let existing = *cache.keys().next().expect("cache has entries");
        cache_insert(&mut cache, existing, resolved.clone());
        assert_eq!(cache.len(), CAPABILITY_CACHE_CAPACITY);

        cache_insert(
            &mut cache,
            CapabilityKey {
                sort_operator_oid: u32::MAX,
                equality_operator_oid: u32::MAX,
                type_oid: u32::MAX,
                nulls_first: true,
            },
            resolved,
        );
        assert_eq!(cache.len(), 1);
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod btree_catalog_tests {
    use super::*;
    use crate::planner::model::{BindingId, ScalarExpr, SlotType};

    fn catalog_oid(query: &str, name: &str) -> u32 {
        Spi::get_one_with_args::<pg_sys::Oid>(query, &[name.into()])
            .expect("catalog lookup should execute")
            .expect("catalog object should exist")
            .to_u32()
    }

    fn type_oid(name: &str) -> u32 {
        catalog_oid("SELECT pg_catalog.to_regtype($1)::oid", name)
    }

    fn operator_oid(signature: &str) -> u32 {
        catalog_oid("SELECT pg_catalog.to_regoperator($1)::oid", signature)
    }

    fn order(type_name: &str, operator_type: &str) -> SortGroupExpr {
        SortGroupExpr {
            expr: ScalarExpr::Input {
                binding: BindingId(1),
            },
            type_: SlotType {
                type_oid: type_oid(type_name),
                typmod: -1,
                collation_oid: 0,
                nullable: true,
            },
            equality_operator_oid: operator_oid(&format!(
                "pg_catalog.=({operator_type},{operator_type})"
            )),
            sort_operator_oid: operator_oid(&format!(
                "pg_catalog.<({operator_type},{operator_type})"
            )),
            nulls_first: false,
            hashable: true,
        }
    }

    fn resolve(order: &SortGroupExpr) -> Result<BtreeOrder, String> {
        Spi::connect_mut(|client| resolve_client(client, order, "catalog test"))
    }

    #[pg_test(schema = "tests")]
    fn resolves_builtin_generic_btree_opclasses_for_concrete_types() {
        Spi::run(
            r#"
            CREATE TYPE tests.btree_enum_key AS ENUM ('first','second');
            CREATE TYPE tests.btree_record_key AS (
                number integer,
                label text
            );
            CREATE DOMAIN tests.btree_array_domain AS integer[]
            "#,
        )
        .expect("catalog test types should be created");

        let cases = [
            ("integer[]", "anyarray", "array_ops"),
            ("tests.btree_array_domain", "anyarray", "array_ops"),
            ("tests.btree_enum_key", "anyenum", "enum_ops"),
            ("tests.btree_record_key", "record", "record_ops"),
            ("int4range", "anyrange", "range_ops"),
            ("int4multirange", "anymultirange", "multirange_ops"),
            ("varchar", "text", "text_ops"),
        ];

        for (actual_type, operator_type, opclass) in cases {
            let capability =
                resolve(&order(actual_type, operator_type)).expect("capability should resolve");
            assert_eq!(capability.opclass, format!("\"pg_catalog\".\"{opclass}\""));
        }
    }

    #[pg_test(schema = "tests")]
    fn rejects_a_generic_operator_for_an_incompatible_actual_type() {
        let error = resolve(&order("integer", "anyarray")).unwrap_err();
        assert!(error.contains("do not accept type"), "{error}");
    }
}
