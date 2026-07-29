//! ProcessUtility hook that reserves `shiba` CTAS declarations.

use crate::query_lowering::{self, LoweredQuery, LoweringError};
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::ffi::CStr;

static mut PREVIOUS_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

pub unsafe fn install_process_utility_hook() {
    PREVIOUS_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
    pg_sys::ProcessUtility_hook = Some(shiba_process_utility);
}

/// Extract and lower the analyzed Query owned by a live CTAS statement.
///
/// # Safety
/// `pstmt` must be null or point to the `PlannedStmt` supplied to the current
/// ProcessUtility hook call.
unsafe fn inspect_ctas(
    pstmt: *mut pg_sys::PlannedStmt,
) -> Option<Result<LoweredQuery, LoweringError>> {
    if pstmt.is_null() || (*pstmt).utilityStmt.is_null() {
        return None;
    }
    let utility = (*pstmt).utilityStmt;
    if (*utility).type_ != pg_sys::NodeTag::T_CreateTableAsStmt {
        return None;
    }
    let statement = utility.cast::<pg_sys::CreateTableAsStmt>();
    if (*statement).query.is_null() || (*(*statement).query).type_ != pg_sys::NodeTag::T_Query {
        return None;
    }
    Some(query_lowering::lower((*statement).query.cast()))
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn shiba_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    query_completion: *mut pg_sys::QueryCompletion,
) {
    prepare_dataflow_drops(pstmt);
    let declaration = is_shiba_table_declaration(pstmt);
    let inspection = inspect_ctas(pstmt);
    let is_dataflow_declaration = declaration && inspection.is_some();
    if declaration && !is_dataflow_declaration && !pg_sys::creating_extension {
        error!(
            "the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... dataflow declarations"
        );
    }
    let inspection = if is_dataflow_declaration {
        Some(match inspection.expect("checked above") {
            Ok(inspection) => inspection,
            Err(error) => error!("Shiba cannot execute this dataflow declaration: {error}"),
        })
    } else {
        None
    };
    if let Some(inspection) = &inspection {
        Spi::run("SELECT shiba._begin_dataflow_registration()")
            .expect("Shiba failed to enter database registration lifecycle");
        let sources = inspection
            .source_oids()
            .into_iter()
            .map(pg_sys::Oid::from)
            .collect::<Vec<_>>();
        let argument = DatumWithOid::new(sources, pg_sys::OIDARRAYOID);
        Spi::run_with_args(
            "SELECT shiba._lock_dataflow_sources($1::oid[])",
            &[argument],
        )
        .expect("Shiba failed to lock the source table for activation");
        suppress_ctas_data(pstmt);
    }
    if let Some(previous_hook) = PREVIOUS_PROCESS_UTILITY_HOOK {
        previous_hook(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            query_completion,
        );
    } else {
        pg_sys::standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            query_completion,
        );
    }
    if is_dataflow_declaration {
        let result_oid = ctas_result_oid(pstmt)
            .unwrap_or_else(|| error!("Shiba could not resolve the new CTAS result relation"));
        let inspection = inspection.expect("checked above");
        let plan = inspection.finish();
        plan.validate()
            .unwrap_or_else(|error| error!("Shiba rejected this dataflow declaration: {error}"));
        let plan = serde_json::to_string(&plan).expect("Shiba dataflow plan is not serializable");
        let arguments = unsafe {
            [
                DatumWithOid::new(result_oid, pg_sys::OIDOID),
                DatumWithOid::new(plan.as_str(), pg_sys::TEXTOID),
            ]
        };
        Spi::run_with_args(
            "SELECT shiba._register_dataflow($1::oid, $2::text)",
            &arguments,
        )
        .expect("Shiba failed to register the dataflow");
    }
}

/// Make PostgreSQL create only the analyzed CTAS result schema. Scan
/// provisioners spool the one authoritative activation snapshot after every
/// source lock is held and before this registration transaction commits.
///
/// # Safety
/// `pstmt` must be the same live CTAS statement passed to the utility hook.
unsafe fn suppress_ctas_data(pstmt: *mut pg_sys::PlannedStmt) {
    let statement = (*pstmt).utilityStmt.cast::<pg_sys::CreateTableAsStmt>();
    (*(*statement).into).skipData = true;
}

/// Resolve the CTAS target after `standard_ProcessUtility` has created it.
///
/// # Safety
/// `pstmt` must be the same live CTAS statement passed to the utility hook.
unsafe fn ctas_result_oid(pstmt: *mut pg_sys::PlannedStmt) -> Option<pg_sys::Oid> {
    if pstmt.is_null() || (*pstmt).utilityStmt.is_null() {
        return None;
    }
    let utility = (*pstmt).utilityStmt;
    if (*utility).type_ != pg_sys::NodeTag::T_CreateTableAsStmt {
        return None;
    }
    let statement = utility.cast::<pg_sys::CreateTableAsStmt>();
    let target = (*statement).into.as_ref()?.rel;
    if target.is_null() {
        return None;
    }
    let oid = pg_sys::RangeVarGetRelidExtended(
        target,
        pg_sys::NoLock as pg_sys::LOCKMODE,
        0,
        None,
        std::ptr::null_mut(),
    );
    (oid != pg_sys::InvalidOid).then_some(oid)
}

unsafe fn prepare_dataflow_drops(pstmt: *mut pg_sys::PlannedStmt) {
    if pstmt.is_null() || (*pstmt).utilityStmt.is_null() {
        return;
    }
    let utility = (*pstmt).utilityStmt;
    if (*utility).type_ == pg_sys::NodeTag::T_DropOwnedStmt {
        let drop_statement = utility.cast::<pg_sys::DropOwnedStmt>();
        guard_drop_owned_ast(drop_statement);
        // DROP OWNED removes directly owned objects even under the default
        // RESTRICT behavior; CASCADE only controls dependent objects.
        lock_all_dataflows_before_indirect_drop();
        return;
    }
    if (*utility).type_ != pg_sys::NodeTag::T_DropStmt {
        return;
    }
    let drop_statement = utility.cast::<pg_sys::DropStmt>();
    if (*drop_statement).removeType == pg_sys::ObjectType::OBJECT_EXTENSION {
        guard_extension_drop_ast(drop_statement);
    }
    if (*drop_statement).behavior == pg_sys::DropBehavior::DROP_CASCADE {
        lock_all_dataflows_before_indirect_drop();
    }
    if (*drop_statement).removeType == pg_sys::ObjectType::OBJECT_EXTENSION {
        return;
    }
    if (*drop_statement).removeType != pg_sys::ObjectType::OBJECT_TABLE {
        return;
    }
    if Spi::get_one::<bool>("SELECT to_regclass('shiba_internal.dataflows') IS NOT NULL")
        .ok()
        .flatten()
        != Some(true)
    {
        return;
    }
    let object_count = pg_sys::list_length((*drop_statement).objects);
    let mut relation_oids = Vec::new();
    for object_index in 0..object_count {
        let names =
            pg_sys::list_nth((*drop_statement).objects, object_index).cast::<pg_sys::List>();
        let name_count = pg_sys::list_length(names);
        if !(1..=3).contains(&name_count) {
            continue;
        }
        let mut qualified_parts = Vec::new();
        for name_index in 0..name_count {
            let name_node = pg_sys::list_nth(names, name_index).cast::<pg_sys::String>();
            if name_node.is_null() || (*name_node).sval.is_null() {
                qualified_parts.clear();
                break;
            }
            let name = CStr::from_ptr((*name_node).sval).to_string_lossy();
            qualified_parts.push(format!("\"{}\"", name.replace('"', "\"\"")));
        }
        if qualified_parts.is_empty() {
            continue;
        }
        let qualified = qualified_parts.join(".");
        let lookup = [DatumWithOid::new(qualified.as_str(), pg_sys::TEXTOID)];
        let Some(result_oid) =
            Spi::get_one_with_args::<i32>("SELECT to_regclass($1)::oid::integer", &lookup)
                .expect("Shiba failed to resolve a result table before DROP")
        else {
            continue;
        };
        relation_oids.push(result_oid);
    }
    relation_oids.sort_unstable();
    relation_oids.dedup();
    for relation_oid in &relation_oids {
        let argument = unsafe { [DatumWithOid::new(*relation_oid, pg_sys::OIDOID)] };
        let is_source = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (
               SELECT 1 FROM shiba_internal.dataflow_sources WHERE source_oid=$1
             )",
            &argument,
        )
        .expect("Shiba failed to inspect source dependencies before DROP")
        .unwrap_or(false);
        if is_source {
            error!(
                "cannot DROP TABLE with OID {} while it is a Shiba source; drop dependent Shiba tables first",
                relation_oid
            );
        }
    }
    let mut result_oids = Vec::new();
    for result_oid in relation_oids {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let is_result = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (
               SELECT 1 FROM shiba_internal.dataflows WHERE result_oid=$1
             )",
            &argument,
        )
        .expect("Shiba failed to inspect a result table before DROP")
        .unwrap_or(false);
        if !is_result {
            continue;
        }
        result_oids.push(result_oid);
    }
    if !result_oids.is_empty() {
        let oid_list = result_oids
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        Spi::run(&format!(
            "SELECT shiba._prepare_dataflow_drops(ARRAY[{oid_list}]::oid[])"
        ))
        .expect("Shiba failed to quiesce result DAGs before DROP");
    }
}

unsafe fn guard_drop_owned_ast(drop_statement: *mut pg_sys::DropOwnedStmt) {
    let Some(extension_owner) =
        Spi::get_one::<pg_sys::Oid>("SELECT extowner FROM pg_extension WHERE extname='shiba'")
            .expect("Shiba failed to inspect its extension owner before DROP OWNED")
    else {
        return;
    };
    let role_count = pg_sys::list_length((*drop_statement).roles);
    for role_index in 0..role_count {
        let role = pg_sys::list_nth((*drop_statement).roles, role_index).cast::<pg_sys::RoleSpec>();
        if !role.is_null() && pg_sys::get_rolespec_oid(role, false) == extension_owner {
            error!(
                "DROP OWNED by the Shiba extension owner is not supported; drop all Shiba results, call shiba.deactivate(), and use DROP EXTENSION shiba explicitly"
            );
        }
    }
}

fn lock_all_dataflows_before_indirect_drop() {
    if Spi::get_one::<bool>("SELECT to_regclass('shiba_internal.dataflows') IS NOT NULL")
        .ok()
        .flatten()
        != Some(true)
    {
        return;
    }
    Spi::run("SELECT shiba_internal._lock_all_dataflows_for_utility()")
        .expect("Shiba failed to serialize indirect DROP with dataflow execution");
}

unsafe fn guard_extension_drop_ast(drop_statement: *mut pg_sys::DropStmt) {
    let object_count = pg_sys::list_length((*drop_statement).objects);
    for object_index in 0..object_count {
        let object =
            pg_sys::list_nth((*drop_statement).objects, object_index).cast::<pg_sys::Node>();
        if object.is_null() {
            continue;
        }
        let drops_shiba = match (*object).type_ {
            pg_sys::NodeTag::T_String => {
                let value = object.cast::<pg_sys::String>();
                !(*value).sval.is_null()
                    && CStr::from_ptr((*value).sval)
                        .to_string_lossy()
                        .eq_ignore_ascii_case("shiba")
            }
            pg_sys::NodeTag::T_List => {
                let names = object.cast::<pg_sys::List>();
                (0..pg_sys::list_length(names)).any(|name_index| {
                    let value = pg_sys::list_nth(names, name_index).cast::<pg_sys::String>();
                    !value.is_null()
                        && !(*value).sval.is_null()
                        && CStr::from_ptr((*value).sval)
                            .to_string_lossy()
                            .eq_ignore_ascii_case("shiba")
                })
            }
            _ => false,
        };
        if drops_shiba {
            ensure_shiba_slot_inactive();
            return;
        }
    }
}

fn ensure_shiba_slot_inactive() {
    let slot_exists = Spi::get_one::<bool>(
        "SELECT EXISTS (
           SELECT 1 FROM pg_replication_slots
           WHERE slot_name=format(
             'shiba_%s',
             (SELECT oid FROM pg_database WHERE datname=current_database())
           )
         )",
    )
    .expect("Shiba failed to inspect its logical slot before DROP EXTENSION")
    .unwrap_or(false);
    let catalog_exists = Spi::get_one::<bool>(
        "SELECT to_regclass('shiba_internal.dataflows') IS NOT NULL
           AND to_regclass('shiba_internal.runtime_state') IS NOT NULL",
    )
    .expect("Shiba failed to inspect its catalog before DROP EXTENSION")
    .unwrap_or(false);
    let lifecycle_clean = if catalog_exists {
        Spi::get_one::<bool>(
            "SELECT NOT EXISTS (SELECT 1 FROM shiba_internal.dataflows)
               AND NOT EXISTS (
                 SELECT 1 FROM shiba_internal.runtime_state WHERE active
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM shiba_internal.ingress_replay_state
                 WHERE state='active'
               )",
        )
        .expect("Shiba failed to inspect worker state before DROP EXTENSION")
        .unwrap_or(false)
    } else {
        false
    };
    if slot_exists || !lifecycle_clean {
        error!("drop all Shiba results and call shiba.deactivate() before DROP EXTENSION shiba");
    }
}

unsafe fn is_shiba_table_declaration(pstmt: *mut pg_sys::PlannedStmt) -> bool {
    if pstmt.is_null() {
        return false;
    }
    let utility = (*pstmt).utilityStmt;
    if utility.is_null() {
        return false;
    }
    let target_relation = match (*utility).type_ {
        pg_sys::NodeTag::T_CreateStmt => utility
            .cast::<pg_sys::CreateStmt>()
            .as_ref()
            .map(|create| create.relation),
        pg_sys::NodeTag::T_CreateTableAsStmt => utility
            .cast::<pg_sys::CreateTableAsStmt>()
            .as_ref()
            .and_then(|create| create.into.as_ref())
            .map(|into| into.rel),
        _ => None,
    };
    let Some(target_relation) = target_relation else {
        return false;
    };
    if target_relation.is_null() {
        return false;
    }
    let target_namespace = pg_sys::RangeVarGetCreationNamespace(target_relation);
    let shiba_namespace = pg_sys::get_namespace_oid(c"shiba".as_ptr(), true);
    shiba_namespace != pg_sys::InvalidOid && target_namespace == shiba_namespace
}
