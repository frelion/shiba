//! ProcessUtility hook that reserves `shiba` CTAS declarations.

use crate::query_tree;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::ffi::CStr;

static mut PREVIOUS_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

pub unsafe fn install_process_utility_hook() {
    PREVIOUS_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
    pg_sys::ProcessUtility_hook = Some(shiba_process_utility);
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
    prepare_stream_drops(pstmt);
    let declaration = shiba_table_declaration(pstmt, query_string);
    let inspection = query_tree::inspect_ctas(pstmt);
    let is_stream_declaration = declaration.is_some() && inspection.is_some();
    if declaration.is_some() && !is_stream_declaration && !pg_sys::creating_extension {
        error!("the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... stream declarations");
    }
    let declaration = is_stream_declaration.then_some(declaration).flatten();
    if declaration.is_some() && inspection.is_none() {
        error!("Shiba could not access PostgreSQL's analyzed CTAS Query tree");
    }
    let inspection = if declaration.is_some() {
        Some(match inspection.expect("checked above") {
            Ok(inspection) => inspection,
            Err(error) => error!("Shiba cannot execute this stream declaration: {error}"),
        })
    } else {
        None
    };
    if let Some(inspection) = &inspection {
        Spi::run("SELECT shiba._begin_stream_registration()")
            .expect("Shiba failed to enter database registration lifecycle");
        let lock_analysis = serde_json::json!({
            "sources": inspection
                .validated
                .sources()
                .into_iter()
                .map(|oid| serde_json::json!({ "oid": oid }))
                .collect::<Vec<_>>(),
            "subqueries": [],
        })
        .to_string();
        let argument = DatumWithOid::new(lock_analysis.as_str(), pg_sys::TEXTOID);
        Spi::run_with_args(
            "SELECT shiba._lock_sources_for_analysis($1::jsonb)",
            &[argument],
        )
        .expect("Shiba failed to lock the source table for backfill");
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
    if let Some(declaration) = declaration {
        let query_analysis = inspection.expect("checked above").wire_json;
        let arguments = [
            DatumWithOid::new(declaration.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(query_analysis.as_str(), pg_sys::TEXTOID),
        ];
        Spi::run_with_args(
            "SELECT shiba._register_stream_table($1, $2::jsonb)",
            &arguments,
        )
        .expect("Shiba failed to register the stream table");
        Spi::run_with_args(
            "SELECT shiba._store_query_analysis($1, $2::jsonb)",
            &arguments,
        )
        .expect("Shiba failed to persist its analyzed Query tree");
    }
}

unsafe fn prepare_stream_drops(pstmt: *mut pg_sys::PlannedStmt) {
    if pstmt.is_null() || (*pstmt).utilityStmt.is_null() {
        return;
    }
    let utility = (*pstmt).utilityStmt;
    if (*utility).type_ == pg_sys::NodeTag::T_DropOwnedStmt {
        let drop_statement = utility.cast::<pg_sys::DropOwnedStmt>();
        guard_drop_owned_ast(drop_statement);
        // DROP OWNED removes directly owned objects even under the default
        // RESTRICT behavior; CASCADE only controls dependent objects.
        lock_all_dags_before_indirect_drop();
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
        lock_all_dags_before_indirect_drop();
    }
    if (*drop_statement).removeType == pg_sys::ObjectType::OBJECT_EXTENSION {
        return;
    }
    if (*drop_statement).removeType != pg_sys::ObjectType::OBJECT_TABLE {
        return;
    }
    if Spi::get_one::<bool>("SELECT to_regclass('shiba_internal.stream_views') IS NOT NULL")
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
               SELECT 1 FROM shiba_internal.stream_views WHERE source_oid=$1
               UNION ALL
               SELECT 1 FROM shiba_internal.inner_join_views WHERE right_source_oid=$1
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
               SELECT 1 FROM shiba_internal.stream_views WHERE result_oid=$1
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
            "SELECT shiba._prepare_stream_drops(ARRAY[{oid_list}]::oid[])"
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

fn lock_all_dags_before_indirect_drop() {
    if Spi::get_one::<bool>("SELECT to_regclass('shiba_internal.stream_views') IS NOT NULL")
        .ok()
        .flatten()
        != Some(true)
    {
        return;
    }
    Spi::run("SELECT shiba_internal._lock_all_dags_for_utility()")
        .expect("Shiba failed to serialize indirect DROP with DAG execution");
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
        "SELECT to_regclass('shiba_internal.stream_views') IS NOT NULL
           AND to_regclass('shiba_internal.runtime_state') IS NOT NULL
           AND to_regclass('shiba_internal.dag_runtime_state') IS NOT NULL",
    )
    .expect("Shiba failed to inspect its catalog before DROP EXTENSION")
    .unwrap_or(false);
    let lifecycle_clean = if catalog_exists {
        Spi::get_one::<bool>(
            "SELECT NOT EXISTS (SELECT 1 FROM shiba_internal.stream_views)
               AND NOT EXISTS (
                 SELECT 1 FROM shiba_internal.runtime_state WHERE active
               )
               AND NOT EXISTS (
                 SELECT 1 FROM shiba_internal.dag_runtime_state WHERE active
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

unsafe fn shiba_table_declaration(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
) -> Option<String> {
    if pstmt.is_null() || query_string.is_null() {
        return None;
    }
    let utility = (*pstmt).utilityStmt;
    if utility.is_null() {
        return None;
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
    }?;
    if target_relation.is_null() {
        return None;
    }
    let target_namespace = pg_sys::RangeVarGetCreationNamespace(target_relation);
    let shiba_namespace = pg_sys::get_namespace_oid(c"shiba".as_ptr(), true);
    if target_namespace != shiba_namespace || shiba_namespace == pg_sys::InvalidOid {
        return None;
    }
    let query_bytes = CStr::from_ptr(query_string).to_bytes();
    let statement_start = (*pstmt).stmt_location;
    if statement_start < 0 || statement_start as usize >= query_bytes.len() {
        return None;
    }
    let statement_start = statement_start as usize;
    let statement_end = if (*pstmt).stmt_len <= 0 {
        query_bytes.len()
    } else {
        statement_start
            .saturating_add((*pstmt).stmt_len as usize)
            .min(query_bytes.len())
    };
    let statement = String::from_utf8_lossy(&query_bytes[statement_start..statement_end])
        .trim()
        .to_string();
    Some(statement)
}
