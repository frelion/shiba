#[cfg(feature = "pg17")]
pub(super) mod execution {
    use pgrx::datum::DatumWithOid;
    use pgrx::prelude::*;

    use crate::execution::StepReceipt;
    use crate::execution::{InputPosition, OutputFacts, PhaseCode, PrimitiveFacts, WorkUsage};
    use crate::planner::model::{
        DataflowPlan, DataflowStage, InputSlot, JoinEquiKey, JoinKind, JoinSpec, OperatorSpec,
        ScalarExpr,
    };
    use crate::planner::scalar_sql::{compile_scalar_expression, SqlBinding};
    use crate::planner::{WorkBudget, WorkQuantum};
    use crate::postgres::{format_lsn, parse_lsn};

    use super::super::*;
    use crate::execution::{
        advance_input, append_frontier, canonical_row_key_sql, chunk, compile_named_outputs,
        compile_stage_bindings, lock_continuation, next_chunk, payload_facts,
        replace_continuation_cas, validate_continuation_abi as validate_typed_continuation_abi,
        validate_output_attributes, BindingInput, ChunkKind, ChunkMeta, ContinuationColumn,
        KernelPhase, OutputAppendTarget, PayloadStorage, ProducerKind, RelationRef, StepContext,
        TypeRef,
    };

    const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
        ContinuationColumn::required("singleton", pg_sys::BOOLOID),
        ContinuationColumn::required("phase", pg_sys::INT2OID),
        ContinuationColumn::required("input_side", pg_sys::INT2OID),
        ContinuationColumn::required("left_stream_id", pg_sys::INT8OID),
        ContinuationColumn::required("left_chunk_seq", pg_sys::INT8OID),
        ContinuationColumn::required("left_row_ordinal", pg_sys::INT8OID),
        ContinuationColumn::required("right_stream_id", pg_sys::INT8OID),
        ContinuationColumn::required("right_chunk_seq", pg_sys::INT8OID),
        ContinuationColumn::required("right_row_ordinal", pg_sys::INT8OID),
        ContinuationColumn::nullable("event_weight", pg_sys::INT8OID),
        ContinuationColumn::nullable("event_bytes", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_row_id", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_multiplicity", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_match_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_unknown_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("candidate_after", pg_sys::INT8OID),
        ContinuationColumn::nullable("accumulated_match_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("accumulated_unknown_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_row_id", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_multiplicity", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_truth", pg_sys::INT2OID),
        ContinuationColumn::nullable("pending_old_match", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_old_unknown", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_new_match", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_new_unknown", pg_sys::INT8OID),
        ContinuationColumn::nullable_as("frontier_lsn", pg_sys::PG_LSNOID, "pg_lsn"),
    ];

    struct Layout {
        left_payload: PayloadStorage,
        right_payload: PayloadStorage,
        output_payload: PayloadStorage,
        left_state: RelationRef,
        right_state: RelationRef,
        left_key_exprs: Vec<String>,
        right_key_exprs: Vec<String>,
        continuation: RelationRef,
        condition: String,
        outputs: String,
    }

    impl Layout {
        fn input_payload(&self, side: InputSide) -> &PayloadStorage {
            match side {
                InputSide::Left => &self.left_payload,
                InputSide::Right => &self.right_payload,
            }
        }

        fn input_type(&self, side: InputSide) -> &TypeRef {
            &self.input_payload(side).row_type
        }

        fn state(&self, side: InputSide) -> &RelationRef {
            match side {
                InputSide::Left => &self.left_state,
                InputSide::Right => &self.right_state,
            }
        }

        fn key_exprs(&self, side: InputSide) -> &[String] {
            match side {
                InputSide::Left => &self.left_key_exprs,
                InputSide::Right => &self.right_key_exprs,
            }
        }

        fn keyed(&self) -> bool {
            !self.left_key_exprs.is_empty() && !self.right_key_exprs.is_empty()
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct JoinTransition {
        usage: WorkUsage,
        continue_in_transaction: bool,
        phase: KernelPhase,
        state_rows: u64,
    }

    impl JoinTransition {
        const fn control(phase: KernelPhase) -> Self {
            Self {
                usage: WorkUsage {
                    input_rows: 0,
                    input_bytes: 0,
                    output_rows: 0,
                    output_bytes: 0,
                },
                continue_in_transaction: true,
                phase,
                state_rows: 0,
            }
        }

        const fn material(
            usage: WorkUsage,
            continue_in_transaction: bool,
            phase: KernelPhase,
            state_rows: u64,
        ) -> Self {
            Self {
                usage,
                continue_in_transaction,
                phase,
                state_rows,
            }
        }
    }

    pub(crate) fn step(
        transaction: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage_id: u32,
    ) -> Result<StepReceipt, String> {
        let stage = plan
            .stages
            .get(usize::try_from(stage_id).map_err(|_| "Join stage ID exceeds usize")?)
            .ok_or_else(|| format!("dataflow has no Join stage {stage_id}"))?;
        let OperatorSpec::Join(spec) = &stage.spec else {
            return Err("Join kernel received another operator".into());
        };
        if transaction.inputs().len() != 2
            || transaction.input(0)?.producer != ProducerKind::Operator
            || transaction.input(1)?.producer != ProducerKind::Operator
        {
            return Err("Join requires exactly two operator-stream inputs".into());
        }
        let layout = load_layout(transaction, plan, stage, spec)?;
        let mut continuation = load_continuation(transaction, &layout.continuation)?;
        crate::execution::validate_continuation_authority(transaction, continuation.is_some())?;
        if continuation.is_none() && spec.kind == JoinKind::Inner {
            if let Some(page) = measure_inner_page(transaction, &layout)? {
                return execute_inner_page(transaction, &layout, page);
            }
        }

        // A quantum publishes at most one immutable output chunk. Clamp the
        // shared budget to that stream's chunk target before any phase runs.
        let mut quantum = WorkQuantum::new(effective_budget(transaction)?, 64);
        let phase = loop {
            let remaining = quantum
                .remaining()
                .ok_or_else(|| "Join quantum exhausted before its first transition".to_string())?;
            transaction.set_transition_budget(remaining);
            let transition = match continuation {
                None => open_next_input(transaction, &layout)?,
                Some(JoinContinuation::Preflight { positions, side }) => {
                    step_preflight(transaction, &layout, positions, side)?
                }
                Some(continuation @ JoinContinuation::Probe(_))
                | Some(continuation @ JoinContinuation::PendingTransition { .. }) => {
                    step_candidates(transaction, &layout, spec, continuation)?
                }
                Some(continuation @ JoinContinuation::Finalize(_)) => {
                    step_finalize(transaction, &layout, spec, continuation)?
                }
                Some(continuation @ JoinContinuation::Frontier(_)) => {
                    step_frontier(transaction, &layout, continuation)?
                }
            };
            transaction.record_state_rows(transition.state_rows)?;
            quantum.record(transition.usage)?;
            if !transition.continue_in_transaction || quantum.remaining().is_none() {
                break transition.phase;
            }
            continuation = load_continuation(transaction, &layout.continuation)?;
        };
        transaction.transition(phase, quantum.usage())
    }

    fn open_next_input(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
    ) -> Result<JoinTransition, String> {
        let left = next_chunk(transaction, 0)?;
        let right = next_chunk(transaction, 1)?;
        let (side, head) = match (left, right) {
            (Some(left), Some(right)) => {
                if (left.lsn, 0_u16) <= (right.lsn, 1_u16) {
                    (InputSide::Left, left)
                } else {
                    (InputSide::Right, right)
                }
            }
            (Some(left), None) => (InputSide::Left, left),
            (None, Some(right)) => (InputSide::Right, right),
            (None, None) => {
                return Err("runnable Join has neither a continuation nor an input chunk".into());
            }
        };
        let positions = consumer_positions(transaction)?;
        let continuation = match head.kind {
            ChunkKind::Data => {
                payload_facts(transaction, &layout.input_payload(side).relation, &head)?;
                JoinContinuation::start_preflight(positions, side)?
            }
            ChunkKind::Frontier => JoinContinuation::start_frontier(FrontierInputFacts::new(
                side, positions, head.lsn,
            )?)?,
        };
        insert_continuation(transaction, &layout.continuation, &continuation)?;
        Ok(JoinTransition::control(KernelPhase::Process))
    }

    fn step_preflight(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        positions: InputPositions,
        side: InputSide,
    ) -> Result<JoinTransition, String> {
        validate_positions_for_side(transaction, positions, side)?;
        let expected = JoinContinuation::start_preflight(positions, side)?;
        let (event, _) = load_event(transaction, layout, positions, side)?;
        let own = load_own_expectation(transaction, layout, event)?;
        let next = JoinContinuation::start_input(event, own)?;
        replace_continuation(transaction, &layout.continuation, &expected, Some(&next))?;
        Ok(JoinTransition::control(KernelPhase::Process))
    }

    fn step_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        spec: &JoinSpec,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join candidate phase omitted input progress".to_string())?;
        validate_positions_for_side(transaction, progress.positions(), progress.side())?;
        let (event, chunk) =
            load_event(transaction, layout, progress.positions(), progress.side())?;
        continuation.validate_input_resume(event)?;
        let budget = effective_budget(transaction)?;
        let page = probe_candidates(
            transaction,
            layout,
            mode(spec.kind),
            &continuation,
            event,
            budget,
        )?;
        let action = plan_actions(mode(spec.kind), &continuation, event, &page, budget)?;
        let output = append_actions(transaction, layout, &chunk, action.actions(), event)?;
        let changed = apply_candidate_changes(
            transaction,
            layout.state(event.side.opposite()),
            action.candidate_changes(),
        )?;
        replace_continuation(
            transaction,
            &layout.continuation,
            &continuation,
            Some(action.next_continuation()),
        )?;
        action.validate_commit(PrimitiveFacts {
            usage: action.usage(),
            state_rows: changed,
            output,
        })?;
        Ok(if action.usage().is_empty() {
            JoinTransition::control(KernelPhase::Process)
        } else {
            JoinTransition::material(action.usage(), true, KernelPhase::Process, changed)
        })
    }

    fn mode(kind: JoinKind) -> JoinMode {
        match kind {
            JoinKind::Inner => JoinMode::Inner,
            JoinKind::Left => JoinMode::Left,
            JoinKind::Right => JoinMode::Right,
            JoinKind::Full => JoinMode::Full,
            JoinKind::Semi => JoinMode::Semi,
            JoinKind::Anti => JoinMode::Anti,
            JoinKind::NullAwareAnti => JoinMode::NullAwareAnti,
        }
    }

    fn load_layout(
        transaction: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage: &DataflowStage,
        spec: &JoinSpec,
    ) -> Result<Layout, String> {
        let left_input = transaction.input(0)?.clone();
        let right_input = transaction.input(1)?.clone();
        let left_payload = transaction.payload_storage(left_input.stream_id)?;
        let right_payload = transaction.payload_storage(right_input.stream_id)?;
        let output = transaction.output()?.clone();
        let output_payload = transaction.payload_storage(output.stream_id)?;
        let left_state = transaction.state_storage(0)?;
        let right_state = transaction.state_storage(1)?;
        let continuation = transaction.continuation_storage()?;

        let left_slots = join_key_slots(stage, spec, 0)?;
        let right_slots = join_key_slots(stage, spec, 1)?;
        validate_state_abi(
            transaction,
            &left_state,
            &left_payload.row_type,
            &left_slots,
        )?;
        validate_state_abi(
            transaction,
            &right_state,
            &right_payload.row_type,
            &right_slots,
        )?;
        validate_continuation_abi(transaction, &continuation)?;
        let bindings = compile_stage_bindings(
            transaction,
            plan,
            stage,
            &[
                BindingInput {
                    row_type: &left_payload.row_type,
                    alias: "left_row",
                },
                BindingInput {
                    row_type: &right_payload.row_type,
                    alias: "right_row",
                },
            ],
        )?;
        let left_key_exprs = key_expressions(&bindings, &spec.equi_keys, true)?;
        let right_key_exprs = key_expressions(&bindings, &spec.equi_keys, false)?;
        let output_attributes = transaction.composite_attributes(&output_payload.row_type)?;
        validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
        let outputs =
            compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "Join")?
                .join(", ");
        Ok(Layout {
            left_payload,
            right_payload,
            output_payload,
            left_state,
            right_state,
            left_key_exprs,
            right_key_exprs,
            continuation,
            condition: compile_scalar_expression(&spec.condition, &bindings)?,
            outputs,
        })
    }

    fn validate_state_abi(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        row_type: &TypeRef,
        key_slots: &[&InputSlot],
    ) -> Result<(), String> {
        let attributes = transaction.relation_attributes(relation.oid())?;
        let expected_len = 6 + key_slots.len();
        if attributes.len() != expected_len {
            return Err("Join arrangement relation changed its ABI".into());
        }
        let fixed = [
            (0, "row_id", pg_sys::INT8OID),
            (1, "row_key", pg_sys::BYTEAOID),
            (2, "row_value", row_type.oid()),
            (3 + key_slots.len(), "multiplicity", pg_sys::INT8OID),
            (4 + key_slots.len(), "match_count", pg_sys::INT8OID),
            (5 + key_slots.len(), "unknown_count", pg_sys::INT8OID),
        ];
        if fixed.iter().any(|(ordinal, name, type_oid)| {
            let attribute = &attributes[*ordinal];
            attribute.name != *name || attribute.type_oid != *type_oid || !attribute.not_null
        }) {
            return Err("Join arrangement relation changed its ABI".into());
        }
        for (ordinal, (attribute, slot)) in attributes[3..3 + key_slots.len()]
            .iter()
            .zip(key_slots)
            .enumerate()
        {
            if attribute.name != format!("key_{ordinal}")
                || attribute.type_oid.to_u32() != slot.type_.type_oid
                || attribute.typmod != slot.type_.typmod
                || attribute.collation_oid.to_u32() != slot.type_.collation_oid
                || attribute.not_null
            {
                return Err("Join equality key columns changed the Join ABI".into());
            }
        }
        let arguments = unsafe { [DatumWithOid::new(relation.oid(), pg_sys::OIDOID)] };
        let indexes = transaction.read(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_index AS identity_index
              WHERE identity_index.indrelid = $1
                AND identity_index.indisunique
                AND identity_index.indisvalid
                AND identity_index.indisready
                AND identity_index.indislive
                AND identity_index.indnkeyatts = 1
                AND identity_index.indnatts = 1
                AND identity_index.indkey[0] = 2
                AND identity_index.indexprs IS NULL
                AND identity_index.indpred IS NULL
            )
            "#,
            &arguments,
        )?;
        if !required_table::<bool>(&indexes.first(), 1, "Join arrangement row-key unique index")? {
            return Err("Join arrangement relation lacks its row-key unique index".into());
        }
        if key_slots.is_empty() {
            return Ok(());
        }
        let index_keys = (0..key_slots.len())
            .map(|ordinal| format!("identity_index.indkey[{}] = {}", ordinal, ordinal + 4))
            .chain(std::iter::once(format!(
                "identity_index.indkey[{}] = 1",
                key_slots.len()
            )))
            .collect::<Vec<_>>()
            .join(" AND ");
        let arity = key_slots.len() + 1;
        let arguments = unsafe { [DatumWithOid::new(relation.oid(), pg_sys::OIDOID)] };
        let indexes = transaction.read(
            &format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_index AS identity_index
                  WHERE identity_index.indrelid = $1
                    AND identity_index.indnkeyatts = {arity}
                    AND identity_index.indnatts = {arity}
                    AND identity_index.indisvalid
                    AND identity_index.indisready
                    AND identity_index.indislive
                    AND identity_index.indexprs IS NULL
                    AND identity_index.indpred IS NULL
                    AND {index_keys}
                )
                "#
            ),
            &arguments,
        )?;
        if !required_table::<bool>(&indexes.first(), 1, "Join equality key index")? {
            return Err("Join arrangement lacks its equality lookup index".into());
        }
        Ok(())
    }

    fn join_key_slots<'a>(
        stage: &'a DataflowStage,
        spec: &JoinSpec,
        input: u16,
    ) -> Result<Vec<&'a InputSlot>, String> {
        spec.equi_keys
            .iter()
            .map(|key| {
                let binding = if input == 0 {
                    key.left_binding
                } else {
                    key.right_binding
                };
                let slot = stage
                    .schema
                    .inputs
                    .iter()
                    .find(|slot| slot.binding == binding)
                    .ok_or_else(|| {
                        format!(
                            "Join equality key references missing BindingId {}",
                            binding.0
                        )
                    })?;
                if slot.input != input {
                    return Err(format!(
                        "Join equality key BindingId {} belongs to input {}, expected {}",
                        binding.0, slot.input, input
                    ));
                }
                Ok(slot)
            })
            .collect()
    }

    fn key_expressions(
        bindings: &[SqlBinding],
        keys: &[JoinEquiKey],
        left: bool,
    ) -> Result<Vec<String>, String> {
        keys.iter()
            .map(|key| {
                let binding = if left {
                    key.left_binding
                } else {
                    key.right_binding
                };
                compile_scalar_expression(&ScalarExpr::Input { binding }, bindings)
            })
            .collect()
    }

    fn validate_continuation_abi(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
    ) -> Result<(), String> {
        validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "Join")
    }

    fn effective_budget(transaction: &StepContext<'_, '_>) -> Result<WorkBudget, String> {
        let budget = transaction.budget();
        let output = transaction.output()?;
        let output_rows =
            usize::try_from(output.target_rows).map_err(|_| "negative Join row target")?;
        let output_bytes =
            usize::try_from(output.target_bytes).map_err(|_| "negative Join byte target")?;
        if output_rows == 0 || output_bytes == 0 {
            return Err("Join output stream has a zero target".into());
        }
        Ok(WorkBudget::new(
            budget.max_input_rows,
            budget.max_input_bytes,
            budget.max_output_rows.min(output_rows),
            budget.max_output_bytes.min(output_bytes),
        ))
    }

    fn key_predicate_sql(
        layout: &Layout,
        current_side: InputSide,
        candidate_alias: &str,
        include_unknown: bool,
    ) -> String {
        if !layout.keyed() {
            return "TRUE".into();
        }
        let exact = key_exact_predicate_sql(layout, current_side, candidate_alias);
        if !include_unknown {
            return format!("({exact})");
        }
        format!(
            "({exact}) OR ({})",
            key_unknown_predicate_sql(layout, current_side, candidate_alias)
        )
    }

    fn key_exact_predicate_sql(
        layout: &Layout,
        current_side: InputSide,
        candidate_alias: &str,
    ) -> String {
        layout
            .key_exprs(current_side)
            .iter()
            .enumerate()
            .map(|(ordinal, expression)| {
                format!("{candidate_alias}.key_{ordinal} = ({expression})")
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn key_unknown_predicate_sql(
        layout: &Layout,
        current_side: InputSide,
        candidate_alias: &str,
    ) -> String {
        let compatible = layout
            .key_exprs(current_side)
            .iter()
            .enumerate()
            .map(|(ordinal, expression)| {
                format!(
                    "({candidate_alias}.key_{ordinal} = ({expression}) OR {candidate_alias}.key_{ordinal} IS NULL OR ({expression}) IS NULL)"
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let any_unknown = layout
            .key_exprs(current_side)
            .iter()
            .enumerate()
            .map(|(ordinal, expression)| {
                format!("{candidate_alias}.key_{ordinal} IS NULL OR ({expression}) IS NULL")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        format!("(({compatible}) AND ({any_unknown}))")
    }

    fn key_join_sql(
        layout: &Layout,
        current_side: InputSide,
        candidate_alias: &str,
        include_unknown: bool,
    ) -> Result<String, String> {
        if !layout.keyed() {
            return Ok(String::new());
        }
        Ok(format!(
            "AND {}",
            key_predicate_sql(layout, current_side, candidate_alias, include_unknown)
        ))
    }

    #[derive(Clone, Debug)]
    struct InnerPage {
        side: InputSide,
        chunk: ChunkMeta,
        output_rows: u64,
        output_bytes: u64,
    }

    /// Measures one complete inner-join input chunk. The persisted row cursor
    /// remains the continuation for chunks whose fanout exceeds the quantum.
    fn measure_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
    ) -> Result<Option<InnerPage>, String> {
        let left = next_chunk(transaction, 0)?;
        let right = next_chunk(transaction, 1)?;
        let (side, head) = match (left, right) {
            (Some(left), Some(right)) => {
                if (left.lsn, 0_u16) <= (right.lsn, 1_u16) {
                    (InputSide::Left, left)
                } else {
                    (InputSide::Right, right)
                }
            }
            (Some(left), None) => (InputSide::Left, left),
            (None, Some(right)) => (InputSide::Right, right),
            (None, None) => return Ok(None),
        };
        if head.kind != ChunkKind::Data {
            return Ok(None);
        }
        let budget = effective_budget(transaction)?;
        if head.rows > usize_to_u64(budget.max_input_rows, "Join page input row budget")?
            || head.bytes > usize_to_u64(budget.max_input_bytes, "Join page input byte budget")?
        {
            return Ok(None);
        }
        let current_payload = layout.input_payload(side);
        payload_facts(transaction, &current_payload.relation, &head)?;

        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let key_join = key_join_sql(layout, side, opposite_alias, false)?;
        let output_row = format!(
            "ROW({})::{}",
            layout.outputs,
            layout.output_payload.row_type.sql()
        );
        let page_predicate =
            format!("{current_alias}.stream_id=$1 AND {current_alias}.chunk_seq=$2");
        let measured = transaction.read(
            &format!(
                r#"
                SELECT count(*)::bigint,
                       coalesce(sum(
                         shiba_internal.effect_row_bytes({output_row})
                       ),0)::bigint
                FROM {current_payload} AS {current_alias}
                JOIN {opposite_state} AS {opposite_alias}
                  ON ({condition}) IS TRUE
                {key_join}
                WHERE {page_predicate}
                "#,
                current_payload = current_payload.relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
                key_join = key_join,
            ),
            &unsafe {
                [
                    DatumWithOid::new(head.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(head.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if measured.len() != 1 {
            return Err("Join page measurement returned no summary".into());
        }
        let measured = measured.first();
        let output_rows = nonnegative(
            required_table(&measured, 1, "Join page output rows")?,
            "Join page output rows",
        )?;
        let output_bytes = nonnegative(
            required_table(&measured, 2, "Join page output bytes")?,
            "Join page output bytes",
        )?;
        if output_rows > usize_to_u64(budget.max_output_rows, "Join page output row budget")?
            || output_bytes > usize_to_u64(budget.max_output_bytes, "Join page output byte budget")?
        {
            return Ok(None);
        }
        Ok(Some(InnerPage {
            side,
            chunk: head,
            output_rows,
            output_bytes,
        }))
    }

    fn execute_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        page: InnerPage,
    ) -> Result<StepReceipt, String> {
        let input = transaction.input(page.side.code() as u16)?.clone();
        let output_facts = append_inner_page(
            transaction,
            layout,
            page.side,
            &page.chunk,
            page.output_rows,
            page.output_bytes,
        )?;
        update_inner_page_candidates(transaction, layout, page.side, &page.chunk)?;
        apply_inner_page_own_state(transaction, layout, page.side, &page.chunk)?;
        advance_input(
            transaction,
            input.port,
            input
                .next_chunk_seq
                .checked_add(1)
                .ok_or_else(|| "Join page input cursor overflow".to_string())?,
            input.consumed_frontier_lsn,
            WorkUsage {
                input_rows: page.chunk.rows,
                input_bytes: page.chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        if page.output_rows == 0 && !matches!(output_facts, OutputFacts::None) {
            return Err("empty Join page unexpectedly published output".into());
        }
        transaction.transition(
            KernelPhase::Process,
            WorkUsage {
                input_rows: page.chunk.rows,
                input_bytes: page.chunk.bytes,
                output_rows: page.output_rows,
                output_bytes: page.output_bytes,
            },
        )
    }

    // Atomic bounded Join page primitive: write typed payload and join-side
    // state for one input page; StepContext owns output publication.
    fn append_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
        expected_rows: u64,
        expected_bytes: u64,
    ) -> Result<OutputFacts, String> {
        if expected_rows == 0 {
            if expected_bytes != 0 {
                return Err("empty Join page measured nonzero output bytes".into());
            }
            return Ok(OutputFacts::None);
        }
        let append_target = transaction.output_append_target(expected_rows, expected_bytes)?;
        let output = transaction.output()?.clone();
        let (target_sequence, row_offset) = match append_target {
            OutputAppendTarget::New { sequence } => (sequence, 0),
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                ..
            } => (sequence, row_offset),
        };
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let key_join = key_join_sql(layout, side, opposite_alias, false)?;
        let output_row = format!(
            "ROW({})::{}",
            layout.outputs,
            layout.output_payload.row_type.sql()
        );
        let inserted = transaction.write(
            &format!(
                r#"
                WITH joined AS MATERIALIZED (
                  SELECT row_number() OVER (
                           ORDER BY {current_alias}.row_ordinal,
                                    {opposite_alias}.row_id
                         ) - 1 AS page_ordinal,
                         {current_alias}.weight
                           * {opposite_alias}.multiplicity AS weight,
                         {output_row} AS row_value
                  FROM {current_payload} AS {current_alias}
                  JOIN {opposite_state} AS {opposite_alias}
                    ON ({condition}) IS TRUE
                  {key_join}
                  WHERE {current_alias}.stream_id=$1
                    AND {current_alias}.chunk_seq=$2
                ),
                stored AS (
                  INSERT INTO {output_payload}(
                    stream_id,chunk_seq,row_ordinal,weight,row_value
                  )
                  SELECT $3,$4,$5+page_ordinal,weight,row_value
                  FROM joined
                  ORDER BY page_ordinal
                  RETURNING shiba_internal.effect_row_bytes(row_value) AS row_bytes
                )
                SELECT count(*)::bigint,
                       coalesce(sum(row_bytes),0)::bigint
                FROM stored
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
                key_join = key_join,
                output_payload = layout.output_payload.relation.sql(),
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                    DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(target_sequence, pg_sys::INT8OID),
                    DatumWithOid::new(i64_from_u64(row_offset)?, pg_sys::INT8OID),
                ]
            },
        )?;
        if inserted.len() != 1 {
            return Err("Join page append returned no summary".into());
        }
        let inserted = inserted.first();
        if nonnegative(
            required_table(&inserted, 1, "Join page inserted rows")?,
            "Join page inserted rows",
        )? != expected_rows
            || nonnegative(
                required_table(&inserted, 2, "Join page inserted bytes")?,
                "Join page inserted bytes",
            )? != expected_bytes
        {
            return Err("Join page append disagrees with its measurement".into());
        }
        transaction.record_output_append(
            append_target,
            expected_rows,
            expected_bytes,
            chunk.lsn,
        )?;
        Ok(OutputFacts::Data {
            chunk_seq: target_sequence,
        })
    }

    fn update_inner_page_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
    ) -> Result<(), String> {
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let key_join = key_join_sql(layout, side, opposite_alias, true)?;
        let updated = transaction.write(
            &format!(
                r#"
                WITH deltas AS MATERIALIZED (
                  SELECT {opposite_alias}.row_id,
                         coalesce(sum({current_alias}.weight)
                           FILTER (WHERE ({condition}) IS TRUE),0)::bigint
                           AS matched_delta,
                         coalesce(sum({current_alias}.weight)
                           FILTER (WHERE ({condition}) IS NULL),0)::bigint
                           AS unknown_delta
                  FROM {current_payload} AS {current_alias}
                  JOIN {opposite_state} AS {opposite_alias}
                    ON TRUE
                  {key_join}
                  WHERE {current_alias}.stream_id=$1
                    AND {current_alias}.chunk_seq=$2
                  GROUP BY {opposite_alias}.row_id
                ),
                changed AS (
                  UPDATE {opposite_state} AS candidate
                  SET match_count=candidate.match_count+deltas.matched_delta,
                      unknown_count=candidate.unknown_count+deltas.unknown_delta
                  FROM deltas
                  WHERE candidate.row_id=deltas.row_id
                    AND candidate.match_count+deltas.matched_delta >= 0
                    AND candidate.unknown_count+deltas.unknown_delta >= 0
                    AND (
                      deltas.matched_delta <> 0
                      OR deltas.unknown_delta <> 0
                    )
                  RETURNING candidate.row_id
                )
                SELECT count(*) FILTER (
                         WHERE matched_delta <> 0 OR unknown_delta <> 0
                       )::bigint,
                       (SELECT count(*)::bigint FROM changed)
                FROM deltas
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
                key_join = key_join,
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if updated.len() != 1 {
            return Err("Join page candidate update returned no summary".into());
        }
        let updated = updated.first();
        let expected = required_table::<i64>(&updated, 1, "Join page candidate changes")?;
        let actual = required_table::<i64>(&updated, 2, "Join page changed candidates")?;
        if expected != actual {
            return Err("Join page candidate counts would underflow".into());
        }
        Ok(())
    }

    fn apply_inner_page_own_state(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
    ) -> Result<(), String> {
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let state = layout.state(side);
        let match_key_join = key_join_sql(layout, side, opposite_alias, false)?;
        let unknown_key_join = key_join_sql(layout, side, opposite_alias, true)?;
        let counts_join = if layout.keyed() {
            String::new()
        } else {
            format!(
                r#"
                  LEFT JOIN LATERAL (
                    SELECT coalesce(sum({opposite_alias}.multiplicity)
                                      FILTER (WHERE ({condition}) IS TRUE),0)::bigint
                               AS match_count,
                           coalesce(sum({opposite_alias}.multiplicity)
                                      FILTER (WHERE ({condition}) IS NULL),0)::bigint
                               AS unknown_count
                    FROM {opposite_state} AS {opposite_alias}
                  ) AS counts ON TRUE
                "#,
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
            )
        };
        let match_count = if layout.keyed() {
            format!(
                r#"coalesce((
                           SELECT sum({opposite_alias}.multiplicity)::bigint
                           FROM LATERAL (
                             SELECT collapsed.row_value
                           ) AS {current_alias}
                           JOIN {opposite_state} AS {opposite_alias}
                             ON ({condition}) IS TRUE
                           {match_key_join}
                         ),0)::bigint"#,
                opposite_state = layout.state(side.opposite()).sql(),
                opposite_alias = opposite_alias,
                current_alias = current_alias,
                condition = layout.condition,
                match_key_join = match_key_join,
            )
        } else {
            "counts.match_count".into()
        };
        let unknown_count = if layout.keyed() {
            format!(
                r#"coalesce((
                           SELECT sum({opposite_alias}.multiplicity)::bigint
                           FROM LATERAL (
                             SELECT collapsed.row_value
                           ) AS {current_alias}
                           JOIN {opposite_state} AS {opposite_alias}
                             ON ({condition}) IS NULL
                           {unknown_key_join}
                         ),0)::bigint"#,
                opposite_state = layout.state(side.opposite()).sql(),
                opposite_alias = opposite_alias,
                current_alias = current_alias,
                condition = layout.condition,
                unknown_key_join = unknown_key_join,
            )
        } else {
            "counts.unknown_count".into()
        };
        let key_columns = layout
            .key_exprs(side)
            .iter()
            .enumerate()
            .map(|(ordinal, _)| format!("key_{ordinal}"))
            .collect::<Vec<_>>();
        let key_projection = layout
            .key_exprs(side)
            .iter()
            .enumerate()
            .map(|(ordinal, expression)| format!("{expression} AS key_{ordinal}"))
            .collect::<Vec<_>>();
        let key_projection = if key_projection.is_empty() {
            String::new()
        } else {
            format!(",{}", key_projection.join(","))
        };
        let insert_columns = if key_columns.is_empty() {
            "row_key,row_value,multiplicity,match_count,unknown_count".to_string()
        } else {
            format!(
                "row_key,row_value,{},multiplicity,match_count,unknown_count",
                key_columns.join(",")
            )
        };
        let insert_values = if key_columns.is_empty() {
            "row_key,row_value,new_multiplicity,match_count,unknown_count".to_string()
        } else {
            format!(
                "row_key,row_value,{},new_multiplicity,match_count,unknown_count",
                key_columns.join(",")
            )
        };
        let row_key = canonical_row_key_sql("effect.row_value", layout.input_type(side));
        let changed = transaction.write(
            &format!(
                r#"
                WITH incoming AS MATERIALIZED (
                  SELECT effect.row_ordinal,effect.row_value,effect.weight,
                         {row_key} AS row_key,
                         sum(effect.weight) OVER (
                           PARTITION BY {row_key}
                           ORDER BY effect.row_ordinal
                           ROWS UNBOUNDED PRECEDING
                         ) AS prefix
                  FROM {current_payload} AS effect
                  WHERE effect.stream_id=$1 AND effect.chunk_seq=$2
                ),
                collapsed AS MATERIALIZED (
                  SELECT row_key,
                         (array_agg(row_value ORDER BY row_ordinal))[1]
                           AS row_value,
                         sum(weight)::bigint AS net_weight,
                         min(prefix)::bigint AS min_prefix
                  FROM incoming
                  GROUP BY row_key
                ),
                desired AS MATERIALIZED (
                  SELECT collapsed.*{key_projection},
                         own.row_id,
                         coalesce(own.multiplicity,0)::bigint AS old_multiplicity,
                         {match_count} AS match_count,
                         {unknown_count} AS unknown_count
                  FROM collapsed
                  CROSS JOIN LATERAL (
                    SELECT collapsed.row_value
                  ) AS {current_alias}
                  {counts_join}
                  LEFT JOIN {state} AS own USING(row_key)
                ),
                valid AS MATERIALIZED (
                  SELECT *,old_multiplicity+net_weight AS new_multiplicity
                  FROM desired
                  WHERE old_multiplicity+min_prefix >= 0
                    AND old_multiplicity+net_weight >= 0
                ),
                removed AS (
                  DELETE FROM {state} AS own
                  USING valid
                  WHERE own.row_id=valid.row_id
                    AND valid.new_multiplicity=0
                  RETURNING own.row_id
                ),
                updated AS (
                  UPDATE {state} AS own
                  SET multiplicity=valid.new_multiplicity,
                      match_count=valid.match_count,
                      unknown_count=valid.unknown_count
                  FROM valid
                  WHERE own.row_id=valid.row_id
                    AND valid.new_multiplicity>0
                  RETURNING own.row_id
                ),
                inserted AS (
                  INSERT INTO {state}({insert_columns})
                  SELECT {insert_values}
                  FROM valid
                  WHERE row_id IS NULL AND new_multiplicity>0
                  ON CONFLICT (row_key) DO NOTHING
                  RETURNING row_id
                )
                SELECT (SELECT count(*)::bigint FROM collapsed),
                       (SELECT count(*)::bigint FROM valid),
                       (SELECT count(*)::bigint FROM removed)
                         +(SELECT count(*)::bigint FROM updated)
                         +(SELECT count(*)::bigint FROM inserted)
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                current_alias = current_alias,
                match_count = match_count,
                unknown_count = unknown_count,
                counts_join = counts_join,
                key_projection = key_projection,
                insert_columns = insert_columns,
                insert_values = insert_values,
                state = state.sql(),
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if changed.len() != 1 {
            return Err("Join page own-state mutation returned no summary".into());
        }
        let changed = changed.first();
        let collapsed = required_table::<i64>(&changed, 1, "Join page collapsed rows")?;
        let valid = required_table::<i64>(&changed, 2, "Join page valid rows")?;
        let mutations = required_table::<i64>(&changed, 3, "Join page state mutations")?;
        if collapsed != valid || valid != mutations {
            return Err("Join page own multiplicity would underflow".into());
        }
        Ok(())
    }

    fn load_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
    ) -> Result<Option<JoinContinuation>, String> {
        lock_continuation(
            transaction,
            relation,
            "phase,input_side,
             left_stream_id,left_chunk_seq,left_row_ordinal,
             right_stream_id,right_chunk_seq,right_row_ordinal,
             event_weight,event_bytes,
             own_row_id,own_multiplicity,
             own_match_count,own_unknown_count,
             candidate_after,
             accumulated_match_count,accumulated_unknown_count,
             pending_row_id,pending_multiplicity,pending_truth,
             pending_old_match,pending_old_unknown,
             pending_new_match,pending_new_unknown,
             frontier_lsn::text",
            "Join",
            |rows| decode_continuation(&rows.first()),
        )
    }

    fn decode_continuation(row: &pgrx::spi::SpiTupleTable<'_>) -> Result<JoinContinuation, String> {
        let phase =
            JoinPhase::from_code(PhaseCode::active(required_table(row, 1, "Join phase")?)?)?;
        let side = InputSide::from_code(required_table(row, 2, "Join input side")?)?;
        let positions = InputPositions::new(
            InputPosition::new(
                required_table(row, 3, "Join left stream")?,
                required_table(row, 4, "Join left chunk")?,
                required_table(row, 5, "Join left row")?,
            )?,
            InputPosition::new(
                required_table(row, 6, "Join right stream")?,
                required_table(row, 7, "Join right chunk")?,
                required_table(row, 8, "Join right row")?,
            )?,
        )?;
        let event_weight = optional_table::<i64>(row, 9)?;
        let event_bytes = optional_nonnegative(row, 10, "Join event bytes")?;
        let own_row_id = optional_table::<i64>(row, 11)?;
        let own_multiplicity = optional_nonnegative(row, 12, "Join own multiplicity")?;
        let own_match = optional_nonnegative(row, 13, "Join own match count")?;
        let own_unknown = optional_nonnegative(row, 14, "Join own unknown count")?;
        let candidate_after = optional_table::<i64>(row, 15)?;
        let accumulated_match = optional_nonnegative(row, 16, "Join accumulated match count")?;
        let accumulated_unknown = optional_nonnegative(row, 17, "Join accumulated unknown count")?;
        let pending_row_id = optional_table::<i64>(row, 18)?;
        let pending_multiplicity = optional_nonnegative(row, 19, "Join pending multiplicity")?;
        let pending_truth = optional_table::<i16>(row, 20)?;
        let pending_old_match = optional_nonnegative(row, 21, "Join pending old match count")?;
        let pending_old_unknown = optional_nonnegative(row, 22, "Join pending old unknown count")?;
        let pending_new_match = optional_nonnegative(row, 23, "Join pending new match count")?;
        let pending_new_unknown = optional_nonnegative(row, 24, "Join pending new unknown count")?;
        let frontier = optional_table::<String>(row, 25)?
            .map(|value| {
                parse_lsn(&value).map_err(|error| format!("invalid Join frontier LSN: {error}"))
            })
            .transpose()?;
        let data_fields = [
            event_weight.is_some(),
            event_bytes.is_some(),
            own_multiplicity.is_some(),
            own_match.is_some(),
            own_unknown.is_some(),
            accumulated_match.is_some(),
            accumulated_unknown.is_some(),
        ];
        let pending_fields = [
            pending_row_id.is_some(),
            pending_multiplicity.is_some(),
            pending_truth.is_some(),
            pending_old_match.is_some(),
            pending_old_unknown.is_some(),
            pending_new_match.is_some(),
            pending_new_unknown.is_some(),
        ];

        match phase {
            JoinPhase::Preflight => {
                if data_fields.into_iter().any(|present| present)
                    || own_row_id.is_some()
                    || candidate_after.is_some()
                    || pending_fields.into_iter().any(|present| present)
                    || frontier.is_some()
                {
                    return Err(
                        "Join Preflight continuation contains phase-incompatible fields".into(),
                    );
                }
                JoinContinuation::start_preflight(positions, side)
            }
            JoinPhase::Frontier => {
                if data_fields.into_iter().any(|present| present)
                    || own_row_id.is_some()
                    || candidate_after.is_some()
                    || pending_fields.into_iter().any(|present| present)
                {
                    return Err("Join Frontier continuation contains data-event fields".into());
                }
                JoinContinuation::start_frontier(FrontierInputFacts::new(
                    side,
                    positions,
                    frontier.ok_or_else(|| {
                        "Join Frontier continuation omitted its frontier".to_string()
                    })?,
                )?)
            }
            JoinPhase::Probe | JoinPhase::PendingTransition | JoinPhase::Finalize => {
                if !data_fields.into_iter().all(|present| present) || frontier.is_some() {
                    return Err("Join data continuation omitted required scalar fields".into());
                }
                let own_counts = MatchCounts::new(
                    own_match.expect("presence was checked"),
                    own_unknown.expect("presence was checked"),
                )?;
                let own = match own_row_id {
                    Some(row_id) => OwnExpectation::present(
                        row_id,
                        own_multiplicity.expect("presence was checked"),
                        own_counts,
                    )?,
                    None => {
                        let absent = OwnExpectation {
                            row_id: None,
                            multiplicity: own_multiplicity.expect("presence was checked"),
                            counts: own_counts,
                        };
                        absent.validate()?;
                        absent
                    }
                };
                let progress = InputProgress::restore(
                    positions,
                    side,
                    event_weight.expect("presence was checked"),
                    event_bytes.expect("presence was checked"),
                    own,
                    candidate_after,
                    MatchCounts::new(
                        accumulated_match.expect("presence was checked"),
                        accumulated_unknown.expect("presence was checked"),
                    )?,
                )?;
                let pending = if pending_fields.into_iter().all(|present| present) {
                    Some(CandidateExpectation::new(
                        pending_row_id.expect("presence was checked"),
                        pending_multiplicity.expect("presence was checked"),
                        MatchTruth::from_code(pending_truth.expect("presence was checked"))?,
                        MatchCounts::new(
                            pending_old_match.expect("presence was checked"),
                            pending_old_unknown.expect("presence was checked"),
                        )?,
                        MatchCounts::new(
                            pending_new_match.expect("presence was checked"),
                            pending_new_unknown.expect("presence was checked"),
                        )?,
                    )?)
                } else if pending_fields.into_iter().any(|present| present) {
                    return Err("Join pending candidate fields are incomplete".into());
                } else {
                    None
                };
                JoinContinuation::restore_input(phase.code(), progress, pending)
            }
        }
    }

    struct JoinFields {
        phase: i16,
        input_side: i16,
        left_stream_id: i64,
        left_chunk_seq: i64,
        left_row_ordinal: i64,
        right_stream_id: i64,
        right_chunk_seq: i64,
        right_row_ordinal: i64,
        event_weight: Option<i64>,
        event_bytes: Option<i64>,
        own_row_id: Option<i64>,
        own_multiplicity: Option<i64>,
        own_match_count: Option<i64>,
        own_unknown_count: Option<i64>,
        candidate_after: Option<i64>,
        accumulated_match_count: Option<i64>,
        accumulated_unknown_count: Option<i64>,
        pending_row_id: Option<i64>,
        pending_multiplicity: Option<i64>,
        pending_truth: Option<i16>,
        pending_old_match: Option<i64>,
        pending_old_unknown: Option<i64>,
        pending_new_match: Option<i64>,
        pending_new_unknown: Option<i64>,
        frontier_lsn: Option<String>,
    }

    fn continuation_fields(continuation: &JoinContinuation) -> Result<JoinFields, String> {
        let (positions, side) = match continuation {
            JoinContinuation::Preflight { positions, side } => (*positions, *side),
            JoinContinuation::Probe(progress)
            | JoinContinuation::PendingTransition { progress, .. }
            | JoinContinuation::Finalize(progress) => (progress.positions(), progress.side()),
            JoinContinuation::Frontier(frontier) => (frontier.positions(), frontier.side()),
        };
        let mut fields = JoinFields {
            phase: continuation.phase().code().value(),
            input_side: side.code(),
            left_stream_id: positions.left.stream_id,
            left_chunk_seq: positions.left.chunk_seq,
            left_row_ordinal: positions.left.row_ordinal,
            right_stream_id: positions.right.stream_id,
            right_chunk_seq: positions.right.chunk_seq,
            right_row_ordinal: positions.right.row_ordinal,
            event_weight: None,
            event_bytes: None,
            own_row_id: None,
            own_multiplicity: None,
            own_match_count: None,
            own_unknown_count: None,
            candidate_after: None,
            accumulated_match_count: None,
            accumulated_unknown_count: None,
            pending_row_id: None,
            pending_multiplicity: None,
            pending_truth: None,
            pending_old_match: None,
            pending_old_unknown: None,
            pending_new_match: None,
            pending_new_unknown: None,
            frontier_lsn: None,
        };
        match continuation {
            JoinContinuation::Preflight { .. } => {}
            JoinContinuation::Frontier(frontier) => {
                fields.frontier_lsn = Some(format_lsn(frontier.frontier()));
            }
            JoinContinuation::Probe(progress) | JoinContinuation::Finalize(progress) => {
                encode_progress(&mut fields, *progress)?;
            }
            JoinContinuation::PendingTransition {
                progress,
                candidate,
            } => {
                encode_progress(&mut fields, *progress)?;
                fields.pending_row_id = Some(candidate.row_id);
                fields.pending_multiplicity =
                    Some(join_i64(candidate.multiplicity, "pending multiplicity")?);
                fields.pending_truth = Some(candidate.truth.code());
                fields.pending_old_match =
                    Some(join_i64(candidate.old_counts.matched, "pending old match")?);
                fields.pending_old_unknown = Some(join_i64(
                    candidate.old_counts.unknown,
                    "pending old unknown",
                )?);
                fields.pending_new_match =
                    Some(join_i64(candidate.new_counts.matched, "pending new match")?);
                fields.pending_new_unknown = Some(join_i64(
                    candidate.new_counts.unknown,
                    "pending new unknown",
                )?);
            }
        }
        Ok(fields)
    }

    fn encode_progress(fields: &mut JoinFields, progress: InputProgress) -> Result<(), String> {
        fields.event_weight = Some(progress.event_weight());
        fields.event_bytes = Some(join_i64(progress.event_bytes(), "event bytes")?);
        let own = progress.expected_own();
        fields.own_row_id = own.row_id;
        fields.own_multiplicity = Some(join_i64(own.multiplicity, "own multiplicity")?);
        fields.own_match_count = Some(join_i64(own.counts.matched, "own match count")?);
        fields.own_unknown_count = Some(join_i64(own.counts.unknown, "own unknown count")?);
        fields.candidate_after = progress.candidate_after();
        fields.accumulated_match_count = Some(join_i64(
            progress.opposite_counts().matched,
            "accumulated match count",
        )?);
        fields.accumulated_unknown_count = Some(join_i64(
            progress.opposite_counts().unknown,
            "accumulated unknown count",
        )?);
        Ok(())
    }

    fn join_i64(value: u64, field: &str) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| format!("Join {field} exceeds bigint"))
    }

    fn continuation_arguments<'a>(fields: &'a JoinFields) -> [DatumWithOid<'a>; 25] {
        unsafe {
            [
                DatumWithOid::new(fields.phase, pg_sys::INT2OID),
                DatumWithOid::new(fields.input_side, pg_sys::INT2OID),
                DatumWithOid::new(fields.left_stream_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.left_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(fields.left_row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_stream_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(fields.event_weight, pg_sys::INT8OID),
                DatumWithOid::new(fields.event_bytes, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_row_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_multiplicity, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_match_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_unknown_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.candidate_after, pg_sys::INT8OID),
                DatumWithOid::new(fields.accumulated_match_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.accumulated_unknown_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_row_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_multiplicity, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_truth, pg_sys::INT2OID),
                DatumWithOid::new(fields.pending_old_match, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_old_unknown, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_new_match, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_new_unknown, pg_sys::INT8OID),
                DatumWithOid::new(fields.frontier_lsn.as_deref(), pg_sys::TEXTOID),
            ]
        }
    }

    fn insert_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        continuation: &JoinContinuation,
    ) -> Result<(), String> {
        let fields = continuation_fields(continuation)?;
        let arguments = continuation_arguments(&fields);
        replace_continuation_cas(
            transaction,
            relation,
            CONTINUATION_COLUMNS,
            None,
            Some(&arguments),
            "Join",
        )
    }

    fn replace_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        expected: &JoinContinuation,
        next: Option<&JoinContinuation>,
    ) -> Result<(), String> {
        let expected_fields = continuation_fields(expected)?;
        let expected_arguments = continuation_arguments(&expected_fields);
        let next_fields = next.map(continuation_fields).transpose()?;
        let next_arguments = next_fields.as_ref().map(continuation_arguments);
        replace_continuation_cas(
            transaction,
            relation,
            CONTINUATION_COLUMNS,
            Some(&expected_arguments),
            next_arguments.as_ref().map(|arguments| &arguments[..]),
            "Join",
        )
    }

    fn consumer_positions(transaction: &StepContext<'_, '_>) -> Result<InputPositions, String> {
        InputPositions::new(
            InputPosition::new(
                transaction.input(0)?.stream_id,
                transaction.input(0)?.next_chunk_seq,
                0,
            )?,
            InputPosition::new(
                transaction.input(1)?.stream_id,
                transaction.input(1)?.next_chunk_seq,
                0,
            )?,
        )
    }

    fn validate_positions_for_side(
        transaction: &StepContext<'_, '_>,
        positions: InputPositions,
        owner: InputSide,
    ) -> Result<(), String> {
        positions.validate()?;
        for side in [InputSide::Left, InputSide::Right] {
            let input = transaction.input(side.code() as u16)?;
            let position = positions.get(side);
            if position.stream_id != input.stream_id
                || position.chunk_seq != input.next_chunk_seq
                || (side != owner && position.row_ordinal != 0)
            {
                return Err("Join continuation is not at its locked consumer positions".into());
            }
        }
        Ok(())
    }

    fn load_event(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        positions: InputPositions,
        side: InputSide,
    ) -> Result<(InputEventFacts, ChunkMeta), String> {
        validate_positions_for_side(transaction, positions, side)?;
        let position = positions.get(side);
        let input = transaction.input(side.code() as u16)?.clone();
        let chunk = chunk(transaction, &input, position.chunk_seq)?
            .ok_or_else(|| "Join continuation references a missing input chunk".to_string())?;
        if chunk.kind != ChunkKind::Data
            || position.row_ordinal < 0
            || u64::try_from(position.row_ordinal).map_err(|_| "negative Join row ordinal")?
                >= chunk.rows
        {
            return Err("Join data continuation references an invalid input row".into());
        }
        let payload = layout.input_payload(side);
        let query = format!(
            r#"
            SELECT effect.weight,
                   shiba_internal.effect_row_bytes(effect.row_value)
            FROM {} AS effect
            WHERE effect.stream_id = $1
              AND effect.chunk_seq = $2
              AND effect.row_ordinal = $3
            "#,
            payload.relation.sql()
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(position.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(position.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(position.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.read(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join input position has no unique typed effect row".into());
        }
        let table = table.first();
        let weight = required_table(&table, 1, "Join input weight")?;
        let row_bytes = nonnegative(
            required_table(&table, 2, "Join input bytes")?,
            "Join input bytes",
        )?;
        Ok((
            InputEventFacts::new(side, positions, weight, row_bytes)?,
            chunk,
        ))
    }

    fn load_own_expectation(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
    ) -> Result<OwnExpectation, String> {
        let position = event.positions.get(event.side);
        let state = layout.state(event.side);
        let payload = layout.input_payload(event.side);
        let row_key = canonical_row_key_sql("input_row.row_value", layout.input_type(event.side));
        let query = format!(
            r#"
            SELECT own.row_id,own.multiplicity,
                   own.match_count,own.unknown_count
            FROM {} AS own
            JOIN {} AS input_row
              ON own.row_key = {row_key}
            WHERE input_row.stream_id = $1
              AND input_row.chunk_seq = $2
              AND input_row.row_ordinal = $3
            FOR UPDATE OF own
            "#,
            state.sql(),
            payload.relation.sql()
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(position.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(position.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(position.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.lock(&query, &arguments)?;
        let own = match table.len() {
            0 => OwnExpectation::absent(),
            1 => {
                let row = table.first();
                OwnExpectation::present(
                    required_table(&row, 1, "Join own row ID")?,
                    nonnegative(
                        required_table(&row, 2, "Join own multiplicity")?,
                        "Join own multiplicity",
                    )?,
                    MatchCounts::new(
                        nonnegative(
                            required_table(&row, 3, "Join own match count")?,
                            "Join own match count",
                        )?,
                        nonnegative(
                            required_table(&row, 4, "Join own unknown count")?,
                            "Join own unknown count",
                        )?,
                    )?,
                )?
            }
            count => {
                return Err(format!(
                    "Join own typed row has {count} arrangement identities"
                ));
            }
        };
        own.validate_event(event.weight)?;
        Ok(own)
    }

    fn probe_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        mode: JoinMode,
        continuation: &JoinContinuation,
        event: InputEventFacts,
        budget: WorkBudget,
    ) -> Result<ProbePage, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join candidate probe has no input progress".to_string())?;
        let current = event.positions.get(event.side);
        let opposite = event.side.opposite();
        let current_alias = side_alias(event.side);
        let candidate_alias = side_alias(opposite);
        let pair_enabled = matches!(
            mode,
            JoinMode::Inner | JoinMode::Left | JoinMode::Right | JoinMode::Full
        );
        let candidate_enabled = candidate_side_is_output(mode, opposite);
        let old_eligible = eligibility_sql(
            mode,
            &format!("{candidate_alias}.match_count"),
            &format!("{candidate_alias}.unknown_count"),
        );
        let new_eligible = eligibility_sql(
            mode,
            &format!("{candidate_alias}.new_match_count"),
            &format!("{candidate_alias}.new_unknown_count"),
        );
        let pair_bytes = if pair_enabled {
            format!(
                "CASE WHEN {candidate_alias}.truth = 1 THEN
                   shiba_internal.effect_row_bytes(
                     ROW({})::{}
                   )
                 END",
                layout.outputs,
                layout.output_payload.row_type.sql()
            )
        } else {
            "NULL::bigint".into()
        };
        let candidate_bytes = if candidate_enabled {
            format!(
                "CASE WHEN ({old_eligible}) IS DISTINCT FROM ({new_eligible})
                   THEN (
                     SELECT shiba_internal.effect_row_bytes(
                              ROW({})::{}
                            )
                     FROM (
                       SELECT NULL::{} AS row_value
                     ) AS {}
                   )
                 END",
                layout.outputs,
                layout.output_payload.row_type.sql(),
                layout.input_type(event.side).sql(),
                current_alias,
            )
        } else {
            "NULL::bigint".into()
        };
        // Keep UNKNOWN counts exact for every Join mode.  Null-aware anti
        // consumes this count for eligibility, while outer/semi/anti modes
        // still need the durable state to describe the full three-valued
        // logic and to remain correct if their mode-specific policy changes.
        let unknown_delta = "CASE WHEN truth_rows.truth = -1 THEN $7::numeric ELSE 0::numeric END";
        let candidate_source = if layout.keyed() {
            let exact_key_predicate = key_exact_predicate_sql(layout, event.side, "candidate");
            let unknown_key_predicate = key_unknown_predicate_sql(layout, event.side, "candidate");
            format!(
                r#"
                SELECT candidate.row_id,candidate.row_value,
                       candidate.multiplicity,candidate.match_count,
                       candidate.unknown_count,
                       shiba_internal.effect_row_bytes(candidate.row_value)
                         AS row_bytes
                FROM {candidate_state} AS candidate
                CROSS JOIN current_input AS {current_alias}
                WHERE candidate.row_id > $4
                  AND ({exact_key_predicate})
                UNION ALL
                SELECT candidate.row_id,candidate.row_value,
                       candidate.multiplicity,candidate.match_count,
                       candidate.unknown_count,
                       shiba_internal.effect_row_bytes(candidate.row_value)
                         AS row_bytes
                FROM {candidate_state} AS candidate
                CROSS JOIN current_input AS {current_alias}
                WHERE candidate.row_id > $4
                  AND ({unknown_key_predicate})
                ORDER BY row_id
                LIMIT $5
                "#,
                candidate_state = layout.state(opposite).sql(),
                current_alias = current_alias,
                exact_key_predicate = exact_key_predicate,
                unknown_key_predicate = unknown_key_predicate,
            )
        } else {
            format!(
                r#"
                SELECT candidate.row_id,candidate.row_value,
                       candidate.multiplicity,candidate.match_count,
                       candidate.unknown_count,
                       shiba_internal.effect_row_bytes(candidate.row_value)
                         AS row_bytes
                FROM {candidate_state} AS candidate
                WHERE candidate.row_id > $4
                ORDER BY candidate.row_id
                LIMIT $5
                "#,
                candidate_state = layout.state(opposite).sql(),
            )
        };
        let query = format!(
            r#"
            WITH current_input AS MATERIALIZED (
              SELECT input_row.row_value
              FROM {current_payload} AS input_row
              WHERE input_row.stream_id = $1
                AND input_row.chunk_seq = $2
                AND input_row.row_ordinal = $3
            ),
            candidate_source AS MATERIALIZED (
              {candidate_source}
            ),
            measured AS MATERIALIZED (
              SELECT candidate_source.*,
                     row_number() OVER (ORDER BY row_id) AS running_rows,
                     sum(row_bytes) OVER (
                       ORDER BY row_id ROWS UNBOUNDED PRECEDING
                     ) AS running_bytes
              FROM candidate_source
            ),
            bounded AS MATERIALIZED (
              SELECT *
              FROM measured
              WHERE running_rows = 1 OR running_bytes <= $6
            ),
            truth_rows AS MATERIALIZED (
              SELECT {candidate_alias}.*,
                     CASE
                       WHEN ({condition}) IS TRUE THEN 1::smallint
                       WHEN ({condition}) IS NULL THEN -1::smallint
                       ELSE 0::smallint
                     END AS truth
              FROM bounded AS {candidate_alias}
              CROSS JOIN current_input AS {current_alias}
            ),
            counted AS MATERIALIZED (
              SELECT truth_rows.*,
                     (
                       truth_rows.match_count::numeric
                       + CASE WHEN truth_rows.truth = 1
                           THEN $7::numeric ELSE 0::numeric END
                     )::bigint AS new_match_count,
                     (
                       truth_rows.unknown_count::numeric
                       + {unknown_delta}
                     )::bigint AS new_unknown_count
              FROM truth_rows
            )
            SELECT {candidate_alias}.row_id,{candidate_alias}.multiplicity,
                   {candidate_alias}.truth,
                   {candidate_alias}.match_count,
                   {candidate_alias}.unknown_count,
                   {candidate_alias}.row_bytes,
                   {pair_bytes} AS pair_bytes,
                   {candidate_bytes} AS candidate_bytes
            FROM counted AS {candidate_alias}
            CROSS JOIN current_input AS {current_alias}
            ORDER BY {candidate_alias}.row_id
            "#,
            current_payload = layout.input_payload(event.side).relation.sql(),
            condition = layout.condition,
            candidate_source = candidate_source,
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(current.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(current.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(current.row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(progress.candidate_after().unwrap_or(0), pg_sys::INT8OID),
                DatumWithOid::new(i64_from_usize(budget.max_input_rows)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_usize(budget.max_input_bytes)?, pg_sys::INT8OID),
                DatumWithOid::new(event.weight, pg_sys::INT8OID),
            ]
        };
        let rows = transaction.read(&query, &arguments)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            candidates.push(CandidateProbe::new(
                required_row(&row, 1, "Join candidate row ID")?,
                nonnegative(
                    required_row(&row, 2, "Join candidate multiplicity")?,
                    "Join candidate multiplicity",
                )?,
                MatchTruth::from_code(required_row(&row, 3, "Join candidate truth")?)?,
                MatchCounts::new(
                    nonnegative(
                        required_row(&row, 4, "Join candidate match count")?,
                        "Join candidate match count",
                    )?,
                    nonnegative(
                        required_row(&row, 5, "Join candidate unknown count")?,
                        "Join candidate unknown count",
                    )?,
                )?,
                nonnegative(
                    required_row(&row, 6, "Join candidate bytes")?,
                    "Join candidate bytes",
                )?,
                ProjectionBytes::new(
                    optional_nonnegative_row(&row, 7, "Join pair bytes")?,
                    optional_nonnegative_row(&row, 8, "Join transition bytes")?,
                )?,
            )?);
        }
        let after = candidates
            .last()
            .map_or(progress.candidate_after().unwrap_or(0), |candidate| {
                candidate.row_id
            });
        let complete_query = if layout.keyed() {
            let exact_key_predicate = key_exact_predicate_sql(layout, event.side, "candidate");
            let unknown_key_predicate = key_unknown_predicate_sql(layout, event.side, "candidate");
            format!(
                r#"
                WITH current_input AS MATERIALIZED (
                  SELECT input_row.row_value
                  FROM {current_payload} AS input_row
                  WHERE input_row.stream_id = $2
                    AND input_row.chunk_seq = $3
                    AND input_row.row_ordinal = $4
                ), remaining AS (
                  SELECT candidate.row_id
                  FROM {candidate_state} AS candidate
                  CROSS JOIN current_input AS {current_alias}
                  WHERE candidate.row_id > $1
                    AND ({exact_key_predicate})
                  UNION ALL
                  SELECT candidate.row_id
                  FROM {candidate_state} AS candidate
                  CROSS JOIN current_input AS {current_alias}
                  WHERE candidate.row_id > $1
                    AND ({unknown_key_predicate})
                )
                SELECT NOT EXISTS (SELECT 1 FROM remaining)
                "#,
                candidate_state = layout.state(opposite).sql(),
                current_payload = layout.input_payload(event.side).relation.sql(),
                current_alias = current_alias,
                exact_key_predicate = exact_key_predicate,
                unknown_key_predicate = unknown_key_predicate,
            )
        } else {
            format!(
                "SELECT NOT EXISTS (SELECT 1 FROM {} WHERE row_id > $1)",
                layout.state(opposite).sql()
            )
        };
        let complete = if layout.keyed() {
            let arguments = unsafe {
                [
                    DatumWithOid::new(after, pg_sys::INT8OID),
                    DatumWithOid::new(current.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(current.chunk_seq, pg_sys::INT8OID),
                    DatumWithOid::new(current.row_ordinal, pg_sys::INT8OID),
                ]
            };
            transaction
                .read(&complete_query, &arguments)?
                .first()
                .get_one::<bool>()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "Join candidate completion probe returned NULL".to_string())?
        } else {
            let arguments = unsafe { [DatumWithOid::new(after, pg_sys::INT8OID)] };
            transaction
                .read(&complete_query, &arguments)?
                .first()
                .get_one::<bool>()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "Join candidate completion probe returned NULL".to_string())?
        };
        ProbePage::new(candidates, complete)
    }

    fn side_alias(side: InputSide) -> &'static str {
        match side {
            InputSide::Left => "left_row",
            InputSide::Right => "right_row",
        }
    }

    fn eligibility_sql(mode: JoinMode, matched: &str, unknown: &str) -> String {
        match mode {
            JoinMode::Inner => "false".into(),
            JoinMode::Left | JoinMode::Right | JoinMode::Full | JoinMode::Anti => {
                format!("{matched} = 0")
            }
            JoinMode::Semi => format!("{matched} > 0"),
            JoinMode::NullAwareAnti => format!("{matched} = 0 AND {unknown} = 0"),
        }
    }

    // Atomic bounded Join action primitive: apply one planned action page and
    // report its facts through the shared output boundary.
    fn append_actions(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        chunk: &ChunkMeta,
        actions: &[OutputAction],
        event: InputEventFacts,
    ) -> Result<OutputFacts, String> {
        if actions.is_empty() {
            return Ok(OutputFacts::None);
        }
        let current_position = event.positions.get(event.side);
        let current_alias = side_alias(event.side);
        let opposite_alias = side_alias(event.side.opposite());
        let mut selects = Vec::with_capacity(actions.len());
        for (ordinal, action) in actions.iter().copied().enumerate() {
            action.validate()?;
            if action.current_side != event.side {
                return Err("Join output action changed its current input side".into());
            }
            let row = format!(
                "ROW({})::{}",
                layout.outputs,
                layout.output_payload.row_type.sql()
            );
            let select = match action.kind {
                OutputActionKind::Pair => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {current_payload} AS {current_alias}
                    JOIN {candidate_state} AS {opposite_alias}
                      ON {opposite_alias}.row_id = {candidate_id}
                    WHERE {current_alias}.stream_id = {stream_id}
                      AND {current_alias}.chunk_seq = {chunk_seq}
                      AND {current_alias}.row_ordinal = {row_ordinal}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    candidate_id = action
                        .candidate_row_id
                        .ok_or_else(|| "Join pair action omitted its candidate".to_string())?,
                    current_payload = layout.input_payload(event.side).relation.sql(),
                    candidate_state = layout.state(event.side.opposite()).sql(),
                    stream_id = current_position.stream_id,
                    chunk_seq = current_position.chunk_seq,
                    row_ordinal = current_position.row_ordinal,
                ),
                OutputActionKind::CandidateEligibility => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {candidate_state} AS {opposite_alias}
                    CROSS JOIN (
                      SELECT NULL::{current_type} AS row_value
                    ) AS {current_alias}
                    WHERE {opposite_alias}.row_id = {candidate_id}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    candidate_id = action.candidate_row_id.ok_or_else(|| {
                        "Join transition action omitted its candidate".to_string()
                    })?,
                    candidate_state = layout.state(event.side.opposite()).sql(),
                    current_type = layout.input_type(event.side).sql(),
                ),
                OutputActionKind::CurrentEligibility => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {current_payload} AS {current_alias}
                    CROSS JOIN (
                      SELECT NULL::{opposite_type} AS row_value
                    ) AS {opposite_alias}
                    WHERE {current_alias}.stream_id = {stream_id}
                      AND {current_alias}.chunk_seq = {chunk_seq}
                      AND {current_alias}.row_ordinal = {row_ordinal}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    current_payload = layout.input_payload(event.side).relation.sql(),
                    opposite_type = layout.input_type(event.side.opposite()).sql(),
                    stream_id = current_position.stream_id,
                    chunk_seq = current_position.chunk_seq,
                    row_ordinal = current_position.row_ordinal,
                ),
            };
            selects.push(select);
        }
        let expected_rows = usize_to_u64(actions.len(), "Join output action count")?;
        let expected_bytes = actions.iter().try_fold(0_u64, |sum, action| {
            sum.checked_add(action.row_bytes)
                .ok_or_else(|| "Join output action bytes overflow".to_string())
        })?;
        let append_target = transaction.output_append_target(expected_rows, expected_bytes)?;
        let output = transaction.output()?.clone();
        let (target_sequence, row_offset) = match append_target {
            OutputAppendTarget::New { sequence } => (sequence, 0),
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                ..
            } => (sequence, row_offset),
        };
        let query = format!(
            r#"
            WITH action_rows AS MATERIALIZED (
              {action_rows}
            ),
            measured AS MATERIALIZED (
              SELECT action_rows.*,
                     shiba_internal.effect_row_bytes(row_value) AS row_bytes
              FROM action_rows
            ),
            stats AS MATERIALIZED (
              SELECT count(*)::bigint AS row_count,
                     coalesce(sum(row_bytes),0)::bigint AS payload_bytes,
                     min(action_ordinal)::bigint AS first_ordinal,
                     max(action_ordinal)::bigint AS last_ordinal
              FROM measured
            ),
            validated AS MATERIALIZED (
              SELECT *
              FROM stats
              WHERE row_count = $3
                AND payload_bytes = $4
                AND first_ordinal = 0
                AND last_ordinal = $3 - 1
            ),
            inserted AS (
              INSERT INTO {output_relation}(
                stream_id,chunk_seq,row_ordinal,weight,row_value
              )
              SELECT $1,$2,$5 + measured.action_ordinal,
                     measured.weight,measured.row_value
              FROM measured
              CROSS JOIN validated
              ORDER BY measured.action_ordinal
              RETURNING shiba_internal.effect_row_bytes(row_value)
                AS stored_bytes
            )
            SELECT stats.row_count,stats.payload_bytes,
                   (SELECT count(*)::bigint FROM inserted),
                   (
                     SELECT coalesce(sum(stored_bytes),0)::bigint
                     FROM inserted
            )
            FROM stats
            "#,
            action_rows = selects.join(" UNION ALL "),
            output_relation = layout.output_payload.relation.sql(),
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(target_sequence, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(expected_rows)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(expected_bytes)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(row_offset)?, pg_sys::INT8OID),
            ]
        };
        let table = transaction.write(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join output primitive returned no summary row".into());
        }
        let table = table.first();
        let rows = nonnegative(
            required_table(&table, 1, "Join evaluated output rows")?,
            "Join evaluated output rows",
        )?;
        let bytes = nonnegative(
            required_table(&table, 2, "Join evaluated output bytes")?,
            "Join evaluated output bytes",
        )?;
        let inserted = nonnegative(
            required_table(&table, 3, "Join inserted output rows")?,
            "Join inserted output rows",
        )?;
        let stored_bytes = nonnegative(
            required_table(&table, 4, "Join stored output bytes")?,
            "Join stored output bytes",
        )?;
        if rows != expected_rows || bytes != expected_bytes {
            return Err("Join output projection changed after its bounded probe".into());
        }
        if inserted != expected_rows || stored_bytes != expected_bytes {
            return Err("Join output staging returned inconsistent payload facts".into());
        }
        transaction.record_output_append(
            append_target,
            expected_rows,
            expected_bytes,
            chunk.lsn,
        )?;
        Ok(OutputFacts::Data {
            chunk_seq: target_sequence,
        })
    }

    fn apply_candidate_changes(
        transaction: &mut StepContext<'_, '_>,
        state: &RelationRef,
        changes: &[CandidateStateChange],
    ) -> Result<u64, String> {
        if changes.is_empty() {
            return Ok(0);
        }
        let values = changes
            .iter()
            .map(|change| {
                let expected = change.expected;
                format!(
                    "({},{},{},{},{},{})",
                    expected.row_id,
                    expected.multiplicity,
                    expected.old_counts.matched,
                    expected.old_counts.unknown,
                    expected.new_counts.matched,
                    expected.new_counts.unknown
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            r#"
            WITH expected(
              row_id,multiplicity,old_match,old_unknown,new_match,new_unknown
            ) AS (VALUES {values}),
            updated AS (
              UPDATE {state} AS arrangement
              SET match_count = expected.new_match,
                  unknown_count = expected.new_unknown
              FROM expected
              WHERE arrangement.row_id = expected.row_id
                AND arrangement.multiplicity = expected.multiplicity
                AND arrangement.match_count = expected.old_match
                AND arrangement.unknown_count = expected.old_unknown
              RETURNING arrangement.row_id
            )
            SELECT count(*)::bigint FROM updated
            "#,
            state = state.sql(),
        );
        let updated = transaction
            .write(&query, &[])?
            .first()
            .get_one::<i64>()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Join candidate update count returned NULL".to_string())?;
        let updated = nonnegative(updated, "Join candidate update count")?;
        if updated != usize_to_u64(changes.len(), "Join candidate update count")? {
            return Err("Join candidate compare-and-set changed an unexpected row count".into());
        }
        Ok(updated)
    }

    fn step_finalize(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        spec: &JoinSpec,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join Finalize phase omitted input progress".to_string())?;
        validate_positions_for_side(transaction, progress.positions(), progress.side())?;
        let (event, chunk) =
            load_event(transaction, layout, progress.positions(), progress.side())?;
        continuation.validate_input_resume(event)?;
        let actual_own = load_own_expectation(transaction, layout, event)?;
        if actual_own != progress.expected_own() {
            return Err("Join own arrangement changed during its fanout".into());
        }
        let output_required =
            current_side_is_eligible(mode(spec.kind), event.side, progress.opposite_counts());
        let output_bytes = if output_required {
            measure_current_output(transaction, layout, event)?
        } else {
            1
        };
        let own_probe = match actual_own.row_id {
            None => OwnStateProbe::absent(output_bytes)?,
            Some(row_id) => OwnStateProbe::present(
                row_id,
                actual_own.multiplicity,
                actual_own.counts,
                output_bytes,
            )?,
        };
        let budget = effective_budget(transaction)?;
        let plan = plan_finalize(mode(spec.kind), &continuation, event, own_probe, budget)?;
        let actions = plan.output().into_iter().collect::<Vec<_>>();
        let output = append_actions(transaction, layout, &chunk, &actions, event)?;
        apply_own_change(transaction, layout, event, plan.own_change())?;

        let position = progress.positions().get(progress.side());
        let next_ordinal = position
            .row_ordinal
            .checked_add(1)
            .ok_or_else(|| "Join input row ordinal overflow".to_string())?;
        let next =
            if u64::try_from(next_ordinal).map_err(|_| "negative Join row ordinal")? < chunk.rows {
                let mut positions = progress.positions();
                match progress.side() {
                    InputSide::Left => positions.left.row_ordinal = next_ordinal,
                    InputSide::Right => positions.right.row_ordinal = next_ordinal,
                }
                Some(JoinContinuation::start_preflight(
                    positions,
                    progress.side(),
                )?)
            } else if u64::try_from(next_ordinal).map_err(|_| "negative Join row ordinal")?
                == chunk.rows
            {
                let input = transaction.input(progress.side().code() as u16)?.clone();
                advance_input(
                    transaction,
                    input.port,
                    input
                        .next_chunk_seq
                        .checked_add(1)
                        .ok_or_else(|| "Join input chunk cursor overflow".to_string())?,
                    input.consumed_frontier_lsn,
                    WorkUsage {
                        input_rows: chunk.rows,
                        input_bytes: chunk.bytes,
                        ..WorkUsage::default()
                    },
                )?;
                None
            } else {
                return Err("Join Finalize advanced beyond its immutable input chunk".into());
            };
        replace_continuation(
            transaction,
            &layout.continuation,
            &continuation,
            next.as_ref(),
        )?;
        let next_present = next.is_some();
        plan.validate_commit(
            PrimitiveFacts {
                usage: plan.usage(),
                state_rows: 1,
                output,
            },
            next_present,
        )?;
        Ok(JoinTransition::material(
            plan.usage(),
            next_present,
            KernelPhase::Process,
            1,
        ))
    }

    fn measure_current_output(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
    ) -> Result<u64, String> {
        let current = event.positions.get(event.side);
        let current_alias = side_alias(event.side);
        let opposite_alias = side_alias(event.side.opposite());
        let query = format!(
            r#"
            SELECT shiba_internal.effect_row_bytes(
                     ROW({})::{}
                   )
            FROM {} AS {}
            CROSS JOIN (
              SELECT NULL::{} AS row_value
            ) AS {}
            WHERE {}.stream_id = $1
              AND {}.chunk_seq = $2
              AND {}.row_ordinal = $3
            "#,
            layout.outputs,
            layout.output_payload.row_type.sql(),
            layout.input_payload(event.side).relation.sql(),
            current_alias,
            layout.input_type(event.side.opposite()).sql(),
            opposite_alias,
            current_alias,
            current_alias,
            current_alias,
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(current.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(current.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(current.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.read(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join current-row projection returned no unique row".into());
        }
        nonnegative(
            required_table(&table.first(), 1, "Join current output bytes")?,
            "Join current output bytes",
        )
    }

    fn apply_own_change(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
        change: OwnStateChange,
    ) -> Result<(), String> {
        let current = event.positions.get(event.side);
        let state = layout.state(event.side);
        let payload = layout.input_payload(event.side);
        let current_alias = side_alias(event.side);
        let row_key = canonical_row_key_sql(
            &format!("{current_alias}.row_value"),
            layout.input_type(event.side),
        );
        let key_columns = layout
            .key_exprs(event.side)
            .iter()
            .enumerate()
            .map(|(ordinal, _)| format!("key_{ordinal}"))
            .collect::<Vec<_>>();
        let insert_columns = if key_columns.is_empty() {
            "row_key,row_value,multiplicity,match_count,unknown_count".to_string()
        } else {
            format!(
                "row_key,row_value,{},multiplicity,match_count,unknown_count",
                key_columns.join(",")
            )
        };
        let insert_values = if key_columns.is_empty() {
            format!("{row_key},{current_alias}.row_value")
        } else {
            format!(
                "{row_key},{current_alias}.row_value,{}",
                layout.key_exprs(event.side).join(",")
            )
        };
        let query = match change.kind {
            OwnStateChangeKind::Insert => format!(
                r#"
                INSERT INTO {state}({insert_columns})
                SELECT {insert_values},{new_multiplicity},{match_count},{unknown_count}
                FROM {payload} AS {current_alias}
                WHERE {current_alias}.stream_id = {stream_id}
                  AND {current_alias}.chunk_seq = {chunk_seq}
                  AND {current_alias}.row_ordinal = {row_ordinal}
                ON CONFLICT (row_key) DO NOTHING
                RETURNING row_id
                "#,
                state = state.sql(),
                insert_columns = insert_columns,
                insert_values = insert_values,
                new_multiplicity = change.new_multiplicity,
                match_count = change.counts.matched,
                unknown_count = change.counts.unknown,
                payload = payload.relation.sql(),
                stream_id = current.stream_id,
                chunk_seq = current.chunk_seq,
                row_ordinal = current.row_ordinal,
            ),
            OwnStateChangeKind::Update => format!(
                r#"
                UPDATE {} SET multiplicity = {}
                WHERE row_id = {}
                  AND multiplicity = {}
                  AND match_count = {}
                  AND unknown_count = {}
                RETURNING row_id
                "#,
                state.sql(),
                change.new_multiplicity,
                change
                    .expected_row_id
                    .ok_or_else(|| "Join own update omitted its row ID".to_string())?,
                change.expected_multiplicity,
                change.counts.matched,
                change.counts.unknown,
            ),
            OwnStateChangeKind::Delete => format!(
                r#"
                DELETE FROM {}
                WHERE row_id = {}
                  AND multiplicity = {}
                  AND match_count = {}
                  AND unknown_count = {}
                RETURNING row_id
                "#,
                state.sql(),
                change
                    .expected_row_id
                    .ok_or_else(|| "Join own delete omitted its row ID".to_string())?,
                change.expected_multiplicity,
                change.counts.matched,
                change.counts.unknown,
            ),
        };
        let changed = transaction.write(&query, &[])?;
        if changed.len() != 1 {
            return Err("Join own arrangement compare-and-set did not affect one row".into());
        }
        Ok(())
    }

    fn step_frontier(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let JoinContinuation::Frontier(frontier) = &continuation else {
            return Err("Join frontier executor received another phase".into());
        };
        let facts =
            FrontierInputFacts::new(frontier.side(), frontier.positions(), frontier.frontier())?;
        continuation.validate_frontier_resume(facts)?;
        validate_positions_for_side(transaction, facts.positions, facts.side)?;
        let position = facts.positions.get(facts.side);
        if position.row_ordinal != 0 {
            return Err("Join frontier continuation has a data row ordinal".into());
        }
        let input = transaction.input(facts.side.code() as u16)?.clone();
        let head = chunk(transaction, &input, position.chunk_seq)?
            .ok_or_else(|| "Join frontier continuation references a missing chunk".to_string())?;
        if head.kind != ChunkKind::Frontier
            || head.rows != 0
            || head.bytes != 0
            || head.lsn != facts.frontier
        {
            return Err("Join frontier continuation changed its immutable chunk".into());
        }
        let output = transaction.output()?.clone();
        let published = output.published_frontier_lsn.unwrap_or(0);
        let state = FrontierState {
            consumed: InputFrontiers {
                left: transaction.input(0)?.consumed_frontier_lsn,
                right: transaction.input(1)?.consumed_frontier_lsn,
            },
            published,
            latest_output_data: output.latest_data_lsn,
        };
        let plan = plan_frontier(&continuation, facts, state, transaction.budget())?;
        let output_facts = if let Some(publish) = plan.publish {
            append_frontier(transaction, publish)?
        } else {
            OutputFacts::None
        };
        advance_input(
            transaction,
            input.port,
            input
                .next_chunk_seq
                .checked_add(1)
                .ok_or_else(|| "Join frontier chunk cursor overflow".to_string())?,
            facts.frontier,
            WorkUsage::default(),
        )?;
        replace_continuation(transaction, &layout.continuation, &continuation, None)?;
        plan.validate_commit(PrimitiveFacts {
            usage: WorkUsage::default(),
            state_rows: 0,
            output: output_facts,
        })?;
        Ok(JoinTransition::material(
            WorkUsage::default(),
            false,
            KernelPhase::Frontier,
            0,
        ))
    }

    fn required_table<T: FromDatum + IntoDatum>(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<T, String> {
        table
            .get::<T>(ordinal)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("database returned NULL {name}"))
    }

    fn optional_table<T: FromDatum + IntoDatum>(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
    ) -> Result<Option<T>, String> {
        table.get::<T>(ordinal).map_err(|error| error.to_string())
    }

    fn required_row<T: FromDatum + IntoDatum>(
        row: &pgrx::spi::SpiHeapTupleData<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<T, String> {
        row.get::<T>(ordinal)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("database returned NULL {name}"))
    }

    fn optional_nonnegative(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<Option<u64>, String> {
        optional_table::<i64>(table, ordinal)?
            .map(|value| nonnegative(value, name))
            .transpose()
    }

    fn optional_nonnegative_row(
        row: &pgrx::spi::SpiHeapTupleData<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<Option<u64>, String> {
        row.get::<i64>(ordinal)
            .map_err(|error| error.to_string())?
            .map(|value| nonnegative(value, name))
            .transpose()
    }

    fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
        u64::try_from(value).map_err(|_| format!("{name} is negative"))
    }

    fn i64_from_u64(value: u64) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| "Join resource count exceeds bigint".into())
    }

    fn i64_from_usize(value: usize) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| "Join resource budget exceeds bigint".into())
    }
}
