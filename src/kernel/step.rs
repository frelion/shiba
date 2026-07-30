use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{Query, SpiClient, SpiTupleTable};

use crate::logical::model::ExecutionSettings;
use crate::logical::{StepExecution, StepOutcome, WorkBudget};
use crate::postgres::parse_lsn;

use super::runner::LifecyclePermit;
use super::storage::{self, AttributeRef, PayloadStorage, RelationRef, TypeRef};
use super::{
    nonnegative, required_row, required_table, AdmissionProgress, KernelTransition, WorkUsage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerKind {
    Source,
    Operator,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InputState {
    pub(crate) port: u16,
    pub(crate) stream_id: i64,
    pub(crate) producer: ProducerKind,
    pub(crate) source_oid: Option<pg_sys::Oid>,
    pub(crate) slot_generation: Option<i64>,
    pub(crate) next_chunk_seq: i64,
    pub(crate) available_chunk_seq: i64,
    pub(crate) activation_lsn: u64,
    pub(crate) consumed_frontier_lsn: u64,
    pub(crate) available_source_frontier_lsn: Option<u64>,
}

impl InputState {
    pub(crate) fn has_pending(&self) -> bool {
        self.next_chunk_seq < self.available_chunk_seq
            || self
                .available_source_frontier_lsn
                .is_some_and(|frontier| frontier > self.consumed_frontier_lsn)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputState {
    pub(crate) stream_id: i64,
    pub(crate) next_chunk_seq: i64,
    pub(crate) target_rows: i64,
    pub(crate) target_bytes: i64,
    pub(crate) latest_data_lsn: Option<u64>,
    pub(crate) published_frontier_lsn: Option<u64>,
    pending_data_chunk: Option<PendingDataChunk>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingDataChunk {
    sequence: i64,
    rows: u64,
    bytes: u64,
    lsn: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutputAppendTarget {
    New {
        sequence: i64,
    },
    Extend {
        sequence: i64,
        row_offset: u64,
        previous_bytes: u64,
    },
}

pub(crate) enum StepContextStart<'client, 'conn> {
    Blocked,
    Idle,
    Ready(Box<StepContext<'client, 'conn>>),
}

/// The only mutable database context for one operator step.
///
/// The background worker owns the surrounding PostgreSQL transaction.
/// `StepContext` owns its deterministic lock order, the checkpoint revision, and
/// the distinction between locking reads and state-changing statements.
pub(crate) struct StepContext<'client, 'conn> {
    client: &'client mut SpiClient<'conn>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    quantum_budget: WorkBudget,
    transition_budget: WorkBudget,
    expected_revision: i64,
    checkpoint_had_continuation: bool,
    continuation_presence: Option<bool>,
    admission: AdmissionProgress,
    inputs: Vec<InputState>,
    output: Option<OutputState>,
}

impl<'client, 'conn> StepContext<'client, 'conn> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn begin(
        client: &'client mut SpiClient<'conn>,
        result_oid: pg_sys::Oid,
        stage_id: u32,
        expected_inputs: u16,
        expects_output: bool,
        settings: &ExecutionSettings,
        budget: WorkBudget,
        _permit: LifecyclePermit,
    ) -> Result<StepContextStart<'client, 'conn>, String> {
        if result_oid == pg_sys::InvalidOid {
            return Err("operator step has an invalid result OID".into());
        }
        let stage_id = i32::try_from(stage_id).map_err(|_| "operator stage ID exceeds integer")?;
        let identity = unsafe {
            [
                DatumWithOid::new(result_oid, pg_sys::OIDOID),
                DatumWithOid::new(stage_id, pg_sys::INT4OID),
            ]
        };

        let active = client
            .select(
                "SELECT EXISTS (
                   SELECT 1
                   FROM shiba_internal.dataflows AS dataflow
                   WHERE dataflow.result_oid = $1::oid
                     AND dataflow.active
                 )",
                Some(1),
                &identity[..1],
            )
            .map_err(|error| format!("could not inspect dataflow state: {error}"))?
            .first()
            .get_one::<bool>()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "dataflow state returned NULL".to_string())?;
        if !active {
            return Ok(StepContextStart::Idle);
        }

        apply_execution_settings(client, settings)?;

        let locked_streams = client
            .update(
                r#"
                SELECT stream.stream_id
                FROM shiba_internal.effect_streams AS stream
                WHERE stream.stream_id IN (
                  SELECT consumer.stream_id
                  FROM shiba_internal.effect_stream_consumers AS consumer
                  WHERE consumer.result_oid = $1::oid
                    AND consumer.consumer_stage_id = $2
                  UNION
                  SELECT output.stream_id
                  FROM shiba_internal.effect_streams AS output
                  WHERE output.producer_kind = 'operator'
                    AND output.producer_result_oid = $1::oid
                    AND output.producer_stage_id = $2
                )
                ORDER BY stream.stream_id
                FOR UPDATE
                "#,
                None,
                &identity,
            )
            .map_err(|error| format!("could not lock operator streams: {error}"))?
            .len();
        let expected_streams = usize::from(expected_inputs) + usize::from(expects_output);
        if locked_streams != expected_streams {
            return Err(format!(
                "operator stage {stage_id} locked {locked_streams} streams, expected {expected_streams}"
            ));
        }

        let checkpoint = client
            .update(
                r#"
                SELECT checkpoint.revision,
                       checkpoint.has_continuation,
                       checkpoint.admitted_rows,
                       checkpoint.admitted_bytes
                FROM shiba_internal.operator_checkpoints AS checkpoint
                WHERE checkpoint.result_oid = $1::oid
                  AND checkpoint.stage_id = $2
                FOR UPDATE
                "#,
                Some(2),
                &identity,
            )
            .map_err(|error| format!("could not lock operator checkpoint: {error}"))?;
        if checkpoint.len() != 1 {
            return Err(format!(
                "operator stage {stage_id} has no unique checkpoint"
            ));
        }
        let checkpoint = checkpoint.first();
        let expected_revision =
            required_table::<i64>(&checkpoint, 1, "operator checkpoint revision")?;
        let checkpoint_had_continuation =
            required_table::<bool>(&checkpoint, 2, "operator continuation flag")?;
        let admission = AdmissionProgress::new(
            nonnegative(
                required_table::<i64>(&checkpoint, 3, "admitted row progress")?,
                "admitted row progress",
            )?,
            nonnegative(
                required_table::<i64>(&checkpoint, 4, "admitted byte progress")?,
                "admitted byte progress",
            )?,
        );

        let input_rows = client
            .update(
                r#"
                SELECT consumer.input_port,
                       consumer.stream_id,
                       stream.producer_kind,
                       stream.source_oid,
                       stream.slot_generation,
                       consumer.next_chunk_seq,
                       stream.next_chunk_seq,
                       consumer.activation_lsn::text,
                       consumer.consumed_frontier_lsn::text,
                       publication.published_lsn::text
                FROM shiba_internal.effect_stream_consumers AS consumer
                JOIN shiba_internal.effect_streams AS stream
                  ON stream.stream_id = consumer.stream_id
                LEFT JOIN shiba_internal.ingress_replay_state AS publication
                  ON stream.producer_kind = 'source'
                 AND publication.slot_generation = stream.slot_generation
                WHERE consumer.result_oid = $1::oid
                  AND consumer.consumer_stage_id = $2
                ORDER BY consumer.stream_id, consumer.input_port
                FOR UPDATE OF consumer
                "#,
                None,
                &identity,
            )
            .map_err(|error| format!("could not lock operator inputs: {error}"))?;
        if input_rows.len() != usize::from(expected_inputs) {
            return Err(format!(
                "operator stage {stage_id} has {} inputs, expected {expected_inputs}",
                input_rows.len()
            ));
        }
        let mut inputs = Vec::with_capacity(input_rows.len());
        for row in input_rows {
            let raw_port = required_row::<i32>(&row, 1, "input port")?;
            let port =
                u16::try_from(raw_port).map_err(|_| format!("invalid input port {raw_port}"))?;
            let producer = match required_row::<String>(&row, 3, "input producer kind")?.as_str() {
                "source" => ProducerKind::Source,
                "operator" => ProducerKind::Operator,
                kind => return Err(format!("invalid input producer kind {kind:?}")),
            };
            let source_oid = row
                .get::<pg_sys::Oid>(4)
                .map_err(|error| error.to_string())?;
            let slot_generation = row.get::<i64>(5).map_err(|error| error.to_string())?;
            match producer {
                ProducerKind::Source if source_oid.is_none() || slot_generation.is_none() => {
                    return Err("source stream omitted its source identity".into());
                }
                ProducerKind::Operator if source_oid.is_some() || slot_generation.is_some() => {
                    return Err("operator stream contains a source identity".into());
                }
                _ => {}
            }
            inputs.push(InputState {
                port,
                stream_id: required_row(&row, 2, "input stream ID")?,
                producer,
                source_oid,
                slot_generation,
                next_chunk_seq: required_row(&row, 6, "consumer chunk cursor")?,
                available_chunk_seq: required_row(&row, 7, "produced chunk cursor")?,
                activation_lsn: parse_lsn_column(&row, 8, "input activation LSN")?,
                consumed_frontier_lsn: parse_lsn_column(&row, 9, "input consumed frontier")?,
                available_source_frontier_lsn: optional_lsn_column(&row, 10, "source frontier")?,
            });
        }
        inputs.sort_by_key(|input| input.port);
        if inputs
            .iter()
            .enumerate()
            .any(|(port, input)| usize::from(input.port) != port)
        {
            return Err(format!(
                "operator stage {stage_id} input ports are not contiguous"
            ));
        }

        let output = if expects_output {
            let output = client
                .select(
                    r#"
                    SELECT output.stream_id,
                           output.next_chunk_seq,
                           output.target_chunk_rows,
                           output.target_chunk_bytes,
                           output.latest_data_lsn::text,
                           output.published_frontier_lsn::text,
                           output.backpressured
                    FROM shiba_internal.effect_streams AS output
                    WHERE output.producer_kind = 'operator'
                      AND output.producer_result_oid = $1::oid
                      AND output.producer_stage_id = $2
                    "#,
                    Some(2),
                    &identity,
                )
                .map_err(|error| format!("could not inspect operator output: {error}"))?;
            if output.len() != 1 {
                return Err(format!(
                    "operator stage {stage_id} has no unique output stream"
                ));
            }
            let output = output.first();
            if required_table::<bool>(&output, 7, "output backpressure")? {
                return Ok(StepContextStart::Blocked);
            }
            Some(OutputState {
                stream_id: required_table(&output, 1, "output stream ID")?,
                next_chunk_seq: required_table(&output, 2, "output chunk cursor")?,
                target_rows: required_table(&output, 3, "output row target")?,
                target_bytes: required_table(&output, 4, "output byte target")?,
                latest_data_lsn: optional_lsn_table(&output, 5, "output latest data LSN")?,
                published_frontier_lsn: optional_lsn_table(&output, 6, "output frontier")?,
                pending_data_chunk: None,
            })
        } else {
            None
        };

        if !checkpoint_had_continuation && !inputs.iter().any(InputState::has_pending) {
            return Ok(StepContextStart::Idle);
        }

        Ok(StepContextStart::Ready(Box::new(Self {
            client,
            result_oid,
            stage_id,
            quantum_budget: budget,
            transition_budget: budget,
            expected_revision,
            checkpoint_had_continuation,
            continuation_presence: None,
            admission,
            inputs,
            output,
        })))
    }

    pub(crate) const fn result_oid(&self) -> pg_sys::Oid {
        self.result_oid
    }

    pub(crate) const fn stage_id(&self) -> i32 {
        self.stage_id
    }

    pub(crate) const fn budget(&self) -> WorkBudget {
        self.transition_budget
    }

    pub(crate) fn set_transition_budget(&mut self, budget: WorkBudget) {
        self.transition_budget = budget;
    }

    pub(crate) fn bind_continuation_authority(&mut self, persisted: bool) -> Result<(), String> {
        if self.checkpoint_had_continuation != persisted {
            return Err("checkpoint and typed continuation authority disagree".into());
        }
        match self.continuation_presence {
            None => self.continuation_presence = Some(persisted),
            Some(current) if current == persisted => {}
            Some(_) => return Err("typed continuation authority was bound twice".into()),
        }
        Ok(())
    }

    pub(crate) fn prepare_continuation_replace(&self, expected: bool) -> Result<(), String> {
        match self.continuation_presence {
            Some(current) if current == expected => Ok(()),
            Some(_) => Err("typed continuation replacement used stale authority".into()),
            None => Err("typed continuation was mutated before it was bound".into()),
        }
    }

    pub(crate) fn record_continuation_replace(&mut self, present: bool) {
        self.continuation_presence = Some(present);
    }

    pub(crate) const fn admission_progress(&self) -> AdmissionProgress {
        self.admission
    }

    pub(crate) fn record_admission(&mut self, usage: WorkUsage) -> Result<bool, String> {
        let (progress, drain) = self.admission.record(
            usage,
            crate::config::admission_rows(),
            crate::config::admission_row_interval_cap(),
            crate::config::admission_bytes(),
            crate::config::admission_byte_interval_cap(),
        )?;
        self.admission = progress;
        Ok(drain)
    }

    pub(crate) fn reset_admission(&mut self) {
        self.admission = AdmissionProgress::default();
    }

    pub(crate) fn inputs(&self) -> &[InputState] {
        &self.inputs
    }

    pub(crate) fn input(&self, port: u16) -> Result<&InputState, String> {
        self.inputs
            .get(usize::from(port))
            .filter(|input| input.port == port)
            .ok_or_else(|| format!("operator stage {} has no input port {port}", self.stage_id))
    }

    pub(crate) fn output(&self) -> Result<&OutputState, String> {
        self.output
            .as_ref()
            .ok_or_else(|| format!("Sink stage {} has no output stream", self.stage_id))
    }

    pub(crate) fn output_append_target(
        &self,
        rows: u64,
        bytes: u64,
    ) -> Result<OutputAppendTarget, String> {
        if rows == 0 || bytes == 0 {
            return Err("output append has no rows or bytes".into());
        }
        let output = self.output()?;
        let target_rows = u64::try_from(output.target_rows)
            .map_err(|_| "output stream has a negative row target".to_string())?;
        let target_bytes = u64::try_from(output.target_bytes)
            .map_err(|_| "output stream has a negative byte target".to_string())?;
        if let Some(open) = output.pending_data_chunk {
            let combined_rows = open
                .rows
                .checked_add(rows)
                .ok_or_else(|| "open output chunk row count overflow".to_string())?;
            let combined_bytes = open
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| "open output chunk byte count overflow".to_string())?;
            if combined_rows <= target_rows && combined_bytes <= target_bytes {
                return Ok(OutputAppendTarget::Extend {
                    sequence: open.sequence,
                    row_offset: open.rows,
                    previous_bytes: open.bytes,
                });
            }
            return Err("pending output exceeded its immutable chunk target".into());
        }
        Ok(OutputAppendTarget::New {
            sequence: output.next_chunk_seq,
        })
    }

    pub(crate) fn record_output_append(
        &mut self,
        target: OutputAppendTarget,
        rows: u64,
        bytes: u64,
        lsn: u64,
    ) -> Result<(), String> {
        let output = self
            .output
            .as_mut()
            .ok_or_else(|| format!("Sink stage {} has no output stream", self.stage_id))?;
        match target {
            OutputAppendTarget::New { sequence } => {
                if sequence != output.next_chunk_seq {
                    return Err("new output append changed its expected sequence".into());
                }
                output.next_chunk_seq = output
                    .next_chunk_seq
                    .checked_add(1)
                    .ok_or_else(|| "output chunk sequence overflow".to_string())?;
                output.pending_data_chunk = Some(PendingDataChunk {
                    sequence,
                    rows,
                    bytes,
                    lsn,
                });
            }
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                previous_bytes,
            } => {
                let open = output
                    .pending_data_chunk
                    .as_mut()
                    .filter(|open| {
                        open.sequence == sequence
                            && open.rows == row_offset
                            && open.bytes == previous_bytes
                    })
                    .ok_or_else(|| "extended output append changed its open chunk".to_string())?;
                open.rows = open
                    .rows
                    .checked_add(rows)
                    .ok_or_else(|| "open output chunk row count overflow".to_string())?;
                open.bytes = open
                    .bytes
                    .checked_add(bytes)
                    .ok_or_else(|| "open output chunk byte count overflow".to_string())?;
                open.lsn = open.lsn.max(lsn);
            }
        }
        Ok(())
    }

    fn publish_pending_output(&mut self) -> Result<(), String> {
        let Some(pending) = self
            .output
            .as_ref()
            .and_then(|output| output.pending_data_chunk)
        else {
            return Ok(());
        };
        let output = self.output()?;
        let lsn = crate::postgres::format_lsn(pending.lsn);
        let arguments = unsafe {
            [
                DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(pending.sequence, pg_sys::INT8OID),
                DatumWithOid::new(
                    i64::try_from(pending.rows).map_err(|_| "pending output rows exceed bigint")?,
                    pg_sys::INT8OID,
                ),
                DatumWithOid::new(
                    i64::try_from(pending.bytes)
                        .map_err(|_| "pending output bytes exceed bigint")?,
                    pg_sys::INT8OID,
                ),
                DatumWithOid::new(lsn.as_str(), pg_sys::TEXTOID),
            ]
        };
        let published = self.write(
            "SELECT outcome,appended_chunk_seq
             FROM shiba_internal.append_effect_stream_chunk(
               $1,$2,'data',$3,$4,$5::pg_lsn
             )",
            &arguments,
        )?;
        if published.len() != 1 {
            return Err("pending output publication returned no result".into());
        }
        let published = published.first();
        let outcome = required_table::<String>(&published, 1, "pending output outcome")?;
        let sequence = required_table::<i64>(&published, 2, "pending output sequence")?;
        if outcome != "appended" || sequence != pending.sequence {
            return Err("pending output publication was blocked or inconsistent".into());
        }
        Ok(())
    }

    pub(crate) fn payload_storage(&mut self, stream_id: i64) -> Result<PayloadStorage, String> {
        storage::payload(self.client, stream_id)
    }

    pub(crate) fn continuation_storage(&mut self) -> Result<RelationRef, String> {
        storage::continuation(self.client, self.result_oid, self.stage_id)
    }

    pub(crate) fn state_storage(&mut self, slot: i32) -> Result<RelationRef, String> {
        storage::state(self.client, self.result_oid, self.stage_id, slot)
    }

    pub(crate) fn result_storage(&mut self) -> Result<RelationRef, String> {
        storage::result(self.client, self.result_oid)
    }

    pub(crate) fn composite_attributes(
        &mut self,
        type_: &TypeRef,
    ) -> Result<Vec<AttributeRef>, String> {
        storage::composite_attributes(self.client, type_)
    }

    pub(crate) fn relation_attributes(
        &mut self,
        relation_oid: pg_sys::Oid,
    ) -> Result<Vec<AttributeRef>, String> {
        storage::relation_attributes(self.client, relation_oid)
    }

    pub(crate) fn lock<Q: Query<'conn>>(
        &mut self,
        query: Q,
        arguments: &[DatumWithOid<'_>],
    ) -> Result<SpiTupleTable<'conn>, String> {
        self.client
            .update(query, None, arguments)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn read<Q: Query<'conn>>(
        &mut self,
        query: Q,
        arguments: &[DatumWithOid<'_>],
    ) -> Result<SpiTupleTable<'conn>, String> {
        self.client
            .select(query, None, arguments)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn write<Q: Query<'conn>>(
        &mut self,
        query: Q,
        arguments: &[DatumWithOid<'_>],
    ) -> Result<SpiTupleTable<'conn>, String> {
        self.client
            .update(query, None, arguments)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn transition(
        &self,
        has_continuation: bool,
        usage: WorkUsage,
    ) -> Result<KernelTransition, String> {
        usage.validate(self.quantum_budget)?;
        if self.continuation_presence != Some(has_continuation) {
            return Err("kernel transition disagrees with typed continuation authority".into());
        }
        Ok(KernelTransition::new(has_continuation, usage))
    }

    /// Commits the logical step by advancing its single CAS authority.
    ///
    /// The operator derives `has_continuation` from the validated typed
    /// continuation mutation performed in this transaction.
    pub(super) fn commit(
        mut self,
        transition: KernelTransition,
        _permit: LifecyclePermit,
    ) -> Result<StepExecution, String> {
        let KernelTransition {
            has_continuation,
            usage,
        } = transition;
        usage.validate(self.quantum_budget)?;
        if self.continuation_presence != Some(has_continuation) {
            return Err("kernel transition disagrees with typed continuation authority".into());
        }
        self.publish_pending_output()?;
        let admitted_rows =
            i64::try_from(self.admission.rows()).map_err(|_| "admitted rows exceed bigint")?;
        let admitted_bytes =
            i64::try_from(self.admission.bytes()).map_err(|_| "admitted bytes exceed bigint")?;
        let arguments = unsafe {
            [
                DatumWithOid::new(self.result_oid, pg_sys::OIDOID),
                DatumWithOid::new(self.stage_id, pg_sys::INT4OID),
                DatumWithOid::new(self.expected_revision, pg_sys::INT8OID),
                DatumWithOid::new(has_continuation, pg_sys::BOOLOID),
                DatumWithOid::new(admitted_rows, pg_sys::INT8OID),
                DatumWithOid::new(admitted_bytes, pg_sys::INT8OID),
            ]
        };
        let updated = self.write(
            r#"
                UPDATE shiba_internal.operator_checkpoints AS checkpoint
                SET revision = checkpoint.revision + 1,
                    has_continuation = $4,
                    admitted_rows = $5,
                    admitted_bytes = $6,
                    updated_at = pg_catalog.clock_timestamp()
                WHERE checkpoint.result_oid = $1::oid
                  AND checkpoint.stage_id = $2
                  AND checkpoint.revision = $3
                RETURNING checkpoint.revision
                "#,
            &arguments,
        )?;
        if updated.len() != 1 {
            return Err("operator checkpoint compare-and-set failed".into());
        }
        let updated = updated.first();
        let revision = required_table::<i64>(&updated, 1, "new checkpoint revision")?;
        if revision != self.expected_revision + 1 {
            return Err("operator checkpoint did not advance exactly once".into());
        }
        let outcome = if has_continuation {
            StepOutcome::Yield
        } else {
            StepOutcome::Progress
        };
        Ok(StepExecution::new(outcome, usage))
    }
}

fn apply_execution_settings(
    client: &mut SpiClient<'_>,
    settings: &ExecutionSettings,
) -> Result<(), String> {
    let extra_float_digits = settings.extra_float_digits.to_string();
    let arguments = unsafe {
        [
            DatumWithOid::new(settings.timezone.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(settings.datestyle.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(settings.intervalstyle.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(extra_float_digits.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(settings.bytea_output.as_str(), pg_sys::TEXTOID),
        ]
    };
    let configured = client
        .update(
            r#"
            SELECT pg_catalog.set_config('TimeZone', $1, true),
                   pg_catalog.set_config('DateStyle', $2, true),
                   pg_catalog.set_config('IntervalStyle', $3, true),
                   pg_catalog.set_config('extra_float_digits', $4, true),
                   pg_catalog.set_config('bytea_output', $5, true)
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not apply dataflow execution settings: {error}"))?;
    if configured.len() != 1 {
        return Err("dataflow execution settings returned no row".into());
    }
    Ok(())
}

fn parse_lsn_column(
    row: &pgrx::spi::SpiHeapTupleData<'_>,
    ordinal: usize,
    name: &str,
) -> Result<u64, String> {
    let value = required_row::<String>(row, ordinal, name)?;
    parse_lsn(&value).map_err(|error| format!("invalid {name}: {error}"))
}

fn optional_lsn_column(
    row: &pgrx::spi::SpiHeapTupleData<'_>,
    ordinal: usize,
    name: &str,
) -> Result<Option<u64>, String> {
    row.get::<String>(ordinal)
        .map_err(|error| error.to_string())?
        .map(|value| parse_lsn(&value).map_err(|error| format!("invalid {name}: {error}")))
        .transpose()
}

fn optional_lsn_table(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<Option<u64>, String> {
    table
        .get::<String>(ordinal)
        .map_err(|error| error.to_string())?
        .map(|value| parse_lsn(&value).map_err(|error| format!("invalid {name}: {error}")))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_frontier_can_wake_an_input_without_a_chunk() {
        let input = InputState {
            port: 0,
            stream_id: 1,
            producer: ProducerKind::Source,
            source_oid: Some(pg_sys::Oid::from(10)),
            slot_generation: Some(1),
            next_chunk_seq: 4,
            available_chunk_seq: 4,
            activation_lsn: 1,
            consumed_frontier_lsn: 10,
            available_source_frontier_lsn: Some(11),
        };
        assert!(input.has_pending());
    }

    #[test]
    fn operator_input_needs_an_unconsumed_chunk() {
        let input = InputState {
            port: 0,
            stream_id: 1,
            producer: ProducerKind::Operator,
            source_oid: None,
            slot_generation: None,
            next_chunk_seq: 4,
            available_chunk_seq: 4,
            activation_lsn: 1,
            consumed_frontier_lsn: 10,
            available_source_frontier_lsn: None,
        };
        assert!(!input.has_pending());
    }
}
