//! PostgreSQL bridge for the durable operator scheduler.
//!
//! `LoadedDataflow` caches only a validated dataflow plan and a fair queue of
//! operator IDs.  PostgreSQL owns every frontier, continuation, effect, and
//! operator state row.  Losing this struct therefore loses no work: the queue
//! is rebuilt from the durable rows after every Runtime restart.

use std::collections::{BTreeSet, HashMap};

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::SpiHeapTupleData;

use super::dataflow::{
    DurableOperatorState, InputFrontier, OperatorId, ReadyQueue, StepExecution, StepOutcome,
    StreamSequence, WorkBudget, WorkQuantum, WorkUsage,
};
use super::model::DataflowPlan;
use super::model::OperatorKind;

pub(crate) struct LoadedDataflow {
    result_oid: pg_sys::Oid,
    plan: DataflowPlan,
    ready: ReadyQueue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperatorStep {
    pub(crate) stage_id: u32,
    pub(crate) outcome: StepOutcome,
    pub(crate) usage: WorkUsage,
    pub(crate) transitions: usize,
}

impl LoadedDataflow {
    pub(crate) fn load(result_oid: pg_sys::Oid) -> Result<Self, String> {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let serialized = Spi::connect_mut(|client| {
            let table = client
                .update(
                    "SELECT dataflow.plan::text
                     FROM shiba_internal.dataflows AS dataflow
                     WHERE dataflow.result_oid = $1::oid
                       AND dataflow.active",
                    None,
                    &argument,
                )
                .map_err(|error| error.to_string())?;
            if table.len() != 1 {
                return Err(format!(
                    "active dataflow load returned {} rows, expected 1",
                    table.len()
                ));
            }
            table
                .first()
                .get::<String>(1)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "dataflow load returned NULL plan".to_string())
        })?;

        let plan: DataflowPlan = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid dataflow plan: {error}"))?;
        plan.validate()?;

        let mut runtime = Self {
            result_oid,
            plan,
            ready: ReadyQueue::default(),
        };
        runtime.ready = ReadyQueue::rebuild(&runtime.durable_states()?);
        Ok(runtime)
    }

    /// Runs one bounded transaction quantum for a single operator.
    ///
    /// The in-memory queue is supplemented from durable state before choosing
    /// work. Once selected, an operator may cross several internal phases in
    /// the same transaction, but all transitions share one row/byte budget.
    /// The next transaction selects from the fair queue again.
    pub(crate) fn step_quantum(
        &mut self,
        budget: WorkBudget,
        max_transitions: usize,
    ) -> Result<Option<OperatorStep>, String> {
        for state in self.durable_states()? {
            if state.is_runnable() {
                self.ready.activate(state.operator);
            }
        }

        let Some(operator) = self.ready.next() else {
            return Ok(None);
        };
        if operator.result_oid != self.result_oid.to_u32()
            || operator.stage_id as usize >= self.plan.stages.len()
        {
            return Err("ready queue contains an operator outside its dataflow plan".into());
        }
        let mut quantum = WorkQuantum::new(budget, max_transitions);
        let mut outcome = StepOutcome::Idle;
        while quantum.remaining().is_some() {
            let execution =
                execute_operator_step(self.result_oid, operator.stage_id, &self.plan, budget)?;
            outcome = execution.outcome;
            if !matches!(outcome, StepOutcome::Progress | StepOutcome::Yield) {
                break;
            }
            quantum.record(execution.usage)?;
            // One set primitive already consumes a complete bounded page.
            // Coalesce adjacent metadata phases without shrinking the next
            // primitive and fragmenting its output chunk.
            if !execution.usage.is_empty() {
                break;
            }
        }
        self.ready.complete(operator, outcome);
        Ok(Some(OperatorStep {
            stage_id: operator.stage_id,
            outcome,
            usage: quantum.usage(),
            transitions: quantum.transitions(),
        }))
    }

    fn durable_states(&self) -> Result<Vec<DurableOperatorState>, String> {
        let arguments = unsafe { [DatumWithOid::new(self.result_oid, pg_sys::OIDOID)] };
        let rows = Spi::connect_mut(|client| {
            let table = client
                .update(
                    "SELECT checkpoint.stage_id,
                            checkpoint.has_continuation,
                            NOT EXISTS (
                              SELECT 1
                              FROM shiba_internal.effect_streams AS output
                              WHERE output.producer_kind = 'operator'
                                AND output.producer_result_oid = checkpoint.result_oid
                                AND output.producer_stage_id = checkpoint.stage_id
                                AND output.backpressured
                            ) AS outputs_have_capacity,
                            (
                              SELECT count(*)
                              FROM shiba_internal.effect_streams AS output
                              WHERE output.producer_kind = 'operator'
                                AND output.producer_result_oid = checkpoint.result_oid
                                AND output.producer_stage_id = checkpoint.stage_id
                            ) AS output_count,
                            consumer.input_port,
                            consumer.next_chunk_seq,
                            input_stream.next_chunk_seq,
                            coalesce(
                              input_stream.producer_kind = 'source'
                              AND EXISTS (
                                SELECT 1
                                FROM shiba_internal.ingress_replay_state
                                     AS publication
                                WHERE publication.slot_generation
                                        = input_stream.slot_generation
                                  AND publication.published_lsn IS NOT NULL
                                  AND consumer.consumed_frontier_lsn
                                        < publication.published_lsn
                              ),
                              false
                            ) AS frontier_pending
                     FROM shiba_internal.operator_checkpoints AS checkpoint
                     LEFT JOIN shiba_internal.effect_stream_consumers AS consumer
                       ON consumer.result_oid = checkpoint.result_oid
                      AND consumer.consumer_stage_id = checkpoint.stage_id
                     LEFT JOIN shiba_internal.effect_streams AS input_stream
                       ON input_stream.stream_id = consumer.stream_id
                     WHERE checkpoint.result_oid = $1::oid
                     ORDER BY checkpoint.stage_id,
                              consumer.input_port,
                              consumer.stream_id",
                    None,
                    &arguments,
                )
                .map_err(|error| error.to_string())?;

            let mut rows = Vec::with_capacity(table.len());
            for row in table {
                rows.push(DurableStateRow {
                    stage_id: required_column(&row, 1, "checkpoint stage ID")?,
                    has_continuation: required_column(&row, 2, "continuation flag")?,
                    outputs_have_capacity: required_column(&row, 3, "output capacity")?,
                    output_count: required_column(&row, 4, "output count")?,
                    input_port: row.get::<i32>(5).map_err(|error| error.to_string())?,
                    consumed: row.get::<i64>(6).map_err(|error| error.to_string())?,
                    available: row.get::<i64>(7).map_err(|error| error.to_string())?,
                    frontier_pending: required_column(&row, 8, "source frontier readiness")?,
                });
            }
            Ok::<Vec<DurableStateRow>, String>(rows)
        })?;

        let stage_by_id: HashMap<_, _> = self
            .plan
            .stages
            .iter()
            .enumerate()
            .map(|(stage_id, stage)| (stage_id as u32, stage))
            .collect();
        let mut by_stage: HashMap<u32, DurableOperatorState> = HashMap::new();
        let mut output_counts = HashMap::new();
        for row in rows {
            let stage_id = u32::try_from(row.stage_id).map_err(|_| {
                format!(
                    "operator checkpoint has an invalid stage ID {}",
                    row.stage_id
                )
            })?;
            stage_by_id.get(&stage_id).ok_or_else(|| {
                format!(
                    "operator checkpoint stage {} is absent from result {} dataflow plan",
                    row.stage_id, self.result_oid
                )
            })?;
            let state = by_stage
                .entry(stage_id)
                .or_insert_with(|| DurableOperatorState {
                    operator: OperatorId::new(self.result_oid.to_u32(), stage_id),
                    inputs: Vec::new(),
                    has_continuation: row.has_continuation,
                    outputs_have_capacity: row.outputs_have_capacity,
                    active: true,
                });
            if state.has_continuation != row.has_continuation
                || state.outputs_have_capacity != row.outputs_have_capacity
            {
                return Err(format!(
                    "operator checkpoint stage {} returned inconsistent durable state",
                    row.stage_id
                ));
            }
            if output_counts
                .insert(stage_id, row.output_count)
                .is_some_and(|count| count != row.output_count)
            {
                return Err(format!(
                    "operator checkpoint stage {} returned inconsistent output metadata",
                    row.stage_id
                ));
            }

            match (row.input_port, row.consumed, row.available) {
                (None, None, None) => {}
                (Some(port), Some(consumed), Some(available)) => {
                    let port = u16::try_from(port).map_err(|_| {
                        format!(
                            "operator stage {} has an invalid input port {port}",
                            row.stage_id
                        )
                    })?;
                    let consumed = u64::try_from(consumed).map_err(|_| {
                        format!(
                            "operator stage {} has a negative input frontier",
                            row.stage_id
                        )
                    })?;
                    let available = u64::try_from(available).map_err(|_| {
                        format!(
                            "operator stage {} has a negative available frontier",
                            row.stage_id
                        )
                    })?;
                    if consumed > available {
                        return Err(format!(
                            "operator stage {} consumed beyond its available input",
                            row.stage_id
                        ));
                    }
                    state.inputs.push(InputFrontier {
                        port,
                        consumed: StreamSequence(consumed),
                        available: StreamSequence(available),
                        frontier_pending: row.frontier_pending,
                    });
                }
                _ => {
                    return Err(format!(
                        "operator stage {} has an incomplete input frontier",
                        row.stage_id
                    ));
                }
            }
        }

        if by_stage.len() != self.plan.stages.len() {
            let missing = self
                .plan
                .stages
                .iter()
                .enumerate()
                .filter(|(stage_id, _)| !by_stage.contains_key(&(*stage_id as u32)))
                .map(|(stage_id, _)| stage_id.to_string())
                .collect::<Vec<_>>();
            return Err(format!(
                "dataflow plan stages are missing checkpoints: {}",
                missing.join(", ")
            ));
        }

        let mut states = Vec::with_capacity(self.plan.stages.len());
        for (stage_index, stage) in self.plan.stages.iter().enumerate() {
            let stage_id = stage_index as u32;
            let mut state = by_stage
                .remove(&stage_id)
                .expect("checkpoint count was validated");
            let ports = state
                .inputs
                .iter()
                .map(|frontier| frontier.port)
                .collect::<BTreeSet<_>>();
            if ports.len() != state.inputs.len() {
                return Err(format!(
                    "operator stage {} has duplicate durable input ports",
                    stage_id
                ));
            }
            let expected_ports = if stage.spec.kind() == OperatorKind::Scan {
                BTreeSet::from([0])
            } else {
                (0..stage.inputs.len()).map(|port| port as u16).collect()
            };
            if ports != expected_ports {
                return Err(format!(
                    "operator stage {} durable inputs do not match its dataflow plan",
                    stage_id
                ));
            }

            let output_count = output_counts[&stage_id];
            let expected_outputs = usize::from(stage.spec.kind() != OperatorKind::Sink) as i64;
            if output_count != expected_outputs {
                return Err(format!(
                    "operator stage {} has {output_count} durable outputs, expected {expected_outputs}",
                    stage_id
                ));
            }
            state.inputs.sort_by_key(|frontier| frontier.port);
            states.push(state);
        }
        Ok(states)
    }
}

struct DurableStateRow {
    stage_id: i32,
    has_continuation: bool,
    outputs_have_capacity: bool,
    output_count: i64,
    input_port: Option<i32>,
    consumed: Option<i64>,
    available: Option<i64>,
    frontier_pending: bool,
}

fn required_column<T: FromDatum + IntoDatum>(
    row: &SpiHeapTupleData<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("dataflow load returned NULL {name}"))
}

fn execute_operator_step(
    result_oid: pg_sys::Oid,
    stage_id: u32,
    plan: &DataflowPlan,
    budget: WorkBudget,
) -> Result<StepExecution, String> {
    crate::kernel::execute_step(result_oid, stage_id, plan, budget)
}
