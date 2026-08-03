use postgres::IsolationLevel;
use shiba_protocol::BootstrapBatchId;
use shiba_runtime::{
    BootstrapBatch, SnapshotRow, complete_bootstrap_scan, process_bootstrap_batch,
};

use crate::{
    IngressError,
    bootstrap::{BootstrapSession, SnapshotProgress},
};

impl BootstrapSession {
    /// Scans and durably applies one bounded batch from the exported snapshot.
    ///
    /// # Errors
    /// Any snapshot, authority, ordering, Apply, or operator failure rolls back
    /// the current Apply transaction and leaves the checkpoint retryable.
    pub fn scan_next(&mut self) -> Result<SnapshotProgress, IngressError> {
        self.config.revalidate(&mut self.apply, false)?;
        if self.current_member >= self.members.len() {
            complete_bootstrap_scan(&mut self.apply, self.spec.graph_id, self.spec.bootstrap_id)?;
            return Ok(SnapshotProgress::ScanComplete);
        }
        let member = &mut self.members[self.current_member];
        let mut scan = self
            .scanner
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        scan.batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            self.snapshot_name
        ))?;
        let batch_limit =
            i64::try_from(self.options.batch_rows()).map_err(|_| IngressError::LimitExceeded)?;
        let rows = scan.query(&member.locator.query, &[&member.last_key, &batch_limit])?;
        let snapshot_rows: Vec<SnapshotRow> = rows
            .into_iter()
            .map(|row| SnapshotRow {
                source_row_id: row.get(0),
                payload: row.get(1),
            })
            .collect();
        scan.commit()?;
        if snapshot_rows.is_empty() {
            self.current_member += 1;
            if self.current_member == self.members.len() {
                complete_bootstrap_scan(
                    &mut self.apply,
                    self.spec.graph_id,
                    self.spec.bootstrap_id,
                )?;
                return Ok(SnapshotProgress::ScanComplete);
            }
            return self.scan_next();
        }
        let count = snapshot_rows.len();
        let last_key = snapshot_rows
            .last()
            .ok_or(IngressError::InvalidEnvelope("empty snapshot batch"))?
            .source_row_id;
        let batch_id = BootstrapBatchId::new(self.spec.bootstrap_id, member.next_ordinal)
            .map_err(|_| IngressError::LimitExceeded)?;
        let batch = BootstrapBatch::new(
            self.spec.graph_id,
            member.source_id,
            batch_id,
            snapshot_rows,
        )?;
        process_bootstrap_batch(&mut self.apply, &batch)?;
        let ordinal = member.next_ordinal;
        member.next_ordinal = ordinal.checked_add(1).ok_or(IngressError::LimitExceeded)?;
        member.last_key = Some(last_key);
        Ok(SnapshotProgress::BatchApplied {
            ordinal,
            rows: count,
        })
    }
}
