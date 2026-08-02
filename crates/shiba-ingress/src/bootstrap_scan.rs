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
        let rows = scan.query(&self.locator.query, &[&self.last_key, &batch_limit])?;
        let snapshot_rows: Vec<SnapshotRow> = rows
            .into_iter()
            .map(|row| SnapshotRow {
                source_row_id: row.get(0),
                payload: row.get(1),
            })
            .collect();
        scan.commit()?;
        if snapshot_rows.is_empty() {
            complete_bootstrap_scan(&mut self.apply, self.spec.source_id, self.spec.bootstrap_id)?;
            return Ok(SnapshotProgress::ScanComplete);
        }
        let count = snapshot_rows.len();
        let last_key = snapshot_rows
            .last()
            .ok_or(IngressError::InvalidEnvelope("empty snapshot batch"))?
            .source_row_id;
        let batch_id = BootstrapBatchId::new(self.spec.bootstrap_id, self.next_ordinal)
            .map_err(|_| IngressError::LimitExceeded)?;
        let batch = BootstrapBatch::new(self.spec.source_id, batch_id, snapshot_rows)?;
        process_bootstrap_batch(&mut self.apply, &batch)?;
        let ordinal = self.next_ordinal;
        self.next_ordinal = ordinal.checked_add(1).ok_or(IngressError::LimitExceeded)?;
        self.last_key = Some(last_key);
        Ok(SnapshotProgress::BatchApplied {
            ordinal,
            rows: count,
        })
    }
}
