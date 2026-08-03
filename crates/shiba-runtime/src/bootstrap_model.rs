use shiba_protocol::{BootstrapBatchDigest, BootstrapBatchId, GraphId, SourceId};

use crate::M2Error;

pub const MAX_BOOTSTRAP_BATCH_ROWS: usize = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRow {
    pub source_row_id: i64,
    pub payload: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapBatch {
    pub(crate) graph_id: GraphId,
    pub(crate) source_id: SourceId,
    pub(crate) batch_id: BootstrapBatchId,
    pub(crate) rows: Vec<SnapshotRow>,
    pub(crate) digest: BootstrapBatchDigest,
}

impl BootstrapBatch {
    /// Constructs one bounded, strictly key-ordered snapshot batch.
    ///
    /// # Errors
    /// Rejects empty, oversized, or non-increasing batches.
    pub fn new(
        graph_id: GraphId,
        source_id: SourceId,
        batch_id: BootstrapBatchId,
        rows: Vec<SnapshotRow>,
    ) -> Result<Self, M2Error> {
        if rows.is_empty() {
            return Err(M2Error::EmptyBootstrapBatch);
        }
        if rows.len() > MAX_BOOTSTRAP_BATCH_ROWS {
            return Err(M2Error::BootstrapBatchLimitExceeded);
        }
        if rows
            .windows(2)
            .any(|pair| pair[0].source_row_id >= pair[1].source_row_id)
        {
            return Err(M2Error::BootstrapRowsOutOfOrder);
        }
        let digest = digest_batch(graph_id, source_id, batch_id, &rows);
        Ok(Self {
            graph_id,
            source_id,
            batch_id,
            rows,
            digest,
        })
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn batch_id(&self) -> BootstrapBatchId {
        self.batch_id
    }

    #[must_use]
    pub fn rows(&self) -> &[SnapshotRow] {
        &self.rows
    }

    #[must_use]
    pub const fn digest(&self) -> BootstrapBatchDigest {
        self.digest
    }
}

fn digest_batch(
    graph_id: GraphId,
    source_id: SourceId,
    batch_id: BootstrapBatchId,
    rows: &[SnapshotRow],
) -> BootstrapBatchDigest {
    let mut canonical = Vec::with_capacity(32 + rows.len() * 17);
    canonical.extend_from_slice(&graph_id.get().to_be_bytes());
    canonical.extend_from_slice(&source_id.get().to_be_bytes());
    canonical.extend_from_slice(&batch_id.bootstrap_id.get().to_be_bytes());
    canonical.extend_from_slice(&batch_id.batch_ordinal().to_be_bytes());
    canonical.extend_from_slice(&(rows.len() as u64).to_be_bytes());
    for row in rows {
        canonical.extend_from_slice(&row.source_row_id.to_be_bytes());
        match row.payload {
            None => canonical.push(0),
            Some(value) => {
                canonical.push(1);
                canonical.extend_from_slice(&value.to_be_bytes());
            }
        }
    }
    BootstrapBatchDigest::for_canonical_bytes(&canonical)
}

#[cfg(test)]
mod tests {
    use shiba_protocol::{BootstrapBatchId, BootstrapId};

    use super::*;

    fn batch(rows: Vec<SnapshotRow>) -> Result<BootstrapBatch, M2Error> {
        BootstrapBatch::new(
            GraphId::new(1).unwrap(),
            SourceId::new(1).unwrap(),
            BootstrapBatchId::new(BootstrapId::new(2).unwrap(), 3).unwrap(),
            rows,
        )
    }

    #[test]
    fn batch_is_bounded_ordered_and_digest_is_deterministic() {
        let rows = vec![
            SnapshotRow {
                source_row_id: 1,
                payload: Some(10),
            },
            SnapshotRow {
                source_row_id: 2,
                payload: None,
            },
        ];
        assert_eq!(
            batch(rows.clone()).unwrap().digest(),
            batch(rows).unwrap().digest()
        );
        assert!(matches!(
            batch(Vec::new()),
            Err(M2Error::EmptyBootstrapBatch)
        ));
        assert!(matches!(
            batch(vec![
                SnapshotRow {
                    source_row_id: 2,
                    payload: None,
                },
                SnapshotRow {
                    source_row_id: 2,
                    payload: Some(1),
                },
            ]),
            Err(M2Error::BootstrapRowsOutOfOrder)
        ));
        let oversized = (0..=MAX_BOOTSTRAP_BATCH_ROWS)
            .map(|key| SnapshotRow {
                source_row_id: i64::try_from(key).unwrap(),
                payload: None,
            })
            .collect();
        assert!(matches!(
            batch(oversized),
            Err(M2Error::BootstrapBatchLimitExceeded)
        ));
        let first = batch(vec![SnapshotRow {
            source_row_id: 1,
            payload: None,
        }])
        .unwrap();
        let second = BootstrapBatch::new(
            GraphId::new(1).unwrap(),
            SourceId::new(1).unwrap(),
            BootstrapBatchId::new(BootstrapId::new(2).unwrap(), 4).unwrap(),
            vec![SnapshotRow {
                source_row_id: 1,
                payload: None,
            }],
        )
        .unwrap();
        assert_ne!(first.digest(), second.digest());
    }
}
