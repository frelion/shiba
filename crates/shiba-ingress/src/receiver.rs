use postgres::Client;
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

use crate::{
    CommittedAssembler, IngressError, ReplicationMessage, ReplicationTransport,
    parse_replication_message,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceivedTransaction {
    pub outcome: ProcessOutcome,
    pub end_lsn: u64,
}

pub struct SourceReceiver {
    transport: ReplicationTransport,
    assembler: CommittedAssembler,
}

impl SourceReceiver {
    /// Opens one exclusive replication connection and enters COPY BOTH.
    ///
    /// # Errors
    /// Rejects invalid configuration, connection failure, or a non-COPY-BOTH
    /// replication response.
    pub fn connect(
        conninfo: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<Self, IngressError> {
        let transport = ReplicationTransport::connect(conninfo)?;
        transport.start(slot, publication, start_lsn)?;
        Ok(Self {
            transport,
            assembler: CommittedAssembler::new(),
        })
    }

    /// Blocks until one complete committed transaction has durably applied.
    ///
    /// # Errors
    /// Fails closed on transport, framing, semantic decode, or Runtime failure.
    pub fn receive_and_apply_one(
        &mut self,
        apply: &mut Client,
        source: PgoutputSource,
    ) -> Result<ReceivedTransaction, IngressError> {
        loop {
            if let Some(assembled) = self.assembler.push(&[])? {
                return apply_assembled(apply, source, &assembled);
            }

            let copy_data = self.transport.receive()?;
            match parse_replication_message(&copy_data)? {
                ReplicationMessage::XLogData { data, .. } => {
                    if let Some(assembled) = self.assembler.push(data)? {
                        return apply_assembled(apply, source, &assembled);
                    }
                }
                ReplicationMessage::Keepalive { .. } => {}
            }
        }
    }
}

fn apply_assembled(
    apply: &mut Client,
    source: PgoutputSource,
    assembled: &crate::AssembledTransaction,
) -> Result<ReceivedTransaction, IngressError> {
    let transaction = decode_committed_changes(&assembled.bytes, source)?;
    let outcome = process(apply, &transaction)?;
    Ok(ReceivedTransaction {
        outcome,
        end_lsn: assembled.end_lsn,
    })
}
