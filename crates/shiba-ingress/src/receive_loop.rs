use crate::{
    AssembledTransaction, IngressError, ReplicationMessage, ShutdownHandle, SourceReceiver,
    parse_replication_message, receiver::Assembly, streamed::StreamTerminal,
};

/// Blocking COPY-BOTH receive loops, separated from receiver state transitions.
impl SourceReceiver {
    pub(super) fn receive_committed_wire(
        &mut self,
        shutdown: &ShutdownHandle,
    ) -> Result<AssembledTransaction, IngressError> {
        loop {
            let Assembly::Committed(assembler) = &mut self.assembly else {
                return Err(IngressError::MessageOrder);
            };
            if let Some(assembled) = assembler.push(&[])? {
                return Ok(assembled);
            }
            let copy_data = self.transport.receive(shutdown)?;
            match parse_replication_message(&copy_data)? {
                ReplicationMessage::XLogData { data, .. } => {
                    if let Some(assembled) = assembler.push(data)? {
                        return Ok(assembled);
                    }
                }
                ReplicationMessage::Keepalive {
                    reply_requested: true,
                    ..
                } => self
                    .transport
                    .send_feedback(self.feedback.last_acknowledged_lsn())?,
                ReplicationMessage::Keepalive { .. } => {}
            }
        }
    }

    pub(super) fn receive_stream_terminal(
        &mut self,
        shutdown: &ShutdownHandle,
    ) -> Result<StreamTerminal, IngressError> {
        loop {
            let Assembly::Streamed(assembler) = &mut self.assembly else {
                return Err(IngressError::MessageOrder);
            };
            if let Some(terminal) = assembler.poll()? {
                return Ok(terminal);
            }
            let copy_data = self.transport.receive(shutdown)?;
            match parse_replication_message(&copy_data)? {
                ReplicationMessage::XLogData {
                    wal_start, data, ..
                } => {
                    if let Some(terminal) = assembler.push(wal_start, data)? {
                        return Ok(terminal);
                    }
                }
                ReplicationMessage::Keepalive {
                    reply_requested: true,
                    ..
                } => self
                    .transport
                    .send_feedback(self.feedback.last_acknowledged_lsn())?,
                ReplicationMessage::Keepalive { .. } => {}
            }
        }
    }
}
