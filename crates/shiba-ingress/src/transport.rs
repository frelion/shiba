use libpq::{Connection, Status, connection::Info};

use crate::{IngressError, ShutdownHandle, assembler::MAX_TRANSACTION_BYTES, encode_feedback};

const COPY_DATA_ENVELOPE_BYTES: usize = 25;
const RECEIVE_POLL_MICROS: i64 = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationMode {
    Committed,
    Streamed,
}

/// Synchronous replication transport with no in-process queue or slot authority.
pub struct ReplicationTransport {
    connection: Connection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExportedSnapshot {
    pub(crate) consistent_point: String,
    pub(crate) snapshot_name: String,
}

impl ReplicationTransport {
    /// Opens a connection whose libpq-parsed conninfo explicitly selects
    /// database-scoped replication mode.
    ///
    /// # Errors
    /// Rejects invalid conninfo, absent/wrong replication mode, or connection
    /// failures.
    pub fn connect(conninfo: &str) -> Result<Self, IngressError> {
        let options = Info::from(conninfo).map_err(IngressError::Libpq)?;
        let replication = options
            .iter()
            .find(|option| option.keyword == "replication")
            .and_then(|option| option.val.as_deref());
        if replication != Some("database") {
            return Err(IngressError::InvalidIdentifier(
                "conninfo must set replication=database",
            ));
        }
        let connection = Connection::new(conninfo).map_err(IngressError::Libpq)?;
        Ok(Self { connection })
    }

    /// Proves that this credential is a database-scoped replication principal
    /// for the exact apply database, without creating, dropping, or starting a
    /// slot. Returns the authenticated session principal for target-table
    /// privilege validation on the apply connection.
    pub(crate) fn preflight(&self, expected_database: &str) -> Result<String, IngressError> {
        let identified = self.connection.exec("IDENTIFY_SYSTEM");
        if identified.status() != Status::TuplesOk
            || identified.ntuples() != 1
            || identified.nfields() != 4
        {
            return Err(IngressError::UnexpectedStatus(identified.status()));
        }
        let database = result_text(&identified, 0, 3)?;
        if database != expected_database {
            return Err(IngressError::Governance(
                "replication credential database differs from apply database",
            ));
        }
        let principal = self.connection.exec("SHOW session_authorization");
        if principal.status() != Status::TuplesOk
            || principal.ntuples() != 1
            || principal.nfields() != 1
        {
            return Err(IngressError::UnexpectedStatus(principal.status()));
        }
        let principal = result_text(&principal, 0, 0)?;
        if principal.is_empty() {
            return Err(IngressError::Governance(
                "replication credential principal is empty",
            ));
        }
        Ok(principal)
    }

    /// Starts pgoutput replication for one existing slot and publication.
    ///
    /// # Errors
    /// Rejects unsafe names, command failures, and responses other than
    /// `CopyBoth`.
    pub(crate) fn start_with_messages(
        &self,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        mode: ReplicationMode,
        messages: bool,
    ) -> Result<(), IngressError> {
        validate_slot(slot)?;
        validate_identifier(publication, "publication")?;
        let options = match (mode, messages) {
            (ReplicationMode::Committed, false) => "proto_version '1'",
            (ReplicationMode::Committed, true) => "proto_version '1', messages 'true'",
            (ReplicationMode::Streamed, false) => "proto_version '2', streaming 'on'",
            (ReplicationMode::Streamed, true) => {
                "proto_version '2', streaming 'on', messages 'true'"
            }
        };
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} \
             ({options}, publication_names '{}')",
            quote_identifier(slot),
            format_lsn(start_lsn),
            publication,
        );
        let result = self.connection.exec(&query);
        let status = result.status();
        if status != Status::CopyBoth {
            return Err(IngressError::UnexpectedStatus(status));
        }
        Ok(())
    }

    pub(crate) fn create_exported_slot(
        &self,
        slot: &str,
    ) -> Result<ExportedSnapshot, IngressError> {
        validate_slot(slot)?;
        let result = self.connection.exec(&format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export')",
            quote_identifier(slot)
        ));
        if result.status() != Status::TuplesOk || result.ntuples() != 1 || result.nfields() != 4 {
            return Err(IngressError::UnexpectedStatus(result.status()));
        }
        for (index, expected) in [
            "slot_name",
            "consistent_point",
            "snapshot_name",
            "output_plugin",
        ]
        .into_iter()
        .enumerate()
        {
            if result.field_name(index).ok().flatten().as_deref() != Some(expected) {
                return Err(IngressError::InvalidEnvelope(
                    "unexpected CREATE_REPLICATION_SLOT response",
                ));
            }
        }
        let value = |index| {
            result
                .value(0, index)
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .map(str::to_owned)
                .ok_or(IngressError::InvalidEnvelope(
                    "invalid CREATE_REPLICATION_SLOT field",
                ))
        };
        if value(0)? != slot || value(3)? != "pgoutput" {
            return Err(IngressError::InvalidEnvelope(
                "CREATE_REPLICATION_SLOT identity mismatch",
            ));
        }
        let consistent_point = value(1)?;
        let snapshot_name = value(2)?;
        if consistent_point == "0/0"
            || snapshot_name.is_empty()
            || !snapshot_name
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        {
            return Err(IngressError::InvalidEnvelope(
                "invalid exported snapshot boundary",
            ));
        }
        Ok(ExportedSnapshot {
            consistent_point,
            snapshot_name,
        })
    }

    /// Drops one exact, caller-validated inactive logical slot.
    ///
    /// This primitive is intentionally not used by ordinary receiver startup.
    /// The bootstrap recovery owner must first reconcile durable attempt
    /// identity and physical slot metadata under the source advisory lock.
    pub(crate) fn drop_slot(&self, slot: &str) -> Result<(), IngressError> {
        validate_slot(slot)?;
        let result = self
            .connection
            .exec(&format!("DROP_REPLICATION_SLOT {}", quote_identifier(slot)));
        if result.status() != Status::CommandOk {
            return Err(IngressError::UnexpectedStatus(result.status()));
        }
        Ok(())
    }

    /// Blocks until libpq returns one complete `CopyData` payload.
    ///
    /// # Errors
    /// Returns the libpq error when COPY ends or the connection fails.
    pub fn receive(&self, shutdown: &ShutdownHandle) -> Result<Vec<u8>, IngressError> {
        loop {
            if shutdown.is_requested() {
                return Err(IngressError::ShutdownRequested);
            }
            match self.connection.copy_data(true) {
                Ok(bytes) => {
                    if bytes.len() > MAX_TRANSACTION_BYTES + COPY_DATA_ENVELOPE_BYTES {
                        return Err(IngressError::LimitExceeded);
                    }
                    return Ok(bytes.to_vec());
                }
                Err(libpq::errors::Error::Backend(message))
                    if message == "COPY still in progress" => {}
                Err(error) => return Err(IngressError::Libpq(error)),
            }
            let deadline = libpq::current_time_usec()
                .checked_add(RECEIVE_POLL_MICROS)
                .ok_or(IngressError::LimitExceeded)?;
            match self.connection.socket_poll(true, false, Some(deadline)) {
                Ok(()) => self.connection.consume_input()?,
                Err(libpq::errors::Error::Timeout) => {}
                Err(error) => return Err(IngressError::Libpq(error)),
            }
        }
    }

    /// Sends a blocking status update for the caller's durable LSN.
    pub(crate) fn send_feedback(&self, durable_lsn: u64) -> Result<(), IngressError> {
        let feedback = encode_feedback(durable_lsn, std::time::SystemTime::now())?;
        self.connection
            .put_copy_data(&feedback)
            .map_err(IngressError::Libpq)?;
        self.connection.flush().map_err(IngressError::Libpq)
    }
}

fn result_text(result: &libpq::Result, row: usize, column: usize) -> Result<String, IngressError> {
    result
        .value(row, column)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .map(str::to_owned)
        .ok_or(IngressError::InvalidEnvelope(
            "invalid replication command response",
        ))
}

pub(crate) fn validate_slot(slot: &str) -> Result<(), IngressError> {
    if slot.is_empty()
        || slot.len() > 63
        || !slot
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(IngressError::InvalidIdentifier("slot"));
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &'static str) -> Result<(), IngressError> {
    let mut bytes = value.bytes();
    if value.len() > 63
        || !matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
    {
        return Err(IngressError::InvalidIdentifier(label));
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & u64::from(u32::MAX))
}
