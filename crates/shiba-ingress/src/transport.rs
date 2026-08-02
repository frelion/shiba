use libpq::{Connection, Status, connection::Info};

use crate::{IngressError, encode_feedback};

/// Synchronous replication transport with no in-process queue or slot authority.
pub struct ReplicationTransport {
    connection: Connection,
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

    /// Starts pgoutput replication for one existing slot and publication.
    ///
    /// # Errors
    /// Rejects unsafe names, command failures, and responses other than
    /// `CopyBoth`.
    pub fn start(&self, slot: &str, publication: &str, start_lsn: u64) -> Result<(), IngressError> {
        validate_slot(slot)?;
        validate_identifier(publication, "publication")?;
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} \
             (proto_version '1', publication_names '{}')",
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

    /// Blocks until libpq returns one complete `CopyData` payload.
    ///
    /// # Errors
    /// Returns the libpq error when COPY ends or the connection fails.
    pub fn receive(&self) -> Result<Vec<u8>, IngressError> {
        self.connection
            .copy_data(false)
            .map(|bytes| bytes.to_vec())
            .map_err(IngressError::Libpq)
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

fn validate_slot(slot: &str) -> Result<(), IngressError> {
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
