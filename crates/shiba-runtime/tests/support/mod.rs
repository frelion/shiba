#![allow(
    dead_code,
    reason = "each integration test compiles only its support subset"
)]

use std::{fs, num::NonZeroU64, path::PathBuf, process::Command};

use postgres::Client;
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::OperatorId;
use shiba_protocol::SourceId;
use shiba_runtime::compile_and_register;

pub(super) fn register_source(client: &mut Client, relation_name: &str) {
    client
        .query_one(
            "SELECT shiba_internal.register_source(1, $1::text::regclass)",
            &[&relation_name],
        )
        .expect("register source relation");
    register_count_operator(client, 1, 1);
}

pub(super) fn register_count_operator(client: &mut Client, source_id: u64, operator_id: u64) {
    let spec = OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("non-zero operator id")),
        source_id: SourceId::new(source_id).expect("non-zero source id"),
        operation: OperatorOperationV1::CountRows,
    };
    compile_and_register(client, &spec).expect("compile and register CountRows operator");
}

pub(super) struct PgoutputCapture {
    pub script: &'static str,
    pub env_prefix: &'static str,
    pub slot: &'static str,
    pub publication: &'static str,
}

impl PgoutputCapture {
    pub(super) fn required(&self, suffix: &str) -> String {
        let name = format!("{}_{suffix}", self.env_prefix);
        std::env::var(&name).unwrap_or_else(|_| panic!("{} must provide {name}", self.script))
    }

    pub(super) fn command(&self, name: &str) -> Command {
        let mut command = Command::new(PathBuf::from(self.required("PG_BINDIR")).join(name));
        command.args([
            "-h",
            &self.required("HOST"),
            "-p",
            &self.required("PORT"),
            "-U",
            &self.required("USER"),
            "-d",
            "postgres",
        ]);
        command
    }

    pub(super) fn create_slot(&self) {
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "-P", "pgoutput", "--create-slot"])
            .status()
            .expect("run pg_recvlogical --create-slot");
        assert!(status.success(), "create logical slot");
    }

    pub(super) fn capture(&self, client: &mut Client, name: &str) -> Vec<u8> {
        let end_lsn: String = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .expect("read capture end LSN")
            .get(0);
        let output = self.capture_path(name);
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "--start", "-f"])
            .arg(&output)
            .args([
                "-n",
                "-E",
                &end_lsn,
                "-o",
                "proto_version=1",
                "-o",
                &format!("publication_names={}", self.publication),
            ])
            .status()
            .expect("capture pgoutput");
        assert!(status.success(), "capture through end LSN {end_lsn}");
        strip_recvlogical_delimiters(&fs::read(output).expect("read captured pgoutput"))
    }

    pub(super) fn capture_streamed(&self, client: &mut Client, name: &str) -> Vec<u8> {
        let end_lsn: String = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .expect("read capture end LSN")
            .get(0);
        let output = self.capture_path(name);
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "--start", "-f"])
            .arg(&output)
            .args([
                "-n",
                "-E",
                &end_lsn,
                "-o",
                "proto_version=2",
                "-o",
                "streaming=on",
                "-o",
                &format!("publication_names={}", self.publication),
            ])
            .status()
            .expect("capture streamed pgoutput");
        assert!(
            status.success(),
            "capture streamed through end LSN {end_lsn}"
        );
        strip_streamed_delimiters(&fs::read(output).expect("read streamed pgoutput"))
    }

    pub(super) fn capture_path(&self, name: &str) -> PathBuf {
        PathBuf::from(self.required("CAPTURE_DIR")).join(name)
    }
}

// pg_recvlogical appends one newline per XLogData payload. Structural message
// lengths identify only that client framing, never tuple content.
pub(super) fn strip_recvlogical_delimiters(capture: &[u8]) -> Vec<u8> {
    assert!(
        framed_message_count(capture).is_some(),
        "incomplete client framing"
    );
    let mut wire = Vec::new();
    let mut start = 0;
    while start < capture.len() {
        let end = message_end(capture, start);
        assert_eq!(capture.get(end), Some(&b'\n'), "missing client delimiter");
        wire.extend_from_slice(&capture[start..end]);
        start = end + 1;
    }
    wire
}

pub(super) fn strip_streamed_delimiters(capture: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut start = 0;
    let mut in_segment = false;
    while start < capture.len() {
        let tag = capture[start];
        let end = stream_message_end(capture, start, in_segment);
        assert_eq!(capture.get(end), Some(&b'\n'), "missing client delimiter");
        wire.extend_from_slice(&capture[start..end]);
        in_segment = match tag {
            b'S' => true,
            b'E' => false,
            _ => in_segment,
        };
        start = end + 1;
    }
    wire
}

pub(super) fn streamed_framed_terminal(capture: &[u8]) -> Option<u8> {
    let mut start = 0;
    let mut in_segment = false;
    let mut terminal = None;
    while start < capture.len() {
        let tag = *capture.get(start)?;
        let end = stream_message_end_checked(capture, start, in_segment)?;
        if capture.get(end) != Some(&b'\n') {
            return None;
        }
        in_segment = match tag {
            b'S' => true,
            b'E' => false,
            _ => in_segment,
        };
        terminal = Some(tag);
        start = end.checked_add(1)?;
    }
    terminal
}

pub(super) fn framed_message_count(capture: &[u8]) -> Option<usize> {
    let mut start = 0;
    let mut count = 0;
    while start < capture.len() {
        let end = message_end_checked(capture, start)?;
        if capture.get(end) != Some(&b'\n') {
            return None;
        }
        start = end.checked_add(1)?;
        count += 1;
    }
    Some(count)
}

pub(super) fn message_end(bytes: &[u8], start: usize) -> usize {
    message_end_checked(bytes, start).expect("complete supported pgoutput message")
}

pub(super) fn stream_message_end(bytes: &[u8], start: usize, in_segment: bool) -> usize {
    stream_message_end_checked(bytes, start, in_segment)
        .expect("complete supported streamed pgoutput message")
}

fn stream_message_end_checked(bytes: &[u8], start: usize, in_segment: bool) -> Option<usize> {
    let mut at = start.checked_add(1)?;
    match *bytes.get(start)? {
        b'S' => bounded_add(at, 5, bytes.len()),
        b'E' => Some(at),
        b'c' => bounded_add(at, 29, bytes.len()),
        b'A' => bounded_add(at, 8, bytes.len()),
        b'R' if in_segment => {
            at = bounded_add(at, 4, bytes.len())?;
            relation_end(bytes, at)
        }
        b'I' if in_segment => {
            at = bounded_add(at, 4, bytes.len())?;
            tuple_change_end(bytes, at)
        }
        _ => None,
    }
}

fn message_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let at = start.checked_add(1)?;
    match *bytes.get(start)? {
        b'B' => bounded_add(at, 20, bytes.len()),
        b'C' => bounded_add(at, 25, bytes.len()),
        b'R' => relation_end(bytes, at),
        b'I' | b'U' | b'D' => tuple_change_end(bytes, at),
        _ => None,
    }
}

fn relation_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    at = bounded_add(at, 4, bytes.len())?;
    at = cstring_end_checked(bytes, at)?;
    at = cstring_end_checked(bytes, at)?;
    at = bounded_add(at, 1, bytes.len())?;
    let columns = read_u16_checked(bytes, at)?;
    at = bounded_add(at, 2, bytes.len())?;
    for _ in 0..columns {
        at = bounded_add(at, 1, bytes.len())?;
        at = cstring_end_checked(bytes, at)?;
        at = bounded_add(at, 8, bytes.len())?;
    }
    Some(at)
}

fn tuple_change_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    at = bounded_add(at, 5, bytes.len())?;
    let columns = read_u16_checked(bytes, at)?;
    at = bounded_add(at, 2, bytes.len())?;
    for _ in 0..columns {
        let kind = *bytes.get(at)?;
        at = bounded_add(at, 1, bytes.len())?;
        if matches!(kind, b't' | b'b') {
            let length = usize::try_from(read_u32_checked(bytes, at)?).ok()?;
            at = bounded_add(at, 4, bytes.len())?;
            at = bounded_add(at, length, bytes.len())?;
        } else if !matches!(kind, b'n' | b'u') {
            return None;
        }
    }
    Some(at)
}

fn bounded_add(at: usize, amount: usize, length: usize) -> Option<usize> {
    at.checked_add(amount).filter(|next| *next <= length)
}

fn cstring_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let offset = bytes.get(start..)?.iter().position(|byte| *byte == 0)?;
    bounded_add(start, offset.checked_add(1)?, bytes.len())
}

pub(super) fn cstring_end(bytes: &[u8], start: usize) -> usize {
    cstring_end_checked(bytes, start).expect("terminated pgoutput string")
}

fn read_u16_checked(bytes: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    Some(u16::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

fn read_u32_checked(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    Some(u32::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

pub(super) fn read_u16(bytes: &[u8], at: usize) -> u16 {
    read_u16_checked(bytes, at).expect("u16 field")
}

pub(super) fn read_u32(bytes: &[u8], at: usize) -> u32 {
    read_u32_checked(bytes, at).expect("u32 field")
}
