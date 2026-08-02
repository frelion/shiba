#![allow(
    dead_code,
    reason = "each integration test compiles only its support subset"
)]

use std::{fs, path::PathBuf, process::Command};

use postgres::Client;

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

fn message_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let mut at = start.checked_add(1)?;
    match *bytes.get(start)? {
        b'B' => bounded_add(at, 20, bytes.len()),
        b'C' => bounded_add(at, 25, bytes.len()),
        b'R' => {
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
        b'I' | b'U' | b'D' => {
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
        _ => None,
    }
}

fn bounded_add(at: usize, amount: usize, length: usize) -> Option<usize> {
    at.checked_add(amount).filter(|next| *next <= length)
}

fn cstring_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let offset = bytes.get(start..)?.iter().position(|byte| *byte == 0)?;
    bounded_add(start, offset.checked_add(1)?, bytes.len())
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
