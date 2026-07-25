//! A deliberately small parser for the `pgoutput` protocol used by PostgreSQL
//! logical decoding. Shiba peeks messages, durably routes them, and only then
//! advances the slot with `pg_logical_slot_get_binary_changes()`.

#[derive(Debug, PartialEq)]
pub enum Message {
    Begin { final_lsn: u64, xid: u32 },
    Commit { commit_lsn: u64, end_lsn: u64 },
    Relation { relid: u32, columns: Vec<String> },
    Insert { relid: u32, row: Tuple },
    Update { relid: u32, old: Tuple, new: Tuple },
    Delete { relid: u32, old: Tuple },
}

pub type Tuple = Vec<Option<String>>;

pub fn parse(input: &[u8]) -> Result<Message, &'static str> {
    let tag = *input.first().ok_or("empty pgoutput message")?;
    match tag {
        b'B' => Ok(Message::Begin {
            final_lsn: read_u64(input, 1)?,
            xid: read_u32(input, 17)?,
        }),
        b'C' => Ok(Message::Commit {
            // Commit is: tag, flags, commit_lsn, end_lsn, commit_time.
            commit_lsn: read_u64(input, 2)?,
            end_lsn: read_u64(input, 10)?,
        }),
        b'R' => parse_relation(input),
        b'I' => {
            let relid = read_u32(input, 1)?;
            let (row, _) = parse_tuple(input, 5)?;
            Ok(Message::Insert { relid, row })
        }
        b'U' => parse_update(input),
        b'D' => {
            let relid = read_u32(input, 1)?;
            let (old, _) = parse_tuple(input, 5)?;
            Ok(Message::Delete { relid, old })
        }
        _ => Err("unsupported or truncated pgoutput message"),
    }
}

fn parse_relation(input: &[u8]) -> Result<Message, &'static str> {
    let relid = read_u32(input, 1)?;
    let (_, namespace_end) = read_cstr(input, 5)?;
    let (_, relation_end) = read_cstr(input, namespace_end)?;
    let column_count = read_u16(input, relation_end + 1)? as usize;
    let mut offset = relation_end + 3;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (_, name_end) = read_cstr(input, offset + 1)?;
        columns.push(read_cstr(input, offset + 1)?.0.to_owned());
        // column flags + name + type OID + type modifier
        offset = name_end
            .checked_add(8)
            .ok_or("truncated relation message")?;
        if offset > input.len() {
            return Err("truncated relation message");
        }
    }
    Ok(Message::Relation { relid, columns })
}

fn parse_update(input: &[u8]) -> Result<Message, &'static str> {
    let relid = read_u32(input, 1)?;
    let tag = *input.get(5).ok_or("truncated update message")?;
    let (old, offset) = match tag {
        b'K' | b'O' => parse_tuple(input, 5)?,
        b'N' => return Err("UPDATE lacks an old tuple; source must use REPLICA IDENTITY FULL"),
        _ => return Err("invalid update tuple tag"),
    };
    if input.get(offset) != Some(&b'N') {
        return Err("UPDATE lacks a new tuple");
    }
    let (new, _) = parse_tuple(input, offset)?;
    Ok(Message::Update { relid, old, new })
}

fn parse_tuple(input: &[u8], offset: usize) -> Result<(Tuple, usize), &'static str> {
    match input.get(offset) {
        Some(b'N') | Some(b'K') | Some(b'O') => {}
        _ => return Err("invalid tuple tag"),
    }
    let count = read_u16(input, offset + 1)? as usize;
    let mut cursor = offset + 3;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        match *input.get(cursor).ok_or("truncated tuple")? {
            b'n' => {
                values.push(None);
                cursor += 1;
            }
            b't' => {
                let length = read_u32(input, cursor + 1)? as usize;
                let start = cursor + 5;
                let end = start.checked_add(length).ok_or("truncated tuple")?;
                let value = std::str::from_utf8(input.get(start..end).ok_or("truncated tuple")?)
                    .map_err(|_| "tuple value is not UTF-8")?;
                values.push(Some(value.to_owned()));
                cursor = end;
            }
            b'u' => return Err("unchanged TOAST value is unsupported by the Shiba MVP"),
            _ => return Err("invalid tuple column tag"),
        }
    }
    Ok((values, cursor))
}

fn read_cstr(input: &[u8], offset: usize) -> Result<(&str, usize), &'static str> {
    let rest = input.get(offset..).ok_or("truncated pgoutput message")?;
    let end = rest
        .iter()
        .position(|byte| *byte == 0)
        .ok_or("unterminated pgoutput string")?;
    let value = std::str::from_utf8(&rest[..end]).map_err(|_| "pgoutput string is not UTF-8")?;
    Ok((value, offset + end + 1))
}

fn read_u16(input: &[u8], offset: usize) -> Result<u16, &'static str> {
    Ok(u16::from_be_bytes(
        input
            .get(offset..offset + 2)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, &'static str> {
    Ok(u32::from_be_bytes(
        input
            .get(offset..offset + 4)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, &'static str> {
    Ok(u64::from_be_bytes(
        input
            .get(offset..offset + 8)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_commit_lsn() {
        let mut message = vec![b'C', 0];
        message.extend_from_slice(&42u64.to_be_bytes());
        message.extend_from_slice(&43u64.to_be_bytes());
        message.extend_from_slice(&[0; 8]);
        assert_eq!(
            parse(&message),
            Ok(Message::Commit {
                commit_lsn: 42,
                end_lsn: 43
            })
        );
    }

    #[test]
    fn reads_text_tuple() {
        let mut message = vec![b'I'];
        message.extend_from_slice(&7u32.to_be_bytes());
        message.push(b'N');
        message.extend_from_slice(&2u16.to_be_bytes());
        message.push(b't');
        message.extend_from_slice(&2u32.to_be_bytes());
        message.extend_from_slice(b"42");
        message.push(b'n');
        assert_eq!(
            parse(&message),
            Ok(Message::Insert {
                relid: 7,
                row: vec![Some("42".into()), None]
            })
        );
    }
}
