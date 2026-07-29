//! A deliberately small parser for pgoutput protocol-v2 messages received as
//! CopyData on Shiba's logical-replication connection.

#[derive(Debug, PartialEq)]
pub enum Message {
    Begin {
        final_lsn: u64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
    },
    StreamStart {
        xid: u32,
        first_segment: bool,
    },
    StreamStop,
    StreamCommit {
        xid: u32,
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_time: i64,
    },
    StreamAbort {
        xid: u32,
        subxid: u32,
    },
    Relation {
        source_xid: Option<u32>,
        relid: u32,
        columns: Vec<String>,
    },
    Type {
        source_xid: Option<u32>,
        typeid: u32,
        namespace: String,
        name: String,
    },
    Origin {
        origin_lsn: u64,
        name: String,
    },
    Logical {
        source_xid: Option<u32>,
        transactional: bool,
        message_lsn: u64,
        prefix: String,
        content: Vec<u8>,
    },
    Insert {
        source_xid: Option<u32>,
        relid: u32,
        row: Tuple,
    },
    Update {
        source_xid: Option<u32>,
        relid: u32,
        old: Tuple,
        new: Tuple,
    },
    Delete {
        source_xid: Option<u32>,
        relid: u32,
        old: Tuple,
    },
    Truncate {
        source_xid: Option<u32>,
        relids: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
}

pub type Tuple = Vec<Option<String>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseContext {
    NonStreaming,
    #[cfg(test)]
    Streaming,
}

#[cfg(test)]
pub fn parse(input: &[u8]) -> Result<Message, &'static str> {
    parse_with_context(input, ParseContext::NonStreaming)
}

pub fn parse_with_context(input: &[u8], context: ParseContext) -> Result<Message, &'static str> {
    let tag = *input.first().ok_or("empty pgoutput message")?;
    match tag {
        b'B' => {
            require_exact_len(input, 21, "invalid begin message length")?;
            Ok(Message::Begin {
                final_lsn: read_u64(input, 1)?,
                xid: read_u32(input, 17)?,
            })
        }
        b'C' => {
            require_exact_len(input, 26, "invalid commit message length")?;
            require_zero_flag(input, 1, "invalid commit flags")?;
            Ok(Message::Commit {
                commit_lsn: read_u64(input, 2)?,
                end_lsn: read_u64(input, 10)?,
            })
        }
        b'S' => parse_stream_start(input),
        b'E' => {
            require_exact_len(input, 1, "invalid stream stop message length")?;
            Ok(Message::StreamStop)
        }
        b'c' => parse_stream_commit(input),
        b'A' => parse_stream_abort(input),
        b'R' => parse_transactional(input, context, parse_relation),
        b'Y' => parse_transactional(input, context, parse_type),
        b'O' => parse_origin(input),
        b'M' => parse_transactional(input, context, parse_logical_message),
        b'I' => parse_transactional(input, context, parse_insert),
        b'U' => parse_transactional(input, context, parse_update),
        b'D' => parse_transactional(input, context, parse_delete),
        b'T' => parse_transactional(input, context, parse_truncate),
        _ => Err("unsupported or truncated pgoutput message"),
    }
}

type TransactionalParser = fn(&[u8], usize, Option<u32>) -> Result<Message, &'static str>;

fn parse_transactional(
    input: &[u8],
    context: ParseContext,
    parser: TransactionalParser,
) -> Result<Message, &'static str> {
    match context {
        ParseContext::NonStreaming => parser(input, 1, None),
        #[cfg(test)]
        ParseContext::Streaming => {
            let xid = read_u32(input, 1)?;
            if xid == 0 {
                return Err("invalid transaction ID in streamed replication transaction");
            }
            parser(input, 5, Some(xid))
        }
    }
}

fn parse_stream_start(input: &[u8]) -> Result<Message, &'static str> {
    require_exact_len(input, 6, "invalid stream start message length")?;
    let first_segment = match input[5] {
        0 => false,
        1 => true,
        _ => return Err("invalid stream start first-segment flag"),
    };
    Ok(Message::StreamStart {
        xid: read_u32(input, 1)?,
        first_segment,
    })
}

fn parse_stream_commit(input: &[u8]) -> Result<Message, &'static str> {
    require_exact_len(input, 30, "invalid stream commit message length")?;
    require_zero_flag(input, 5, "invalid stream commit flags")?;
    Ok(Message::StreamCommit {
        xid: read_u32(input, 1)?,
        flags: input[5],
        commit_lsn: read_u64(input, 6)?,
        end_lsn: read_u64(input, 14)?,
        commit_time: read_i64(input, 22)?,
    })
}

fn parse_stream_abort(input: &[u8]) -> Result<Message, &'static str> {
    require_exact_len(input, 9, "invalid stream abort message length")?;
    Ok(Message::StreamAbort {
        xid: read_u32(input, 1)?,
        subxid: read_u32(input, 5)?,
    })
}

fn parse_relation(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let relid = read_u32(input, offset)?;
    let (_, namespace_end) = read_cstr(input, offset + 4)?;
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
    require_exact_len(input, offset, "invalid relation message length")?;
    Ok(Message::Relation {
        source_xid,
        relid,
        columns,
    })
}

fn parse_type(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let typeid = read_u32(input, offset)?;
    let (namespace, namespace_end) = read_cstr(input, offset + 4)?;
    let (name, name_end) = read_cstr(input, namespace_end)?;
    require_exact_len(input, name_end, "invalid type message length")?;
    Ok(Message::Type {
        source_xid,
        typeid,
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    })
}

fn parse_origin(input: &[u8]) -> Result<Message, &'static str> {
    let origin_lsn = read_u64(input, 1)?;
    let (name, name_end) = read_cstr(input, 9)?;
    require_exact_len(input, name_end, "invalid origin message length")?;
    Ok(Message::Origin {
        origin_lsn,
        name: name.to_owned(),
    })
}

fn parse_logical_message(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let transactional = match input.get(offset) {
        Some(0) => false,
        Some(1) => true,
        Some(_) => return Err("invalid logical message flags"),
        None => return Err("truncated pgoutput message"),
    };
    let message_lsn = read_u64(input, offset + 1)?;
    let (prefix, prefix_end) = read_cstr(input, offset + 9)?;
    let content_len = read_u32(input, prefix_end)? as usize;
    let content_start = prefix_end
        .checked_add(4)
        .ok_or("truncated logical message")?;
    let content_end = content_start
        .checked_add(content_len)
        .ok_or("truncated logical message")?;
    let content = input
        .get(content_start..content_end)
        .ok_or("truncated logical message")?;
    require_exact_len(input, content_end, "invalid logical message length")?;
    Ok(Message::Logical {
        source_xid,
        transactional,
        message_lsn,
        prefix: prefix.to_owned(),
        content: content.to_vec(),
    })
}

fn parse_insert(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let relid = read_u32(input, offset)?;
    let tuple_offset = offset + 4;
    if input.get(tuple_offset) != Some(&b'N') {
        return Err("invalid insert tuple tag");
    }
    let (row, tuple_end) = parse_tuple(input, tuple_offset)?;
    require_exact_len(input, tuple_end, "invalid insert message length")?;
    Ok(Message::Insert {
        source_xid,
        relid,
        row,
    })
}

fn parse_update(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let relid = read_u32(input, offset)?;
    let tuple_offset = offset + 4;
    let tag = *input.get(tuple_offset).ok_or("truncated update message")?;
    let (old, offset) = match tag {
        b'K' | b'O' => parse_tuple(input, tuple_offset)?,
        b'N' => return Err("UPDATE lacks an old tuple; source must use REPLICA IDENTITY FULL"),
        _ => return Err("invalid update tuple tag"),
    };
    if input.get(offset) != Some(&b'N') {
        return Err("UPDATE lacks a new tuple");
    }
    let (new, tuple_end) = parse_tuple(input, offset)?;
    require_exact_len(input, tuple_end, "invalid update message length")?;
    Ok(Message::Update {
        source_xid,
        relid,
        old,
        new,
    })
}

fn parse_delete(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let relid = read_u32(input, offset)?;
    let tuple_offset = offset + 4;
    if !matches!(input.get(tuple_offset), Some(b'K' | b'O')) {
        return Err("invalid delete tuple tag");
    }
    let (old, tuple_end) = parse_tuple(input, tuple_offset)?;
    require_exact_len(input, tuple_end, "invalid delete message length")?;
    Ok(Message::Delete {
        source_xid,
        relid,
        old,
    })
}

fn parse_truncate(
    input: &[u8],
    offset: usize,
    source_xid: Option<u32>,
) -> Result<Message, &'static str> {
    let relation_count = read_u32(input, offset)? as usize;
    let options = *input.get(offset + 4).ok_or("truncated truncate message")?;
    if options & !0b11 != 0 {
        return Err("invalid truncate options");
    }
    let relids_start = offset.checked_add(5).ok_or("truncated truncate message")?;
    let relids_bytes = relation_count
        .checked_mul(4)
        .ok_or("truncated truncate message")?;
    let expected_len = relids_start
        .checked_add(relids_bytes)
        .ok_or("truncated truncate message")?;
    require_exact_len(input, expected_len, "invalid truncate message length")?;

    let mut relids = Vec::with_capacity(relation_count);
    for index in 0..relation_count {
        relids.push(read_u32(input, relids_start + index * 4)?);
    }
    Ok(Message::Truncate {
        source_xid,
        relids,
        cascade: options & 0b01 != 0,
        restart_identity: options & 0b10 != 0,
    })
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
    let end = offset.checked_add(2).ok_or("truncated pgoutput message")?;
    Ok(u16::from_be_bytes(
        input
            .get(offset..end)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, &'static str> {
    let end = offset.checked_add(4).ok_or("truncated pgoutput message")?;
    Ok(u32::from_be_bytes(
        input
            .get(offset..end)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, &'static str> {
    let end = offset.checked_add(8).ok_or("truncated pgoutput message")?;
    Ok(u64::from_be_bytes(
        input
            .get(offset..end)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn read_i64(input: &[u8], offset: usize) -> Result<i64, &'static str> {
    let end = offset.checked_add(8).ok_or("truncated pgoutput message")?;
    Ok(i64::from_be_bytes(
        input
            .get(offset..end)
            .ok_or("truncated pgoutput message")?
            .try_into()
            .map_err(|_| "truncated pgoutput message")?,
    ))
}

fn require_zero_flag(input: &[u8], offset: usize, error: &'static str) -> Result<(), &'static str> {
    match input.get(offset) {
        Some(0) => Ok(()),
        Some(_) => Err(error),
        None => Err("truncated pgoutput message"),
    }
}

fn require_exact_len(
    input: &[u8],
    expected: usize,
    error: &'static str,
) -> Result<(), &'static str> {
    if input.len() == expected {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn begin(final_lsn: u64, xid: u32) -> Vec<u8> {
        let mut message = vec![b'B'];
        message.extend_from_slice(&final_lsn.to_be_bytes());
        message.extend_from_slice(&123u64.to_be_bytes());
        message.extend_from_slice(&xid.to_be_bytes());
        message
    }

    fn commit(commit_lsn: u64, end_lsn: u64) -> Vec<u8> {
        let mut message = vec![b'C', 0];
        message.extend_from_slice(&commit_lsn.to_be_bytes());
        message.extend_from_slice(&end_lsn.to_be_bytes());
        message.extend_from_slice(&123u64.to_be_bytes());
        message
    }

    fn stream_start(xid: u32, first_segment: u8) -> Vec<u8> {
        let mut message = vec![b'S'];
        message.extend_from_slice(&xid.to_be_bytes());
        message.push(first_segment);
        message
    }

    fn stream_commit(
        xid: u32,
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_time: i64,
    ) -> Vec<u8> {
        let mut message = vec![b'c'];
        message.extend_from_slice(&xid.to_be_bytes());
        message.push(flags);
        message.extend_from_slice(&commit_lsn.to_be_bytes());
        message.extend_from_slice(&end_lsn.to_be_bytes());
        message.extend_from_slice(&commit_time.to_be_bytes());
        message
    }

    fn stream_abort(xid: u32, subxid: u32) -> Vec<u8> {
        let mut message = vec![b'A'];
        message.extend_from_slice(&xid.to_be_bytes());
        message.extend_from_slice(&subxid.to_be_bytes());
        message
    }

    fn tuple(tag: u8, columns: &[Option<&[u8]>]) -> Vec<u8> {
        let mut tuple = vec![tag];
        tuple.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for column in columns {
            match column {
                None => tuple.push(b'n'),
                Some(value) => {
                    tuple.push(b't');
                    tuple.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    tuple.extend_from_slice(value);
                }
            }
        }
        tuple
    }

    fn dml(tag: u8, relid: u32, tuples: &[Vec<u8>]) -> Vec<u8> {
        let mut message = vec![tag];
        message.extend_from_slice(&relid.to_be_bytes());
        for tuple in tuples {
            message.extend_from_slice(tuple);
        }
        message
    }

    fn relation(relid: u32, namespace: &[u8], name: &[u8], columns: &[&[u8]]) -> Vec<u8> {
        let mut message = vec![b'R'];
        message.extend_from_slice(&relid.to_be_bytes());
        message.extend_from_slice(namespace);
        message.push(0);
        message.extend_from_slice(name);
        message.push(0);
        message.push(b'd');
        message.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for column in columns {
            message.push(0);
            message.extend_from_slice(column);
            message.push(0);
            message.extend_from_slice(&23u32.to_be_bytes());
            message.extend_from_slice(&(-1i32).to_be_bytes());
        }
        message
    }

    fn type_message(typeid: u32, namespace: &[u8], name: &[u8]) -> Vec<u8> {
        let mut message = vec![b'Y'];
        message.extend_from_slice(&typeid.to_be_bytes());
        message.extend_from_slice(namespace);
        message.push(0);
        message.extend_from_slice(name);
        message.push(0);
        message
    }

    fn origin(origin_lsn: u64, name: &[u8]) -> Vec<u8> {
        let mut message = vec![b'O'];
        message.extend_from_slice(&origin_lsn.to_be_bytes());
        message.extend_from_slice(name);
        message.push(0);
        message
    }

    fn logical_message(flags: u8, message_lsn: u64, prefix: &[u8], content: &[u8]) -> Vec<u8> {
        let mut message = vec![b'M', flags];
        message.extend_from_slice(&message_lsn.to_be_bytes());
        message.extend_from_slice(prefix);
        message.push(0);
        message.extend_from_slice(&(content.len() as u32).to_be_bytes());
        message.extend_from_slice(content);
        message
    }

    fn truncate(options: u8, relids: &[u32]) -> Vec<u8> {
        let mut message = vec![b'T'];
        message.extend_from_slice(&(relids.len() as u32).to_be_bytes());
        message.push(options);
        for relid in relids {
            message.extend_from_slice(&relid.to_be_bytes());
        }
        message
    }

    fn streamed(xid: u32, message: &[u8]) -> Vec<u8> {
        let mut streamed = vec![message[0]];
        streamed.extend_from_slice(&xid.to_be_bytes());
        streamed.extend_from_slice(&message[1..]);
        streamed
    }

    fn assert_every_strict_prefix_is_rejected(message: &[u8]) {
        for length in 0..message.len() {
            assert!(
                parse(&message[..length]).is_err(),
                "accepted prefix of length {length} from {message:?}"
            );
        }
    }

    fn assert_every_streaming_prefix_is_rejected(message: &[u8]) {
        for length in 0..message.len() {
            assert!(
                parse_with_context(&message[..length], ParseContext::Streaming).is_err(),
                "accepted streaming prefix of length {length} from {message:?}"
            );
        }
    }

    #[test]
    fn rejects_empty_and_unknown_tags() {
        assert_eq!(parse(&[]), Err("empty pgoutput message"));
        assert_eq!(
            parse(b"?anything"),
            Err("unsupported or truncated pgoutput message")
        );
    }

    #[test]
    fn reads_begin() {
        assert_eq!(
            parse(&begin(u64::MAX, u32::MAX)),
            Ok(Message::Begin {
                final_lsn: u64::MAX,
                xid: u32::MAX
            })
        );
    }

    #[test]
    fn reads_commit_lsn() {
        assert_eq!(
            parse(&commit(42, 43)),
            Ok(Message::Commit {
                commit_lsn: 42,
                end_lsn: 43
            })
        );
    }

    #[test]
    fn reads_stream_start_first_and_later_segments() {
        assert_eq!(
            parse(&stream_start(u32::MAX, 1)),
            Ok(Message::StreamStart {
                xid: u32::MAX,
                first_segment: true,
            })
        );
        assert_eq!(
            parse(&stream_start(42, 0)),
            Ok(Message::StreamStart {
                xid: 42,
                first_segment: false,
            })
        );
    }

    #[test]
    fn reads_stream_stop() {
        assert_eq!(parse(b"E"), Ok(Message::StreamStop));
    }

    #[test]
    fn reads_stream_commit() {
        assert_eq!(
            parse(&stream_commit(u32::MAX, 0, u64::MAX, 43, i64::MIN)),
            Ok(Message::StreamCommit {
                xid: u32::MAX,
                flags: 0,
                commit_lsn: u64::MAX,
                end_lsn: 43,
                commit_time: i64::MIN,
            })
        );
    }

    #[test]
    fn reads_stream_abort() {
        assert_eq!(
            parse(&stream_abort(u32::MAX, u32::MAX - 1)),
            Ok(Message::StreamAbort {
                xid: u32::MAX,
                subxid: u32::MAX - 1,
            })
        );
    }

    #[test]
    fn rejects_invalid_streaming_tags_and_flags() {
        for tag in *b"sea" {
            assert_eq!(
                parse(&[tag]),
                Err("unsupported or truncated pgoutput message")
            );
        }

        for first_segment in [2, u8::MAX] {
            assert_eq!(
                parse(&stream_start(1, first_segment)),
                Err("invalid stream start first-segment flag")
            );
        }

        for flags in [1, u8::MAX] {
            assert_eq!(
                parse(&stream_commit(1, flags, 2, 3, 4)),
                Err("invalid stream commit flags")
            );
        }
    }

    #[test]
    fn rejects_invalid_normal_commit_flags() {
        for flags in [1, u8::MAX] {
            let mut message = commit(1, 2);
            message[1] = flags;
            assert_eq!(parse(&message), Err("invalid commit flags"));
        }
    }

    #[test]
    fn streaming_messages_require_exact_lengths() {
        for mut message in [
            stream_start(1, 1),
            vec![b'E'],
            stream_commit(2, 0, 3, 4, 5),
            stream_abort(6, 7),
        ] {
            assert!(parse(&message).is_ok(), "fixture is invalid: {message:?}");
            assert_every_strict_prefix_is_rejected(&message);
            message.push(0);
            assert!(
                parse(&message).is_err(),
                "accepted trailing bytes: {message:?}"
            );
        }
    }

    #[test]
    fn reads_streamed_relation_with_embedded_xid() {
        let relation = streamed(101, &relation(9, b"public", b"things", &[b"id", b"value"]));
        assert_eq!(
            parse_with_context(&relation, ParseContext::Streaming),
            Ok(Message::Relation {
                source_xid: Some(101),
                relid: 9,
                columns: vec!["id".into(), "value".into()],
            })
        );
        assert_every_streaming_prefix_is_rejected(&relation);
    }

    #[test]
    fn reads_type_origin_logical_message_and_truncate() {
        assert_eq!(
            parse(&type_message(23, b"pg_catalog", b"int4")),
            Ok(Message::Type {
                source_xid: None,
                typeid: 23,
                namespace: "pg_catalog".into(),
                name: "int4".into(),
            })
        );
        assert_eq!(
            parse(&origin(42, b"upstream")),
            Ok(Message::Origin {
                origin_lsn: 42,
                name: "upstream".into(),
            })
        );
        assert_eq!(
            parse(&logical_message(1, 43, b"extension", b"\0binary\xff")),
            Ok(Message::Logical {
                source_xid: None,
                transactional: true,
                message_lsn: 43,
                prefix: "extension".into(),
                content: b"\0binary\xff".to_vec(),
            })
        );
        assert_eq!(
            parse(&truncate(0b11, &[7, 8])),
            Ok(Message::Truncate {
                source_xid: None,
                relids: vec![7, 8],
                cascade: true,
                restart_identity: true,
            })
        );
        assert_eq!(
            parse(&truncate(0, &[])),
            Ok(Message::Truncate {
                source_xid: None,
                relids: vec![],
                cascade: false,
                restart_identity: false,
            })
        );
    }

    #[test]
    fn reads_streamed_type_logical_message_and_truncate_with_current_subxid() {
        let subxid = 202;
        let messages = [
            (
                streamed(subxid, &type_message(23, b"pg_catalog", b"int4")),
                Message::Type {
                    source_xid: Some(subxid),
                    typeid: 23,
                    namespace: "pg_catalog".into(),
                    name: "int4".into(),
                },
            ),
            (
                streamed(subxid, &logical_message(0, 44, b"notice", b"payload")),
                Message::Logical {
                    source_xid: Some(subxid),
                    transactional: false,
                    message_lsn: 44,
                    prefix: "notice".into(),
                    content: b"payload".to_vec(),
                },
            ),
            (
                streamed(subxid, &truncate(0b10, &[9, 10])),
                Message::Truncate {
                    source_xid: Some(subxid),
                    relids: vec![9, 10],
                    cascade: false,
                    restart_identity: true,
                },
            ),
        ];

        for (message, expected) in messages {
            assert_eq!(
                parse_with_context(&message, ParseContext::Streaming),
                Ok(expected)
            );
            assert_every_streaming_prefix_is_rejected(&message);
        }
    }

    #[test]
    fn metadata_and_truncate_messages_reject_truncation_and_trailing_bytes() {
        for message in [
            begin(1, 2),
            commit(3, 4),
            relation(5, b"public", b"items", &[b"id", b"value"]),
            type_message(23, b"pg_catalog", b"int4"),
            origin(42, b"upstream"),
            logical_message(1, 43, b"extension", b"payload"),
            dml(b'I', 6, &[tuple(b'N', &[Some(b"new")])]),
            dml(
                b'U',
                7,
                &[tuple(b'O', &[Some(b"old")]), tuple(b'N', &[Some(b"new")])],
            ),
            dml(b'D', 8, &[tuple(b'O', &[Some(b"old")])]),
            truncate(0b11, &[7, 8]),
        ] {
            assert!(parse(&message).is_ok(), "fixture is invalid: {message:?}");
            assert_every_strict_prefix_is_rejected(&message);
            let mut with_trailing_byte = message;
            with_trailing_byte.push(0);
            assert!(
                parse(&with_trailing_byte).is_err(),
                "accepted trailing byte: {with_trailing_byte:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_logical_message_flags_and_truncate_options() {
        for flags in [2, u8::MAX] {
            assert_eq!(
                parse(&logical_message(flags, 1, b"prefix", b"payload")),
                Err("invalid logical message flags")
            );
        }
        for options in [0b100, u8::MAX] {
            assert_eq!(
                parse(&truncate(options, &[1])),
                Err("invalid truncate options")
            );
        }
    }

    #[test]
    fn reads_streamed_insert_update_and_delete_with_embedded_xid() {
        let messages = [
            (
                streamed(102, &dml(b'I', 7, &[tuple(b'N', &[Some(b"new"), None])])),
                Message::Insert {
                    source_xid: Some(102),
                    relid: 7,
                    row: vec![Some("new".into()), None],
                },
            ),
            (
                streamed(
                    103,
                    &dml(
                        b'U',
                        8,
                        &[tuple(b'O', &[Some(b"old")]), tuple(b'N', &[Some(b"new")])],
                    ),
                ),
                Message::Update {
                    source_xid: Some(103),
                    relid: 8,
                    old: vec![Some("old".into())],
                    new: vec![Some("new".into())],
                },
            ),
            (
                streamed(104, &dml(b'D', 9, &[tuple(b'K', &[Some(b"key")])])),
                Message::Delete {
                    source_xid: Some(104),
                    relid: 9,
                    old: vec![Some("key".into())],
                },
            ),
        ];

        for (message, expected) in messages {
            assert_eq!(
                parse_with_context(&message, ParseContext::Streaming),
                Ok(expected)
            );
            assert_every_streaming_prefix_is_rejected(&message);
        }
    }

    #[test]
    fn streamed_messages_preserve_subxid_distinct_from_stream_top_level_xid() {
        let top_level_xid = 500;
        let subxid = 501;
        assert_eq!(
            parse(&stream_start(top_level_xid, 1)),
            Ok(Message::StreamStart {
                xid: top_level_xid,
                first_segment: true,
            })
        );

        let messages = [
            (
                streamed(
                    subxid,
                    &relation(9, b"public", b"things", &[b"id", b"value"]),
                ),
                Message::Relation {
                    source_xid: Some(subxid),
                    relid: 9,
                    columns: vec!["id".into(), "value".into()],
                },
            ),
            (
                streamed(subxid, &dml(b'I', 9, &[tuple(b'N', &[Some(b"new"), None])])),
                Message::Insert {
                    source_xid: Some(subxid),
                    relid: 9,
                    row: vec![Some("new".into()), None],
                },
            ),
            (
                streamed(
                    subxid,
                    &dml(
                        b'U',
                        9,
                        &[tuple(b'O', &[Some(b"old")]), tuple(b'N', &[Some(b"new")])],
                    ),
                ),
                Message::Update {
                    source_xid: Some(subxid),
                    relid: 9,
                    old: vec![Some("old".into())],
                    new: vec![Some("new".into())],
                },
            ),
            (
                streamed(subxid, &dml(b'D', 9, &[tuple(b'K', &[Some(b"key")])])),
                Message::Delete {
                    source_xid: Some(subxid),
                    relid: 9,
                    old: vec![Some("key".into())],
                },
            ),
        ];

        for (message, expected) in messages {
            assert_eq!(
                parse_with_context(&message, ParseContext::Streaming),
                Ok(expected)
            );
        }
    }

    #[test]
    fn contexts_do_not_silently_accept_the_other_dml_layout() {
        let ordinary = dml(b'I', 7, &[tuple(b'N', &[Some(b"value")])]);
        let streaming = streamed(105, &ordinary);
        assert!(parse_with_context(&ordinary, ParseContext::Streaming).is_err());
        assert!(parse(&streaming).is_err());
    }

    #[test]
    fn rejects_zero_xid_in_streamed_transactional_messages() {
        for message in [
            streamed(0, &relation(1, b"public", b"things", &[b"id"])),
            streamed(0, &type_message(23, b"pg_catalog", b"int4")),
            streamed(0, &logical_message(0, 1, b"prefix", b"payload")),
            streamed(0, &dml(b'I', 1, &[tuple(b'N', &[Some(b"value")])])),
            streamed(
                0,
                &dml(
                    b'U',
                    1,
                    &[tuple(b'O', &[Some(b"old")]), tuple(b'N', &[Some(b"new")])],
                ),
            ),
            streamed(0, &dml(b'D', 1, &[tuple(b'K', &[Some(b"key")])])),
            streamed(0, &truncate(0, &[1])),
        ] {
            assert_eq!(
                parse_with_context(&message, ParseContext::Streaming),
                Err("invalid transaction ID in streamed replication transaction")
            );
        }
    }

    #[test]
    fn reads_relation_including_empty_names_and_columns() {
        assert_eq!(
            parse(&relation(9, b"", b"", &[b"id", b""])),
            Ok(Message::Relation {
                source_xid: None,
                relid: 9,
                columns: vec!["id".into(), "".into()]
            })
        );
        assert_eq!(
            parse(&relation(10, b"public", b"empty", &[])),
            Ok(Message::Relation {
                source_xid: None,
                relid: 10,
                columns: vec![]
            })
        );
    }

    #[test]
    fn reads_insert_text_null_and_empty_tuple() {
        assert_eq!(
            parse(&dml(
                b'I',
                7,
                &[tuple(b'N', &[Some(b"42"), None, Some(b"")])]
            )),
            Ok(Message::Insert {
                source_xid: None,
                relid: 7,
                row: vec![Some("42".into()), None, Some("".into())]
            })
        );
        assert_eq!(
            parse(&dml(b'I', 8, &[tuple(b'N', &[])])),
            Ok(Message::Insert {
                source_xid: None,
                relid: 8,
                row: vec![]
            })
        );
    }

    #[test]
    fn reads_update_with_key_or_full_old_tuple() {
        for old_tag in *b"KO" {
            assert_eq!(
                parse(&dml(
                    b'U',
                    11,
                    &[
                        tuple(old_tag, &[Some(b"old")]),
                        tuple(b'N', &[Some(b"new"), None])
                    ]
                )),
                Ok(Message::Update {
                    source_xid: None,
                    relid: 11,
                    old: vec![Some("old".into())],
                    new: vec![Some("new".into()), None]
                })
            );
        }
    }

    #[test]
    fn reads_delete_with_key_or_full_old_tuple() {
        for old_tag in *b"KO" {
            assert_eq!(
                parse(&dml(b'D', 12, &[tuple(old_tag, &[Some(b"gone"), None])])),
                Ok(Message::Delete {
                    source_xid: None,
                    relid: 12,
                    old: vec![Some("gone".into()), None]
                })
            );
        }
    }

    #[test]
    fn rejects_wrong_dml_tuple_tags_and_missing_update_tuples() {
        for tag in *b"KO" {
            assert_eq!(
                parse(&dml(b'I', 1, &[tuple(tag, &[])])),
                Err("invalid insert tuple tag")
            );
        }
        assert_eq!(
            parse(&dml(b'D', 1, &[tuple(b'N', &[])])),
            Err("invalid delete tuple tag")
        );
        assert_eq!(
            parse(&dml(b'U', 1, &[tuple(b'N', &[])])),
            Err("UPDATE lacks an old tuple; source must use REPLICA IDENTITY FULL")
        );
        assert_eq!(parse(&dml(b'U', 1, &[])), Err("truncated update message"));
        assert_eq!(
            parse(&dml(b'U', 1, &[tuple(b'K', &[])])),
            Err("UPDATE lacks a new tuple")
        );
        let mut wrong_new = dml(b'U', 1, &[tuple(b'O', &[])]);
        wrong_new.extend_from_slice(&tuple(b'K', &[]));
        assert_eq!(parse(&wrong_new), Err("UPDATE lacks a new tuple"));
        let mut invalid_old = dml(b'U', 1, &[]);
        invalid_old.push(b'?');
        assert_eq!(parse(&invalid_old), Err("invalid update tuple tag"));
    }

    #[test]
    fn rejects_invalid_tuple_column_tags_and_unchanged_toast() {
        for (column_tag, error) in [
            (
                b'u',
                "unchanged TOAST value is unsupported by the Shiba MVP",
            ),
            (b'?', "invalid tuple column tag"),
        ] {
            let mut raw_tuple = tuple(b'N', &[]);
            raw_tuple[1..3].copy_from_slice(&1u16.to_be_bytes());
            raw_tuple.push(column_tag);
            assert_eq!(parse(&dml(b'I', 1, &[raw_tuple])), Err(error));
        }
    }

    #[test]
    fn rejects_invalid_utf8_in_tuple_and_every_relation_string() {
        assert_eq!(
            parse(&dml(b'I', 1, &[tuple(b'N', &[Some(&[0xff])])])),
            Err("tuple value is not UTF-8")
        );
        assert_eq!(
            parse(&relation(1, &[0xff], b"table", &[])),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&relation(1, b"public", &[0xff], &[])),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&relation(1, b"public", b"table", &[&[0xff]])),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&type_message(1, &[0xff], b"type")),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&type_message(1, b"public", &[0xff])),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&origin(1, &[0xff])),
            Err("pgoutput string is not UTF-8")
        );
        assert_eq!(
            parse(&logical_message(0, 1, &[0xff], b"payload")),
            Err("pgoutput string is not UTF-8")
        );
    }

    #[test]
    fn rejects_unterminated_relation_strings() {
        let mut namespace = vec![b'R'];
        namespace.extend_from_slice(&1u32.to_be_bytes());
        namespace.extend_from_slice(b"public");
        assert_eq!(parse(&namespace), Err("unterminated pgoutput string"));

        let mut table = vec![b'R'];
        table.extend_from_slice(&1u32.to_be_bytes());
        table.extend_from_slice(b"public\0table");
        assert_eq!(parse(&table), Err("unterminated pgoutput string"));
    }

    #[test]
    fn rejects_declared_lengths_larger_than_available_input() {
        let mut text = tuple(b'N', &[]);
        text[1..3].copy_from_slice(&1u16.to_be_bytes());
        text.push(b't');
        text.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(parse(&dml(b'I', 1, &[text])), Err("truncated tuple"));

        let mut columns = relation(1, b"public", b"table", &[]);
        let count_offset = b"R".len() + 4 + b"public\0".len() + b"table\0".len() + 1;
        columns[count_offset..count_offset + 2].copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(parse(&columns).is_err());

        let mut logical = logical_message(0, 1, b"prefix", b"payload");
        let content_length_offset = b"M".len() + 1 + 8 + b"prefix\0".len();
        logical[content_length_offset..content_length_offset + 4]
            .copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(parse(&logical), Err("truncated logical message"));

        let mut truncated_relations = truncate(0, &[1]);
        truncated_relations[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            parse(&truncated_relations),
            Err("invalid truncate message length")
        );
    }

    #[test]
    fn integer_readers_reject_overflowing_offsets_instead_of_panicking() {
        assert_eq!(read_u16(&[], usize::MAX), Err("truncated pgoutput message"));
        assert_eq!(read_u32(&[], usize::MAX), Err("truncated pgoutput message"));
        assert_eq!(read_u64(&[], usize::MAX), Err("truncated pgoutput message"));
        assert_eq!(read_i64(&[], usize::MAX), Err("truncated pgoutput message"));
    }

    #[test]
    fn rejects_every_truncation_point_of_each_message_shape() {
        let messages = [
            begin(1, 2),
            commit(3, 4),
            stream_start(5, 1),
            vec![b'E'],
            stream_commit(6, 0, 7, 8, 9),
            stream_abort(10, 11),
            relation(14, b"public", b"things", &[b"id", b"value"]),
            dml(b'I', 15, &[tuple(b'N', &[Some(b"text"), None, Some(b"")])]),
            dml(
                b'U',
                16,
                &[
                    tuple(b'O', &[Some(b"old"), None]),
                    tuple(b'N', &[Some(b"new"), Some(b"value")]),
                ],
            ),
            dml(b'D', 17, &[tuple(b'K', &[Some(b"key")])]),
        ];
        for message in &messages {
            assert!(parse(message).is_ok(), "fixture is invalid: {message:?}");
            assert_every_strict_prefix_is_rejected(message);
        }
    }
}
