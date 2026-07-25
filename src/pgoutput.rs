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
            end_lsn: {
                let end_lsn = read_u64(input, 10)?;
                // Validate the ignored commit_time too, so a truncated fixed-size
                // message is not accepted as a complete COMMIT.
                read_u64(input, 18)?;
                end_lsn
            },
        }),
        b'R' => parse_relation(input),
        b'I' => {
            let relid = read_u32(input, 1)?;
            if input.get(5) != Some(&b'N') {
                return Err("invalid insert tuple tag");
            }
            let (row, _) = parse_tuple(input, 5)?;
            Ok(Message::Insert { relid, row })
        }
        b'U' => parse_update(input),
        b'D' => {
            let relid = read_u32(input, 1)?;
            if !matches!(input.get(5), Some(b'K' | b'O')) {
                return Err("invalid delete tuple tag");
            }
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

    fn assert_every_strict_prefix_is_rejected(message: &[u8]) {
        for length in 0..message.len() {
            assert!(
                parse(&message[..length]).is_err(),
                "accepted prefix of length {length} from {message:?}"
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
    fn reads_relation_including_empty_names_and_columns() {
        assert_eq!(
            parse(&relation(9, b"", b"", &[b"id", b""])),
            Ok(Message::Relation {
                relid: 9,
                columns: vec!["id".into(), "".into()]
            })
        );
        assert_eq!(
            parse(&relation(10, b"public", b"empty", &[])),
            Ok(Message::Relation {
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
                relid: 7,
                row: vec![Some("42".into()), None, Some("".into())]
            })
        );
        assert_eq!(
            parse(&dml(b'I', 8, &[tuple(b'N', &[])])),
            Ok(Message::Insert {
                relid: 8,
                row: vec![]
            })
        );
    }

    #[test]
    fn reads_update_with_key_or_full_old_tuple() {
        for old_tag in [b'K', b'O'] {
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
                    relid: 11,
                    old: vec![Some("old".into())],
                    new: vec![Some("new".into()), None]
                })
            );
        }
    }

    #[test]
    fn reads_delete_with_key_or_full_old_tuple() {
        for old_tag in [b'K', b'O'] {
            assert_eq!(
                parse(&dml(b'D', 12, &[tuple(old_tag, &[Some(b"gone"), None])])),
                Ok(Message::Delete {
                    relid: 12,
                    old: vec![Some("gone".into()), None]
                })
            );
        }
    }

    #[test]
    fn rejects_wrong_dml_tuple_tags_and_missing_update_tuples() {
        for tag in [b'K', b'O'] {
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
    }

    #[test]
    fn integer_readers_reject_overflowing_offsets_instead_of_panicking() {
        assert_eq!(read_u16(&[], usize::MAX), Err("truncated pgoutput message"));
        assert_eq!(read_u32(&[], usize::MAX), Err("truncated pgoutput message"));
        assert_eq!(read_u64(&[], usize::MAX), Err("truncated pgoutput message"));
    }

    #[test]
    fn rejects_every_truncation_point_of_each_message_shape() {
        let messages = [
            begin(1, 2),
            commit(3, 4),
            relation(5, b"public", b"things", &[b"id", b"value"]),
            dml(b'I', 6, &[tuple(b'N', &[Some(b"text"), None, Some(b"")])]),
            dml(
                b'U',
                7,
                &[
                    tuple(b'O', &[Some(b"old"), None]),
                    tuple(b'N', &[Some(b"new"), Some(b"value")]),
                ],
            ),
            dml(b'D', 8, &[tuple(b'K', &[Some(b"key")])]),
        ];
        for message in &messages {
            assert!(parse(message).is_ok(), "fixture is invalid: {message:?}");
            assert_every_strict_prefix_is_rejected(message);
        }
    }
}
