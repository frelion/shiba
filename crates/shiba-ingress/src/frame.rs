use crate::IngressError;

pub(crate) enum FrameStatus {
    NeedMore,
    Complete {
        len: usize,
        tag: u8,
        terminal_end_lsn: Option<u64>,
    },
}

pub(crate) enum StreamFrameStatus {
    NeedMore,
    Complete {
        len: usize,
        tag: u8,
        xid: Option<u32>,
        first_segment: Option<bool>,
        commit: Option<StreamCommit>,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct StreamCommit {
    pub(crate) flags: u8,
    pub(crate) commit_lsn: u64,
    pub(crate) end_lsn: u64,
}

pub(crate) fn frame_status(input: &[u8]) -> Result<FrameStatus, IngressError> {
    let Some(&tag) = input.first() else {
        return Ok(FrameStatus::NeedMore);
    };
    let end = match tag {
        b'B' => fixed_end(input, 21),
        b'C' => fixed_end(input, 26),
        b'R' => relation_end(input)?,
        b'I' => insert_end(input)?,
        b'U' => update_end(input)?,
        b'D' => delete_end(input)?,
        _ => return Err(IngressError::InvalidFrame),
    };
    let Some(len) = end else {
        return Ok(FrameStatus::NeedMore);
    };
    let terminal_end_lsn = if tag == b'C' {
        Some(read_u64(input, 10).expect("complete fixed COMMIT frame"))
    } else {
        None
    };
    Ok(FrameStatus::Complete {
        len,
        tag,
        terminal_end_lsn,
    })
}

pub(crate) fn stream_frame_status(input: &[u8]) -> Result<StreamFrameStatus, IngressError> {
    let Some(&tag) = input.first() else {
        return Ok(StreamFrameStatus::NeedMore);
    };
    let end = match tag {
        b'S' => fixed_end(input, 6),
        b'E' => fixed_end(input, 1),
        b'c' => fixed_end(input, 30),
        b'A' => fixed_end(input, 9),
        b'R' => streamed_relation_end(input)?,
        b'I' => streamed_insert_end(input)?,
        _ => return Err(IngressError::InvalidFrame),
    };
    let Some(len) = end else {
        return Ok(StreamFrameStatus::NeedMore);
    };
    let xid = if tag == b'E' {
        None
    } else {
        Some(read_u32(input, 1).expect("complete streamed frame XID"))
    };
    if tag == b'A' && read_u32(input, 5) != xid {
        return Err(IngressError::MessageOrder);
    }
    let first_segment = (tag == b'S').then(|| input[5] == 1);
    if tag == b'S' && input[5] > 1 {
        return Err(IngressError::InvalidFrame);
    }
    let commit = (tag == b'c').then(|| StreamCommit {
        flags: input[5],
        commit_lsn: read_u64(input, 6).expect("complete stream COMMIT LSN"),
        end_lsn: read_u64(input, 14).expect("complete stream end LSN"),
    });
    Ok(StreamFrameStatus::Complete {
        len,
        tag,
        xid,
        first_segment,
        commit,
    })
}

fn fixed_end(input: &[u8], len: usize) -> Option<usize> {
    (input.len() >= len).then_some(len)
}

fn relation_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(mut at) = advance(input, 1, 4)? else {
        return Ok(None);
    };
    relation_body_end(input, &mut at)
}

fn streamed_relation_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(mut at) = advance(input, 1, 8)? else {
        return Ok(None);
    };
    relation_body_end(input, &mut at)
}

fn relation_body_end(input: &[u8], at: &mut usize) -> Result<Option<usize>, IngressError> {
    let Some(next) = cstring_end(input, *at)? else {
        return Ok(None);
    };
    *at = next;
    let Some(next) = cstring_end(input, *at)? else {
        return Ok(None);
    };
    *at = next;
    let Some(next) = advance(input, *at, 1)? else {
        return Ok(None);
    };
    *at = next;
    let Some(columns) = read_u16(input, *at) else {
        return Ok(None);
    };
    *at = checked_add(*at, 2)?;
    for _ in 0..columns {
        let Some(next) = advance(input, *at, 1)? else {
            return Ok(None);
        };
        *at = next;
        let Some(next) = cstring_end(input, *at)? else {
            return Ok(None);
        };
        *at = next;
        let Some(next) = advance(input, *at, 8)? else {
            return Ok(None);
        };
        *at = next;
    }
    Ok(Some(*at))
}

fn insert_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(at) = advance(input, 1, 4)? else {
        return Ok(None);
    };
    let Some(&tuple_tag) = input.get(at) else {
        return Ok(None);
    };
    if tuple_tag != b'N' {
        return Err(IngressError::InvalidFrame);
    }
    tuple_end(input, checked_add(at, 1)?)
}

fn streamed_insert_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(at) = advance(input, 1, 8)? else {
        return Ok(None);
    };
    let Some(&tuple_tag) = input.get(at) else {
        return Ok(None);
    };
    if tuple_tag != b'N' {
        return Err(IngressError::InvalidFrame);
    }
    tuple_end(input, checked_add(at, 1)?)
}

fn update_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(mut at) = advance(input, 1, 4)? else {
        return Ok(None);
    };
    let Some(&first_tag) = input.get(at) else {
        return Ok(None);
    };
    if matches!(first_tag, b'K' | b'O') {
        let Some(next) = tuple_end(input, checked_add(at, 1)?)? else {
            return Ok(None);
        };
        at = next;
    } else if first_tag != b'N' {
        return Err(IngressError::InvalidFrame);
    }
    let Some(&new_tag) = input.get(at) else {
        return Ok(None);
    };
    if new_tag != b'N' {
        return Err(IngressError::InvalidFrame);
    }
    tuple_end(input, checked_add(at, 1)?)
}

fn delete_end(input: &[u8]) -> Result<Option<usize>, IngressError> {
    let Some(at) = advance(input, 1, 4)? else {
        return Ok(None);
    };
    let Some(&tuple_tag) = input.get(at) else {
        return Ok(None);
    };
    if !matches!(tuple_tag, b'K' | b'O') {
        return Err(IngressError::InvalidFrame);
    }
    tuple_end(input, checked_add(at, 1)?)
}

fn tuple_end(input: &[u8], mut at: usize) -> Result<Option<usize>, IngressError> {
    let Some(columns) = read_u16(input, at) else {
        return Ok(None);
    };
    at = checked_add(at, 2)?;
    for _ in 0..columns {
        let Some(&kind) = input.get(at) else {
            return Ok(None);
        };
        at = checked_add(at, 1)?;
        match kind {
            b'n' | b'u' => {}
            b't' | b'b' => {
                let Some(length) = read_u32(input, at) else {
                    return Ok(None);
                };
                at = checked_add(at, 4)?;
                let length = usize::try_from(length).map_err(|_| IngressError::LimitExceeded)?;
                let Some(next) = advance(input, at, length)? else {
                    return Ok(None);
                };
                at = next;
            }
            _ => return Err(IngressError::InvalidFrame),
        }
    }
    Ok(Some(at))
}

fn cstring_end(input: &[u8], at: usize) -> Result<Option<usize>, IngressError> {
    let Some(rest) = input.get(at..) else {
        return Ok(None);
    };
    let Some(offset) = rest.iter().position(|byte| *byte == 0) else {
        return Ok(None);
    };
    Ok(Some(checked_add(at, checked_add(offset, 1)?)?))
}

fn advance(input: &[u8], at: usize, amount: usize) -> Result<Option<usize>, IngressError> {
    let end = checked_add(at, amount)?;
    Ok((end <= input.len()).then_some(end))
}

fn checked_add(at: usize, amount: usize) -> Result<usize, IngressError> {
    at.checked_add(amount).ok_or(IngressError::LimitExceeded)
}

fn read_u16(input: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    Some(u16::from_be_bytes(input.get(at..end)?.try_into().ok()?))
}

fn read_u32(input: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    Some(u32::from_be_bytes(input.get(at..end)?.try_into().ok()?))
}

fn read_u64(input: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    Some(u64::from_be_bytes(input.get(at..end)?.try_into().ok()?))
}
