use crate::{
    IngressError,
    frame::{FrameStatus, frame_status},
};

pub(crate) const FENCE_PREFIX: &str = "shiba.m11.bootstrap.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassifiedFence {
    pub(crate) message_lsn: u64,
    pub(crate) end_lsn: u64,
}

pub(crate) fn classify(
    bytes: &[u8],
    expected_content: &str,
) -> Result<Option<ClassifiedFence>, IngressError> {
    let mut at = 0;
    let mut count = 0;
    let mut tags = [0; 3];
    let mut starts = [0; 3];
    let mut saw_message = false;
    while at < bytes.len() {
        let FrameStatus::Complete { len, tag, .. } = frame_status(&bytes[at..])? else {
            return Err(IngressError::InvalidFrame);
        };
        if count < tags.len() {
            tags[count] = tag;
            starts[count] = at;
        }
        saw_message |= tag == b'M';
        count = count.checked_add(1).ok_or(IngressError::LimitExceeded)?;
        at = at.checked_add(len).ok_or(IngressError::LimitExceeded)?;
    }
    if !saw_message {
        return Ok(None);
    }
    if count != 3 || tags != [b'B', b'M', b'C'] {
        return Err(IngressError::MessageOrder);
    }
    parse_exact_message(
        &bytes[starts[1]..starts[2]],
        &bytes[starts[2]..],
        expected_content,
    )
    .map(Some)
}

fn parse_exact_message(
    message: &[u8],
    commit: &[u8],
    expected_content: &str,
) -> Result<ClassifiedFence, IngressError> {
    if message.first() != Some(&b'M') || message.get(1) != Some(&1) || commit.len() != 26 {
        return Err(IngressError::InvalidFrame);
    }
    let message_lsn = read_u64(message, 2).ok_or(IngressError::InvalidFrame)?;
    let prefix_start = 10;
    let prefix_end = message
        .get(prefix_start..)
        .and_then(|rest| rest.iter().position(|byte| *byte == 0))
        .and_then(|offset| prefix_start.checked_add(offset))
        .ok_or(IngressError::InvalidFrame)?;
    if message.get(prefix_start..prefix_end) != Some(FENCE_PREFIX.as_bytes()) {
        return Err(IngressError::InvalidFrame);
    }
    let length_at = prefix_end
        .checked_add(1)
        .ok_or(IngressError::LimitExceeded)?;
    let content_len =
        usize::try_from(read_u32(message, length_at).ok_or(IngressError::InvalidFrame)?)
            .map_err(|_| IngressError::LimitExceeded)?;
    let content_at = length_at
        .checked_add(4)
        .ok_or(IngressError::LimitExceeded)?;
    let content_end = content_at
        .checked_add(content_len)
        .ok_or(IngressError::LimitExceeded)?;
    if content_end != message.len()
        || message.get(content_at..content_end) != Some(expected_content.as_bytes())
    {
        return Err(IngressError::InvalidFrame);
    }
    let commit_lsn = read_u64(commit, 2).ok_or(IngressError::InvalidFrame)?;
    let end_lsn = read_u64(commit, 10).ok_or(IngressError::InvalidFrame)?;
    if commit.first() != Some(&b'C')
        || commit.get(1) != Some(&0)
        || message_lsn == 0
        || commit_lsn == 0
        || end_lsn < commit_lsn
    {
        return Err(IngressError::InvalidFrame);
    }
    Ok(ClassifiedFence {
        message_lsn,
        end_lsn,
    })
}

fn read_u32(input: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    Some(u32::from_be_bytes(input.get(at..end)?.try_into().ok()?))
}

fn read_u64(input: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    Some(u64::from_be_bytes(input.get(at..end)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(content: &str) -> Vec<u8> {
        let mut bytes = vec![b'B'];
        bytes.extend_from_slice(&10_u64.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes.extend_from_slice(&7_u32.to_be_bytes());
        bytes.push(b'M');
        bytes.push(1);
        bytes.extend_from_slice(&11_u64.to_be_bytes());
        bytes.extend_from_slice(FENCE_PREFIX.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&u32::try_from(content.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(content.as_bytes());
        bytes.push(b'C');
        bytes.push(0);
        bytes.extend_from_slice(&12_u64.to_be_bytes());
        bytes.extend_from_slice(&13_u64.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes
    }

    #[test]
    fn exact_attempt_bound_fence_is_closed_and_terminal() {
        let bytes = exact("1:2:token");
        assert_eq!(
            classify(&bytes, "1:2:token").unwrap(),
            Some(ClassifiedFence {
                message_lsn: 11,
                end_lsn: 13,
            })
        );
        assert!(classify(&bytes, "1:3:token").is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(classify(&trailing, "1:2:token").is_err());
    }

    #[test]
    fn nontransactional_or_mixed_message_fails_closed() {
        let mut nontransactional = exact("1:2:token");
        nontransactional[22] = 0;
        assert!(classify(&nontransactional, "1:2:token").is_err());

        let mut mixed = exact("1:2:token");
        mixed.splice(21..21, [b'I', 0, 0, 0, 1, b'N', 0, 0]);
        assert!(classify(&mixed, "1:2:token").is_err());
    }
}
