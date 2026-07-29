//! Small, PostgreSQL-specific text encodings shared across the extension.

pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn as u32)
}

pub(crate) fn parse_lsn(lsn: &str) -> Result<u64, &'static str> {
    let (high, low) = lsn.split_once('/').ok_or("LSN is missing slash")?;
    let high = u32::from_str_radix(high, 16).map_err(|_| "invalid high LSN word")?;
    let low = u32::from_str_radix(low, 16).map_err(|_| "invalid low LSN word")?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted_without_becoming_sql() {
        assert_eq!(quote_identifier("ordinary"), "\"ordinary\"");
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
    }

    #[test]
    fn lsn_text_round_trips_at_word_boundaries() {
        for (lsn, formatted) in [
            (0, "0/00000000"),
            (1, "0/00000001"),
            (u32::MAX as u64, "0/FFFFFFFF"),
            (1_u64 << 32, "1/00000000"),
            (u64::MAX, "FFFFFFFF/FFFFFFFF"),
        ] {
            assert_eq!(format_lsn(lsn), formatted);
            assert_eq!(parse_lsn(formatted), Ok(lsn));
        }
        assert!(parse_lsn("not-an-lsn").is_err());
        assert!(parse_lsn("100000000/0").is_err());
    }
}
