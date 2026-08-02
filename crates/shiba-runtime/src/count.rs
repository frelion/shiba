use crate::M2Error;

pub(crate) fn advance(current: i64, inserted: usize) -> Result<i64, M2Error> {
    let inserted = i64::try_from(inserted).map_err(|_| M2Error::CountOverflow)?;
    current.checked_add(inserted).ok_or(M2Error::CountOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_is_deterministic_and_checked() {
        assert_eq!(advance(4, 3).expect("small count"), 7);
        assert!(matches!(advance(i64::MAX, 1), Err(M2Error::CountOverflow)));
    }
}
