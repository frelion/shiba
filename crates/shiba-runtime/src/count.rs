use crate::M2Error;

pub(crate) fn advance(current: i64, delta: i64) -> Result<i64, M2Error> {
    current
        .checked_add(delta)
        .filter(|next| *next >= 0)
        .ok_or(M2Error::CountOutOfRange)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_is_deterministic_and_checked() {
        assert_eq!(advance(4, 3).expect("small count"), 7);
        assert!(matches!(
            advance(i64::MAX, 1),
            Err(M2Error::CountOutOfRange)
        ));
        assert!(matches!(advance(0, -1), Err(M2Error::CountOutOfRange)));
    }
}
