use crate::{IngressError, feedback::FeedbackState, tokens::EmptyCommitted};

#[test]
fn empty_feedback_requires_exact_receiver_authorization() {
    let mut feedback = FeedbackState::new(10);
    feedback.mark_empty(20, 7);
    let exact = EmptyCommitted::new(1, 19, 20, 1, 7);
    let old_token = EmptyCommitted::new(1, 18, 19, 1, 7);
    let foreign = EmptyCommitted::new(1, 19, 20, 1, 8);

    assert!(matches!(
        feedback.require_empty(old_token.end_lsn(), old_token.authorization()),
        Err(IngressError::FeedbackMismatch)
    ));
    assert!(matches!(
        feedback.require_empty(foreign.end_lsn(), foreign.authorization()),
        Err(IngressError::FeedbackMismatch)
    ));
    feedback
        .require_empty(exact.end_lsn(), exact.authorization())
        .expect("exact empty authorization");
    assert_eq!(feedback.pending_lsn(), Some(20));
    feedback.complete(20);
    assert_eq!(feedback.last_acknowledged_lsn(), 20);
    assert_eq!(feedback.pending_lsn(), None);
}
