use super::*;
use crate::MAX_ACTIVE_CONNECTIONS;

#[test]
fn attach_options_require_bounded_statement_timeout() {
    assert!(AttachOptions::new(ReplicationMode::Committed, Duration::ZERO).is_err());
    assert!(
        AttachOptions::new(
            ReplicationMode::Committed,
            Duration::from_millis(2_147_483_648),
        )
        .is_err()
    );
    let options = AttachOptions::new(ReplicationMode::Streamed, Duration::from_secs(1)).unwrap();
    assert_eq!(options.mode(), ReplicationMode::Streamed);
    assert_eq!(options.statement_timeout(), Duration::from_secs(1));
}

#[test]
fn conninfo_requires_explicit_database_and_positive_timeout() {
    let valid = "host=/tmp dbname=test replication=database connect_timeout=1";
    assert_eq!(replication_database(valid).unwrap(), "test");
    assert!(replication_database("host=/tmp dbname=test replication=database").is_err());
    assert!(replication_database("host=/tmp dbname=test connect_timeout=1").is_err());
}

#[test]
fn advisory_key_mapping_is_bijective_and_bigint_bounded() {
    let one = GraphId::new(1).unwrap();
    let two = GraphId::new(2).unwrap();
    assert_eq!(advisory_key(one).unwrap(), -4_611_686_018_427_387_903);
    assert_eq!(advisory_key(two).unwrap(), -4_611_686_018_427_387_902);
    assert_ne!(advisory_key(one).unwrap(), advisory_key(two).unwrap());
    assert!(advisory_key(GraphId::new(u64::MAX).unwrap()).is_err());
    assert_eq!(MAX_ACTIVE_CONNECTIONS, 64);
}
