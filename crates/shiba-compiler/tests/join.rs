use shiba_compiler::{
    IdentityIndexDescriptor, JoinSpecV1, OPERATOR_SPEC_VERSION, SourceColumnDescriptor,
    SourceDescriptor, compile_join,
};
use shiba_operator::{ObjectAddress, OperatorNodeKind};
use shiba_protocol::{GraphId, SourceId};

fn address(object: u32, sub: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id: object,
        sub_id: sub,
    }
}

fn source(id: u64, object: u32, columns: &[(&str, i32, bool)]) -> SourceDescriptor {
    SourceDescriptor {
        source_id: SourceId::new(id).unwrap(),
        relation: address(object, 0),
        columns: columns
            .iter()
            .map(|(name, sub, nullable)| SourceColumnDescriptor {
                name: (*name).into(),
                address: address(object, *sub),
                type_oid: 20,
                nullable: *nullable,
            })
            .collect(),
    }
}

fn fixture() -> (
    JoinSpecV1,
    SourceDescriptor,
    SourceDescriptor,
    IdentityIndexDescriptor,
) {
    let left = source(2, 20_000, &[("id", 1, false), ("right_key", 2, true)]);
    let right = source(1, 30_000, &[("id", 1, false), ("payload", 2, true)]);
    let index_address = address(31_000, 0);
    (
        JoinSpecV1 {
            version: OPERATOR_SPEC_VERSION,
            graph_id: GraphId::new(9).unwrap(),
            left_source_id: left.source_id,
            right_source_id: right.source_id,
            left_id_column: "id".into(),
            left_right_key_column: "right_key".into(),
            right_id_column: "id".into(),
            right_payload_column: "payload".into(),
            right_identity_index: index_address,
        },
        left,
        right.clone(),
        IdentityIndexDescriptor {
            address: index_address,
            relation: right.relation,
            key_column: right.columns[0].address,
            unique: true,
            valid: true,
            ready: true,
            has_expression: false,
            has_predicate: false,
            effective_replica_identity: true,
        },
    )
}

#[test]
fn join_compiles_to_canonical_graph_owned_by_graph_id() {
    let (spec, left, right, index) = fixture();
    let first = compile_join(&spec, &left, &right, &index).unwrap();
    let second = compile_join(&spec, &left, &right, &index).unwrap();
    assert_eq!(first.graph_id, spec.graph_id);
    assert_eq!(first.sources.len(), 2);
    assert!(first.sources[0].source_id < first.sources[1].source_id);
    assert_eq!(first.sources[0].identity_index, Some(index.address));
    assert_eq!(first.canonical_payload, second.canonical_payload);
    assert_eq!(first.digest, second.digest);
    assert!(matches!(
        first.nodes[0].kind,
        OperatorNodeKind::InnerJoin { .. }
    ));
    first.validate().unwrap();
    let encoded = serde_json::to_vec(&spec).unwrap();
    assert_eq!(
        serde_json::from_slice::<JoinSpecV1>(&encoded).unwrap(),
        spec
    );
    let mut unknown = encoded;
    unknown.pop();
    unknown.extend_from_slice(br#","alias":"x"}"#);
    assert!(serde_json::from_slice::<JoinSpecV1>(&unknown).is_err());
}

#[test]
fn join_rejects_identity_and_descriptor_drift() {
    let (spec, left, right, index) = fixture();
    let mut cases = Vec::new();
    let mut wrong_oid = index.clone();
    wrong_oid.address = address(31_001, 0);
    cases.push(wrong_oid);
    let mut not_effective = index.clone();
    not_effective.effective_replica_identity = false;
    cases.push(not_effective);
    let mut predicate = index.clone();
    predicate.has_predicate = true;
    cases.push(predicate);
    let mut invalid = index.clone();
    invalid.valid = false;
    cases.push(invalid);
    for invalid in cases {
        assert!(compile_join(&spec, &left, &right, &invalid).is_err());
    }
    let mut wrong_type = right.clone();
    wrong_type.columns[1].type_oid = 25;
    assert!(compile_join(&spec, &left, &wrong_type, &index).is_err());
    let mut wrong_nullability = right.clone();
    wrong_nullability.columns[1].nullable = false;
    assert!(compile_join(&spec, &left, &wrong_nullability, &index).is_err());
    let mut extra = right.clone();
    extra.columns.push(SourceColumnDescriptor {
        name: "extra".into(),
        address: address(30_000, 3),
        type_oid: 20,
        nullable: true,
    });
    assert!(compile_join(&spec, &left, &extra, &index).is_err());
    let mut duplicate = left.clone();
    duplicate.columns.push(duplicate.columns[0].clone());
    assert!(compile_join(&spec, &duplicate, &right, &index).is_err());
    let mut blank = spec;
    blank.left_id_column = " ".into();
    assert!(compile_join(&blank, &left, &right, &index).is_err());
}
