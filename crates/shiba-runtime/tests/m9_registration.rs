use std::num::NonZeroU64;

use postgres::{Client, NoTls};
use shiba_compiler::{CompilerError, OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::{CompiledPlan, OperatorId, OutputContract, PlanImplementation};
use shiba_protocol::SourceId;
use shiba_runtime::{M2Error, RegistrationError, compile_and_register};

mod support;

use support::PgoutputCapture;

const ENVIRONMENT: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-registration.sh",
    env_prefix: "SHIBA_M9_REGISTRATION",
    slot: "unused_m9_registration_slot",
    publication: "unused_m9_registration_publication",
};

fn spec(operator_id: u64, source_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("non-zero operator id")),
        source_id: SourceId::new(source_id).expect("non-zero source id"),
        operation,
    }
}

fn authority_counts(client: &mut Client) -> (i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.operator_definition),
                (SELECT count(*) FROM shiba_internal.operator_state),
                (SELECT count(*) FROM shiba.operator_result)",
            &[],
        )
        .expect("query operator authority counts");
    (row.get(0), row.get(1), row.get(2))
}

fn assert_definition(client: &mut Client, operator_id: i64, expected_shape: &str) -> CompiledPlan {
    let row = client
        .query_one(
            "SELECT source_id, compiler_version, plan_format_version,
                    plan_payload, plan_digest, state_codec_version,
                    output_shape, encode(state.state_payload, 'hex')
             FROM shiba_internal.operator_definition AS definition
             JOIN shiba_internal.operator_state AS state USING (operator_id)
             WHERE definition.operator_id = $1",
            &[&operator_id],
        )
        .expect("query generic operator definition");
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i32>(2), 1);
    assert_eq!(row.get::<_, i32>(5), 1);
    assert_eq!(row.get::<_, &str>(6), expected_shape);
    assert_eq!(row.get::<_, &str>(7), "0000000000000000");
    let payload: Vec<u8> = row.get(3);
    let digest: Vec<u8> = row.get(4);
    CompiledPlan::from_canonical_payload(&payload, digest.try_into().expect("32-byte plan digest"))
        .expect("decode durable canonical plan")
}

fn assert_sum_definition(client: &mut Client, plan: &CompiledPlan) {
    let row = client
        .query_one(
            "SELECT 'pg_class'::regclass::oid::bigint,
                    'source_m9.events'::regclass::oid::bigint,
                    attnum
             FROM pg_catalog.pg_attribute
             WHERE attrelid = 'source_m9.events'::regclass
               AND attname = 'payload'",
            &[],
        )
        .expect("query live SumInt8 ObjectAddress");
    let expected = (
        u32::try_from(row.get::<_, i64>(0)).expect("class oid"),
        u32::try_from(row.get::<_, i64>(1)).expect("relation oid"),
        row.get::<_, i16>(2).into(),
    );
    match plan.implementation {
        PlanImplementation::SumInt8 { input, .. } => {
            assert_eq!((input.class_id, input.object_id, input.sub_id), expected);
        }
        _ => panic!("expected SumInt8 plan"),
    }
}

fn install_result_failure(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m9_registration_test;
             CREATE FUNCTION m9_registration_test.fail_result()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'injected result registration failure';
             END
             $$;
             CREATE TRIGGER m9_fail_result
             BEFORE INSERT ON shiba.operator_result
             FOR EACH ROW EXECUTE FUNCTION m9_registration_test.fail_result();",
        )
        .expect("install result registration failure");
}

fn prove_failures_leave_no_partial_rows(client: &mut Client) {
    let before = authority_counts(client);
    let missing_source = spec(10, 2, OperatorOperationV1::CountRows);
    assert!(matches!(
        compile_and_register(client, &missing_source),
        Err(RegistrationError::Runtime(M2Error::SourceBindingMissing))
    ));
    assert_eq!(authority_counts(client), before);

    let missing_column = spec(
        11,
        1,
        OperatorOperationV1::SumInt8 {
            input_column: "missing".to_owned(),
        },
    );
    assert!(matches!(
        compile_and_register(client, &missing_column),
        Err(RegistrationError::Compiler(CompilerError::MissingColumn(column)))
            if column == "missing"
    ));
    assert_eq!(authority_counts(client), before);

    let wrong_type = spec(
        12,
        1,
        OperatorOperationV1::SumInt8 {
            input_column: "label".to_owned(),
        },
    );
    assert!(matches!(
        compile_and_register(client, &wrong_type),
        Err(RegistrationError::Compiler(CompilerError::WrongColumnType {
            column, type_oid: 25
        })) if column == "label"
    ));
    assert_eq!(authority_counts(client), before);

    let duplicate = spec(1, 1, OperatorOperationV1::CountRows);
    assert!(matches!(
        compile_and_register(client, &duplicate),
        Err(RegistrationError::Runtime(M2Error::Postgres(_)))
    ));
    assert_eq!(authority_counts(client), before);
}

fn prove_permissions(client: &mut Client) {
    client
        .batch_execute("CREATE ROLE m9_reader; SET ROLE m9_reader")
        .expect("enter ordinary role");
    let visible: i64 = client
        .query_one("SELECT count(*) FROM shiba.operator_result", &[])
        .expect("ordinary role reads results")
        .get(0);
    assert_eq!(visible, 2);
    assert!(
        client
            .execute("UPDATE shiba.operator_result SET value_bigint = 9", &[])
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.operator_definition", &[])
            .is_err()
    );
    client.batch_execute("RESET ROLE").expect("restore owner");
    assert_eq!(authority_counts(client), (2, 2, 2));
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m9-registration.sh"]
fn m9_live_compile_and_registration_are_atomic_and_private() {
    let connection = ENVIRONMENT.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m9;
             CREATE TABLE source_m9.events (
                 id bigint PRIMARY KEY, payload bigint, label text
             );
             SELECT shiba_internal.register_source(
                 1, 'source_m9.events'::regclass);",
        )
        .expect("install and bind live source");
    let old_tables = client
        .query_one(
            "SELECT to_regclass('shiba_internal.count_state') IS NULL,
                    to_regclass('shiba.count_result') IS NULL",
            &[],
        )
        .expect("query removed count authorities");
    assert!(old_tables.get::<_, bool>(0));
    assert!(old_tables.get::<_, bool>(1));

    install_result_failure(&mut client);
    let count = spec(1, 1, OperatorOperationV1::CountRows);
    assert!(matches!(
        compile_and_register(&mut client, &count),
        Err(RegistrationError::Runtime(M2Error::Postgres(_)))
    ));
    assert_eq!(authority_counts(&mut client), (0, 0, 0));
    client
        .batch_execute("DROP SCHEMA m9_registration_test CASCADE")
        .expect("remove registration failure");
    let compiled_count = compile_and_register(&mut client, &count).expect("register CountRows");
    assert!(matches!(
        compiled_count.implementation,
        PlanImplementation::CountRows
    ));
    assert_eq!(authority_counts(&mut client), (1, 1, 1));
    let durable_count = assert_definition(&mut client, 1, "scalar");
    assert_eq!(durable_count, compiled_count);
    assert!(matches!(
        durable_count.output_contract,
        OutputContract::Scalar { .. }
    ));

    let sum = spec(
        2,
        1,
        OperatorOperationV1::SumInt8 {
            input_column: "payload".to_owned(),
        },
    );
    let compiled_sum = compile_and_register(&mut client, &sum).expect("register live SumInt8");
    assert!(matches!(
        compiled_sum.implementation,
        PlanImplementation::SumInt8 { .. }
    ));
    assert_eq!(authority_counts(&mut client), (2, 2, 2));
    let durable_sum = assert_definition(&mut client, 2, "scalar");
    assert_eq!(durable_sum, compiled_sum);
    assert_sum_definition(&mut client, &durable_sum);
    prove_failures_leave_no_partial_rows(&mut client);
    prove_permissions(&mut client);
}
