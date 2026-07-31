use super::*;

fn budget(rows: usize, bytes: usize) -> WorkBudget {
    WorkBudget::new(1, 16, rows, bytes)
}

#[test]
fn positive_weight_is_split_by_both_output_limits() {
    let page = plan_weight_page(10, None, 4, budget(3, 9)).unwrap();
    assert_eq!(page.applied_weight, 2);
    assert_eq!(page.remaining_weight, Some(8));
    assert_eq!(page.usage.output_rows, 2);
    assert_eq!(page.usage.output_bytes, 8);
}

#[test]
fn negative_weight_keeps_a_signed_durable_suffix() {
    let page = plan_weight_page(-7, Some(-5), 3, budget(2, 20)).unwrap();
    assert_eq!(page.applied_weight, -2);
    assert_eq!(page.remaining_weight, Some(-3));
}

#[test]
fn one_oversized_row_is_the_only_byte_exception() {
    let page = plan_weight_page(4, None, 17, budget(8, 16)).unwrap();
    assert_eq!(page.applied_weight, 1);
    assert_eq!(page.remaining_weight, Some(3));
    assert_eq!(page.usage.output_rows, 1);
    assert_eq!(page.usage.output_bytes, 17);
    page.usage.validate(budget(8, 16)).unwrap();
}

#[test]
fn minimum_bigint_weight_resumes_without_overflow() {
    let first = plan_weight_page(i64::MIN, None, 1, budget(3, 10)).unwrap();
    assert_eq!(first.applied_weight, -3);
    assert_eq!(first.remaining_weight, Some(i64::MIN + 3));
    let last = plan_weight_page(-3, Some(-1), 1, budget(3, 10)).unwrap();
    assert_eq!(last.applied_weight, -1);
    assert_eq!(last.remaining_weight, None);
}

#[test]
fn remaining_weight_must_be_a_signed_suffix() {
    assert!(plan_weight_page(5, Some(-1), 1, budget(1, 1)).is_err());
    assert!(plan_weight_page(5, Some(6), 1, budget(1, 1)).is_err());
    assert!(plan_weight_page(-5, Some(-6), 1, budget(1, 1)).is_err());
    assert!(plan_weight_page(5, Some(0), 1, budget(1, 1)).is_err());
}

#[test]
fn negative_mutation_batches_ctid_ranking_before_locking_victims() {
    let production = include_str!("runtime.rs");
    let mutation = production
        .split_once("fn mutate_result_page(")
        .expect("Sink must have a result mutation primitive")
        .1
        .split_once("pub(super) fn plan_weight_page(")
        .expect("Sink result mutation must end before weight planning")
        .0;
    assert!(mutation.contains("ranked_targets AS MATERIALIZED"));
    assert!(mutation
        .contains("row_number() OVER (\n                   PARTITION BY {target_partition}"));
    assert!(mutation.contains("JOIN {result} AS target ON target.ctid=ranked.ctid"));
    assert!(mutation.contains("FOR UPDATE OF target"));
    assert!(!mutation.contains("OFFSET effect.copies_before"));
}
