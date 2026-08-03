use super::model::{
    Call, Change, FORMAT_VERSION, FUNCTION_VERSION, Function, Having, Plan, Row, Value,
};

pub fn all_calls_plan() -> Plan {
    Plan {
        version: FORMAT_VERSION,
        input_width: 1,
        calls: vec![
            Call {
                ordinal: 1,
                function_version: FUNCTION_VERSION,
                function: Function::CountStar,
            },
            Call {
                ordinal: 2,
                function_version: FUNCTION_VERSION,
                function: Function::Count { slot: 0 },
            },
            Call {
                ordinal: 3,
                function_version: FUNCTION_VERSION,
                function: Function::Sum { slot: 0 },
            },
            Call {
                ordinal: 4,
                function_version: FUNCTION_VERSION,
                function: Function::Min { slot: 0 },
            },
            Call {
                ordinal: 5,
                function_version: FUNCTION_VERSION,
                function: Function::Max { slot: 0 },
            },
        ],
        having: None,
    }
}

pub fn having_sum_plan() -> Plan {
    let mut plan = all_calls_plan();
    plan.having = Some(Having {
        ordinal: 3,
        greater_than: 0,
    });
    plan
}

pub fn grouped_plan() -> Plan {
    Plan {
        version: FORMAT_VERSION,
        input_width: 2,
        calls: vec![
            Call {
                ordinal: 1,
                function_version: FUNCTION_VERSION,
                function: Function::CountStar,
            },
            Call {
                ordinal: 2,
                function_version: FUNCTION_VERSION,
                function: Function::Sum { slot: 1 },
            },
            Call {
                ordinal: 3,
                function_version: FUNCTION_VERSION,
                function: Function::Min { slot: 1 },
            },
            Call {
                ordinal: 4,
                function_version: FUNCTION_VERSION,
                function: Function::Max { slot: 1 },
            },
        ],
        having: None,
    }
}

pub fn grouped_without_count_plan() -> Plan {
    Plan {
        version: FORMAT_VERSION,
        input_width: 2,
        calls: vec![
            Call {
                ordinal: 1,
                function_version: FUNCTION_VERSION,
                function: Function::Sum { slot: 1 },
            },
            Call {
                ordinal: 2,
                function_version: FUNCTION_VERSION,
                function: Function::Min { slot: 1 },
            },
        ],
        having: None,
    }
}

pub fn grouped_row(key: i64, value: Value) -> Row {
    vec![Value::Int8(key), value]
}

pub fn row(value: Value) -> Row {
    vec![value]
}

pub fn insert(value: Value) -> Change {
    Change {
        before: None,
        after: Some(row(value)),
    }
}

pub fn delete(value: Value) -> Change {
    Change {
        before: Some(row(value)),
        after: None,
    }
}

pub fn update(before: Value, after: Value) -> Change {
    Change {
        before: Some(row(before)),
        after: Some(row(after)),
    }
}
