use postgres::Transaction;
use shiba_compiler::{CompilerError, QuerySpecV1};
use shiba_operator::OperatorGraph;

use crate::registration::RegistrationError;

pub(super) fn compile_current(
    transaction: &mut Transaction<'_>,
    spec: &QuerySpecV1,
) -> Result<OperatorGraph, RegistrationError> {
    let mut lock_order = spec.sources.iter().copied().enumerate().collect::<Vec<_>>();
    lock_order.sort_unstable_by_key(|(_, source_id)| source_id.get());
    if lock_order.windows(2).any(|pair| pair[0].1 == pair[1].1) {
        return Err(CompilerError::InvalidSpec.into());
    }

    let mut resolved = (0..spec.sources.len()).map(|_| None).collect::<Vec<_>>();
    for (ordinal, source_id) in lock_order {
        let (descriptor, identity) =
            crate::registration_descriptor::source_descriptor(transaction, source_id)?;
        resolved[ordinal] = Some((descriptor, identity));
    }
    let (descriptors, indexes): (Vec<_>, Vec<_>) = resolved
        .into_iter()
        .map(|entry| entry.ok_or(CompilerError::InvalidSpec))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    Ok(shiba_compiler::compile_query_with_optional_identities(
        spec,
        &descriptors,
        &indexes,
    )?)
}
