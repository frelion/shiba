use crate::{EncodedOperatorState, KernelError, TypedValue, ValueType};

pub(crate) fn left_state() -> EncodedOperatorState {
    EncodedOperatorState {
        codec_version: 1,
        payload: Vec::new(),
    }
}

pub(crate) fn decode_left(state: &EncodedOperatorState) -> Result<(), KernelError> {
    if state.codec_version == 1 && state.payload.is_empty() {
        Ok(())
    } else {
        Err(KernelError::InvalidState)
    }
}

pub(crate) fn encode_right(value: &TypedValue) -> Result<EncodedOperatorState, KernelError> {
    if !matches!(
        value,
        TypedValue::Int8(_) | TypedValue::Null(ValueType::Int8)
    ) {
        return Err(KernelError::WrongType);
    }
    Ok(EncodedOperatorState {
        codec_version: 1,
        payload: value
            .to_canonical_json()
            .map_err(|_| KernelError::InvalidState)?,
    })
}

pub(crate) fn decode_right(state: &EncodedOperatorState) -> Result<TypedValue, KernelError> {
    if state.codec_version != 1 {
        return Err(KernelError::InvalidState);
    }
    let value: TypedValue =
        serde_json::from_slice(&state.payload).map_err(|_| KernelError::InvalidState)?;
    if !matches!(
        value,
        TypedValue::Int8(_) | TypedValue::Null(ValueType::Int8)
    ) || value
        .to_canonical_json()
        .map_err(|_| KernelError::InvalidState)?
        != state.payload
    {
        return Err(KernelError::InvalidState);
    }
    Ok(value)
}
