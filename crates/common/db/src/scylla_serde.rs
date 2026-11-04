#[macro_export]
macro_rules! scylla_deserialize_val {
    ($row:expr, $col_name:ident, $var_type:ty) => {{
        let col = $row.next().ok_or_else(|| {
            DeserializationError::new(MissingColumnError(stringify!($col_name).to_string()))
        })??;
        <$var_type as ::scylla::deserialize::value::DeserializeValue>::deserialize(
            col.spec.typ(),
            col.slice,
        )?
    }};
}

#[macro_export]
macro_rules! scylla_typecheck_native {
    ($specs:expr, $col_typ:ident, $index:expr, $rust_name:expr, $native_type:path) => {{
        let $col_typ = $specs[$index].typ();
        match $col_typ {
            ColumnType::Native($native_type) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: $rust_name,
                cql_type: $col_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native($native_type)],
                },
            })),
        }?;
    }};
}

#[macro_export]
macro_rules! scylla_typecheck_serialize_columns_len {
    ($ctx:expr, $expected_cnt:expr, $rust_name:expr) => {{
        if $ctx.columns().len() != $expected_cnt {
            use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
            return Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: $rust_name,
                kind: BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: $expected_cnt,
                    cql_cols: $ctx.columns().len(),
                },
            }));
        }
    }};
}

#[macro_export]
macro_rules! scylla_typecheck_deserialize_len_check_end_return {
    ($ctx:expr, $expected_cnt:expr, $rust_name:expr) => {{
        if $ctx.len() != $expected_cnt {
            use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
            return Err(::scylla::_macro_internal::TypeCheckError::new(
                BuiltinTypeCheckError {
                    rust_name: $rust_name,
                    cql_type: ::scylla::_macro_internal::ColumnType::Native(
                        ::scylla::cluster::metadata::NativeType::Text,
                    ),
                    kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
                },
            ));
        }
    }};
}

#[macro_export]
macro_rules! scylla_table_test_spec {
    (bool, $name:expr, $table_spec:expr) => {{
        ::scylla::frame::response::result::ColumnSpec::owned(
            ($name).to_string(),
            ::scylla::frame::response::result::ColumnType::Native(
                ::scylla::frame::response::result::NativeType::Boolean,
            ),
            ($table_spec).clone(),
        )
    }};
    (i32, $name:expr, $table_spec:expr) => {{
        ::scylla::frame::response::result::ColumnSpec::owned(
            ($name).to_string(),
            ::scylla::frame::response::result::ColumnType::Native(
                ::scylla::frame::response::result::NativeType::Int,
            ),
            ($table_spec).clone(),
        )
    }};
    (i64, $name:expr, $table_spec:expr) => {{
        ::scylla::frame::response::result::ColumnSpec::owned(
            ($name).to_string(),
            ::scylla::frame::response::result::ColumnType::Native(
                ::scylla::frame::response::result::NativeType::BigInt,
            ),
            ($table_spec).clone(),
        )
    }};
    (String, $name:expr, $table_spec:expr) => {{
        ::scylla::frame::response::result::ColumnSpec::owned(
            ($name).to_string(),
            ::scylla::frame::response::result::ColumnType::Native(
                ::scylla::frame::response::result::NativeType::Text,
            ),
            ($table_spec).clone(),
        )
    }};
    (Vec<u8>, $name:expr, $table_spec:expr) => {{
        ::scylla::frame::response::result::ColumnSpec::owned(
            ($name).to_string(),
            ::scylla::frame::response::result::ColumnType::Native(
                ::scylla::frame::response::result::NativeType::Blob,
            ),
            ($table_spec).clone(),
        )
    }};
}
