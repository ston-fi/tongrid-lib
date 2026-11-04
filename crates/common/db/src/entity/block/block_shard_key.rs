use scylla::_macro_internal::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use ton::ton_core::constants::{TON_MASTERCHAIN, TON_SHARD_FULL};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DBBlockKey {
    pub mc_seqno: u32,
    pub workchain: i32,
    pub shard: i64,
    pub seqno: u32,
}

impl DBBlockKey {
    pub fn new_mc(mc_seqno: u32) -> Self {
        Self {
            mc_seqno,
            workchain: TON_MASTERCHAIN,
            shard: TON_SHARD_FULL as i64,
            seqno: mc_seqno,
        }
    }
}

const DB_BLOCK_KEY_FIELDS_CNT: usize = 4;

impl SerializeRow for DBBlockKey {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        if ctx.columns().len() != DB_BLOCK_KEY_FIELDS_CNT {
            use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
            return Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: "DBBlockKey",
                kind: BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: DB_BLOCK_KEY_FIELDS_CNT,
                    cql_cols: ctx.columns().len(),
                },
            }));
        }
        let mc_seqno = self.mc_seqno as i32;
        let seqno = self.seqno as i32;

        let fields: [&dyn SerializeValue; DB_BLOCK_KEY_FIELDS_CNT] =
            [&mc_seqno, &self.workchain, &self.shard, &seqno];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}
