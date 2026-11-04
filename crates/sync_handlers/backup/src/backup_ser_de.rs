mod account;
mod block;
mod jetton;
mod msg;
mod simple_lib;
mod tvm_code;
mod tvm_code_traits;
mod tx;

pub trait ProtoSerial: Sized {
    type ProtoType;
    fn into_proto(self) -> Self::ProtoType;
    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self>;
}
