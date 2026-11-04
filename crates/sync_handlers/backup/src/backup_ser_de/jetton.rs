use num_bigint::BigInt;
use tongrid_db::entity::{jetton_balance::DBJettonBalance, jetton_wallet::DBJettonWallet};

use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::{JettonBalanceProto, JettonWalletProto};

impl ProtoSerial for DBJettonWallet {
    type ProtoType = JettonWalletProto;

    fn into_proto(self) -> Self::ProtoType {
        JettonWalletProto {
            wallet_address: self.wallet_address,
            is_legit: self.is_legit,
            master_address: self.master_address,
            owner_address: self.owner_address,
            mc_seqno: self.mc_seqno as i32,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(DBJettonWallet {
            wallet_address: proto.wallet_address,
            is_legit: proto.is_legit,
            master_address: proto.master_address,
            owner_address: proto.owner_address,
            mc_seqno: proto.mc_seqno as u32,
        })
    }
}

impl ProtoSerial for DBJettonBalance {
    type ProtoType = JettonBalanceProto;

    fn into_proto(self) -> Self::ProtoType {
        JettonBalanceProto {
            wallet_address: self.wallet_address,
            tx_lt: self.tx_lt as i64,
            is_valid: self.is_valid,
            prev_balance: self.prev_balance.map(|b| b.to_string()),
            balance: self.balance.map(|b| b.to_string()),
            tx_utime: self.tx_utime as i64,
            mc_seqno: self.mc_seqno as i32,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(DBJettonBalance {
            wallet_address: proto.wallet_address,
            tx_lt: proto.tx_lt as u64,
            is_valid: proto.is_valid,
            prev_balance: proto
                .prev_balance
                .map(|s| s.parse::<BigInt>())
                .transpose()?,
            balance: proto.balance.map(|s| s.parse::<BigInt>()).transpose()?,
            tx_utime: proto.tx_utime as u32,
            mc_seqno: proto.mc_seqno as u32,
        })
    }
}
