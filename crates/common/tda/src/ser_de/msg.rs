use super::state_init::{
    extract_state_init_data, fill_state_init_code, fill_state_init_simple_libs,
};
use crate::ser_de::address::tlb_address_to_str;
use std::collections::{HashMap, HashSet};
use ton::block_tlb::Msg;
use ton::ton_core::cell::{TonCellUtils, TonHash};
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::scylla_types::DBMsg;
use tongrid_db::entity::TvmCode;

pub fn msg_serdb(msg: &Msg) -> anyhow::Result<(DBMsg, HashSet<TvmCode>)> {
    let src = tlb_address_to_str(msg.src())?;
    let dst = tlb_address_to_str(msg.dst())?;

    let body_lib_ids = TonCellUtils::extract_lib_ids([&msg.body.value])?
        .into_iter()
        .collect();

    let mut msg_stripped = msg.clone();

    let state_init_data = extract_state_init_data(msg_stripped.init.as_deref_mut())?;

    let db_msg = DBMsg {
        src,
        dst,
        created_lt: msg.created_lt().unwrap_or(0),
        boc_stripped: msg_stripped.to_boc()?,
        body_lib_ids,
        code_hash: state_init_data.code_hash,
        data_lib_ids: state_init_data.data_lib_ids,
        simple_libs: state_init_data.simple_libs,
    };

    Ok((db_msg, state_init_data.tvm_codes))
}

pub fn msg_deserdb(
    db_msg: &DBMsg,
    tvm_codes: &HashMap<TonHash, TvmCode>,
) -> anyhow::Result<(Msg, HashSet<TonHash>)> {
    let mut msg_lib_ids = db_msg.body_lib_ids.iter().cloned().collect::<HashSet<_>>();

    let data_lib_ids = db_msg.data_lib_ids.iter().cloned();
    msg_lib_ids.extend(data_lib_ids);

    let mut msg = Msg::from_boc(db_msg.boc_stripped.to_vec())?;

    let lib_ids = fill_state_init_code(
        msg.init.as_deref_mut(),
        db_msg.code_hash.as_ref(),
        tvm_codes,
    )?;
    msg_lib_ids.extend(lib_ids);

    let lib_ids =
        fill_state_init_simple_libs(msg.init.as_deref_mut(), &db_msg.simple_libs, tvm_codes)?;
    msg_lib_ids.extend(lib_ids);

    Ok((msg, msg_lib_ids))
}
