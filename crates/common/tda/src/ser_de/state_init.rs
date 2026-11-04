use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use ton::block_tlb::{SimpleLib, StateInit};
use ton::ton_core::cell::{TonCell, TonCellUtils, TonHash};
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::scylla_types::DBSimpleLib;
use tongrid_db::entity::TvmCode;

#[derive(Default)]
pub(super) struct DBStateInitData {
    pub code_hash: Option<TonHash>,
    pub data_lib_ids: Vec<TonHash>,
    pub simple_libs: Vec<DBSimpleLib>,
    pub tvm_codes: HashSet<TvmCode>, // state_init.code and simple_libs code if presented
}

// function also erases extracted data from provided state_init
pub(super) fn extract_state_init_data(
    maybe_state_init: Option<&mut StateInit>,
) -> Result<DBStateInitData> {
    let mut state_init_data = DBStateInitData::default();

    let state_init = match maybe_state_init {
        Some(init) => init,
        None => return Ok(state_init_data),
    };

    if let Some(code) = &state_init.code {
        let lib_ids = TonCellUtils::extract_lib_ids([code.deref()])?;
        let tvm_code = TvmCode::new(*code.hash()?, code.to_boc()?, lib_ids);
        state_init_data.code_hash = Some(*code.hash()?);
        state_init_data.tvm_codes.insert(tvm_code);
    }
    state_init.code = None; // erase extracted part

    if let Some(data) = &state_init.data {
        let lib_ids = TonCellUtils::extract_lib_ids([data.deref()])?;
        state_init_data.data_lib_ids = lib_ids.into_iter().collect();
    }

    for (root_hash, lib) in &state_init.library {
        let internal_lib_ids = TonCellUtils::extract_lib_ids([lib.root.deref()])?;
        let db_simple_lib = DBSimpleLib::new(*root_hash, lib.public);
        state_init_data.simple_libs.push(db_simple_lib);
        let tvm_code = TvmCode::new(*root_hash, lib.root.to_boc()?, internal_lib_ids);
        state_init_data.tvm_codes.insert(tvm_code);
    }
    state_init.library = HashMap::new(); // erase extracted part
    Ok(state_init_data)
}

pub(super) fn fill_state_init_code(
    maybe_state_init: Option<&mut StateInit>,
    maybe_code_hash: Option<&TonHash>,
    tvm_codes: &HashMap<TonHash, TvmCode>,
) -> Result<HashSet<TonHash>> {
    let Some(code_hash) = maybe_code_hash else {
        return Ok(HashSet::new());
    };
    let Some(state_init) = maybe_state_init else {
        bail!("code_hash={code_hash}, but state_init is None");
    };
    let Some(tvm_code) = tvm_codes.get(code_hash) else {
        bail!("TvmCode for code_hash={code_hash} not found");
    };
    state_init.code = Some(TonCell::from_boc(tvm_code.boc.to_vec())?.into());

    Ok(tvm_code.lib_ids.iter().cloned().collect())
}

pub(super) fn fill_state_init_simple_libs(
    maybe_state_init: Option<&mut StateInit>,
    simple_libs: &[DBSimpleLib],
    tvm_code: &HashMap<TonHash, TvmCode>,
) -> Result<HashSet<TonHash>> {
    let Some(state_init) = maybe_state_init else {
        return Ok(HashSet::new());
    };

    let mut simple_lib_internal_lib_ids = HashSet::new();
    let mut library = HashMap::new();
    for db_lib in simple_libs {
        let Some(tvm_code) = tvm_code.get(&db_lib.code_hash) else {
            bail!("TvmCode for simple_lib={db_lib:?} not found");
        };
        library.insert(
            tvm_code.code_hash,
            SimpleLib {
                public: db_lib.public,
                root: TonCell::from_boc(tvm_code.boc.to_vec())?.into(),
            },
        );
        simple_lib_internal_lib_ids.extend(tvm_code.lib_ids.iter().cloned());
    }
    state_init.library = library;
    Ok(simple_lib_internal_lib_ids)
}
