use crate::ton_db_adapter::TonDBAdapter;
use futures_util::future::try_join_all;
use std::collections::{HashMap, HashSet, VecDeque};
use ton::libs_dict::LibsDict;
use ton::ton_core::cell::{TonCell, TonCellUtils, TonHash};
use ton::ton_core::traits::tlb::TLB;

use log::warn;
use std::ops::SubAssign;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use tongrid_db::entity::TvmCode;
use tongrid_ton::tg_ton::TGTon;

const MAX_RECURSIVE_LIB_DEPTH: usize = 100;

#[derive(Default, Debug)]
pub struct LibSearchStats {
    pub from_scylla: AtomicU64,
    pub from_bc: AtomicU64,
    pub not_found_onchain: AtomicU64,
    pub node_search_disabled: AtomicU64,
    pub no_lib: AtomicU64,
}

impl SubAssign<&LibSearchStats> for LibSearchStats {
    fn sub_assign(&mut self, rhs: &LibSearchStats) {
        self.from_scylla
            .fetch_sub(rhs.from_scylla.load(Relaxed), Relaxed);
        self.from_bc.fetch_sub(rhs.from_bc.load(Relaxed), Relaxed);
        self.not_found_onchain
            .fetch_sub(rhs.not_found_onchain.load(Relaxed), Relaxed);
        self.node_search_disabled
            .fetch_sub(rhs.node_search_disabled.load(Relaxed), Relaxed);
        self.no_lib.fetch_sub(rhs.no_lib.load(Relaxed), Relaxed);
    }
}

impl LibSearchStats {
    pub fn snapshot(&self) -> LibSearchStats {
        LibSearchStats {
            from_scylla: AtomicU64::new(self.from_scylla.load(Relaxed)),
            from_bc: AtomicU64::new(self.from_bc.load(Relaxed)),
            not_found_onchain: AtomicU64::new(self.not_found_onchain.load(Relaxed)),
            node_search_disabled: AtomicU64::new(self.node_search_disabled.load(Relaxed)),
            no_lib: AtomicU64::new(self.no_lib.load(Relaxed)),
        }
    }
}

pub struct LibsAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for LibsAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> LibsAdapter<'a> {
    pub fn stats(&self) -> &LibSearchStats {
        &self.tda.inner().lib_search_stats
    }

    /// Get a library by its hash from ScyllaDB
    pub async fn get_lib_by_hash(&self, lib_hash: &TonHash) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(lib) = self.tda.db().tvm_code().get_opt(lib_hash).await? {
            self.tda
                .inner()
                .lib_search_stats
                .from_scylla
                .fetch_add(1, Relaxed);
            return Ok(Some(lib.boc));
        }

        if !self.tda.inner().try_fetch_lib_onchain {
            warn!("try_fetch_lib_onchain is false, skipping onchain lookup for hash {lib_hash}");
            return Ok(None);
        }

        let ton = &self.tda.inner().ton;

        if let Some(rz) = self.get_lib_onchain(ton, lib_hash).await? {
            self.save_lib(*lib_hash, rz.clone()).await?;
            self.tda
                .inner()
                .lib_search_stats
                .from_bc
                .fetch_add(1, Relaxed);
            Ok(Some(rz))
        } else {
            self.tda
                .inner()
                .lib_search_stats
                .not_found_onchain
                .fetch_add(1, Relaxed);
            Ok(None)
        }
    }

    async fn get_lib_onchain(
        &self,
        ton: &TGTon,
        lib_hash: &TonHash,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let libs = ton
            .lite_client
            .get_libs(std::slice::from_ref(lib_hash), None)
            .await?;
        let Some(lib) = libs.get(lib_hash) else {
            warn!("Lib not found on-chain for hash {}", lib_hash);
            return Ok(None);
        };
        let lib_boc = lib.to_boc()?;
        if lib_boc.is_empty() {
            warn!("Lib data is empty for hash {}", lib_hash);
            return Ok(None);
        }
        Ok(Some(lib_boc))
    }

    async fn save_lib(&self, hash: TonHash, lib_boc: Vec<u8>) -> anyhow::Result<()> {
        let lib_cell = TonCell::from_boc(lib_boc.clone())?;
        let lib_ids = TonCellUtils::extract_lib_ids([&lib_cell])?;
        self.tda
            .db()
            .tvm_code()
            .insert([TvmCode::new(hash, lib_boc, lib_ids)])
            .await?;
        Ok(())
    }

    pub async fn get_libs_dict<'b, I>(&self, lib_ids: I) -> anyhow::Result<Option<Vec<u8>>>
    where
        I: IntoIterator<Item = &'b TonHash>,
    {
        let mut pending_libs = lib_ids
            .into_iter()
            .cloned()
            .map(|lib_id| (lib_id, 0))
            .collect::<VecDeque<_>>();
        let mut visited_libs = HashSet::new();
        let mut known_libs: HashMap<TonHash, TonCell> = HashMap::new();

        while !pending_libs.is_empty() {
            let lib_ids_vec = pending_libs
                .drain(..)
                .filter(|(lib_id, _)| visited_libs.insert(*lib_id))
                .collect::<Vec<_>>();

            let tvm_code_mapper = self.tda.db().tvm_code();
            let tvm_codes_futs = lib_ids_vec
                .iter()
                .map(|(lib_id, _)| tvm_code_mapper.get_opt(lib_id));
            let tvm_codes = try_join_all(tvm_codes_futs).await?;

            for ((_, depth), tvm_code) in lib_ids_vec.into_iter().zip(tvm_codes) {
                if let Some(tvm_code) = tvm_code {
                    let lib_cell = TonCell::from_boc(tvm_code.boc)?;
                    if depth < MAX_RECURSIVE_LIB_DEPTH {
                        pending_libs.extend(
                            TonCellUtils::extract_lib_ids([&lib_cell])?
                                .into_iter()
                                .filter(|nested_lib_hash| !visited_libs.contains(nested_lib_hash))
                                .map(|nested_lib_hash| (nested_lib_hash, depth + 1)),
                        );
                    }
                    known_libs.insert(tvm_code.code_hash, lib_cell);
                }
            }
        }

        if known_libs.is_empty() {
            return Ok(None);
        }

        Ok(Some(LibsDict::from(known_libs).to_boc()?))
    }

    pub async fn get_libs_by_hashes<'b, I>(
        &self,
        lib_hashes: I,
    ) -> anyhow::Result<(Option<Vec<u8>>, Vec<TonHash>)>
    where
        I: IntoIterator<Item = &'b TonHash>,
    {
        let lib_hashes_vec: Vec<TonHash> = lib_hashes.into_iter().cloned().collect();

        if lib_hashes_vec.is_empty() {
            self.tda
                .inner()
                .lib_search_stats
                .no_lib
                .fetch_add(1, Relaxed);
            return Ok((None, vec![]));
        }

        let mut pending_libs = lib_hashes_vec
            .into_iter()
            .map(|lib_hash| (lib_hash, 0))
            .collect::<VecDeque<_>>();
        let mut visited_libs = HashSet::new();
        let mut known_libs: HashMap<TonHash, TonCell> = HashMap::new();
        let mut unknown_libs: Vec<TonHash> = Vec::new();

        while !pending_libs.is_empty() {
            let lib_hashes_vec = pending_libs
                .drain(..)
                .filter(|(lib_hash, _)| visited_libs.insert(*lib_hash))
                .collect::<Vec<_>>();

            let libs = try_join_all(
                lib_hashes_vec
                    .iter()
                    .map(|(lib_hash, _)| self.get_lib_by_hash(lib_hash)),
            )
            .await?;

            for ((lib_hash, depth), lib) in lib_hashes_vec.into_iter().zip(libs) {
                let Some(lib_boc) = lib else {
                    unknown_libs.push(lib_hash);
                    continue;
                };

                let lib_cell = TonCell::from_boc(lib_boc)?;
                if depth < MAX_RECURSIVE_LIB_DEPTH {
                    pending_libs.extend(
                        TonCellUtils::extract_lib_ids([&lib_cell])?
                            .into_iter()
                            .filter(|nested_lib_hash| !visited_libs.contains(nested_lib_hash))
                            .map(|nested_lib_hash| (nested_lib_hash, depth + 1)),
                    );
                }
                known_libs.insert(lib_hash, lib_cell);
            }
        }

        if known_libs.is_empty() {
            return Ok((None, unknown_libs));
        }

        let libs_dict = LibsDict::new(known_libs.into_values())?;
        Ok((Some(libs_dict.to_boc()?), unknown_libs))
    }
}
