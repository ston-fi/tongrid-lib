use futures::try_join;
use std::collections::HashSet;
use std::ops::Deref;
use std::str::FromStr;
use tokio_test::{assert_err, assert_ok};
use ton::ton_core::cell::TonHash;
use tongrid_db::db::DB;
use tongrid_db::entity::block::db_block::DBBlock;
use tongrid_db::entity::jetton_balance::DBJettonBalance;
use tongrid_db::entity::jetton_wallet::DBJettonWallet;
use tongrid_db::entity::ongoing_tx::OngoingTx;
use tongrid_db::entity::out_msg_query_id::out_msg_query_id::OutMsgQueryId;
use tongrid_db::entity::scylla_types::{DBMsg, DBSimpleLib};
use tongrid_db::entity::tvm_code_traits::tvm_code_traits::TvmCodeTraits;
use tongrid_db::entity::tx::tx_key::DB_TX_KEY_ZERO;
use tongrid_db::entity::TxKeyLookup;
use tongrid_db::entity::{account::db_account::DBAccount, msg_txs::msg_txs::MsgTxs, DBTx, DBTxKey};
use tongrid_db::paginator::LTPage;
use tongrid_db::test_support::init_test_db;
use tongrid_proto::v3::ContractTrait;
use tongrid_test_utils::ton::make_db_ton_hash;

// works only for entities which doesn't affect other entities
// (for example, account usually depends on tx)
#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn test_db_entities() -> anyhow::Result<()> {
    let (_scylla_env, db) = init_test_db().await?;

    let results = try_join!(
        assert_block_shard(&db),
        assert_transaction(&db),
        assert_msg_txs(&db),
        assert_out_msg_query_id(&db),
        assert_tvm_code_traits(&db),
        assert_jetton_wallet(&db),
        assert_jetton_balance(&db),
        assert_ongoing_transaction(&db)
    );

    if let Err(err) = &results {
        println!("{:?}\n backtrace: {}", err, err.backtrace());
    }
    assert_ok!(results);
    Ok(())
}

async fn assert_block_shard(db: &DB) -> anyhow::Result<()> {
    let mapper = db.block();
    let mc_block = DBBlock {
        mc_seqno: 1,
        workchain: -1,
        shard: 13,
        seqno: 1100500,
        root_hash: make_db_ton_hash(&[3, 2, 3, 4, 5]),
        file_hash: make_db_ton_hash(&[5, 7, 8, 9, 0]),
        end_lt: 7,
        key_block_seqno: 12,
        utime: 22331,
        rand_seed: TonHash::ZERO,
        boc_stripped: vec![1, 2, 3, 6, 5].into(),
    };

    let wc_block = DBBlock {
        mc_seqno: 1,
        workchain: 2,
        shard: 3,
        seqno: 100500,
        root_hash: make_db_ton_hash(&[1, 2, 3, 4, 5]),
        file_hash: make_db_ton_hash(&[6, 7, 8, 9, 0]),
        end_lt: 4,
        key_block_seqno: 5,
        utime: 22,
        rand_seed: TonHash::ZERO,
        boc_stripped: vec![0, 9, 8, 7, 6].into(),
    };

    // full
    mapper.insert(&wc_block).await?;
    mapper
        .insert_many(&[wc_block.clone(), mc_block.clone()])
        .await?;

    let res_blocks = mapper.get_by_mc_seqno(1).await?;
    assert_eq!(
        res_blocks.deref(),
        &vec![mc_block.clone(), wc_block.clone()]
    );

    let res_blocks = mapper.get_by_mc_seqnos(&[1]).await?;
    assert_eq!(
        res_blocks.deref(),
        &vec![mc_block.clone(), wc_block.clone()]
    );

    let res_block = mapper
        .get_shard(1, wc_block.workchain, wc_block.shard, wc_block.seqno)
        .await?;
    assert_eq!(res_block.deref(), &wc_block);
    assert_err!(
        mapper
            .get_shard(1, wc_block.workchain, wc_block.shard, wc_block.seqno + 1)
            .await
    );

    let res_block = mapper.get_master_by_mc_seqno_opt(1).await?;
    assert_eq!(res_block.unwrap().deref(), &mc_block);

    // lite
    let res_blocks = db.block().get_master_by_mc_seqno_opt(1).await?;
    assert!(res_blocks.is_some());
    assert_eq!(res_blocks.unwrap().deref(), &mc_block);
    Ok(())
}

async fn assert_transaction(db: &DB) -> anyhow::Result<()> {
    let mut my_txs = vec![];
    for utime in [10, 20, 30] {
        let msg_lt = utime * 100 - 1;
        let tx_utime = utime as u32;
        let tx_lt = (tx_utime as u64) * 100 - 1;

        let msg = DBMsg {
            src: "source".to_string(),
            dst: "destination".to_string(),
            created_lt: msg_lt,
            ..DBMsg::default()
        };
        let db_tx = DBTx {
            mc_seqno: 10,
            address: "address".to_string(),
            tx_lt,
            block_workchain: 2,
            block_shard: 3,
            block_seqno: 4,
            tx_hash: make_db_ton_hash(&[1, 2, 3, 4, tx_utime as u8]),
            tx_utime,
            boc_stripped: vec![6, 7, 8, 9, 0].into(),
            in_msg: Some(msg.clone()),
            out_msgs: vec![msg.clone(), msg.clone()],
            in_msg_hash: Some(make_db_ton_hash(&[6, 4, 3, 2])),
            in_msg_hash_normalized: Some(make_db_ton_hash(&[3, 2, 1, 0])),
            initial_tx: DB_TX_KEY_ZERO.clone(),
        };

        db.tx().insert(&db_tx).await?;
        my_txs.push(db_tx);
    }

    // get
    assert_eq!(
        db.tx()
            .get_opt(&my_txs[0].tx_key())
            .await?
            .as_ref()
            .unwrap(),
        &my_txs[0]
    );
    assert!(db.tx_lite().get_opt(&my_txs[0].tx_key()).await?.is_some()); // just check it works

    let mut tr2_key = my_txs[0].tx_key();
    tr2_key.tx_lt += 1;
    assert_eq!(db.tx().get_opt(&tr2_key).await?, None);

    // get_many
    let keys = my_txs.iter().map(|x| x.tx_key()).collect::<Vec<_>>();
    let txs = db.tx().get_many(&keys).await?;
    assert_eq!(txs, my_txs);

    // get_by_mc_seqno(s)
    assert_eq!(db.tx().get_by_mc_seqno(10).await?.len(), 3);
    assert_eq!(db.tx().get_by_mc_seqnos(&[10, 11]).await?.len(), 3);

    // lookups:
    // TxKeyLookup::Hash
    let lookup_key = TxKeyLookup::tx_hash(&my_txs[0].tx_hash);
    assert_eq!(&db.tx().lookup(lookup_key).await?, &my_txs[0]);
    let lookup_key = TxKeyLookup::tx_hash(&[1, 2, 3, 4, 5, 6]);
    assert_eq!(db.tx().lookup_opt(lookup_key).await?, None);

    // TxKeyLookup::AddressLT
    let address = &my_txs[0].address;
    let lookup_key = TxKeyLookup::address_lt(address, my_txs[0].tx_lt);
    assert_eq!(&db.tx().lookup(lookup_key).await?, &my_txs[0]);

    // TxKeyLookup::AddressLatest
    let lookup_key = TxKeyLookup::address_latest(address, my_txs[0].mc_seqno);
    assert_eq!(&db.tx().lookup(lookup_key).await?, &my_txs[2]);

    // TxKeyLookup::AddressLTRange
    let mut lt_page = LTPage {
        from_lt: 0,
        to_lt: i64::MAX as u64,
        order_desc: false,
        limit: 100,
    };

    let lookup_key = TxKeyLookup::address_lt_range(address, &lt_page);
    assert_eq!(db.tx().lookup_many(lookup_key).await?.len(), 3);
    let lookup_key = TxKeyLookup::address_lt_range("not_a_account", &lt_page);
    assert!(db.tx().lookup_many(lookup_key).await?.is_empty());

    lt_page.from_lt = my_txs[0].tx_lt - 1;
    let lookup_key = TxKeyLookup::address_lt_range(address, &lt_page);
    assert_eq!(db.tx().lookup_many(lookup_key).await?.len(), 3);

    lt_page.from_lt = my_txs[0].tx_lt + 1;
    let lookup_key = TxKeyLookup::address_lt_range(address, &lt_page);
    assert_eq!(db.tx().lookup_many(lookup_key).await?.len(), 2);

    lt_page.from_lt = my_txs[1].tx_lt - 1;
    lt_page.order_desc = true;
    let lookup_key = TxKeyLookup::address_lt_range(address, &lt_page);
    let txs = db.tx().lookup_many(lookup_key).await?;
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[0].tx_lt, my_txs[2].tx_lt);
    assert_eq!(txs[1].tx_lt, my_txs[1].tx_lt);

    lt_page.from_lt = 0;
    lt_page.order_desc = false;
    lt_page.limit = 2;
    let lookup_key = TxKeyLookup::address_lt_range(address, &lt_page);
    let txs = db.tx().lookup_many(lookup_key).await?;
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[0].tx_lt, my_txs[0].tx_lt);
    assert_eq!(txs[1].tx_lt, my_txs[1].tx_lt);

    // TxKeyLookup::AddressUtimeRange
    let lookup_key = TxKeyLookup::address_utime_range(address, 0, i32::MAX as u32, false, 100);
    assert_eq!(db.tx().lookup_many(lookup_key).await?.len(), 3);

    // TxKeyLookup::AddressQueryId
    // is not covered

    // TxKeyLookup::InMsgHash
    let lookup_key = TxKeyLookup::in_msg_hash(my_txs[0].in_msg_hash.as_ref().unwrap().as_slice());
    assert_eq!(&db.tx().lookup(lookup_key).await?, &my_txs[2]); // latest

    // TxKeyLookup::InMsgHashNormalized
    let lookup_key = TxKeyLookup::in_msg_hash_norm(
        my_txs[1]
            .in_msg_hash_normalized
            .as_ref()
            .unwrap()
            .as_slice(),
    );
    assert_eq!(&db.tx().lookup(lookup_key).await?, &my_txs[2]); // latest

    // TxKeyLookup::SeqnosCodeHashes
    // is not covered
    Ok(())
}

async fn assert_msg_txs(db: &DB) -> anyhow::Result<()> {
    let r1 = db.msg_txs().get("source", "destination", 1).await?;
    assert!(r1.is_none());
    let msg_data1 = MsgTxs {
        src: "source".to_string(),
        dst: "destination".to_string(),
        created_lt: 1,
        src_mc_seqno: Some(2),
        src_tx_lt: Some(4),
        dst_mc_seqno: None,
        dst_tx_lt: None,
    };
    db.msg_txs().insert_out_msg(&msg_data1).await?;
    let r2 = db
        .msg_txs()
        .get(&msg_data1.src, &msg_data1.dst, msg_data1.created_lt)
        .await?;
    assert_eq!(r2, Some(msg_data1.clone()));
    let msg_data2 = MsgTxs {
        src: "source".to_string(),
        dst: "destination".to_string(),
        created_lt: 1,
        src_mc_seqno: None,
        src_tx_lt: None,
        dst_mc_seqno: Some(7),
        dst_tx_lt: Some(9),
    };
    db.msg_txs().insert_in_msg(&msg_data2).await?;
    let r3 = db
        .msg_txs()
        .get(&msg_data2.src, &msg_data2.dst, msg_data2.created_lt)
        .await?;
    let msg_data3 = MsgTxs {
        src: "source".to_string(),
        dst: "destination".to_string(),
        created_lt: 1,
        src_mc_seqno: Some(2),
        src_tx_lt: Some(4),
        dst_mc_seqno: Some(7),
        dst_tx_lt: Some(9),
    };
    assert_eq!(r3, Some(msg_data3));
    Ok(())
}

async fn assert_out_msg_query_id(db: &DB) -> anyhow::Result<()> {
    let msg1 = OutMsgQueryId {
        address: "address".to_string(),
        query_id: 1,
        tx_utime: 1,
        tx_lt: 1,
        mc_seqno: 1,
    };

    let msg2 = OutMsgQueryId {
        address: "address".to_string(),
        query_id: 1,
        tx_utime: 2,
        tx_lt: 2,
        mc_seqno: 2,
    };
    db.out_msg_query_id().insert(&msg1).await?;
    db.out_msg_query_id().insert(&msg2).await?;

    let lookup = TxKeyLookup::address_query_id("address", 1, 1);
    let tx_keys = db.tx().lookup_keys(lookup).await?;
    assert_eq!(tx_keys, vec![msg1.tx_key(), msg2.tx_key()]);

    let lookup = TxKeyLookup::address_query_id("address", 1, 2);
    let tx_keys = db.tx().lookup_keys(lookup).await?;
    assert_eq!(tx_keys, vec![msg2.tx_key()]);
    Ok(())
}

async fn assert_jetton_wallet(db: &DB) -> anyhow::Result<()> {
    let mapper = db.jetton_wallet();

    let wallets = [
        DBJettonWallet {
            wallet_address: "1".to_string(),
            is_legit: true,
            master_address: "1".to_string(),
            owner_address: "1".to_string(),
            mc_seqno: 1,
        },
        DBJettonWallet {
            wallet_address: "2".to_string(),
            is_legit: true,
            master_address: "2".to_string(),
            owner_address: "2".to_string(),
            mc_seqno: 2,
        },
        DBJettonWallet {
            wallet_address: "3".to_string(),
            is_legit: true,
            master_address: "2".to_string(),
            owner_address: "3".to_string(),
            mc_seqno: 3,
        },
        DBJettonWallet {
            wallet_address: "4".to_string(),
            is_legit: false,
            master_address: "2".to_string(),
            owner_address: "3".to_string(),
            mc_seqno: 4,
        },
    ];
    mapper.insert_many(&wallets).await?;

    let wallet = mapper.get_opt("1").await?.unwrap();
    assert_eq!(wallet, wallets[0]);
    let wallet = mapper.get_opt("2").await?.unwrap();
    assert_eq!(wallet, wallets[1]);

    let master_wallets = mapper.get_legit_by_master("2", None, Some(1)).await?;
    assert_eq!(master_wallets.len(), 1);
    assert_eq!(master_wallets[0], wallets[1]);
    let master_wallets = mapper.get_legit_by_master("2", Some("2"), Some(1)).await?;
    assert_eq!(master_wallets.len(), 1);
    assert_eq!(master_wallets[0], wallets[2]);
    // non-legit is skipped
    let master_wallets = mapper.get_legit_by_master("2", None, Some(5)).await?;
    assert_eq!(master_wallets.len(), 2);
    assert_eq!(master_wallets.as_slice(), &wallets[1..=2]);

    let master_wallets = mapper.get_legit_by_master("3", None, Some(1)).await?;
    assert_eq!(master_wallets.len(), 0);

    let master_wallets = mapper.get_legit_by_owner("3", None, Some(1)).await?;
    assert_eq!(master_wallets.len(), 1);
    assert_eq!(master_wallets[0], wallets[2]);

    let single_wallet = mapper.get_legit_by_owner_and_master("3", "2").await?;
    assert_eq!(single_wallet, Some(wallets[2].clone()));

    let not_a_single_wallet = mapper.get_legit_by_owner_and_master("3", "3").await?;
    assert_eq!(not_a_single_wallet, None);

    let wallet_by_mc_seqno = mapper.get_by_mc_seqno(4).await?;
    assert_eq!(wallet_by_mc_seqno.len(), 1);
    assert_eq!(wallet_by_mc_seqno[0], wallets[3]);

    Ok(())
}

async fn assert_jetton_balance(db: &DB) -> anyhow::Result<()> {
    let mapper = db.jetton_balance();

    let balances = [
        DBJettonBalance {
            wallet_address: "1".to_string(),
            tx_lt: 1,
            is_valid: false,
            prev_balance: Some(1.into()),
            balance: Some(1.into()),
            tx_utime: 0,
            mc_seqno: 1,
        },
        DBJettonBalance {
            wallet_address: "1".to_string(),
            tx_lt: 2,
            is_valid: true,
            prev_balance: Some(1.into()),
            balance: Some(1.into()),
            tx_utime: 0,
            mc_seqno: 2,
        },
        DBJettonBalance {
            wallet_address: "1".to_string(),
            tx_lt: 3,
            is_valid: false,
            prev_balance: Some(1.into()),
            balance: Some(1.into()),
            tx_utime: 0,
            mc_seqno: 3,
        },
        DBJettonBalance {
            wallet_address: "1".to_string(),
            tx_lt: 4,
            is_valid: true,
            prev_balance: Some(1.into()),
            balance: Some(2.into()),
            tx_utime: 200,
            mc_seqno: 4,
        },
    ];
    mapper.insert_many(&balances).await?;
    let all = mapper.get_page("1", &LTPage::default()).await?;
    assert_eq!(all.len(), 4);

    let from_lt_1 = mapper
        .get_page(
            "1",
            &LTPage {
                from_lt: 1,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(from_lt_1.len(), 4);

    let from_lt_limit_1 = mapper
        .get_page(
            "1",
            &LTPage {
                from_lt: 1,
                limit: 1,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(from_lt_limit_1.len(), 1);
    assert_eq!(from_lt_limit_1[0], balances[0]);

    let latest = mapper.get_latest("1", i64::MAX as u64).await?.unwrap();
    assert_eq!(latest, balances[3]);
    let before_lt = mapper.get_latest("1", 2).await?.unwrap();
    assert_eq!(before_lt, balances[1]);
    let before_lt = mapper.get_latest("1", 4).await?.unwrap();
    assert_eq!(before_lt, balances[3]);
    let before_lt = mapper.get_latest_valid("1", 3).await?.unwrap();
    assert_eq!(before_lt, balances[1]);

    let mut lt_page = LTPage {
        from_lt: 0,
        to_lt: 5,
        order_desc: true,
        limit: 10,
    };
    let all = mapper.get_page("1", &lt_page).await?;
    assert_eq!(all, balances.iter().cloned().rev().collect::<Vec<_>>());
    let all_valid = mapper.get_page_valid("1", &lt_page).await?;
    assert_eq!(all_valid, vec![balances[3].clone(), balances[1].clone()]);

    lt_page.order_desc = false;
    let all_valid = mapper.get_page_valid("1", &lt_page).await?;
    assert_eq!(all_valid, vec![balances[1].clone(), balances[3].clone()]);

    lt_page.limit = 1;
    let all_valid_lim_1 = mapper.get_page_valid("1", &lt_page).await?;
    assert_eq!(all_valid_lim_1, vec![balances[1].clone()]);

    lt_page.limit = 10;
    lt_page.to_lt = 3;
    let all_valid_last = mapper.get_page_valid("1", &lt_page).await?;
    assert_eq!(all_valid_last, vec![balances[1].clone()]);

    lt_page.to_lt = 1;
    let all_valid_last = mapper.get_page_valid("1", &lt_page).await?;
    assert_eq!(all_valid_last, vec![]);

    let balance_by_mc_seqno = mapper.get_by_mc_seqnos(&[4]).await?;
    assert_eq!(balance_by_mc_seqno.len(), 1);
    assert_eq!(balance_by_mc_seqno[0], balances[3]);

    Ok(())
}

async fn assert_ongoing_transaction(db: &DB) -> anyhow::Result<()> {
    let mapper = db.ongoing_tx();
    let txs = [
        OngoingTx {
            address: "1".to_string(),
            count: 0,
            tx_lt: 1,
            mc_seqno: 0,
            last_processed_mc_seqno: 0,
        },
        OngoingTx {
            address: "1".to_string(),
            count: 1,
            tx_lt: 2,
            mc_seqno: 0,
            last_processed_mc_seqno: 0,
        },
        OngoingTx {
            address: "2".to_string(),
            count: 2,
            tx_lt: 2,
            mc_seqno: 0,
            last_processed_mc_seqno: 0,
        },
        OngoingTx {
            address: "3".to_string(),
            count: 3,
            tx_lt: 2,
            mc_seqno: 0,
            last_processed_mc_seqno: 0,
        },
    ];

    mapper.insert_many(&txs).await?;

    let tx = mapper.get_ongoing_txs_for_lt("1", 1).await?.remove(0);
    assert_eq!(
        tx,
        OngoingTx {
            address: "1".to_string(),
            count: 0,
            tx_lt: 1,
            mc_seqno: 0,
            last_processed_mc_seqno: 0,
        }
    );

    let txs = mapper.get_ongoing_txs_from_lt("1", 1).await?;
    assert_eq!(
        txs,
        vec![
            OngoingTx {
                address: "1".to_string(),
                count: 0,
                tx_lt: 1,
                mc_seqno: 0,
                last_processed_mc_seqno: 0,
            },
            OngoingTx {
                address: "1".to_string(),
                count: 1,
                tx_lt: 2,
                mc_seqno: 0,
                last_processed_mc_seqno: 0,
            }
        ]
    );

    mapper.delete("1", 1).await?;
    assert!(mapper.get_opt("1", 1).await?.is_none());

    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn test_account_state_update_simple_libs() -> anyhow::Result<()> {
    let (_scylla_env, db) = init_test_db().await?;

    let state = DBAccount {
        mc_seqno: 1,
        tx_hash: make_db_ton_hash(&[1, 2, 3, 4, 5]),
        tx_lt: 7,
        address: "address".to_string(),
        is_valid: false,
        code_hash: Some(TonHash::from([1u8; 32])),
        last_code_hash: None,
        data_lib_ids: vec![TonHash::from([2u8; 32])],
        simple_libs: vec![DBSimpleLib {
            code_hash: TonHash::from([3u8; 32]),
            public: true,
        }],
        boc_stripped: vec![0, 1, 2].into(),
    };
    db.account().insert(&state).await?;
    let new_state = db.account().get_opt(&state.tx_key()).await?;
    assert_eq!(new_state, Some(state));
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn assert_account_state() -> anyhow::Result<()> {
    let (_scylla_env, db) = assert_ok!(init_test_db().await);

    let tx_key = DBTxKey::new(1, "address1".to_string(), 1);
    let state1 = mock_shard_account_from_tx_key(&tx_key);
    assert!(!db.account().any_for_mc_seqno(1).await?);
    assert_eq!(db.account().get_opt(&tx_key).await?, None);
    assert_ok!(db.account().insert(&state1).await);
    assert!(db.account().any_for_mc_seqno(1).await?);
    let resp_get = assert_ok!(db.account().get_opt(&tx_key).await);
    assert_eq!(resp_get, Some(state1.clone()));

    let mut state2 = mock_shard_account_from_tx_key(&tx_key);
    state2.tx_lt += 1;
    assert_ok!(db.account().insert(&state2).await);

    let resp_get_by_master = assert_ok!(db.account().get_by_mc_seqno(1).await);
    assert_eq!(resp_get_by_master.len(), 2);
    let resp_get_by_master = assert_ok!(db.account().get_by_mc_seqno(2).await);
    assert!(resp_get_by_master.is_empty());
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn test_account_state_by_code_hash_works() -> anyhow::Result<()> {
    let (_scylla_env, db) = assert_ok!(init_test_db().await);

    let account_state_first = DBAccount {
        mc_seqno: 1,
        tx_hash: make_db_ton_hash(&[1, 2, 3, 4, 5]),
        tx_lt: 1,
        address: "address1".to_string(),
        is_valid: false,
        boc_stripped: vec![].into(),
        code_hash: Some(TonHash::from_str(
            "n3MmlhpRvPrI0y6O8LeQB7tTkLdEo1p1ar3xQnE7Y/A=",
        )?),
        last_code_hash: None,
        data_lib_ids: vec![],
        simple_libs: vec![],
    };
    let account_state_second = DBAccount {
        mc_seqno: 1,
        tx_lt: 2,
        address: "address2".to_string(),
        code_hash: Some(TonHash::from_str(
            "EMRygKN5MCx1QoK9AiLuitxHGRdKA3qxnUKj+S70nsE=",
        )?),
        ..Default::default()
    };

    assert_ok!(db.account().insert(&account_state_first).await);
    assert_ok!(db.account().insert(&account_state_second).await);

    let code1_hash = &[account_state_first.code_hash.as_ref().unwrap().as_slice()];
    let both_hashes = &mut [
        account_state_first.code_hash.as_ref().unwrap().as_slice(),
        account_state_second.code_hash.as_ref().unwrap().as_slice(),
    ];

    // test with few seqnos
    let lookup_key1 = TxKeyLookup::seqnos_code_hashes(&[1, 2], code1_hash);
    assert_eq!(
        db.tx().lookup_keys(lookup_key1).await?,
        vec![account_state_first.clone().tx_key()]
    );

    let lookup_key2 = TxKeyLookup::seqnos_code_hashes(&[1], both_hashes);
    assert_eq!(
        db.tx().lookup_keys(lookup_key2).await?,
        vec![
            account_state_first.clone().tx_key(),
            account_state_second.clone().tx_key(),
        ]
    );

    // Test if order remains the same for every order of code hashes
    both_hashes.reverse();
    let lookup_key2 = TxKeyLookup::seqnos_code_hashes(&[1], both_hashes);
    assert_eq!(
        db.tx().lookup_keys(lookup_key2).await?,
        vec![
            account_state_first.clone().tx_key(),
            account_state_second.clone().tx_key(),
        ]
    );

    let unknown_hashes = &[[0u8; 32].as_slice()];

    let lookup_key3 = TxKeyLookup::seqnos_code_hashes(&[1], unknown_hashes);
    assert_eq!(db.tx().lookup_keys(lookup_key3).await?, vec![]);
    Ok(())
}

async fn assert_tvm_code_traits(db: &DB) -> anyhow::Result<()> {
    let mapper = db.tvm_code_traits();
    // versioning tests
    let hash_1 = TonHash::from([1; 32]);
    let trait1_v1 = TvmCodeTraits {
        code_hash: hash_1,
        version: 1,
        traits: HashSet::from([ContractTrait::Unknown]),
        mc_seqno: 1,
    };
    let trait1_v2 = TvmCodeTraits {
        code_hash: hash_1,
        version: 2,
        traits: HashSet::from([ContractTrait::Config]),
        mc_seqno: 2,
    };
    let trait1_v3 = TvmCodeTraits {
        code_hash: hash_1,
        version: 3,
        traits: HashSet::from([ContractTrait::System, ContractTrait::Config]),
        mc_seqno: 3,
    };

    mapper.insert_many([trait1_v1.clone()]).await?;
    mapper.insert_many([trait1_v2.clone()]).await?;

    let r1 = mapper.get_by_code_hash(&hash_1).await?;
    assert_eq!(r1, Some(trait1_v2.clone()));

    let r2 = mapper.get_by_code_hash(&hash_1).await?;
    assert_eq!(r2, Some(trait1_v2.clone()));

    mapper.insert_many([trait1_v3.clone()]).await?;

    let r3 = mapper.get_by_code_hash(&hash_1).await?;
    assert_eq!(r3, Some(trait1_v3.clone()));

    mapper.delete_old_versions(hash_1, 2).await?;
    let r4 = mapper.get_by_code_hash(&hash_1).await?;
    assert_eq!(r4, Some(trait1_v3.clone()));

    // test getters
    let code_traits3 = TvmCodeTraits {
        code_hash: TonHash::from([3; 32]),
        version: 1,
        traits: HashSet::from([ContractTrait::JettonMaster, ContractTrait::System]),
        mc_seqno: 1,
    };

    let code_traits4 = TvmCodeTraits {
        code_hash: TonHash::from([4; 32]),
        version: 1,
        traits: HashSet::from([ContractTrait::JettonMaster]),
        mc_seqno: 1,
    };

    mapper
        .insert_many([code_traits3.clone(), code_traits4.clone()])
        .await?;

    let actual_contract1 = mapper
        .get_by_code_hashes([&code_traits3.code_hash])
        .await?
        .get(&code_traits3.code_hash)
        .unwrap()
        .clone();
    let actual_contract2 = mapper
        .get_by_code_hashes([&code_traits4.code_hash])
        .await?
        .get(&code_traits4.code_hash)
        .unwrap()
        .clone();
    assert_eq!(code_traits3, actual_contract1);
    assert_eq!(code_traits4, actual_contract2);

    Ok(())
}

fn mock_shard_account_from_tx_key(tx_key: &DBTxKey) -> DBAccount {
    DBAccount {
        mc_seqno: tx_key.mc_seqno,
        address: tx_key.address.clone(),
        tx_lt: tx_key.tx_lt,
        ..Default::default()
    }
}
