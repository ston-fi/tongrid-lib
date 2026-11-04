use tongrid_db::entity::out_msg_query_id::out_msg_query_id::OutMsgQueryId;
use tongrid_db::entity::{DBTxKey, TxKeyLookup};
use tongrid_db::test_support::init_test_db;

#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn test_tx_key_lookup_address_query_id() -> anyhow::Result<()> {
    let (_scylla_env, db) = init_test_db().await?;

    let tx_key = DBTxKey::new(0, "my_addr0".to_string(), 3);

    let query_id_unsigned = 10129524535658642876u64;
    let out_msg_query_id = OutMsgQueryId {
        mc_seqno: 0,
        address: "my_addr0".to_string(),
        query_id: query_id_unsigned as i64,
        tx_utime: 2,
        tx_lt: 3,
    };
    db.out_msg_query_id().insert(&out_msg_query_id).await?;

    let key_lookup = TxKeyLookup::address_query_id("my_addr0", query_id_unsigned, 2);
    let txs = db.tx().lookup_keys(key_lookup).await?;
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0], tx_key);

    let key_lookup = TxKeyLookup::address_query_id("my_addr0", query_id_unsigned, 3);
    let txs = db.tx().lookup_keys(key_lookup).await?;
    assert_eq!(txs, vec![]);

    Ok(())
}
