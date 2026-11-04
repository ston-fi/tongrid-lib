CREATE KEYSPACE IF NOT EXISTS [[KEYSPACE_NAME]]
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : [[REPLICATION_FACTOR]]}
AND tablets = {'enabled': false}
AND durable_writes = true;

USE [[KEYSPACE_NAME]];

CREATE TYPE IF NOT EXISTS simple_lib (
    code_hash blob, // tvm_code key
    public boolean,
);

CREATE TYPE IF NOT EXISTS msg (
    src          text,                     // "" (empty string) is used for ext addresses
    dst          text,
    created_lt   bigint,
    boc_stripped blob,                     // doesn't contains state_init::code and state_init::library
    body_lib_ids frozen<list<blob>>,       // (library code hashes extracted from msg.body)
    code_hash    blob,                     // tvm_code key (msg.state_init.code)
    data_lib_ids frozen<list<blob>>,       // tvm_code keys (library code hashes extracted from msg.state_init.data)
    simple_libs  frozen<list<simple_lib>>, // deployed libraries
);

CREATE TYPE IF NOT EXISTS tx_key (
    mc_seqno int,
    address  text,
    tx_lt    bigint,
);


CREATE TABLE IF NOT EXISTS sync_state (
    id    text,
    value text,
    PRIMARY KEY ( id )
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS block (
    mc_seqno        int,
    workchain       int,
    shard           bigint,
    seqno           int,
    root_hash       blob,
    file_hash       blob,
    boc_stripped    blob, // block#11ef55aa without state_update, extra::in_msg_descr, extra::out_msg_descr, extra::account_blocks (txs)
    end_lt          bigint,
    key_block_seqno int,
    utime           bigint,
    rand_seed       blob,
    PRIMARY KEY ( (mc_seqno), workchain, shard, seqno)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

// === TX & TX_VIEWS ===
CREATE TABLE IF NOT EXISTS tx (
    mc_seqno               int,
    address                text,
    tx_lt                  bigint,
    tx_utime               bigint,
    tx_hash                blob,
    initial_tx             tx_key,
    block_workchain        int,
    block_shard            bigint,
    block_seqno            int,
    boc_stripped           blob, // transaction$0111: https://github.com/ton-blockchain/ton/blob/master/crypto/block/block.tlb#L271 without in/out msgs
    in_msg                 frozen<msg>,
    out_msgs               frozen<list<msg>>,
    in_msg_hash            blob,
    in_msg_hash_normalized blob,
    PRIMARY KEY ( (mc_seqno), address, tx_lt)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_hash AS
SELECT tx_hash, mc_seqno, address, tx_lt FROM tx
WHERE tx_hash IS NOT NULL
  AND address IS NOT NULL
  AND tx_lt IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ((tx_hash), address, tx_lt, mc_seqno)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_address_lt AS
SELECT mc_seqno, address, tx_lt FROM tx
WHERE address IS NOT NULL
  AND tx_lt IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ( (address), tx_lt, mc_seqno)
WITH
    compaction = {'class': 'LeveledCompactionStrategy'}
    AND synchronous_updates = true;

// most useful case - "get last state at specific height", usually at head
CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_address_mc_seqno_order_desc AS
SELECT mc_seqno, address, tx_lt FROM tx
WHERE address IS NOT NULL
  AND mc_seqno IS NOT NULL
  AND tx_lt IS NOT NULL
PRIMARY KEY ( (address), mc_seqno, tx_lt)
WITH
    CLUSTERING ORDER BY (mc_seqno DESC, tx_lt desc)
    AND compaction = {'class': 'LeveledCompactionStrategy'}
    AND synchronous_updates = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_address_utime_order_desc AS
SELECT mc_seqno, address, tx_lt, tx_utime FROM tx
WHERE address IS NOT NULL
  AND tx_utime IS NOT NULL
  AND tx_lt IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ( (address), tx_utime, tx_lt, mc_seqno)
WITH
    CLUSTERING ORDER BY (tx_utime DESC, tx_lt desc, mc_seqno desc)
    AND compaction = {'class': 'LeveledCompactionStrategy'}
    AND synchronous_updates = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_in_msg AS
SELECT in_msg_hash, mc_seqno, address, tx_lt FROM tx
WHERE in_msg_hash IS NOT NULL
  AND address IS NOT NULL
  AND tx_lt IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ((in_msg_hash), mc_seqno, address, tx_lt)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS tx_by_in_msg_normalized AS
SELECT in_msg_hash_normalized, mc_seqno, address, tx_lt FROM tx
WHERE in_msg_hash_normalized IS NOT NULL
  AND address IS NOT NULL
  AND tx_lt IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ((in_msg_hash_normalized), mc_seqno, address, tx_lt)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;

CREATE TABLE IF NOT EXISTS ongoing_tx (
    address                     text,
    tx_lt                       bigint,
    mc_seqno                    int,
    count                       int,
    last_processed_mc_seqno     int,
PRIMARY KEY ((address), tx_lt))
WITH compaction = {'class': 'LeveledCompactionStrategy'} AND default_time_to_live = 3600;

// allows to find tx by msg data (src, dst, created_lt)
CREATE TABLE IF NOT EXISTS msg_txs (
    src          text,
    dst          text,
    created_lt   bigint,
    src_mc_seqno int,
    src_tx_lt    bigint,
    dst_mc_seqno int,
    dst_tx_lt    bigint,
    PRIMARY KEY ( (src, dst), created_lt)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

// allows to get txs by address & query_id
CREATE TABLE IF NOT EXISTS tx_by_out_msg_query_id (
    address   text,
    query_id  bigint,
    tx_utime  bigint,
    tx_lt     bigint,
    mc_seqno  int,
    PRIMARY KEY ((address, query_id), tx_utime, tx_lt, mc_seqno)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS ongoing_tx (
    address                     text,
    tx_lt                       bigint,
    mc_seqno                    int,
    count                       int,
    last_processed_mc_seqno     int,
    PRIMARY KEY ((address), tx_lt)
) WITH
        compaction = {'class': 'LeveledCompactionStrategy'}
   AND default_time_to_live = 3600;
// === END TX ===

// StateInit::code, SimpleLib or LibraryEntry
CREATE TABLE IF NOT EXISTS tvm_code (
    code_hash    blob,
    boc          blob,
    lib_ids      frozen<list<blob>>, // tvm_code keys. Libraries found in `boc`
    PRIMARY KEY (code_hash)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};


CREATE TABLE IF NOT EXISTS account (
    mc_seqno          int,
    address           text,
    tx_lt             bigint,
    tx_hash           blob,
    is_valid          boolean, // false if emulation failed
    boc_stripped      blob,    // state_init::code and stats_init::library are stripped, empty if emulation failed
    code_hash         blob,    // tvm_code key
    last_code_hash    blob,    
    data_lib_ids      frozen<list<blob>>,       // tvm_code keys
    simple_libs       frozen<list<simple_lib>>, // deployed libraries
    PRIMARY KEY ( (mc_seqno), address, tx_lt)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE MATERIALIZED VIEW IF NOT EXISTS account_invalid AS
SELECT mc_seqno, address, tx_lt, is_valid FROM account
WHERE is_valid = false
  AND mc_seqno IS NOT NULL
  AND address IS NOT NULL
  AND tx_lt IS NOT NULL
PRIMARY KEY ((mc_seqno), address, tx_lt, is_valid)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;


CREATE MATERIALIZED VIEW IF NOT EXISTS account_by_code_hash AS
SELECT code_hash, mc_seqno, address, tx_lt FROM account
WHERE code_hash IS NOT NULL
    AND mc_seqno IS NOT NULL
    AND address IS NOT NULL
    AND tx_lt IS NOT NULL
    PRIMARY KEY ((code_hash), mc_seqno, address, tx_lt)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;


CREATE MATERIALIZED VIEW IF NOT EXISTS account_by_last_code_hash AS
SELECT last_code_hash, mc_seqno, address, tx_lt FROM account
WHERE last_code_hash IS NOT NULL
  AND mc_seqno IS NOT NULL
  AND address IS NOT NULL
  AND tx_lt IS NOT NULL
PRIMARY KEY ((last_code_hash), mc_seqno, address, tx_lt)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;


CREATE TABLE IF NOT EXISTS tvm_code_traits (
    code_hash blob,
    version   int,
    traits    frozen<set<text>>,
    mc_seqno  int, // height when we wrote this traits
    PRIMARY KEY ((code_hash), version)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

// is used only for incremental backups (select rows from corresponding range)
CREATE MATERIALIZED VIEW IF NOT EXISTS tvm_code_traits_by_mc_seqno AS
SELECT mc_seqno, code_hash, version FROM tvm_code_traits
WHERE mc_seqno IS NOT NULL
  AND code_hash IS NOT NULL
  AND version IS NOT NULL
PRIMARY KEY ((mc_seqno), code_hash, version)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;


// === START JETTON SYNC ===
CREATE TABLE IF NOT EXISTS jetton_wallet (
    wallet_address      text,
    is_legit            boolean, // true if master.get_wallet_address(owner) == wallet_address
    master_address      text,
    owner_address       text,
	mc_seqno            int,
PRIMARY KEY ((wallet_address), is_legit, master_address, owner_address))
WITH compaction = {'class': 'LeveledCompactionStrategy'};

// mc_seqno in PK because it needed for `mc_seqno is not null` to work 
CREATE MATERIALIZED VIEW IF NOT EXISTS jetton_wallet_by_owner AS
SELECT owner_address, master_address, wallet_address, is_legit, mc_seqno
FROM jetton_wallet
WHERE owner_address IS NOT NULL
  AND is_legit = true
  AND wallet_address IS NOT NULL
  AND master_address IS NOT NULL
  AND mc_seqno IS NOT NULL
PRIMARY KEY ((owner_address), master_address, wallet_address, is_legit, mc_seqno)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;

// mc_seqno in PK because it needed for `mc_seqno is not null` to work 
CREATE MATERIALIZED VIEW IF NOT EXISTS jetton_wallet_by_master AS
SELECT master_address, owner_address, wallet_address, is_legit, mc_seqno
FROM jetton_wallet
WHERE master_address IS NOT NULL
  AND is_legit = true
  AND wallet_address IS NOT NULL
  AND owner_address is NOT NULL
  AND mc_seqno is NOT NULL
PRIMARY KEY ((master_address), owner_address, wallet_address, is_legit, mc_seqno)
WITH
    compaction = {'class': 'LeveledCompactionStrategy'}
    AND synchronous_updates = true;

CREATE TABLE IF NOT EXISTS jetton_balance_order_desc (
    wallet_address text,
    tx_lt          bigint,
    is_valid       boolean,
    prev_balance   varint,
    balance        varint,
    tx_utime       bigint,
    mc_seqno       int,
PRIMARY KEY ((wallet_address), tx_lt))
WITH
    CLUSTERING ORDER BY (tx_lt DESC)
    AND compaction = {'class': 'LeveledCompactionStrategy'}
;
// === END JETTON SYNC ===

// Extra view for backups
CREATE MATERIALIZED VIEW IF NOT EXISTS jetton_wallet_by_mc_seqno AS
SELECT mc_seqno, wallet_address, is_legit, master_address, owner_address
FROM jetton_wallet
WHERE mc_seqno IS NOT NULL
  AND wallet_address IS NOT NULL
  AND is_legit IS NOT NULL
  AND master_address IS NOT NULL
  AND owner_address IS NOT NULL
PRIMARY KEY ((mc_seqno), wallet_address, is_legit, master_address, owner_address)
WITH compaction = {'class': 'LeveledCompactionStrategy'} and synchronous_updates = true;

// Extra view for backups
CREATE MATERIALIZED VIEW IF NOT EXISTS jetton_balance_by_mc_seqno AS
SELECT mc_seqno, wallet_address, tx_lt, is_valid, prev_balance, balance, tx_utime
FROM jetton_balance_order_desc
WHERE mc_seqno IS NOT NULL
  AND wallet_address IS NOT NULL
  AND tx_lt IS NOT NULL
PRIMARY KEY ((mc_seqno), wallet_address, tx_lt)
WITH
    compaction = {'class': 'LeveledCompactionStrategy'}
    AND synchronous_updates = true;
// === END JETTON SYNC ===
