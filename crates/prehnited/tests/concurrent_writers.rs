//! Wire-level concurrent transaction test for `prehnited`.
//!
//! Boots the server in-process on a random localhost port, opens two TCP
//! connections, runs interleaved `BEGIN..COMMIT` transactions, and
//! verifies a third connection sees both writes. Exercises the v0.27
//! per-statement writer lock end-to-end through the wire protocol.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use prehnitedb::protocol::{read_response, write_request, Request, Response};
use prehnitedb::Value;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Hold the temp db path and clean its files on drop.
struct TempServer {
    db_path: PathBuf,
    wal_path: PathBuf,
    clog_path: PathBuf,
    addr: String,
}

impl TempServer {
    /// Bind on a random local port, bootstrap the engine, spawn the
    /// accept loop on a background thread, and return the bound address.
    /// The listener thread is detached; it ends when the listener drops
    /// (which happens when this struct drops, since we hold the only
    /// reference inside the closure).
    fn new() -> TempServer {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let stem = format!("prehnited-concur-{}-{n}.db", std::process::id());
        let db_path = std::env::temp_dir().join(&stem);
        let wal_path = std::env::temp_dir().join(format!("{stem}-wal"));
        let clog_path = std::env::temp_dir().join(format!("{stem}-clog"));
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_file(&clog_path);

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
        let addr = listener.local_addr().expect("local_addr").to_string();
        let (pool, tx_state) =
            prehnited::bootstrap(db_path.to_str().unwrap()).expect("bootstrap");
        let db_path_arc: Arc<str> = Arc::from(db_path.to_str().unwrap());
        thread::spawn(move || {
            prehnited::serve_on(listener, db_path_arc, pool, tx_state);
        });

        TempServer {
            db_path,
            wal_path,
            clog_path,
            addr,
        }
    }
}

impl Drop for TempServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db_path);
        let _ = std::fs::remove_file(&self.wal_path);
        let _ = std::fs::remove_file(&self.clog_path);
    }
}

/// Open a new TCP connection to the server.
fn connect(addr: &str) -> TcpStream {
    // The listener thread may not have called `accept` on the first
    // attempt yet; retry briefly.
    let mut last_err = None;
    for _ in 0..50 {
        match TcpStream::connect(addr) {
            Ok(s) => {
                s.set_nodelay(true).ok();
                return s;
            }
            Err(e) => {
                last_err = Some(e);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    panic!("failed to connect to {addr}: {:?}", last_err.unwrap());
}

/// Send one SQL query and collect the full response sequence.
fn query(stream: &mut TcpStream, sql: &str) -> QueryReply {
    write_request(stream, &Request::Query(sql.into())).expect("write_request");
    match read_response(stream).expect("first frame") {
        Response::Ack(message) => QueryReply::Ack(message),
        Response::Error(message) => QueryReply::Error(message),
        Response::RowsBegin { columns } => {
            let mut rows = Vec::new();
            loop {
                match read_response(stream).expect("row frame") {
                    Response::Row { values } => rows.push(values),
                    Response::RowsEnd => return QueryReply::Rows { columns, rows },
                    Response::Error(message) => return QueryReply::Error(message),
                    other => panic!("unexpected mid-row frame: {other:?}"),
                }
            }
        }
        other => panic!("unexpected first frame: {other:?}"),
    }
}

#[derive(Debug)]
enum QueryReply {
    Ack(String),
    Error(String),
    Rows {
        #[allow(dead_code)]
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

impl QueryReply {
    fn assert_ack(self) -> String {
        match self {
            QueryReply::Ack(m) => m,
            other => panic!("expected Ack, got {other:?}"),
        }
    }

    fn assert_rows(self) -> Vec<Vec<Value>> {
        match self {
            QueryReply::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }
}

#[test]
fn two_clients_can_have_transactions_open_simultaneously_over_tcp() {
    // v0.27: per-statement writer lock. Two TCP clients each open an
    // explicit transaction, interleave inserts at the wire, then commit.
    // A third client must see both writes after the commits.
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (id INT, label TEXT)").assert_ack();
    query(&mut setup, "INSERT INTO t VALUES (1, 'seed')").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    let mut b = connect(&server.addr);

    query(&mut a, "BEGIN").assert_ack();
    query(&mut a, "INSERT INTO t VALUES (2, 'from-a')").assert_ack();

    // Crucial — without per-statement locking, B's BEGIN would block on
    // A's writer mutex held across A's open transaction. v0.27's
    // per-statement lock lets B proceed.
    query(&mut b, "BEGIN").assert_ack();
    query(&mut b, "INSERT INTO t VALUES (3, 'from-b')").assert_ack();

    // Each writer sees its own insert via `own_tx`, but not the other's
    // — the other writer's TX is in flight per the clog.
    let a_view = query(&mut a, "SELECT id FROM t ORDER BY id").assert_rows();
    let b_view = query(&mut b, "SELECT id FROM t ORDER BY id").assert_rows();
    assert_eq!(int_ids(&a_view), vec![1, 2]);
    assert_eq!(int_ids(&b_view), vec![1, 3]);

    query(&mut a, "COMMIT").assert_ack();
    query(&mut b, "COMMIT").assert_ack();

    // A fresh connection sees both rows committed.
    let mut reader = connect(&server.addr);
    let all = query(&mut reader, "SELECT id FROM t ORDER BY id").assert_rows();
    assert_eq!(int_ids(&all), vec![1, 2, 3]);
}

#[test]
fn wire_level_write_write_conflict_aborts_the_loser() {
    // Two TCP clients race for the same row. v0.26's FUW detection
    // aborts the second writer cleanly; the conflict surfaces as an
    // `Error` frame at the wire.
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (id INT, n INT)").assert_ack();
    query(&mut setup, "INSERT INTO t VALUES (1, 10), (2, 20)").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    let mut b = connect(&server.addr);

    query(&mut a, "BEGIN").assert_ack();
    query(&mut a, "UPDATE t SET n = 99 WHERE id = 1").assert_ack();

    query(&mut b, "BEGIN").assert_ack();
    let conflict = query(&mut b, "UPDATE t SET n = 88 WHERE id = 1");
    match conflict {
        QueryReply::Error(m) => assert!(
            m.contains("conflict"),
            "expected conflict error, got: {m}"
        ),
        other => panic!("expected conflict Error, got {other:?}"),
    }
    query(&mut b, "ROLLBACK").assert_ack();
    query(&mut a, "COMMIT").assert_ack();

    let mut reader = connect(&server.addr);
    let final_rows = query(&mut reader, "SELECT id, n FROM t ORDER BY id").assert_rows();
    let pairs: Vec<(i64, i64)> = final_rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Int(id), Value::Int(n)) => (*id, *n),
            other => panic!("non-int row: {other:?}"),
        })
        .collect();
    assert_eq!(pairs, vec![(1, 99), (2, 20)]);
}

#[test]
fn parallel_inserts_from_many_clients_dont_corrupt_pages() {
    // Hammer the meta-coherence path: spawn N client threads that each
    // BEGIN / INSERT-many / COMMIT against the same table. Each insert
    // batch is big enough to allocate fresh pages, so without
    // `reload_for_write` (or some equivalent meta refresh) one client
    // would double-allocate a page another client already wrote into.
    // After the dust settles, the row count and table contents must
    // exactly match the sum of every client's inserts.
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (writer INT, n INT)").assert_ack();
    drop(setup);

    const WRITERS: usize = 4;
    const ROWS_PER_WRITER: i64 = 200;

    let addr = server.addr.clone();
    let mut handles = Vec::new();
    for writer in 0..WRITERS as i64 {
        let addr = addr.clone();
        handles.push(thread::spawn(move || {
            let mut conn = connect(&addr);
            query(&mut conn, "BEGIN").assert_ack();
            for n in 0..ROWS_PER_WRITER {
                let sql = format!("INSERT INTO t VALUES ({writer}, {n})");
                query(&mut conn, &sql).assert_ack();
            }
            query(&mut conn, "COMMIT").assert_ack();
        }));
    }
    for h in handles {
        h.join().expect("writer thread");
    }

    let mut reader = connect(&server.addr);
    let total = query(&mut reader, "SELECT writer, n FROM t").assert_rows();
    assert_eq!(
        total.len(),
        WRITERS * ROWS_PER_WRITER as usize,
        "row count mismatch — page allocation likely corrupted"
    );

    // Every (writer, n) pair must appear exactly once.
    let mut seen = std::collections::HashSet::new();
    for row in &total {
        match (&row[0], &row[1]) {
            (Value::Int(w), Value::Int(n)) => {
                assert!(seen.insert((*w, *n)), "duplicate row ({w}, {n})");
            }
            other => panic!("unexpected types: {other:?}"),
        }
    }
}

#[test]
fn rolled_back_transaction_over_tcp_leaves_no_visible_rows() {
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (n INT)").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    query(&mut a, "BEGIN").assert_ack();
    query(&mut a, "INSERT INTO t VALUES (1), (2), (3)").assert_ack();
    query(&mut a, "ROLLBACK").assert_ack();

    let mut reader = connect(&server.addr);
    let rows = query(&mut reader, "SELECT n FROM t").assert_rows();
    assert!(rows.is_empty(), "rolled-back rows should be invisible; got {rows:?}");
}

#[test]
fn writes_to_different_tables_run_in_parallel() {
    // v0.28: the per-statement writer mutex is gone. Two writers
    // touching DIFFERENT tables each take their own per-table mutex,
    // never contending — they execute truly in parallel (modulo the
    // brief commit-window serialisation on shared meta).
    //
    // This is a timing-shaped correctness assertion: spawn N threads
    // each inserting K rows into its OWN table, then verify every row
    // is present. v0.27's global writer-lock model would still finish,
    // just serialised; v0.28 fans them out across CPU cores. The
    // important property we check is that no inserts go missing under
    // parallel allocation (each thread allocates its own pages from
    // shared meta).
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    const WRITERS: usize = 4;
    const ROWS_PER_WRITER: i64 = 100;
    for w in 0..WRITERS {
        let sql = format!("CREATE TABLE t{w} (id INT, payload TEXT)");
        query(&mut setup, &sql).assert_ack();
    }
    drop(setup);

    let addr = server.addr.clone();
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let addr = addr.clone();
        handles.push(thread::spawn(move || {
            let mut conn = connect(&addr);
            query(&mut conn, "BEGIN").assert_ack();
            for n in 0..ROWS_PER_WRITER {
                let sql =
                    format!("INSERT INTO t{w} VALUES ({n}, 'row-{w}-{n}-padding-padding-padding')");
                query(&mut conn, &sql).assert_ack();
            }
            query(&mut conn, "COMMIT").assert_ack();
        }));
    }
    for h in handles {
        h.join().expect("writer thread");
    }

    let mut reader = connect(&server.addr);
    for w in 0..WRITERS {
        let sql = format!("SELECT id FROM t{w}");
        let rows = query(&mut reader, &sql).assert_rows();
        assert_eq!(
            rows.len(),
            ROWS_PER_WRITER as usize,
            "table t{w} should have {ROWS_PER_WRITER} rows"
        );
        let ids: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| match r[0] {
                Value::Int(n) => n,
                ref other => panic!("non-int id in t{w}: {other:?}"),
            })
            .collect();
        assert_eq!(ids.len(), ROWS_PER_WRITER as usize, "duplicate ids in t{w}");
    }
}

#[test]
fn one_writers_open_transaction_does_not_block_another_tables_writer() {
    // The defining v0.28 property: a writer with an open transaction
    // on table A does not hold any lock that touches table B. A second
    // writer's INSERT into B must succeed *while A's transaction is
    // still open*.
    let server = TempServer::new();
    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE a (n INT)").assert_ack();
    query(&mut setup, "CREATE TABLE b (n INT)").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    query(&mut a, "BEGIN").assert_ack();
    query(&mut a, "INSERT INTO a VALUES (1), (2), (3)").assert_ack();
    // Hold A's transaction open (do NOT commit).

    let mut b = connect(&server.addr);
    // B operates entirely on its own table — must succeed without
    // waiting on A. With v0.27's global writer_lock held across A's
    // BEGIN..COMMIT this would deadlock; with v0.28's per-table locks
    // it returns immediately.
    query(&mut b, "INSERT INTO b VALUES (10), (20)").assert_ack();

    // Now A commits.
    query(&mut a, "COMMIT").assert_ack();

    let mut reader = connect(&server.addr);
    let a_rows = query(&mut reader, "SELECT n FROM a ORDER BY n").assert_rows();
    let b_rows = query(&mut reader, "SELECT n FROM b ORDER BY n").assert_rows();
    assert_eq!(int_ids(&a_rows), vec![1, 2, 3]);
    assert_eq!(int_ids(&b_rows), vec![10, 20]);
}

fn int_ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            ref other => panic!("non-int id: {other:?}"),
        })
        .collect()
}
