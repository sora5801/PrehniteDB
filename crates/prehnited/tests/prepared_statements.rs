//! v0.55: end-to-end Prepare/Execute over the wire.
//!
//! Boots `prehnited` in-process, opens a TCP connection, and drives the
//! new Prepare / Execute / Deallocate frames against it. Exercises:
//!
//! - one Prepare + multiple Execute reuses the same cached plan
//! - per-connection isolation: client A's handle is invisible to B
//! - row streaming after Execute on a SELECT
//! - DML through Execute writes and is visible to a plain Query
//! - Deallocate frees the cache slot and the handle becomes stale
//!
//! These are the wire-protocol companions to the library-level tests
//! in `engine::database::tests::prepared_*`.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use prehnitedb::protocol::{read_response, write_request, Request, Response};
use prehnitedb::Value;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempServer {
    db_path: PathBuf,
    wal_path: PathBuf,
    clog_path: PathBuf,
    addr: String,
}

impl TempServer {
    fn new() -> TempServer {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let stem = format!("prehnited-prep-{}-{n}.db", std::process::id());
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

fn connect(addr: &str) -> TcpStream {
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

/// Send a Query and collect either an Ack, an Error, or a streamed row set.
fn query(stream: &mut TcpStream, sql: &str) -> Reply {
    write_request(stream, &Request::Query(sql.into())).expect("write_request");
    read_reply(stream)
}

/// Send a Prepare and require a Prepared frame back; panic otherwise.
fn prepare(stream: &mut TcpStream, sql: &str) -> u64 {
    write_request(stream, &Request::Prepare(sql.into())).expect("write_request prepare");
    match read_response(stream).expect("first frame after prepare") {
        Response::Prepared { handle } => handle,
        Response::Error(m) => panic!("prepare failed: {m}"),
        other => panic!("expected Prepared, got {other:?}"),
    }
}

/// Send an Execute and collect the reply (same shape as `query`'s).
fn execute(stream: &mut TcpStream, handle: u64, params: Vec<Value>) -> Reply {
    write_request(stream, &Request::Execute { handle, params }).expect("write_request execute");
    read_reply(stream)
}

/// Send a Deallocate; ack is the only legal reply.
fn deallocate(stream: &mut TcpStream, handle: u64) {
    write_request(stream, &Request::Deallocate { handle }).expect("write_request deallocate");
    match read_response(stream).expect("first frame after deallocate") {
        Response::Ack(_) => {}
        other => panic!("expected Ack after Deallocate, got {other:?}"),
    }
}

fn read_reply(stream: &mut TcpStream) -> Reply {
    match read_response(stream).expect("first reply frame") {
        Response::Ack(m) => Reply::Ack(m),
        Response::Error(m) => Reply::Error(m),
        Response::RowsBegin { columns } => {
            let mut rows = Vec::new();
            loop {
                match read_response(stream).expect("row frame") {
                    Response::Row { values } => rows.push(values),
                    Response::RowsEnd => return Reply::Rows { columns, rows },
                    Response::Error(m) => return Reply::Error(m),
                    other => panic!("unexpected mid-row frame: {other:?}"),
                }
            }
        }
        other => panic!("unexpected first reply frame: {other:?}"),
    }
}

#[derive(Debug)]
enum Reply {
    Ack(String),
    Error(String),
    Rows {
        #[allow(dead_code)]
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

impl Reply {
    fn assert_ack(self) -> String {
        match self {
            Reply::Ack(m) => m,
            other => panic!("expected Ack, got {other:?}"),
        }
    }
    fn assert_rows(self) -> Vec<Vec<Value>> {
        match self {
            Reply::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }
    fn assert_error(self) -> String {
        match self {
            Reply::Error(m) => m,
            other => panic!("expected Error, got {other:?}"),
        }
    }
}

#[test]
fn one_prepare_serves_many_executes_over_the_wire() {
    let server = TempServer::new();
    let mut c = connect(&server.addr);

    query(&mut c, "CREATE TABLE t (id INT, name TEXT)").assert_ack();
    query(
        &mut c,
        "INSERT INTO t VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')",
    )
    .assert_ack();

    let handle = prepare(&mut c, "SELECT name FROM t WHERE id = ?");
    assert!(handle != 0, "Prepared returned handle 0 (sentinel)");

    let r1 = execute(&mut c, handle, vec![Value::Int(1)]).assert_rows();
    let r2 = execute(&mut c, handle, vec![Value::Int(2)]).assert_rows();
    let r3 = execute(&mut c, handle, vec![Value::Int(3)]).assert_rows();
    assert_eq!(r1, vec![vec![Value::Text("ada".into())]]);
    assert_eq!(r2, vec![vec![Value::Text("grace".into())]]);
    assert_eq!(r3, vec![vec![Value::Text("edsger".into())]]);
}

#[test]
fn prepared_handles_are_per_connection() {
    // Postgres-style session-level prepared statements: A's handle
    // must not be visible to B. Each connection gets its own
    // Database (and so its own cache) inside `serve_client`.
    let server = TempServer::new();

    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (n INT)").assert_ack();
    query(&mut setup, "INSERT INTO t VALUES (10), (20), (30)").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    let mut b = connect(&server.addr);

    let handle_a = prepare(&mut a, "SELECT n FROM t WHERE n > ?");
    // B never saw `handle_a`'s Prepare; on B's connection it must error.
    let err = execute(&mut b, handle_a, vec![Value::Int(0)]).assert_error();
    assert!(
        err.contains("prepared statement") && err.contains(&handle_a.to_string()),
        "unexpected error: {err}"
    );

    // A's own handle still works.
    let rows = execute(&mut a, handle_a, vec![Value::Int(15)]).assert_rows();
    let ns: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r[0] {
            Value::Int(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(ns, vec![20, 30]);
}

#[test]
fn prepared_dml_writes_and_is_visible_to_plain_query() {
    let server = TempServer::new();
    let mut c = connect(&server.addr);

    query(&mut c, "CREATE TABLE t (id INT, label TEXT)").assert_ack();
    let ins = prepare(&mut c, "INSERT INTO t VALUES (?, ?)");

    execute(&mut c, ins, vec![Value::Int(1), Value::Text("alpha".into())]).assert_ack();
    execute(&mut c, ins, vec![Value::Int(2), Value::Text("beta".into())]).assert_ack();
    execute(&mut c, ins, vec![Value::Int(3), Value::Text("gamma".into())]).assert_ack();

    let rows = query(&mut c, "SELECT id, label FROM t ORDER BY id").assert_rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("alpha".into()));
    assert_eq!(rows[2][1], Value::Text("gamma".into()));

    // A fresh connection sees the rows too — they were committed.
    let mut reader = connect(&server.addr);
    let rows = query(&mut reader, "SELECT id FROM t ORDER BY id").assert_rows();
    assert_eq!(rows.len(), 3);
}

#[test]
fn deallocate_frees_the_handle_and_the_server_acks_unknowns() {
    let server = TempServer::new();
    let mut c = connect(&server.addr);

    query(&mut c, "CREATE TABLE t (n INT)").assert_ack();
    let handle = prepare(&mut c, "SELECT n FROM t");
    execute(&mut c, handle, vec![]).assert_rows();

    // First deallocate kills the slot; subsequent Execute errors.
    deallocate(&mut c, handle);
    let err = execute(&mut c, handle, vec![]).assert_error();
    assert!(
        err.contains(&handle.to_string()),
        "stale-handle error didn't name the handle: {err}"
    );

    // A second deallocate on the same (now-unknown) handle still acks.
    deallocate(&mut c, handle);
    // Unknown handles too — deallocate has SQL-DEALLOCATE semantics.
    deallocate(&mut c, 0xDEAD_BEEFu64);
}

#[test]
fn schema_change_invalidates_prepared_statements_over_the_wire() {
    // v0.56: DDL on one connection bumps the global schema_version
    // in SharedMeta; a prepared plan on any connection that was
    // tagged at an older version is detected as stale at Execute
    // and returns a clean Error frame instead of a corruption-flavored
    // failure. The handle is dropped from the cache, so a retry
    // gets the cleaner unknown-handle error.
    let server = TempServer::new();

    let mut setup = connect(&server.addr);
    query(&mut setup, "CREATE TABLE t (n INT)").assert_ack();
    query(&mut setup, "INSERT INTO t VALUES (1), (2), (3)").assert_ack();
    drop(setup);

    let mut a = connect(&server.addr);
    let h = prepare(&mut a, "SELECT n FROM t");
    let rows = execute(&mut a, h, vec![]).assert_rows();
    assert_eq!(rows.len(), 3);

    // A different connection does the DDL.
    let mut b = connect(&server.addr);
    query(&mut b, "DROP TABLE t").assert_ack();

    // A's cached plan is now stale. The Execute reply is an Error
    // frame, not a connection drop.
    let err = execute(&mut a, h, vec![]).assert_error();
    assert!(
        err.contains("stale") && err.contains(&h.to_string()),
        "stale error didn't name the handle: {err}"
    );

    // The stale entry was evicted, so the second Execute returns
    // the cleaner unknown-handle error.
    let err2 = execute(&mut a, h, vec![]).assert_error();
    assert!(
        !err2.contains("stale"),
        "second execute should not be 'stale': {err2}"
    );
    assert!(
        err2.contains("no prepared statement"),
        "expected unknown-handle error, got: {err2}"
    );

    // Connection A is still usable.
    query(&mut a, "CREATE TABLE t (n INT)").assert_ack();
    query(&mut a, "INSERT INTO t VALUES (42)").assert_ack();
    let h2 = prepare(&mut a, "SELECT n FROM t");
    let rows = execute(&mut a, h2, vec![]).assert_rows();
    assert_eq!(rows.len(), 1);
}

#[test]
fn execute_arity_mismatch_returns_an_error_frame() {
    let server = TempServer::new();
    let mut c = connect(&server.addr);

    query(&mut c, "CREATE TABLE t (n INT)").assert_ack();
    let handle = prepare(&mut c, "SELECT n FROM t WHERE n = ?");
    let err = execute(&mut c, handle, vec![]).assert_error();
    assert!(
        err.contains("placeholder"),
        "bind error didn't mention placeholder: {err}"
    );
    // The connection survives the error; the next Execute on the same
    // handle with proper params works.
    let rows = execute(&mut c, handle, vec![Value::Int(99)]).assert_rows();
    assert!(rows.is_empty());
}
