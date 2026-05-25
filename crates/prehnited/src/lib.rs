//! `prehnited` — the PrehniteDB network server, as a library.
//!
//! [`main`](../prehnited/index.html) wraps this with arg parsing; the
//! integration tests open the server in-process via [`serve_on`] so the
//! wire protocol and the lock model can be tested end-to-end.
//!
//! The architecture is straightforward: one [`TcpListener`], one thread
//! per accepted connection, each running [`serve_client`] until the
//! socket closes. Every connection has its own [`Database`] handle —
//! independent pager/catalog/txn state, but sharing the buffer pool, the
//! MVCC [`TxState`], and the commit log with every peer connection.
//!
//! v0.28 drops the global per-statement writer mutex in favour of
//! per-table mutexes (from `TxState`). Two writers on *different*
//! tables run truly in parallel; two on the same table serialise.
//! `CREATE TABLE` / `DROP TABLE` take the catalog mutex instead.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use prehnitedb::protocol::{read_request, write_response, Request, Response};
use prehnitedb::{Database, Execution, SharedPool, TableAccess, TxState, Value, WriteScope};

/// Start serving on the given listener. Blocks until the listener stops
/// returning connections — i.e. until the listener is dropped or the
/// underlying socket errors fatally. Each accepted connection runs on
/// its own thread.
pub fn serve_on(listener: TcpListener, db_path: Arc<str>, pool: SharedPool, tx_state: TxState) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let db_path = Arc::clone(&db_path);
                let pool = pool.clone();
                let tx_state = tx_state.clone();
                thread::spawn(move || serve_client(stream, db_path, pool, tx_state));
            }
            Err(e) => eprintln!("prehnited: rejected a connection: {e}"),
        }
    }
}

/// Bootstrap the engine for the database file at `db_path`. Creates the
/// file (and its sidecar clog) if absent, then returns a shared pool +
/// `TxState` the caller can hand to [`serve_on`].
pub fn bootstrap(db_path: &str) -> Result<(SharedPool, TxState), prehnitedb::Error> {
    let pool = SharedPool::new();
    let bootstrap = Database::open_with_pool(db_path, pool.clone())?;
    let tx_state = bootstrap.tx_state();
    drop(bootstrap);
    Ok((pool, tx_state))
}

/// How often the background reclaimer thread wakes up to GC dead
/// MVCC rows. v0.36 ships a fixed interval; a future version could
/// make it adaptive to write rate or tombstone count.
const RECLAIM_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Spawn a daemon thread that runs incremental in-place reclamation
/// every [`RECLAIM_INTERVAL`]. Returns immediately; the thread keeps
/// running until the process exits. Errors are logged to stderr and
/// don't kill the thread — the next tick tries again.
fn spawn_reclaimer(db_path: Arc<str>, pool: SharedPool, tx_state: TxState) {
    thread::Builder::new()
        .name("prehnited-reclaimer".into())
        .spawn(move || {
            let mut db = match Database::open_shared(&*db_path, pool, tx_state) {
                Ok(db) => db,
                Err(e) => {
                    eprintln!("prehnited: reclaimer failed to open Database: {e}");
                    return;
                }
            };
            loop {
                thread::sleep(RECLAIM_INTERVAL);
                // v0.57: `truncate_clog` runs `reclaim_dead_rows`
                // internally (ordering matters — see its doc), then
                // truncates the commit log. We log both the reclaim
                // count and the new floor for operator visibility.
                match db.truncate_clog() {
                    Ok(0) => {}
                    Ok(floor) => eprintln!(
                        "prehnited: clog truncated below TX {floor}"
                    ),
                    Err(e) => eprintln!("prehnited: clog truncate failed: {e}"),
                }
                // v0.49: auto-analyze. At most one table per tick —
                // see `Database::auto_analyze_pass`. Idle for empty
                // schemas or freshly-analyzed ones; cheap to call.
                match db.auto_analyze_pass() {
                    Ok(None) => {}
                    Ok(Some(name)) => {
                        eprintln!("prehnited: auto-analyzed table '{name}'");
                    }
                    Err(e) => eprintln!("prehnited: auto-analyze failed: {e}"),
                }
            }
        })
        .expect("OS refused to spawn reclaimer thread");
}

/// Bind, bootstrap, and serve. The convenience used by `main`.
pub fn run(db_path: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, tx_state) = bootstrap(db_path)?;
    let listener = TcpListener::bind(addr)?;
    let db_path: Arc<str> = Arc::from(db_path);
    println!(
        "PrehniteDB v{} — serving '{}' on {}",
        env!("CARGO_PKG_VERSION"),
        db_path,
        addr
    );
    println!("ready for connections (Ctrl-C to stop)");
    spawn_reclaimer(Arc::clone(&db_path), pool.clone(), tx_state.clone());
    serve_on(listener, db_path, pool, tx_state);
    Ok(())
}

/// Serve one client until it disconnects or the connection breaks.
pub fn serve_client(
    mut stream: TcpStream,
    db_path: Arc<str>,
    pool: SharedPool,
    tx_state: TxState,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    stream.set_nodelay(true).ok();
    eprintln!("prehnited: {peer} connected");

    // Per-connection Database. Pool + tx_state are shared; everything
    // else (pager, catalog cache, current transaction) is local.
    let mut db = match Database::open_shared(&*db_path, pool, tx_state.clone()) {
        Ok(db) => db,
        Err(e) => {
            let _ = write_response(&mut stream, &Response::Error(e.to_string()));
            eprintln!("prehnited: {peer}: open failed: {e}");
            return;
        }
    };

    loop {
        match read_request(&mut stream) {
            Ok(Some(Request::Query(sql))) => {
                let outcome = if prehnitedb::is_read_only(&sql) {
                    // MVCC read: snapshot at statement start, no locks.
                    respond(&mut stream, &mut db, &sql)
                } else {
                    // Take the right granularity of lock for the write.
                    // Two writers on different tables run truly in
                    // parallel; same-table writes serialise.
                    run_write(&mut stream, &mut db, &tx_state, &sql)
                };
                if let Err(e) = outcome {
                    eprintln!("prehnited: {peer}: send failed: {e}");
                    break;
                }
            }
            Ok(Some(Request::Prepare(sql))) => {
                // v0.55: parse+plan only. No locks; the catalog walk
                // happens through the engine's catalog mutex on its
                // own. The cache lives inside `db`, so each
                // connection's handles are isolated from every other's.
                let outcome = match db.prepare(&sql) {
                    Ok(handle) => write_response(&mut stream, &Response::Prepared { handle }),
                    Err(e) => write_response(&mut stream, &Response::Error(e.to_string())),
                };
                if let Err(e) = outcome {
                    eprintln!("prehnited: {peer}: send failed: {e}");
                    break;
                }
            }
            Ok(Some(Request::Execute { handle, params })) => {
                // v0.55: dispatch based on what was prepared. SELECT/EXPLAIN
                // go lockless; DML takes its per-table lock; DDL takes
                // the catalog lock — exactly as Query does, just with
                // the WriteScope derived from the cached Plan instead
                // of from the SQL text.
                let outcome = match db.prepared_write_scope(handle) {
                    Ok(WriteScope::None) => {
                        respond_prepared(&mut stream, &mut db, handle, &params)
                    }
                    Ok(scope) => {
                        run_write_prepared(&mut stream, &mut db, &tx_state, handle, &params, scope)
                    }
                    Err(e) => write_response(&mut stream, &Response::Error(e.to_string())),
                };
                if let Err(e) = outcome {
                    eprintln!("prehnited: {peer}: send failed: {e}");
                    break;
                }
            }
            Ok(Some(Request::Deallocate { handle })) => {
                // v0.55: pure in-memory cache eviction. Always acks.
                db.deallocate_prepared(handle);
                let outcome =
                    write_response(&mut stream, &Response::Ack("deallocated".to_string()));
                if let Err(e) = outcome {
                    eprintln!("prehnited: {peer}: send failed: {e}");
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = write_response(&mut stream, &Response::Error(e.to_string()));
                eprintln!("prehnited: {peer}: {e}");
                break;
            }
        }
    }

    // Client disconnected mid-transaction — roll it back via the clog
    // so its rows become invisible to every future snapshot. The clog
    // write serialises itself, and the rollback only drops our own
    // pager state — no outer lock needed.
    if db.in_transaction() {
        db.abort_transaction();
    }
    eprintln!("prehnited: {peer} disconnected");
}

/// Acquire the right lock(s) for one write statement and run it.
///
/// The locking discipline:
///
/// - `WriteScope::Table(t)`: per-table mutex on `t`. Two writers on
///   different tables run truly in parallel; their schema updates inside
///   `Catalog::put` serialise on the engine-internal `catalog_lock`
///   that `TxState` hands to every `Catalog` at open.
/// - `WriteScope::Catalog`: the server takes *no* outer lock. The
///   `Catalog::put` / `Catalog::remove` calls inside `CREATE TABLE`,
///   `DROP TABLE`, and `DROP INDEX` are themselves serialised by the
///   same engine-internal `catalog_lock`. Taking it again here would
///   self-deadlock (std `Mutex` is not re-entrant). `VACUUM` is
///   stricter — it requires no other writers in flight; v0.28 keeps
///   the single-writer-VACUUM invariant from earlier versions.
/// - `WriteScope::Unknown`: the SQL didn't parse. Run it anyway and
///   let the engine return the parse error.
/// - `WriteScope::None`: BEGIN / COMMIT / ROLLBACK — engine-side
///   transactional bookkeeping, no runtime lock.
fn run_write(
    stream: &mut TcpStream,
    db: &mut Database,
    tx_state: &TxState,
    sql: &str,
) -> prehnitedb::Result<()> {
    match prehnitedb::write_scope(sql) {
        WriteScope::Table(table, TableAccess::Shared) => {
            // Shared on the table — multiple writers may run together;
            // the B+tree's per-page latches serialise them at finer
            // granularity.
            let lock = tx_state.table_lock(&table);
            let _guard = lock.read().unwrap();
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond(stream, db, sql)
        }
        WriteScope::Table(table, TableAccess::Exclusive) => {
            // Exclusive on the table — historically held for
            // CREATE INDEX (full rebuild); v0.58 moved CREATE INDEX
            // to TableOnline. This arm is kept for future
            // table-level exclusive operations (none today).
            let lock = tx_state.table_lock(&table);
            let _guard = lock.write().unwrap();
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond(stream, db, sql)
        }
        WriteScope::TableOnline(_) => {
            // v0.58: engine handles per-phase locking itself
            // (Database::create_index_online). Server takes no
            // outer lock — both an outer shared and our phase-1
            // exclusive would deadlock.
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond(stream, db, sql)
        }
        WriteScope::Catalog | WriteScope::Unknown => {
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond(stream, db, sql)
        }
        WriteScope::None => {
            // BEGIN / COMMIT / ROLLBACK — no catalog touch, no
            // allocations; just engine-side transactional bookkeeping.
            respond(stream, db, sql)
        }
    }
}

/// Run one statement against `db` and write its reply to `stream` — an `Ack`
/// frame, or a `RowsBegin` / `Row` … / `RowsEnd` sequence streamed a row at a
/// time. A statement or mid-stream fault is written as an `Error` frame; the
/// returned `Err` is reserved for a connection that has actually broken.
fn respond(stream: &mut TcpStream, db: &mut Database, sql: &str) -> prehnitedb::Result<()> {
    let execution = db.execute_streaming(sql);
    stream_execution(stream, db, execution)
}

/// v0.55: prepared-statement equivalent of [`respond`]. Read-only path —
/// no lock, no `reload_for_write`. The handle's plan was validated
/// against the catalog at Prepare time; the catalog can drift under us
/// before Execute (a peer can DROP the table, say), in which case the
/// executor fails the row stream and we write an Error frame to close
/// it.
fn respond_prepared(
    stream: &mut TcpStream,
    db: &mut Database,
    handle: u64,
    params: &[Value],
) -> prehnitedb::Result<()> {
    let execution = db.execute_prepared_streaming(handle, params);
    stream_execution(stream, db, execution)
}

/// v0.55: prepared-statement equivalent of [`run_write`]. Takes the
/// same lock as a SQL-text write of the same shape (per-table shared
/// for DML, per-table exclusive for CREATE INDEX, catalog for DDL),
/// then dispatches to `db.execute_prepared_streaming`. Mirrors the
/// `run_write` branches one-for-one.
fn run_write_prepared(
    stream: &mut TcpStream,
    db: &mut Database,
    tx_state: &TxState,
    handle: u64,
    params: &[Value],
    scope: WriteScope,
) -> prehnitedb::Result<()> {
    match scope {
        WriteScope::Table(table, TableAccess::Shared) => {
            let lock = tx_state.table_lock(&table);
            let _guard = lock.read().unwrap();
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond_prepared(stream, db, handle, params)
        }
        WriteScope::Table(table, TableAccess::Exclusive) => {
            let lock = tx_state.table_lock(&table);
            let _guard = lock.write().unwrap();
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond_prepared(stream, db, handle, params)
        }
        WriteScope::TableOnline(_) => {
            // v0.58: same as the SQL-text path — engine handles its
            // own per-phase locking inside Database::create_index_online.
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond_prepared(stream, db, handle, params)
        }
        WriteScope::Catalog | WriteScope::Unknown => {
            if let Err(e) = db.reload_for_write() {
                return write_response(stream, &Response::Error(e.to_string()));
            }
            respond_prepared(stream, db, handle, params)
        }
        WriteScope::None => {
            // BEGIN/COMMIT/ROLLBACK can't be prepared (Database::prepare
            // refuses them), so the only way to land here for a prepared
            // statement is SELECT/EXPLAIN — already handled by the
            // lockless `respond_prepared` branch in serve_client.
            respond_prepared(stream, db, handle, params)
        }
    }
}

/// Stream a result of an `execute_streaming`-style call to the wire —
/// shared by [`respond`] (SQL text) and [`respond_prepared`]
/// (prepared-statement handle). The two paths only differ in how they
/// obtain the [`Execution`]; the framing afterward is identical.
fn stream_execution(
    stream: &mut TcpStream,
    db: &mut Database,
    execution: prehnitedb::Result<Execution>,
) -> prehnitedb::Result<()> {
    match execution {
        Ok(Execution::Ack(message)) => write_response(stream, &Response::Ack(message)),
        Ok(Execution::Rows(mut rows)) => {
            let begin = Response::RowsBegin {
                columns: rows.columns().to_vec(),
            };
            write_response(stream, &begin)?;
            loop {
                match db.stream_next(&mut rows) {
                    Ok(Some(values)) => write_response(stream, &Response::Row { values })?,
                    Ok(None) => return write_response(stream, &Response::RowsEnd),
                    Err(e) => return write_response(stream, &Response::Error(e.to_string())),
                }
            }
        }
        Err(e) => write_response(stream, &Response::Error(e.to_string())),
    }
}
