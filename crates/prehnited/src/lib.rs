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
//! Writes serialise through a shared per-statement `write_lock`.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use prehnitedb::protocol::{read_request, write_response, Request, Response};
use prehnitedb::{Database, Execution, SharedPool, TxState};

/// Start serving on the given listener. Blocks until the listener stops
/// returning connections — i.e. until the listener is dropped or the
/// underlying socket errors fatally. Each accepted connection runs on
/// its own thread.
pub fn serve_on(
    listener: TcpListener,
    db_path: Arc<str>,
    pool: SharedPool,
    tx_state: TxState,
    write_lock: Arc<Mutex<()>>,
) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let db_path = Arc::clone(&db_path);
                let pool = pool.clone();
                let tx_state = tx_state.clone();
                let write_lock = Arc::clone(&write_lock);
                thread::spawn(move || serve_client(stream, db_path, pool, tx_state, write_lock));
            }
            Err(e) => eprintln!("prehnited: rejected a connection: {e}"),
        }
    }
}

/// Bootstrap the engine for the database file at `db_path`. Creates the
/// file (and its sidecar clog) if absent, then returns a shared pool +
/// `TxState` + writer lock the caller can hand to [`serve_on`].
pub fn bootstrap(
    db_path: &str,
) -> Result<(SharedPool, TxState, Arc<Mutex<()>>), prehnitedb::Error> {
    let pool = SharedPool::new();
    let bootstrap = Database::open_with_pool(db_path, pool.clone())?;
    let tx_state = bootstrap.tx_state();
    drop(bootstrap);
    Ok((pool, tx_state, Arc::new(Mutex::new(()))))
}

/// Bind, bootstrap, and serve. The convenience used by `main`.
pub fn run(db_path: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, tx_state, write_lock) = bootstrap(db_path)?;
    let listener = TcpListener::bind(addr)?;
    let db_path: Arc<str> = Arc::from(db_path);
    println!(
        "PrehniteDB v{} — serving '{}' on {}",
        env!("CARGO_PKG_VERSION"),
        db_path,
        addr
    );
    println!("ready for connections (Ctrl-C to stop)");
    serve_on(listener, db_path, pool, tx_state, write_lock);
    Ok(())
}

/// Serve one client until it disconnects or the connection breaks.
pub fn serve_client(
    mut stream: TcpStream,
    db_path: Arc<str>,
    pool: SharedPool,
    tx_state: TxState,
    write_lock: Arc<Mutex<()>>,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    stream.set_nodelay(true).ok();
    eprintln!("prehnited: {peer} connected");

    // Per-connection Database. Pool + tx_state are shared; everything
    // else (pager metadata, catalog, current transaction) is local.
    let mut db = match Database::open_shared(&*db_path, pool, tx_state) {
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
                    // Write — serialise on the per-statement lock.
                    // Released between statements of a `BEGIN..COMMIT`,
                    // so a peer writer can run between ours.
                    let _guard = write_lock.lock().unwrap();
                    // Pick up any header changes a peer committed while
                    // we were idle.
                    if let Err(e) = db.reload_for_write() {
                        let _ = write_response(&mut stream, &Response::Error(e.to_string()));
                        eprintln!("prehnited: {peer}: reload failed: {e}");
                        break;
                    }
                    respond(&mut stream, &mut db, &sql)
                };
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
    // so its rows become invisible to every future snapshot.
    if db.in_transaction() {
        let _guard = write_lock.lock().unwrap();
        db.abort_transaction();
    }
    eprintln!("prehnited: {peer} disconnected");
}

/// Run one statement against `db` and write its reply to `stream` — an `Ack`
/// frame, or a `RowsBegin` / `Row` … / `RowsEnd` sequence streamed a row at a
/// time. A statement or mid-stream fault is written as an `Error` frame; the
/// returned `Err` is reserved for a connection that has actually broken.
fn respond(stream: &mut TcpStream, db: &mut Database, sql: &str) -> prehnitedb::Result<()> {
    match db.execute_streaming(sql) {
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
