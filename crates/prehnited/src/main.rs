//! `prehnited` — the PrehniteDB network server daemon.
//!
//! It opens one database file, listens on a TCP socket, and serves each client
//! on its own thread. A reader-writer lock guards the shared database: a write
//! takes it exclusively, while a read-only statement takes it shared and runs
//! on its own pager — so readers run concurrently, and never alongside a
//! writer. Every pager, the writer's and the readers', shares one buffer pool,
//! so a reader runs against a warm cache instead of filling a private one.
//!
//! A result set is *streamed*: the server pulls one row from the query
//! pipeline and writes it to the socket before pulling the next, so a `SELECT`
//! of any size costs the server only one row of memory at a time.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::thread;

use prehnitedb::protocol::{read_request, write_response, Request, Response};
use prehnitedb::{Database, Execution, SharedPool};

const DEFAULT_ADDR: &str = "127.0.0.1:7654";
const DEFAULT_DB: &str = "prehnite.db";

const USAGE: &str = "\
usage: prehnited [OPTIONS]

  --db <path>         database file to open or create (default: prehnite.db)
  --addr <host:port>  address to listen on (default: 127.0.0.1:7654)
  -h, --help          print this help and exit";

struct Config {
    db_path: String,
    addr: String,
}

fn main() {
    let mut db_path = DEFAULT_DB.to_string();
    let mut addr = DEFAULT_ADDR.to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            "--db" => match args.next() {
                Some(value) => db_path = value,
                None => fail("--db requires a path"),
            },
            "--addr" => match args.next() {
                Some(value) => addr = value,
                None => fail("--addr requires a host:port"),
            },
            other => fail(&format!("unknown argument '{other}'")),
        }
    }

    if let Err(e) = run(Config { db_path, addr }) {
        eprintln!("prehnited: fatal: {e}");
        std::process::exit(1);
    }
}

fn fail(message: &str) -> ! {
    eprintln!("prehnited: {message}");
    eprintln!("{USAGE}");
    std::process::exit(2);
}

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    // One buffer pool, shared by the writer and every reader.
    let pool = SharedPool::new();
    let database = Arc::new(RwLock::new(Database::open_with_pool(
        &config.db_path,
        pool.clone(),
    )?));
    // Readers open their own pager on this path; the lock keeps the file
    // stable while they do, and `pool` keeps their cache warm.
    let db_path: Arc<str> = Arc::from(config.db_path.as_str());
    let listener = TcpListener::bind(&config.addr)?;

    println!(
        "PrehniteDB v{} — serving '{}' on {}",
        env!("CARGO_PKG_VERSION"),
        config.db_path,
        config.addr
    );
    println!("ready for connections (Ctrl-C to stop)");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let database = Arc::clone(&database);
                let db_path = Arc::clone(&db_path);
                let pool = pool.clone();
                thread::spawn(move || serve_client(stream, database, db_path, pool));
            }
            Err(e) => eprintln!("prehnited: rejected a connection: {e}"),
        }
    }
    Ok(())
}

/// Serve one client until it disconnects or the connection breaks.
fn serve_client(
    mut stream: TcpStream,
    database: Arc<RwLock<Database>>,
    db_path: Arc<str>,
    pool: SharedPool,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    stream.set_nodelay(true).ok();
    eprintln!("prehnited: {peer} connected");

    // Set only while this connection has a transaction open: the write lock,
    // held across requests so the transaction excludes every other client.
    let mut held: Option<RwLockWriteGuard<Database>> = None;

    loop {
        match read_request(&mut stream) {
            Ok(Some(Request::Query(sql))) => {
                let outcome = if let Some(db) = held.as_mut() {
                    // Inside a transaction: reuse the write lock we hold.
                    respond(&mut stream, db, &sql)
                } else if prehnitedb::is_read_only(&sql) {
                    // A read takes the lock shared and runs on its own pager,
                    // so it never blocks — nor is blocked by — another reader.
                    // The lock is held for the whole streamed reply, keeping
                    // the file stable while the reader pulls row after row.
                    let _gate = database.read().unwrap();
                    match Database::open_with_pool(&*db_path, pool.clone()) {
                        Ok(mut reader) => respond(&mut stream, &mut reader, &sql),
                        Err(e) => write_response(&mut stream, &Response::Error(e.to_string())),
                    }
                } else {
                    // A write takes the lock exclusively. If it opened a
                    // transaction, keep the guard for the requests to come.
                    let mut db = database.write().unwrap();
                    let outcome = respond(&mut stream, &mut db, &sql);
                    if db.in_transaction() {
                        held = Some(db);
                    }
                    outcome
                };
                // A COMMIT or ROLLBACK closes the transaction: drop the guard.
                if held.as_ref().is_some_and(|db| !db.in_transaction()) {
                    held = None;
                }
                if let Err(e) = outcome {
                    eprintln!("prehnited: {peer}: send failed: {e}");
                    break;
                }
            }
            Ok(None) => break, // client closed the connection cleanly
            Err(e) => {
                // Tell the client what went wrong, then drop the connection.
                let _ = write_response(&mut stream, &Response::Error(e.to_string()));
                eprintln!("prehnited: {peer}: {e}");
                break;
            }
        }
    }

    // A client that drops mid-transaction leaves staged writes behind; roll
    // them back so the next writer starts from a clean slate.
    if let Some(mut db) = held {
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
