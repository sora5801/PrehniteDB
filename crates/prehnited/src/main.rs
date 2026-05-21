//! `prehnited` — the PrehniteDB network server daemon.
//!
//! It opens one database file, listens on a TCP socket, and serves each client
//! on its own thread. The database is shared behind a single `Mutex`, so a
//! statement runs to completion — and commits or rolls back — without
//! interleaving with another connection's statement.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use prehnitedb::protocol::{read_request, write_response, Request, Response};
use prehnitedb::Database;

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
    let database = Arc::new(Mutex::new(Database::open(&config.db_path)?));
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
                thread::spawn(move || serve_client(stream, database));
            }
            Err(e) => eprintln!("prehnited: rejected a connection: {e}"),
        }
    }
    Ok(())
}

/// Serve one client until it disconnects or the connection breaks.
fn serve_client(mut stream: TcpStream, database: Arc<Mutex<Database>>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    stream.set_nodelay(true).ok();
    eprintln!("prehnited: {peer} connected");

    loop {
        match read_request(&mut stream) {
            Ok(Some(Request::Query(sql))) => {
                let response = execute(&database, &sql);
                if let Err(e) = write_response(&mut stream, &response) {
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
    eprintln!("prehnited: {peer} disconnected");
}

/// Run one statement under the database lock and turn the outcome — success or
/// failure — into a [`Response`].
fn execute(database: &Mutex<Database>, sql: &str) -> Response {
    let mut db = database.lock().unwrap();
    match db.execute(sql) {
        Ok(result) => Response::from(result),
        Err(e) => Response::Error(e.to_string()),
    }
}
