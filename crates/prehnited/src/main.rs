//! `prehnited` — the PrehniteDB network server binary.
//!
//! A thin wrapper around the [`prehnited`] library: parse args, then call
//! [`prehnited::run`]. The integration tests use the library directly via
//! [`prehnited::serve_on`] so the wire protocol and the lock model can be
//! exercised in-process.

const DEFAULT_ADDR: &str = "127.0.0.1:7654";
const DEFAULT_DB: &str = "prehnite.db";

const USAGE: &str = "\
usage: prehnited [OPTIONS]

  --db <path>         database file to open or create (default: prehnite.db)
  --addr <host:port>  address to listen on (default: 127.0.0.1:7654)
  -h, --help          print this help and exit";

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

    if let Err(e) = prehnited::run(&db_path, &addr) {
        eprintln!("prehnited: fatal: {e}");
        std::process::exit(1);
    }
}

fn fail(message: &str) -> ! {
    eprintln!("prehnited: {message}");
    eprintln!("{USAGE}");
    std::process::exit(2);
}
