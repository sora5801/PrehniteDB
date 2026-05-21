//! `prehnite` — the interactive PrehniteDB command-line client.
//!
//! It connects to a `prehnited` server and runs a read-eval-print loop:
//! accumulate typed input until a `;` ends a statement, send it, and render the
//! reply. A statement may span lines; `;` inside a string literal does not end
//! it.

use std::io::{self, BufRead, Write};
use std::net::TcpStream;

use prehnitedb::protocol::{read_response, write_request, Request, Response};
use prehnitedb::QueryResult;

const DEFAULT_ADDR: &str = "127.0.0.1:7654";

const USAGE: &str = "\
usage: prehnite [OPTIONS]

  --addr <host:port>  PrehniteDB server to connect to (default: 127.0.0.1:7654)
  -h, --help          print this help and exit";

const HELP: &str = "\
  <sql> ;     run a statement — terminate it with a semicolon
  \\help       show this help
  \\q          quit (Ctrl-D also works)";

fn main() {
    let mut addr = DEFAULT_ADDR.to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            "--addr" => match args.next() {
                Some(value) => addr = value,
                None => exit_usage("--addr requires a host:port"),
            },
            other => exit_usage(&format!("unknown argument '{other}'")),
        }
    }

    if let Err(e) = run(&addr) {
        eprintln!("prehnite: {e}");
        std::process::exit(1);
    }
}

fn exit_usage(message: &str) -> ! {
    eprintln!("prehnite: {message}");
    eprintln!("{USAGE}");
    std::process::exit(2);
}

fn run(addr: &str) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr)
        .map_err(|e| io::Error::new(e.kind(), format!("cannot connect to {addr}: {e}")))?;
    stream.set_nodelay(true).ok();
    println!("connected to PrehniteDB at {addr}");
    println!("type \\help for help, \\q to quit");

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut buffer = String::new();

    loop {
        prompt(buffer.is_empty());
        let Some(line) = lines.next() else {
            break; // end of input
        };
        let line = line?;

        // Meta-commands are only recognized at the start of a statement.
        if buffer.is_empty() {
            match line.trim() {
                "\\q" | "quit" | "exit" => break,
                "\\help" => {
                    println!("{HELP}");
                    continue;
                }
                "" => continue,
                _ => {}
            }
        }

        buffer.push_str(&line);
        buffer.push('\n');

        while let Some(end) = statement_end(&buffer) {
            let statement: String = buffer.drain(..end).collect();
            let statement = statement.trim();
            if statement != ";" && !statement.is_empty() {
                run_statement(&mut stream, statement)?;
            }
        }
        if buffer.trim().is_empty() {
            buffer.clear();
        }
    }
    Ok(())
}

fn prompt(fresh: bool) {
    print!("{}", if fresh { "prehnite> " } else { "      ..> " });
    let _ = io::stdout().flush();
}

/// Send one statement and render the server's reply. Only a broken connection
/// returns `Err`; a rejected statement is reported and execution continues.
fn run_statement(stream: &mut TcpStream, sql: &str) -> io::Result<()> {
    let request = Request::Query(sql.to_string());
    write_request(stream, &request)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("send failed: {e}")))?;
    let response =
        read_response(stream).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    match response {
        Response::Ack(message) => println!("{message}"),
        Response::Error(message) => eprintln!("error: {message}"),
        Response::Rows { columns, rows } => {
            println!("{}", QueryResult::Rows { columns, rows });
        }
    }
    Ok(())
}

/// Byte index just past the first top-level `;` in `buf`, or `None` if the
/// buffer holds no complete statement yet. A `;` inside a `'...'` string
/// literal (with `''` as an escaped quote) does not count.
fn statement_end(buf: &str) -> Option<usize> {
    let bytes = buf.as_bytes();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if in_string && bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_string = !in_string;
            }
            b';' if !in_string => return Some(i + 1),
            _ => {}
        }
        i += 1;
    }
    None
}
