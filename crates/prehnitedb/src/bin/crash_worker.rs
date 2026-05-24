//! `crash_worker` — a tiny standalone process the crash-recovery
//! integration test spawns, then kills.
//!
//! Opens the database at `argv[1]`, creates table `t (id INT, n INT)`
//! if absent, then loops forever inserting monotonically-increasing
//! ids. After each successful insert it appends the id (as a decimal
//! line) to the log file at `argv[2]` and `fsync`s. Run until
//! `SIGKILL`/`TerminateProcess` arrives.
//!
//! Durability contract under test: every id written to the log
//! before the kill should be present in the table when a new
//! `Database::open` runs against the same path. Anything inserted
//! but not yet logged (the kill landed between the DB ack and the
//! log fsync) may or may not be present — the test doesn't care.
//!
//! The worker is intentionally **read-no-stdin / write-no-stdout**:
//! the test only needs the log file and the database file.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use prehnitedb::Database;

fn main() {
    let mut args = std::env::args().skip(1);
    let db_path = args.next().expect("usage: crash_worker <db> <log>");
    let log_path: PathBuf = args.next().expect("usage: crash_worker <db> <log>").into();

    let mut db = Database::open(&db_path).expect("crash_worker: open db");
    // Idempotent setup — re-running after a crash finds the table there.
    let _ = db.execute("CREATE TABLE t (id INT, n INT)");

    // Append-only log of ACKed inserts; reopened in append mode so a
    // previous run's entries are preserved (the test never reuses a
    // log across iterations, but this is the safer default).
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .expect("crash_worker: open log");

    // Resume id past whatever's already in the table, so a crash-
    // restart-crash cycle keeps making forward progress.
    let mut next_id = next_id_after_existing(&mut db);

    loop {
        let id = next_id;
        next_id += 1;
        let n = id * 100;
        let sql = format!("INSERT INTO t VALUES ({id}, {n})");
        match db.execute(&sql) {
            Ok(_) => {
                // Log + fsync only after the DB acked. If the kill
                // lands between these two lines, the row is on disk
                // but the log doesn't say so — the test tolerates
                // that (it only checks logged ids).
                writeln!(log, "{id}").expect("crash_worker: log write");
                log.flush().expect("crash_worker: log flush");
                log.sync_all().expect("crash_worker: log fsync");
            }
            Err(e) => {
                // Couldn't commit — back off briefly and try the
                // next id. (Doesn't happen in practice for the
                // workload, but defensively keeps the loop alive.)
                eprintln!("crash_worker: insert failed: {e}");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

/// The highest existing id in `t`, plus one — so each spawn picks
/// up where the previous left off and keeps making fresh inserts.
fn next_id_after_existing(db: &mut Database) -> i64 {
    use prehnitedb::{QueryResult, Value};
    match db.execute("SELECT id FROM t ORDER BY id DESC LIMIT 1") {
        Ok(QueryResult::Rows { rows, .. }) => match rows.first() {
            Some(row) => match row.first() {
                Some(Value::Int(n)) => n + 1,
                _ => 1,
            },
            None => 1,
        },
        _ => 1,
    }
}
