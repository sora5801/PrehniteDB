//! `crash_worker_concurrent` — v0.46's concurrent counterpart to
//! [`crash_worker`]. Spawns N writer threads against one shared
//! `Database`, each looping autocommit INSERTs and fsync-logging
//! their ACKed ids. The crash-recovery harness kills this binary
//! mid-flight; after restart, every logged id must be present.
//!
//! What v0.46 stresses that v0.38 didn't:
//! - **v0.42 group commit** under genuine writer contention.
//!   N writers stack up in the clog's `pending` buffer while one
//!   leader's fsync is in flight; the kill can land at every step
//!   of the leader/follower handoff.
//! - **v0.30 per-page B+tree latches** on the same table from
//!   multiple writers — leaf splits, optimistic-vs-pessimistic
//!   transitions, the shared `next_rowid` atomic.
//! - **v0.28 per-table mutexes** in shared mode for INSERT.
//! - **v0.42 durability-before-visibility** on the clog: a record
//!   in `pending` but not yet fsynced must not be visible to a
//!   reader, and must not survive the kill.
//!
//! Each thread owns its own log file (`<base>.log.<thread_id>`),
//! its own `Database` handle (shared `SharedPool` + `TxState`), and
//! a disjoint id range so concurrent inserts never collide on the
//! `PRIMARY KEY`. After the kill, the harness reads every log
//! file and checks the union of fsync'd ids is present in the
//! restarted database.
//!
//! Usage: `crash_worker_concurrent <db_path> <log_base> <n_threads>`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use prehnitedb::{Database, SharedPool};

/// Range size per thread — each thread's ids live in
/// `[thread_id * STRIDE, (thread_id + 1) * STRIDE)` so concurrent
/// inserts can't collide on the PK.
const STRIDE: i64 = 100_000_000;

fn main() {
    let mut args = std::env::args().skip(1);
    let db_path: Arc<str> = Arc::from(
        args.next()
            .expect("usage: crash_worker_concurrent <db> <log_base> <n_threads>")
            .as_str(),
    );
    let log_base = args
        .next()
        .expect("usage: crash_worker_concurrent <db> <log_base> <n_threads>");
    let n_threads: usize = args
        .next()
        .expect("usage: crash_worker_concurrent <db> <log_base> <n_threads>")
        .parse()
        .expect("n_threads must be a positive integer");

    // Bootstrap: one Database to create the table and seed the shared
    // TxState, then dropped. The pool stays alive for every thread.
    let pool = SharedPool::new();
    let tx_state = {
        let mut bootstrap = Database::open_with_pool(&*db_path, pool.clone())
            .expect("bootstrap db open");
        // Idempotent setup — re-running after a crash finds the table.
        // PRIMARY KEY both validates the uniqueness invariant under
        // concurrent inserts AND auto-creates a unique index whose
        // B+tree gets exercised by every thread.
        let _ = bootstrap
            .execute("CREATE TABLE t (id INT PRIMARY KEY, thread INT, n INT)");
        bootstrap.tx_state()
    };

    let mut handles = Vec::with_capacity(n_threads);
    for thread_id in 0..n_threads {
        let db_path = Arc::clone(&db_path);
        let log_path: PathBuf = format!("{log_base}.{thread_id}").into();
        let pool = pool.clone();
        let tx_state = tx_state.clone();
        handles.push(thread::spawn(move || {
            // Per-thread Database handle — shares the pool's buffer
            // cache and the in-flight TX bookkeeping with every
            // other thread, exactly the way prehnited's
            // per-connection Databases do at runtime.
            let mut db = Database::open_shared(&*db_path, pool, tx_state)
                .expect("per-thread db open");

            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .expect("per-thread log open");

            let start = (thread_id as i64) * STRIDE;
            // Resume past the highest id this thread previously wrote
            // — multiple spawn-kill cycles per harness iteration would
            // need this; the current harness uses fresh paths per
            // iteration, but keep it robust.
            let mut next_id = start + next_offset_after_existing(&mut db, thread_id);
            let end = start + STRIDE;

            while next_id < end {
                let id = next_id;
                next_id += 1;
                let n = id;
                let sql = format!(
                    "INSERT INTO t VALUES ({id}, {thread_id}, {n})"
                );
                match db.execute(&sql) {
                    Ok(_) => {
                        // Log + fsync only after the DB acked. Kill
                        // between DB ack and log fsync = row exists
                        // on disk but log doesn't know — harness
                        // tolerates that gap and only checks logged
                        // ids. Killing between log write and log
                        // fsync = same thing one fsync earlier.
                        writeln!(log, "{id}").expect("log write");
                        log.flush().expect("log flush");
                        log.sync_all().expect("log fsync");
                    }
                    Err(e) => {
                        // INSERT may fail under a Serialization or
                        // Conflict abort on concurrent writers. The
                        // ID isn't logged → harness doesn't expect
                        // it. Keep going.
                        eprintln!("thread {thread_id}: insert {id} failed: {e}");
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panic");
    }
}

/// The highest id this thread has previously written, expressed as
/// an offset from the thread's `start`. Returns 0 for a fresh DB.
/// Uses a bounded SELECT so a million-row table doesn't slow startup.
fn next_offset_after_existing(db: &mut Database, thread_id: usize) -> i64 {
    use prehnitedb::{QueryResult, Value};
    let lo = (thread_id as i64) * STRIDE;
    let hi = lo + STRIDE;
    let sql = format!(
        "SELECT id FROM t WHERE id >= {lo} AND id < {hi} ORDER BY id DESC LIMIT 1"
    );
    match db.execute(&sql) {
        Ok(QueryResult::Rows { rows, .. }) => match rows.first() {
            Some(row) => match row.first() {
                Some(Value::Int(n)) => (n - lo) + 1,
                _ => 0,
            },
            None => 0,
        },
        _ => 0,
    }
}
