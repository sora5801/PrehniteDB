//! Crash-recovery property test.
//!
//! Spawns the [`crash_worker`] binary (see
//! `crates/prehnitedb/src/bin/crash_worker.rs`) as a child process,
//! lets it churn through autocommit inserts for a randomised
//! duration, then kills it with `SIGKILL`/`TerminateProcess`. After
//! the kill we open a fresh `Database` against the same file and
//! assert the property:
//!
//! > every id the worker wrote to the log (and `fsync`ed) before
//! > the kill is present in the table.
//!
//! Anything inserted but not yet logged is unconstrained — the kill
//! may have landed between the DB ack and the log fsync. That gap
//! is intentional: durability claims about ACKed-and-fsynced data
//! are what we test; everything else can go either way.
//!
//! Runs several iterations with different kill timings so the kill
//! lands at different points in the engine's commit pipeline
//! (mid-WAL-write, mid-fsync, mid-clog-write, between statements).

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prehnitedb::{Database, QueryResult, Value};

/// Number of spawn-kill-verify iterations per test run. Each
/// iteration is ~150–500 ms of worker plus a quick verify, so
/// 8 cycles is around 2–4 seconds total.
const ITERATIONS: usize = 8;

/// Tiny LCG so we don't drag in `rand` (the project has no
/// external dependencies). Seeded from wall-clock at first use.
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Lcg {
        Lcg { state: seed }
    }
    fn next(&mut self) -> u64 {
        // Numerical Recipes constants — perfectly fine for picking
        // kill durations in a test.
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    fn millis_between(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A scratch path under temp_dir, unique per call.
fn temp_paths() -> (PathBuf, PathBuf) {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stem = format!("prehnite-crash-{}-{n}", std::process::id());
    let mut db = std::env::temp_dir();
    db.push(format!("{stem}.db"));
    let mut log = std::env::temp_dir();
    log.push(format!("{stem}.log"));
    (db, log)
}

/// Remove `path` plus the engine's sidecar files (`-clog`, `-wal-*`).
fn cleanup(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let mut clog = path.clone().into_os_string();
    clog.push("-clog");
    let _ = fs::remove_file(PathBuf::from(clog));
    // Per-pager WAL files: <db>-wal-<id> for some integer id.
    let dir = path.parent().unwrap_or(std::path::Path::new("."));
    let stem = path.file_name().unwrap_or_default().to_os_string();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let stem_str = stem.to_string_lossy();
            if name_str.starts_with(&*stem_str) && name_str.contains("-wal") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// Read the worker's log file and return every id it managed to
/// fsync before the kill. The log is one decimal id per line.
fn read_logged_ids(path: &PathBuf) -> Vec<i64> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(n) = trimmed.parse::<i64>() {
            ids.push(n);
        }
    }
    ids
}

/// Open the database and read every id present in `t`.
fn read_db_ids(path: &PathBuf) -> Vec<i64> {
    let mut db = Database::open(path).expect("open db after crash");
    let result = db.execute("SELECT id FROM t").expect("scan t");
    let QueryResult::Rows { rows, .. } = result else {
        panic!("SELECT returned non-rows result");
    };
    rows.into_iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!("non-int id"),
        })
        .collect()
}

#[test]
fn acked_inserts_survive_random_kills() {
    // The kill point is randomised; we run several iterations so
    // the kill lands at different stages of the commit pipeline.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xDEAD_BEEF);
    let mut rng = Lcg::new(seed);

    let worker = env!("CARGO_BIN_EXE_crash_worker");

    for iteration in 0..ITERATIONS {
        let (db_path, log_path) = temp_paths();
        cleanup(&db_path);
        let _ = fs::remove_file(&log_path);

        // Spawn the worker.
        let mut child = Command::new(worker)
            .arg(&db_path)
            .arg(&log_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn crash_worker");

        // Let it churn for somewhere between 150 ms and 500 ms,
        // then SIGKILL it. The variance gives different kill
        // points across iterations.
        let life = Duration::from_millis(rng.millis_between(150, 500));
        let start = Instant::now();
        std::thread::sleep(life);
        let elapsed = start.elapsed();

        child.kill().expect("kill worker");
        let _ = child.wait();

        // Verify durability: every logged id must be in the DB.
        let logged = read_logged_ids(&log_path);
        let actual: std::collections::HashSet<i64> =
            read_db_ids(&db_path).into_iter().collect();
        for id in &logged {
            assert!(
                actual.contains(id),
                "iter {iteration} (lived {elapsed:?}): logged id {id} \
                 is missing from the DB after restart. \
                 logged_count={}, actual_count={}",
                logged.len(),
                actual.len(),
            );
        }

        cleanup(&db_path);
        let _ = fs::remove_file(&log_path);
    }
}
