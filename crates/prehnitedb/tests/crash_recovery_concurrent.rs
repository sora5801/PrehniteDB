//! Concurrent crash-recovery property test (v0.46).
//!
//! Where [`crash_recovery`] tested a single writer, this spawns
//! the multi-threaded [`crash_worker_concurrent`] binary and kills
//! it mid-flight while N threads hammer the same `Database`. The
//! property under test is identical:
//!
//! > Every id any thread fsync'd to its log before the kill must
//! > be present in the table after restart.
//!
//! v0.46 stresses things the v0.38 single-writer test couldn't:
//! - v0.42 group commit's leader/follower fsync handoff
//! - v0.30 per-page B+tree latches under same-table contention
//! - v0.28 per-table mutexes in shared INSERT mode
//! - v0.43 PRIMARY KEY's unique-index check under concurrent inserts
//!
//! The per-thread id stride means concurrent inserts can't collide
//! on the PK; any failure must be a recovery bug, not a duplicate.
//!
//! As in v0.38, the worker may be killed between DB ack and log
//! fsync, leaving a row on disk with no log entry. That gap is
//! intentional — we only assert *durability of logged ids*, which
//! is the actual durability claim.

use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prehnitedb::{Database, QueryResult, Value};

/// Number of spawn-kill-verify iterations. Each iteration runs the
/// worker for ~200–600 ms then verifies recovery, so 5 iterations
/// is around 1.5–3.5 seconds plus restart-and-scan time.
const ITERATIONS: usize = 5;

/// Number of concurrent writer threads inside one worker process.
/// 8 is plenty to exercise group commit's batching (each thread
/// can stack up in `pending` while one fsync is in flight) without
/// over-saturating the test machine.
const THREADS: usize = 8;

/// Tiny LCG so we don't drag in `rand` — the project has no
/// external dependencies. Seeded from wall-clock at first use.
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Lcg {
        Lcg { state: seed }
    }
    fn next(&mut self) -> u64 {
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

fn temp_paths() -> (PathBuf, PathBuf) {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stem = format!("prehnite-crash-conc-{}-{n}", std::process::id());
    let mut db = std::env::temp_dir();
    db.push(format!("{stem}.db"));
    let mut log_base = std::env::temp_dir();
    log_base.push(format!("{stem}.log"));
    (db, log_base)
}

/// Remove `db` plus the engine's sidecar files (clog + per-pager
/// WAL files), and every per-thread log file under `log_base.*`.
fn cleanup(db: &Path, log_base: &Path) {
    let _ = fs::remove_file(db);
    let mut clog = db.as_os_str().to_os_string();
    clog.push("-clog");
    let _ = fs::remove_file(PathBuf::from(clog));
    let dir = db.parent().unwrap_or(Path::new("."));
    let stem = db.file_name().unwrap_or_default().to_os_string();
    let log_stem = log_base.file_name().unwrap_or_default().to_os_string();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let stem_str = stem.to_string_lossy();
            let log_stem_str = log_stem.to_string_lossy();
            // Per-pager WAL: `<db>-wal-<id>`.
            if name_str.starts_with(&*stem_str) && name_str.contains("-wal") {
                let _ = fs::remove_file(entry.path());
            }
            // Per-thread log: `<log_base>.<tid>`.
            if name_str.starts_with(&*log_stem_str) && name_str != log_stem_str {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// Walk every per-thread log file under `log_base.*` and return the
/// union of fsync'd ids across threads. A missing log file just
/// contributes no ids.
fn read_logged_ids(log_base: &Path) -> Vec<i64> {
    let dir = log_base.parent().unwrap_or(Path::new("."));
    let log_stem = log_base.file_name().unwrap_or_default().to_os_string();
    let mut ids = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return ids;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let log_stem_str = log_stem.to_string_lossy();
        if !name_str.starts_with(&*log_stem_str) || name_str == log_stem_str {
            continue;
        }
        let Ok(file) = fs::File::open(entry.path()) else {
            continue;
        };
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(n) = trimmed.parse::<i64>() {
                ids.push(n);
            }
        }
    }
    ids
}

/// Open the database and read every id from `t`.
fn read_db_ids(path: &Path) -> Vec<i64> {
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
fn concurrent_acked_inserts_survive_random_kills() {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xDEAD_BEEF);
    let mut rng = Lcg::new(seed);

    let worker = env!("CARGO_BIN_EXE_crash_worker_concurrent");

    for iteration in 0..ITERATIONS {
        let (db_path, log_base) = temp_paths();
        cleanup(&db_path, &log_base);

        let mut child = Command::new(worker)
            .arg(&db_path)
            .arg(&log_base)
            .arg(THREADS.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn crash_worker_concurrent");

        // 200–600 ms range: long enough for several batches of
        // group-commit fsyncs to land, short enough to keep the
        // suite quick.
        let life = Duration::from_millis(rng.millis_between(200, 600));
        let start = Instant::now();
        std::thread::sleep(life);
        let elapsed = start.elapsed();

        child.kill().expect("kill worker");
        let _ = child.wait();

        // Property check.
        let logged: HashSet<i64> = read_logged_ids(&log_base).into_iter().collect();
        let actual: HashSet<i64> = read_db_ids(&db_path).into_iter().collect();
        for id in &logged {
            assert!(
                actual.contains(id),
                "iter {iteration} (lived {elapsed:?}, threads={THREADS}): \
                 logged id {id} is missing from the DB after restart. \
                 logged_count={}, actual_count={}",
                logged.len(),
                actual.len(),
            );
        }
        // The DB may legitimately have *more* rows than the logs
        // recorded — those are the "killed between ack and log
        // fsync" rows. That's allowed; we only assert the logged
        // ids survived.

        cleanup(&db_path, &log_base);
    }
}
