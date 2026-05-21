//! End-to-end tests that drive PrehniteDB through its public API only.
//!
//! These exercise the whole stack — parser, planner, executor, B+tree, pager,
//! and WAL — the way a real embedding would, and confirm that data survives a
//! close and reopen.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use prehnitedb::{Database, QueryResult, Value};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A scratch database file that deletes itself (and its WAL) on drop.
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> TempDb {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("prehnite-it-{}-{n}.db", std::process::id()));
        cleanup(&path);
        TempDb { path }
    }

    fn open(&self) -> Database {
        Database::open(&self.path).unwrap()
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        cleanup(&self.path);
    }
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let mut wal = path.clone().into_os_string();
    wal.push("-wal");
    let _ = std::fs::remove_file(PathBuf::from(wal));
}

fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected a result set, got {other:?}"),
    }
}

#[test]
fn large_dataset_round_trips_and_survives_reopen() {
    let tmp = TempDb::new();
    const TOTAL: i64 = 1500;

    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE items (id INT, label TEXT, qty INT)")
            .unwrap();

        // Insert in chunks so the table B+tree grows several leaves deep.
        let mut id = 0;
        while id < TOTAL {
            let mut sql = String::from("INSERT INTO items VALUES ");
            for j in 0..250 {
                if j > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("({id}, 'label-{id}', {})", id * 3));
                id += 1;
            }
            db.execute(&sql).unwrap();
        }

        assert_eq!(
            rows(db.execute("SELECT id FROM items").unwrap()).len(),
            1500
        );
    }

    // Reopen from disk: every row must still be there, in order.
    let mut db = tmp.open();
    let all = rows(db.execute("SELECT id, label, qty FROM items").unwrap());
    assert_eq!(all.len(), 1500);
    assert_eq!(all[0][0], Value::Int(0));
    assert_eq!(all[0][1], Value::Text("label-0".into()));
    assert_eq!(all[1499][2], Value::Int(1499 * 3));

    // qty = id*3 >= 4400 means id >= 1467; combined with id < 1490 that is 23 rows.
    let filtered = rows(
        db.execute("SELECT id FROM items WHERE qty >= 4400 AND id < 1490")
            .unwrap(),
    );
    assert_eq!(filtered.len(), 23);
}

#[test]
fn statements_are_atomic() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30)").unwrap();

    // A multi-row INSERT whose last tuple has the wrong type must leave the
    // table exactly as it was.
    assert!(db
        .execute("INSERT INTO t VALUES (40), (50), ('oops')")
        .is_err());
    let remaining = rows(db.execute("SELECT n FROM t").unwrap());
    assert_eq!(remaining.len(), 3);

    // The connection is still healthy afterward.
    db.execute("INSERT INTO t VALUES (40)").unwrap();
    assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 4);
}

#[test]
fn mixed_types_update_and_delete() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE accounts (id INT, name TEXT, balance REAL, vip BOOL)")
        .unwrap();
    db.execute(
        "INSERT INTO accounts VALUES \
         (1, 'ada', 100.5, false), \
         (2, 'grace', 250.0, true), \
         (3, 'edsger', 0.0, false)",
    )
    .unwrap();

    // Integer literal widening into a REAL column.
    db.execute("UPDATE accounts SET balance = 500 WHERE name = 'ada'")
        .unwrap();
    let ada = rows(
        db.execute("SELECT balance FROM accounts WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(ada, vec![vec![Value::Real(500.0)]]);

    // Expression that reads the row's current value.
    db.execute("UPDATE accounts SET balance = balance * 2 WHERE vip = true")
        .unwrap();
    let grace = rows(
        db.execute("SELECT balance FROM accounts WHERE id = 2")
            .unwrap(),
    );
    assert_eq!(grace, vec![vec![Value::Real(500.0)]]);

    db.execute("DELETE FROM accounts WHERE balance = 0.0")
        .unwrap();
    assert_eq!(
        rows(db.execute("SELECT id FROM accounts").unwrap()).len(),
        2
    );
}

#[test]
fn several_tables_are_independent() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (y TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES ('one')").unwrap();

    assert_eq!(rows(db.execute("SELECT x FROM a").unwrap()).len(), 2);
    assert_eq!(rows(db.execute("SELECT y FROM b").unwrap()).len(), 1);

    let mut names = db.table_names().unwrap();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);

    // Dropping one table leaves the other untouched.
    db.execute("DROP TABLE a").unwrap();
    assert!(db.execute("SELECT x FROM a").is_err());
    assert_eq!(rows(db.execute("SELECT y FROM b").unwrap()).len(), 1);
}

#[test]
fn null_handling_follows_three_valued_logic() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, note TEXT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'hello')").unwrap();

    // `note = 'hello'` is NULL (not TRUE) for the rows whose note is NULL, so
    // only row 3 matches.
    let matched = rows(db.execute("SELECT id FROM t WHERE note = 'hello'").unwrap());
    assert_eq!(matched, vec![vec![Value::Int(3)]]);

    let missing = rows(db.execute("SELECT id FROM t WHERE note IS NULL").unwrap());
    assert_eq!(missing.len(), 2);

    let present = rows(
        db.execute("SELECT id FROM t WHERE note IS NOT NULL")
            .unwrap(),
    );
    assert_eq!(present, vec![vec![Value::Int(3)]]);
}
