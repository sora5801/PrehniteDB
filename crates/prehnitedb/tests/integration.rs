//! End-to-end tests that drive PrehniteDB through its public API only.
//!
//! These exercise the whole stack — parser, planner, executor, B+tree, pager,
//! and WAL — the way a real embedding would, and confirm that data survives a
//! close and reopen.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use prehnitedb::{Database, Execution, QueryResult, SharedPool, Value};

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

#[test]
fn indexed_lookup_matches_full_scan() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE events (id INT, kind TEXT)")
        .unwrap();

    // 600 rows; `kind` cycles through three values, so each value repeats.
    let kinds = ["click", "view", "purchase"];
    let mut id: i64 = 0;
    while id < 600 {
        let mut sql = String::from("INSERT INTO events VALUES ");
        for j in 0..200 {
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, '{}')", kinds[(id % 3) as usize]));
            id += 1;
        }
        db.execute(&sql).unwrap();
    }

    // Answer produced by a full scan, before any index exists.
    let before = rows(
        db.execute("SELECT id FROM events WHERE kind = 'click'")
            .unwrap(),
    );
    assert_eq!(before.len(), 200);

    db.execute("CREATE INDEX by_kind ON events (kind)").unwrap();

    // The same query is now served by the index — and must agree exactly.
    let after = rows(
        db.execute("SELECT id FROM events WHERE kind = 'click'")
            .unwrap(),
    );
    assert_eq!(after, before);

    // A value matched by no row.
    assert!(rows(
        db.execute("SELECT id FROM events WHERE kind = 'nope'")
            .unwrap()
    )
    .is_empty());
}

#[test]
fn index_tracks_inserts_updates_and_deletes() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, city TEXT)")
        .unwrap();
    // The index exists first, so entries are maintained as rows arrive.
    db.execute("CREATE INDEX by_city ON users (city)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'paris'), (2, 'paris'), (3, 'oslo'), (4, 'paris')")
        .unwrap();
    assert_eq!(
        rows(
            db.execute("SELECT id FROM users WHERE city = 'paris'")
                .unwrap()
        )
        .len(),
        3
    );

    // Move one user out of Paris; the index must follow the change.
    db.execute("UPDATE users SET city = 'oslo' WHERE id = 2")
        .unwrap();
    assert_eq!(
        rows(
            db.execute("SELECT id FROM users WHERE city = 'paris'")
                .unwrap()
        )
        .len(),
        2
    );
    assert_eq!(
        rows(
            db.execute("SELECT id FROM users WHERE city = 'oslo'")
                .unwrap()
        )
        .len(),
        2
    );

    // Delete a Paris user; only id 4 should remain.
    db.execute("DELETE FROM users WHERE id = 1").unwrap();
    assert_eq!(
        rows(
            db.execute("SELECT id FROM users WHERE city = 'paris'")
                .unwrap()
        ),
        vec![vec![Value::Int(4)]]
    );
}

#[test]
fn integer_index_built_on_existing_data_and_persisted() {
    let tmp = TempDb::new();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE m (k INT, label TEXT)").unwrap();
        let mut sql = String::from("INSERT INTO m VALUES ");
        for i in 0..400i64 {
            if i > 0 {
                sql.push(',');
            }
            // `k` spans negatives and positives to exercise the
            // order-preserving integer index-key encoding.
            sql.push_str(&format!("({}, 'row{i}')", i - 200));
        }
        db.execute(&sql).unwrap();
        db.execute("CREATE INDEX by_k ON m (k)").unwrap();
        let hit = rows(db.execute("SELECT label FROM m WHERE k = -50").unwrap());
        assert_eq!(hit, vec![vec![Value::Text("row150".into())]]);
    }
    // Reopen: the index B-tree and its catalog entry must still be on disk.
    let mut db = tmp.open();
    assert_eq!(
        rows(db.execute("SELECT label FROM m WHERE k = 0").unwrap()),
        vec![vec![Value::Text("row200".into())]]
    );
    assert!(rows(db.execute("SELECT label FROM m WHERE k = 999").unwrap()).is_empty());
}

#[test]
fn dropping_an_index_falls_back_to_a_scan() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (2)")
        .unwrap();
    db.execute("CREATE INDEX by_n ON t (n)").unwrap();
    assert_eq!(
        rows(db.execute("SELECT n FROM t WHERE n = 2").unwrap()).len(),
        2
    );

    db.execute("DROP INDEX by_n").unwrap();
    // The query still answers correctly — now via a full scan.
    assert_eq!(
        rows(db.execute("SELECT n FROM t WHERE n = 2").unwrap()).len(),
        2
    );

    // The name is free again; a genuine duplicate is still rejected.
    db.execute("CREATE INDEX by_n ON t (n)").unwrap();
    assert!(db.execute("CREATE INDEX by_n ON t (n)").is_err());
    assert!(db.execute("DROP INDEX ghost").is_err());
}

#[test]
fn range_scans_through_an_index_match_full_scans() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE nums (n INT, tag TEXT)").unwrap();
    let mut sql = String::from("INSERT INTO nums VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 'row{i}')"));
    }
    db.execute(&sql).unwrap();

    let count = |db: &mut Database, predicate: &str| -> usize {
        rows(
            db.execute(&format!("SELECT n FROM nums WHERE {predicate}"))
                .unwrap(),
        )
        .len()
    };

    // Full-scan answers, captured before the index exists.
    let before_ge = count(&mut db, "n >= 480");
    let before_lt = count(&mut db, "n < 7");
    let before_between = count(&mut db, "n > 100 AND n <= 110");

    db.execute("CREATE INDEX by_n ON nums (n)").unwrap();

    // The same predicates, now served by index range scans, must agree —
    // and the absolute counts must be right.
    assert_eq!(count(&mut db, "n >= 480"), before_ge);
    assert_eq!(count(&mut db, "n >= 480"), 20); // 480..=499
    assert_eq!(count(&mut db, "n < 7"), before_lt);
    assert_eq!(count(&mut db, "n < 7"), 7); // 0..=6
    assert_eq!(count(&mut db, "n > 100 AND n <= 110"), before_between);
    assert_eq!(count(&mut db, "n > 100 AND n <= 110"), 10); // 101..=110

    // A one-row half-open range still reaches the right row.
    let one = rows(
        db.execute("SELECT tag FROM nums WHERE n >= 250 AND n < 251")
            .unwrap(),
    );
    assert_eq!(one, vec![vec![Value::Text("row250".into())]]);
}

#[test]
fn composite_index_serves_leftmost_prefixes() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (region TEXT, year INT, amount INT)")
        .unwrap();
    // 4 regions x 10 years = 40 rows; amount is set equal to the year.
    let mut sql = String::from("INSERT INTO sales VALUES ");
    let mut first = true;
    for region in ["north", "south", "east", "west"] {
        for year in 2016..2026i64 {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("('{region}', {year}, {year})"));
        }
    }
    db.execute(&sql).unwrap();
    db.execute("CREATE INDEX by_region_year ON sales (region, year)")
        .unwrap();

    // Leading column alone — the index serves a prefix scan.
    assert_eq!(
        rows(
            db.execute("SELECT year FROM sales WHERE region = 'north'")
                .unwrap()
        )
        .len(),
        10
    );
    // Full prefix — both columns pinned.
    assert_eq!(
        rows(
            db.execute("SELECT amount FROM sales WHERE region = 'south' AND year = 2020")
                .unwrap()
        ),
        vec![vec![Value::Int(2020)]]
    );
    // Leading equality plus a trailing range on the second column.
    assert_eq!(
        rows(
            db.execute("SELECT year FROM sales WHERE region = 'east' AND year >= 2022")
                .unwrap()
        )
        .len(),
        4 // 2022, 2023, 2024, 2025
    );
    // Only the non-leading column constrained — the index cannot help, but a
    // full scan must still answer correctly.
    assert_eq!(
        rows(
            db.execute("SELECT region FROM sales WHERE year = 2019")
                .unwrap()
        )
        .len(),
        4
    );

    // Incremental multi-column maintenance: a row inserted after the index
    // exists is still found through it.
    db.execute("INSERT INTO sales VALUES ('north', 2030, 999)")
        .unwrap();
    assert_eq!(
        rows(
            db.execute("SELECT amount FROM sales WHERE region = 'north' AND year = 2030")
                .unwrap()
        ),
        vec![vec![Value::Int(999)]]
    );
}

#[test]
fn order_by_sorts_results() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE p (id INT, name TEXT, age INT)")
        .unwrap();
    db.execute("INSERT INTO p VALUES (3,'cara',30),(1,'alice',25),(4,'dan',25),(2,'bob',40)")
        .unwrap();

    // Ascending by a text column.
    let by_name = rows(db.execute("SELECT name FROM p ORDER BY name").unwrap());
    assert_eq!(
        by_name,
        vec![
            vec![Value::Text("alice".into())],
            vec![Value::Text("bob".into())],
            vec![Value::Text("cara".into())],
            vec![Value::Text("dan".into())],
        ]
    );

    // Descending.
    let by_id_desc = rows(db.execute("SELECT id FROM p ORDER BY id DESC").unwrap());
    assert_eq!(
        by_id_desc,
        vec![
            vec![Value::Int(4)],
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]
    );

    // Two keys: age ascending, then name descending within an age.
    let multi = rows(
        db.execute("SELECT name FROM p ORDER BY age, name DESC")
            .unwrap(),
    );
    assert_eq!(
        multi,
        vec![
            vec![Value::Text("dan".into())],   // age 25
            vec![Value::Text("alice".into())], // age 25
            vec![Value::Text("cara".into())],  // age 30
            vec![Value::Text("bob".into())],   // age 40
        ]
    );

    // ORDER BY may reference a column the projection omits.
    let ids = rows(db.execute("SELECT id FROM p ORDER BY name").unwrap());
    assert_eq!(
        ids,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );
}

#[test]
fn order_by_served_by_an_index_is_still_sorted() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (k INT, v TEXT)").unwrap();
    // Insert in descending key order, so a plain table scan is *not* sorted.
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..300i64 {
        if i > 0 {
            sql.push(',');
        }
        let k = 299 - i;
        sql.push_str(&format!("({k}, 'v{k}')"));
    }
    db.execute(&sql).unwrap();
    db.execute("CREATE INDEX by_k ON t (k)").unwrap();

    // The WHERE drives the index and ORDER BY k matches the index order, so the
    // planner skips the sort — the result must still come back ascending.
    let result = rows(
        db.execute("SELECT k FROM t WHERE k >= 0 ORDER BY k")
            .unwrap(),
    );
    assert_eq!(result.len(), 300);
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row[0], Value::Int(i as i64));
    }
}

#[test]
fn aggregates_compute_over_the_table() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (region TEXT, amount INT)")
        .unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         ('east',100),('east',200),('west',50),('west',150),('west',300)",
    )
    .unwrap();
    db.execute("INSERT INTO sales (region) VALUES ('north')")
        .unwrap(); // amount defaults to NULL

    // COUNT(*) counts every row; COUNT(col) skips NULLs.
    assert_eq!(
        rows(
            db.execute("SELECT COUNT(*), COUNT(amount) FROM sales")
                .unwrap()
        ),
        vec![vec![Value::Int(6), Value::Int(5)]]
    );

    // SUM / MIN / MAX / AVG over the five non-null amounts.
    assert_eq!(
        rows(
            db.execute("SELECT SUM(amount), MIN(amount), MAX(amount) FROM sales")
                .unwrap()
        ),
        vec![vec![Value::Int(800), Value::Int(50), Value::Int(300)]]
    );
    assert_eq!(
        rows(db.execute("SELECT AVG(amount) FROM sales").unwrap()),
        vec![vec![Value::Real(160.0)]]
    );

    // Aggregates honour the WHERE clause.
    assert_eq!(
        rows(
            db.execute("SELECT COUNT(*), SUM(amount) FROM sales WHERE region = 'west'")
                .unwrap()
        ),
        vec![vec![Value::Int(3), Value::Int(500)]]
    );

    // MIN / MAX work over text, too.
    assert_eq!(
        rows(
            db.execute("SELECT MIN(region), MAX(region) FROM sales")
                .unwrap()
        ),
        vec![vec![Value::Text("east".into()), Value::Text("west".into())]]
    );

    // Over an empty selection, COUNT is 0 but SUM is NULL.
    assert_eq!(
        rows(
            db.execute("SELECT COUNT(*), SUM(amount) FROM sales WHERE region = 'south'")
                .unwrap()
        ),
        vec![vec![Value::Int(0), Value::Null]]
    );

    // SUM of a non-numeric column, and a bare column with no GROUP BY, are errors.
    assert!(db.execute("SELECT SUM(region) FROM sales").is_err());
    assert!(db.execute("SELECT region, COUNT(*) FROM sales").is_err());
}

#[test]
fn group_by_aggregates_each_group() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (region TEXT, product TEXT, amount INT)")
        .unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         ('east','pen',10),('east','pen',20),('east','ink',5),\
         ('west','pen',100),('west','ink',50),('west','ink',70)",
    )
    .unwrap();

    // One row per region, ordered by the grouping column.
    assert_eq!(
        rows(
            db.execute(
                "SELECT region, COUNT(*), SUM(amount) FROM sales \
                 GROUP BY region ORDER BY region"
            )
            .unwrap()
        ),
        vec![
            vec![Value::Text("east".into()), Value::Int(3), Value::Int(35)],
            vec![Value::Text("west".into()), Value::Int(3), Value::Int(220)],
        ]
    );

    // Grouping by two columns yields one row per (region, product) pair.
    assert_eq!(
        rows(
            db.execute("SELECT region, product, COUNT(*) FROM sales GROUP BY region, product")
                .unwrap()
        )
        .len(),
        4
    );

    // A WHERE clause filters rows before they are grouped.
    assert_eq!(
        rows(
            db.execute(
                "SELECT region, SUM(amount) FROM sales \
                 WHERE amount >= 20 GROUP BY region ORDER BY region"
            )
            .unwrap()
        ),
        vec![
            vec![Value::Text("east".into()), Value::Int(20)],
            vec![Value::Text("west".into()), Value::Int(220)],
        ]
    );

    // A whole-table aggregate (no GROUP BY) still produces one row.
    assert_eq!(
        rows(
            db.execute("SELECT COUNT(*), SUM(amount) FROM sales")
                .unwrap()
        ),
        vec![vec![Value::Int(6), Value::Int(255)]]
    );

    // A bare column outside GROUP BY, and SELECT * with GROUP BY, are errors.
    assert!(db
        .execute("SELECT region, product FROM sales GROUP BY region")
        .is_err());
    assert!(db.execute("SELECT * FROM sales GROUP BY region").is_err());
}

#[test]
fn large_text_values_round_trip() {
    let tmp = TempDb::new();
    let blob = |c: char| c.to_string().repeat(9000); // far larger than a page
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE docs (id INT, body TEXT)").unwrap();
        db.execute(&format!("INSERT INTO docs VALUES (1, '{}')", blob('a')))
            .unwrap();
        db.execute(&format!("INSERT INTO docs VALUES (2, '{}')", blob('b')))
            .unwrap();
        db.execute("INSERT INTO docs VALUES (3, 'tiny')").unwrap();

        assert_eq!(
            rows(db.execute("SELECT body FROM docs WHERE id = 1").unwrap()),
            vec![vec![Value::Text(blob('a'))]]
        );

        // Overwrite a spilled value with another spilled value.
        db.execute(&format!(
            "UPDATE docs SET body = '{}' WHERE id = 2",
            blob('c')
        ))
        .unwrap();
        assert_eq!(
            rows(db.execute("SELECT body FROM docs WHERE id = 2").unwrap()),
            vec![vec![Value::Text(blob('c'))]]
        );

        // A whole-table scan reassembles every spilled value.
        assert_eq!(rows(db.execute("SELECT id FROM docs").unwrap()).len(), 3);
    }

    // Spilled values survive a close and reopen.
    let mut db = tmp.open();
    assert_eq!(
        rows(db.execute("SELECT body FROM docs WHERE id = 1").unwrap()),
        vec![vec![Value::Text(blob('a'))]]
    );
    // DROP reclaims the overflow chains; the table is then gone.
    db.execute("DROP TABLE docs").unwrap();
    assert!(db.execute("SELECT id FROM docs").is_err());
}

#[test]
fn having_filters_groups_by_their_aggregates() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (region TEXT, amount INT)")
        .unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         ('east',10),('east',20),('west',100),('west',5),('south',3)",
    )
    .unwrap();

    // Keep only groups whose total exceeds 25: east=30, west=105 pass; south=3 not.
    assert_eq!(
        rows(
            db.execute(
                "SELECT region, SUM(amount) FROM sales \
                 GROUP BY region HAVING SUM(amount) > 25 ORDER BY region"
            )
            .unwrap()
        ),
        vec![
            vec![Value::Text("east".into()), Value::Int(30)],
            vec![Value::Text("west".into()), Value::Int(105)],
        ]
    );

    // HAVING may name an aggregate that is not in the SELECT list.
    assert_eq!(
        rows(
            db.execute(
                "SELECT region FROM sales GROUP BY region \
                 HAVING COUNT(*) >= 2 ORDER BY region"
            )
            .unwrap()
        ),
        vec![
            vec![Value::Text("east".into())],
            vec![Value::Text("west".into())],
        ]
    );

    // HAVING on a whole-table aggregate keeps or drops the single result row.
    assert_eq!(
        rows(
            db.execute("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 0")
                .unwrap()
        ),
        vec![vec![Value::Int(5)]]
    );
    assert!(rows(
        db.execute("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 99")
            .unwrap()
    )
    .is_empty());
}

#[test]
fn vacuum_shrinks_the_file_and_keeps_data() {
    let tmp = TempDb::new();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT, label TEXT)").unwrap();
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in 0..2000i64 {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({i}, 'label-{i}')"));
        }
        db.execute(&sql).unwrap();
        // Delete most rows — node merging frees pages, but the file stays big.
        db.execute("DELETE FROM t WHERE n >= 100").unwrap();
        assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 100);
    }
    let bloated = std::fs::metadata(&tmp.path).unwrap().len();

    let mut db = tmp.open();
    db.execute("VACUUM").unwrap();
    let compacted = std::fs::metadata(&tmp.path).unwrap().len();
    assert!(
        compacted < bloated,
        "VACUUM should shrink the file (compacted {compacted} vs {bloated})"
    );

    // Every surviving row is intact and correct after the rewrite.
    let all = rows(db.execute("SELECT n, label FROM t").unwrap());
    assert_eq!(all.len(), 100);
    assert_eq!(all[0][0], Value::Int(0));
    assert_eq!(all[99][1], Value::Text("label-99".into()));

    // The compacted database still works and survives a reopen.
    db.execute("INSERT INTO t VALUES (5000, 'fresh')").unwrap();
    drop(db);
    let mut reopened = tmp.open();
    assert_eq!(
        rows(reopened.execute("SELECT n FROM t").unwrap()).len(),
        101
    );
}

#[test]
fn limit_and_offset_bound_the_result() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE nums (n INT)").unwrap();
    let mut sql = String::from("INSERT INTO nums VALUES ");
    for i in 0..100i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    db.execute(&sql).unwrap();

    // LIMIT caps the row count to the table's first k rows.
    let first5 = rows(db.execute("SELECT n FROM nums LIMIT 5").unwrap());
    assert_eq!(
        first5,
        (0..5).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );

    // OFFSET skips rows before LIMIT takes them.
    let window = rows(db.execute("SELECT n FROM nums LIMIT 3 OFFSET 10").unwrap());
    assert_eq!(
        window,
        (10..13).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );

    // LIMIT 0 returns nothing; a LIMIT past the end returns the whole table.
    assert!(rows(db.execute("SELECT n FROM nums LIMIT 0").unwrap()).is_empty());
    assert_eq!(
        rows(db.execute("SELECT n FROM nums LIMIT 9999").unwrap()).len(),
        100
    );

    // LIMIT composes with ORDER BY — the top of the *sorted* order.
    let top3_desc = rows(
        db.execute("SELECT n FROM nums ORDER BY n DESC LIMIT 3")
            .unwrap(),
    );
    assert_eq!(
        top3_desc,
        vec![
            vec![Value::Int(99)],
            vec![Value::Int(98)],
            vec![Value::Int(97)],
        ]
    );

    // LIMIT composes with WHERE.
    let filtered = rows(
        db.execute("SELECT n FROM nums WHERE n >= 50 LIMIT 4")
            .unwrap(),
    );
    assert_eq!(
        filtered,
        (50..54).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );

    // LIMIT also trims a grouped result.
    db.execute("CREATE TABLE g (k INT)").unwrap();
    db.execute("INSERT INTO g VALUES (1),(1),(2),(2),(3),(3),(4)")
        .unwrap();
    let groups = rows(
        db.execute("SELECT k, COUNT(*) FROM g GROUP BY k ORDER BY k LIMIT 2")
            .unwrap(),
    );
    assert_eq!(
        groups,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
}

#[test]
fn inner_join_relates_two_tables() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT, user_id INT, total INT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1,'ada'),(2,'grace'),(3,'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10,1,100),(11,1,200),(12,2,50)")
        .unwrap();

    // INNER JOIN: edsger has no order, so drops out.
    let joined = rows(
        db.execute(
            "SELECT users.name, orders.total FROM users \
             JOIN orders ON users.id = orders.user_id ORDER BY orders.total",
        )
        .unwrap(),
    );
    assert_eq!(
        joined,
        vec![
            vec![Value::Text("grace".into()), Value::Int(50)],
            vec![Value::Text("ada".into()), Value::Int(100)],
            vec![Value::Text("ada".into()), Value::Int(200)],
        ]
    );

    // Aliases, plus a WHERE clause over the joined rows.
    let big = rows(
        db.execute(
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id \
             WHERE o.total >= 100 ORDER BY o.total",
        )
        .unwrap(),
    );
    assert_eq!(
        big,
        vec![
            vec![Value::Text("ada".into())],
            vec![Value::Text("ada".into())],
        ]
    );

    // GROUP BY and LIMIT both compose with a join.
    let totals = rows(
        db.execute(
            "SELECT u.name, SUM(o.total) FROM users u JOIN orders o \
             ON u.id = o.user_id GROUP BY u.name ORDER BY u.name",
        )
        .unwrap(),
    );
    assert_eq!(
        totals,
        vec![
            vec![Value::Text("ada".into()), Value::Int(300)],
            vec![Value::Text("grace".into()), Value::Int(50)],
        ]
    );
    assert_eq!(
        rows(
            db.execute("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id LIMIT 2")
                .unwrap()
        )
        .len(),
        2
    );

    // `id` is in both tables — a bare reference to it is ambiguous.
    assert!(db
        .execute("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
        .is_err());
}

#[test]
fn left_and_cross_joins() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (user_id INT, total INT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1,'ada'),(2,'grace'),(3,'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,100),(2,50)")
        .unwrap();

    // LEFT JOIN keeps edsger, padding the missing order with NULL.
    let left = rows(
        db.execute(
            "SELECT u.name, o.total FROM users u \
             LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name",
        )
        .unwrap(),
    );
    assert_eq!(
        left,
        vec![
            vec![Value::Text("ada".into()), Value::Int(100)],
            vec![Value::Text("edsger".into()), Value::Null],
            vec![Value::Text("grace".into()), Value::Int(50)],
        ]
    );

    // CROSS JOIN pairs every user with every order: 3 x 2 = 6 rows.
    assert_eq!(
        rows(
            db.execute("SELECT u.name, o.total FROM users u CROSS JOIN orders o")
                .unwrap()
        )
        .len(),
        6
    );
}

#[test]
fn multi_way_and_self_joins() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE a (id INT, label TEXT)").unwrap();
    db.execute("CREATE TABLE b (a_id INT, c_id INT)").unwrap();
    db.execute("CREATE TABLE c (id INT, note TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,'one'),(2,'two')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (1,100),(2,200)").unwrap();
    db.execute("INSERT INTO c VALUES (100,'hundred'),(200,'two-hundred')")
        .unwrap();

    // A three-table chain a -> b -> c.
    let chain = rows(
        db.execute(
            "SELECT a.label, c.note FROM a \
             JOIN b ON a.id = b.a_id \
             JOIN c ON b.c_id = c.id ORDER BY a.id",
        )
        .unwrap(),
    );
    assert_eq!(
        chain,
        vec![
            vec![Value::Text("one".into()), Value::Text("hundred".into())],
            vec![Value::Text("two".into()), Value::Text("two-hundred".into()),],
        ]
    );

    // A self-join — aliases tell the two copies of `emp` apart.
    db.execute("CREATE TABLE emp (id INT, name TEXT, manager INT)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1,'boss',1),(2,'alice',1),(3,'bob',2)")
        .unwrap();
    let reports = rows(
        db.execute(
            "SELECT e.name, m.name FROM emp e JOIN emp m ON e.manager = m.id \
             WHERE e.id <> e.manager ORDER BY e.name",
        )
        .unwrap(),
    );
    assert_eq!(
        reports,
        vec![
            vec![Value::Text("alice".into()), Value::Text("boss".into())],
            vec![Value::Text("bob".into()), Value::Text("alice".into())],
        ]
    );
}

#[test]
fn index_driven_join_matches_a_plain_join() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (user_id INT, total INT)")
        .unwrap();

    // 50 users; 200 orders whose user_id cycles 0..40 — so users 0..40 each
    // have several orders and users 40..50 have none.
    let mut users = String::from("INSERT INTO users VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            users.push(',');
        }
        users.push_str(&format!("({i}, 'user-{i}')"));
    }
    db.execute(&users).unwrap();
    let mut orders = String::from("INSERT INTO orders VALUES ");
    for i in 0..200i64 {
        if i > 0 {
            orders.push(',');
        }
        orders.push_str(&format!("({}, {i})", i % 40));
    }
    db.execute(&orders).unwrap();

    let inner = "SELECT u.name, o.total FROM users u JOIN orders o \
                 ON u.id = o.user_id ORDER BY u.name, o.total";
    let left = "SELECT u.name, o.total FROM users u LEFT JOIN orders o \
                ON u.id = o.user_id ORDER BY u.name, o.total";

    // Run both joins with no index — plain nested-loop, full inner rescan.
    let inner_plain = rows(db.execute(inner).unwrap());
    let left_plain = rows(db.execute(left).unwrap());
    assert!(!inner_plain.is_empty());
    // LEFT keeps users with no orders, padded with NULL.
    assert!(left_plain.iter().any(|row| row[1] == Value::Null));

    // The same joins, now with an index on the inner join column, drive an
    // index lookup per left row — and must give byte-for-byte the same answer.
    db.execute("CREATE INDEX by_user ON orders (user_id)")
        .unwrap();
    assert_eq!(rows(db.execute(inner).unwrap()), inner_plain);
    assert_eq!(rows(db.execute(left).unwrap()), left_plain);
}

#[test]
fn concurrent_readers_share_one_pool() {
    let tmp = TempDb::new();
    let pool = SharedPool::new();

    // One writer fills the table, through a Database on the shared pool.
    {
        let mut db = Database::open_with_pool(&tmp.path, pool.clone()).unwrap();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        let mut id = 0;
        while id < 500 {
            let mut sql = String::from("INSERT INTO t VALUES ");
            for j in 0..100 {
                if j > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("({id}, 'name-{id}')"));
                id += 1;
            }
            db.execute(&sql).unwrap();
        }
    }

    // Eight readers run at once, each its own Database but all over the one
    // shared pool — so they split a single warm cache. Every reader must see
    // the whole table; a deadlock or a torn read would fail the join below.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let path = tmp.path.clone();
        handles.push(std::thread::spawn(move || {
            let mut reader = Database::open_with_pool(&path, pool).unwrap();
            let scanned = rows(reader.execute("SELECT id FROM t WHERE id >= 100").unwrap());
            assert_eq!(scanned.len(), 400);
            let counted = rows(reader.execute("SELECT COUNT(*) FROM t").unwrap());
            assert_eq!(counted, vec![vec![Value::Int(500)]]);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn streaming_yields_the_same_rows_as_execute() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    let mut id = 0;
    while id < 300 {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for j in 0..100 {
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, 'name-{id}')"));
            id += 1;
        }
        db.execute(&sql).unwrap();
    }

    // Pulling a streaming RowStream row by row must produce exactly what the
    // materializing `execute` returns — across the volcano and buffered paths.
    for query in [
        "SELECT id, name FROM t",
        "SELECT id FROM t WHERE id >= 100 LIMIT 50",
        "SELECT id FROM t ORDER BY id DESC LIMIT 5",
        "SELECT COUNT(*) FROM t",
        "SELECT id, COUNT(*) FROM t GROUP BY id ORDER BY id",
    ] {
        let materialized = rows(db.execute(query).unwrap());
        let streamed = match db.execute_streaming(query).unwrap() {
            Execution::Rows(mut stream) => {
                let mut collected = Vec::new();
                while let Some(row) = db.stream_next(&mut stream).unwrap() {
                    collected.push(row);
                }
                collected
            }
            Execution::Ack(_) => panic!("a SELECT must stream rows, not Ack"),
        };
        assert_eq!(streamed, materialized, "mismatch for `{query}`");
    }

    // A non-SELECT through the streaming API yields an Ack, not a row stream.
    match db
        .execute_streaming("INSERT INTO t VALUES (9999, 'last')")
        .unwrap()
    {
        Execution::Ack(_) => {}
        Execution::Rows(_) => panic!("an INSERT must not stream rows"),
    }
}

#[test]
fn hash_join_handles_duplicate_and_null_keys() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE l (k INT, tag TEXT)").unwrap();
    db.execute("CREATE TABLE r (k INT, note TEXT)").unwrap();
    // No index on r.k, so `l JOIN r ON l.k = r.k` runs as a hash join. The
    // `(col)`-form INSERT leaves the unnamed `k` column NULL.
    db.execute("INSERT INTO l VALUES (1,'a'),(1,'b'),(2,'c')")
        .unwrap();
    db.execute("INSERT INTO l (tag) VALUES ('d')").unwrap();
    db.execute("INSERT INTO r VALUES (1,'x'),(1,'y'),(3,'z')")
        .unwrap();
    db.execute("INSERT INTO r (note) VALUES ('w')").unwrap();

    // INNER: key 1 pairs both left rows with both right rows — every
    // combination — while key 2 and the NULL key match nothing.
    let inner = rows(
        db.execute("SELECT l.tag, r.note FROM l JOIN r ON l.k = r.k ORDER BY l.tag, r.note")
            .unwrap(),
    );
    assert_eq!(
        inner,
        vec![
            vec![Value::Text("a".into()), Value::Text("x".into())],
            vec![Value::Text("a".into()), Value::Text("y".into())],
            vec![Value::Text("b".into()), Value::Text("x".into())],
            vec![Value::Text("b".into()), Value::Text("y".into())],
        ]
    );

    // LEFT: every left row survives; the unmatched ones — key 2 and the NULL
    // key — are padded with NULL.
    let left = rows(
        db.execute("SELECT l.tag, r.note FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.tag")
            .unwrap(),
    );
    assert_eq!(left.len(), 6); // four matched pairs, plus 'c' and 'd' padded
    assert!(left
        .iter()
        .any(|row| row[0] == Value::Text("c".into()) && row[1] == Value::Null));
    assert!(left
        .iter()
        .any(|row| row[0] == Value::Text("d".into()) && row[1] == Value::Null));

    // A non-equi `ON` has no key to hash, so it falls back to the nested-loop
    // join — and is still correct: `l.k <> r.k` keeps only differing, non-NULL
    // pairs.
    let non_equi = rows(
        db.execute("SELECT l.tag, r.note FROM l JOIN r ON l.k <> r.k ORDER BY l.tag, r.note")
            .unwrap(),
    );
    assert_eq!(
        non_equi,
        vec![
            vec![Value::Text("a".into()), Value::Text("z".into())],
            vec![Value::Text("b".into()), Value::Text("z".into())],
            vec![Value::Text("c".into()), Value::Text("x".into())],
            vec![Value::Text("c".into()), Value::Text("y".into())],
            vec![Value::Text("c".into()), Value::Text("z".into())],
        ]
    );
}
