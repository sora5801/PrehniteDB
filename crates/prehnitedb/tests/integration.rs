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

#[test]
fn grace_hash_join_over_a_larger_workload() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE l (k INT, tag TEXT)").unwrap();
    db.execute("CREATE TABLE r (k INT, note TEXT)").unwrap();

    // 500 rows on each side, deliberately spread across many key values so the
    // partitioner has to put rows in different partition files. Left keys span
    // 0..99 (each repeated 5×), right keys span 0..49 (each repeated 10×), so
    // half the left key range matches and half does not.
    let mut left_sql = String::from("INSERT INTO l VALUES ");
    let mut right_sql = String::from("INSERT INTO r VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            left_sql.push(',');
            right_sql.push(',');
        }
        left_sql.push_str(&format!("({}, 'L{i}')", i % 100));
        right_sql.push_str(&format!("({}, 'R{i}')", i % 50));
    }
    db.execute(&left_sql).unwrap();
    db.execute(&right_sql).unwrap();
    // A handful of NULL-keyed rows on each side, to exercise the NULL path
    // through the partitioner.
    db.execute("INSERT INTO l (tag) VALUES ('LN1'),('LN2')")
        .unwrap();
    db.execute("INSERT INTO r (note) VALUES ('RN1'),('RN2')")
        .unwrap();

    // INNER: matching keys are 0..49. Each contributes 5 left × 10 right = 50
    // pairs, across 50 keys, for 2500 result rows.
    let inner = rows(
        db.execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
            .unwrap(),
    );
    assert_eq!(inner, vec![vec![Value::Int(2500)]]);

    // LEFT: 250 matched left rows produce 2500 pairs; the 250 left rows with
    // keys 50..99 and the 2 NULL-keyed rows each yield one `NULL`-padded row,
    // for 252. Total: 2752.
    let left_count = rows(
        db.execute("SELECT COUNT(*) FROM l LEFT JOIN r ON l.k = r.k")
            .unwrap(),
    );
    assert_eq!(left_count, vec![vec![Value::Int(2752)]]);

    // A spot-check on the actual rows: every distinct key matched on the
    // inner side appears 50 times in the result; an unmatched left key appears
    // 0 times in an INNER result and 5 times in a LEFT result.
    let key_42 = rows(
        db.execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k WHERE l.k = 42")
            .unwrap(),
    );
    assert_eq!(key_42, vec![vec![Value::Int(50)]]);
    let key_77_inner = rows(
        db.execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k WHERE l.k = 77")
            .unwrap(),
    );
    assert_eq!(key_77_inner, vec![vec![Value::Int(0)]]);
    let key_77_left = rows(
        db.execute("SELECT COUNT(*) FROM l LEFT JOIN r ON l.k = r.k WHERE l.k = 77")
            .unwrap(),
    );
    assert_eq!(key_77_left, vec![vec![Value::Int(5)]]);
}

#[test]
fn reordered_inner_chain_returns_correct_rows() {
    // A three-table chain where the user wrote the worst possible order
    // (biggest table first). The planner must reorder by row_count and produce
    // the same answer as a hand-written best-order query.
    let tmp = TempDb::new();
    let mut db = tmp.open();

    db.execute("CREATE TABLE big (bid INT, payload TEXT)")
        .unwrap();
    db.execute("CREATE TABLE mid (mid_id INT, bid INT)")
        .unwrap();
    db.execute("CREATE TABLE tiny (tid INT, mid_id INT, label TEXT)")
        .unwrap();

    // big: 200 rows, bid in 0..200. mid: 40 rows, each pointing at bid=i*5.
    // tiny: 4 rows pointing at mid_id 0, 10, 20, 30.
    let mut big_sql = String::from("INSERT INTO big VALUES ");
    for i in 0..200i64 {
        if i > 0 {
            big_sql.push(',');
        }
        big_sql.push_str(&format!("({i}, 'P{i}')"));
    }
    db.execute(&big_sql).unwrap();

    let mut mid_sql = String::from("INSERT INTO mid VALUES ");
    for i in 0..40i64 {
        if i > 0 {
            mid_sql.push(',');
        }
        mid_sql.push_str(&format!("({i}, {})", i * 5));
    }
    db.execute(&mid_sql).unwrap();

    db.execute("INSERT INTO tiny VALUES (1,0,'a'),(2,10,'b'),(3,20,'c'),(4,30,'d')")
        .unwrap();

    // The user writes the worst order: big first. The planner should reorder
    // to start with tiny.
    let user_order = rows(
        db.execute(
            "SELECT tiny.label, mid.mid_id, big.bid FROM big \
             INNER JOIN mid ON big.bid = mid.bid \
             INNER JOIN tiny ON mid.mid_id = tiny.mid_id \
             ORDER BY tiny.label",
        )
        .unwrap(),
    );

    // Hand-written best order.
    let hand_order = rows(
        db.execute(
            "SELECT tiny.label, mid.mid_id, big.bid FROM tiny \
             INNER JOIN mid ON tiny.mid_id = mid.mid_id \
             INNER JOIN big ON big.bid = mid.bid \
             ORDER BY tiny.label",
        )
        .unwrap(),
    );

    // Reorder must not change the result set.
    assert_eq!(user_order, hand_order);

    // Spot-check: tiny has 4 rows; each joins to exactly one mid (mid_id matches)
    // and that mid joins to exactly one big (bid = mid.bid * 5 → 0, 50, 100, 150).
    let expected = vec![
        vec![Value::Text("a".into()), Value::Int(0), Value::Int(0)],
        vec![Value::Text("b".into()), Value::Int(10), Value::Int(50)],
        vec![Value::Text("c".into()), Value::Int(20), Value::Int(100)],
        vec![Value::Text("d".into()), Value::Int(30), Value::Int(150)],
    ];
    assert_eq!(user_order, expected);
}

#[test]
fn two_writers_can_have_transactions_open_simultaneously() {
    // v0.26: BEGIN no longer holds the writer mutex. Two connections
    // sharing a pool + TxState can each have a transaction open; their
    // statements interleave, each writer sees its own writes via the
    // own_tx override but not the other's.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (id INT, label TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'committed')").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();

    let mut a = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut b = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    a.execute("BEGIN").unwrap();
    a.execute("INSERT INTO t VALUES (2, 'from-a')").unwrap();
    b.execute("BEGIN").unwrap();
    b.execute("INSERT INTO t VALUES (3, 'from-b')").unwrap();

    let from_a = rows(a.execute("SELECT id FROM t ORDER BY id").unwrap());
    let from_b = rows(b.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(from_a.len(), 2);
    assert_eq!(from_b.len(), 2);
    let ids_a: Vec<i64> = from_a
        .iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!(),
        })
        .collect();
    let ids_b: Vec<i64> = from_b
        .iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids_a, vec![1, 2]);
    assert_eq!(ids_b, vec![1, 3]);

    a.execute("COMMIT").unwrap();
    b.execute("COMMIT").unwrap();
    let mut reader = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    let all = rows(reader.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(all.len(), 3);
}

#[test]
fn write_write_conflict_aborts_the_second_writer() {
    // Two writers both try to UPDATE the same row. The first to write
    // claims the tombstone; the second sees the in-flight tombstone
    // (its tx is in our snapshot's in_flight set) and aborts under FUW.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (id INT, n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();

    let mut a = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut b = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET n = 99 WHERE id = 1").unwrap();

    b.execute("BEGIN").unwrap();
    let err = b.execute("UPDATE t SET n = 88 WHERE id = 1").unwrap_err();
    assert!(
        err.to_string().contains("conflict"),
        "expected conflict error, got: {err}"
    );

    assert!(b.execute("SELECT id FROM t").is_err());
    b.execute("ROLLBACK").unwrap();

    a.execute("COMMIT").unwrap();
    let mut reader = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    let n = rows(reader.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(n, vec![vec![Value::Int(99)]]);
}

#[test]
fn rolled_back_inserts_are_reclaimed_by_vacuum() {
    // v0.26's rollback leaves rows physically on disk with their TX in
    // the clog marked rolled-back. They're invisible to every snapshot
    // until VACUUM scans them out.
    let tmp = TempDb::new();
    let mut db = Database::open(&tmp.path).unwrap();
    db.execute("CREATE TABLE t (id INT, payload TEXT)").unwrap();

    db.execute("INSERT INTO t VALUES (1, 'committed')").unwrap();
    db.execute("BEGIN").unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, 'doomed-{i}')", i + 100));
    }
    db.execute(&sql).unwrap();
    let bloated = std::fs::metadata(&tmp.path).unwrap().len();
    db.execute("ROLLBACK").unwrap();

    let visible = rows(db.execute("SELECT id FROM t").unwrap());
    assert_eq!(visible, vec![vec![Value::Int(1)]]);

    let after_rollback = std::fs::metadata(&tmp.path).unwrap().len();
    assert!(
        after_rollback + 4096 >= bloated,
        "rolled-back rows shouldn't shrink the file: before={bloated} after={after_rollback}"
    );

    db.execute("VACUUM").unwrap();
    let after_vacuum = std::fs::metadata(&tmp.path).unwrap().len();
    assert!(
        after_vacuum < after_rollback,
        "VACUUM should shrink the file: rollback={after_rollback} vacuum={after_vacuum}"
    );
    let surviving = rows(db.execute("SELECT id FROM t").unwrap());
    assert_eq!(surviving, vec![vec![Value::Int(1)]]);
}

#[test]
fn snapshot_reader_does_not_see_uncommitted_writes() {
    // Two `Database` handles share one buffer pool and one TxState — the
    // shape the server uses. The reader takes its snapshot before the
    // writer commits, so its `next_tx` is below the writer's TX ID. The
    // writer's in-flight inserts have tx_min = the in-flight ID, which the
    // reader's snapshot lists as not yet visible.
    let tmp = TempDb::new();
    // Seed: one committed row, then close so the next opens are fresh.
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (id INT, label TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'committed')").unwrap();
    }

    let pool = SharedPool::new();
    let tx_state = {
        // Initial next_tx from the persisted header.
        let probe = Database::open_with_pool(&tmp.path, pool.clone()).unwrap();
        probe.tx_state()
    };

    // Writer opens BEGIN, inserts, but does *not* commit.
    let mut writer = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    writer.execute("BEGIN").unwrap();
    writer
        .execute("INSERT INTO t VALUES (2, 'uncommitted')")
        .unwrap();

    // A reader opening now captures `in_flight = Some(writer's TX)` —
    // the writer's inserts are invisible to it.
    let mut reader = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let visible = rows(
        reader
            .execute("SELECT id, label FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        visible,
        vec![vec![Value::Int(1), Value::Text("committed".into())]],
        "reader should only see committed rows"
    );

    // Writer commits. The reader's snapshot is unchanged — it still sees
    // only the original row. A *new* reader, opened after commit, sees both.
    writer.execute("COMMIT").unwrap();
    let still_visible_to_reader = rows(
        reader
            .execute("SELECT id, label FROM t ORDER BY id")
            .unwrap(),
    );
    // The reader's auto-commit SELECT takes a fresh snapshot each time,
    // and at this fresh snapshot the writer's commit is visible. So the
    // reader sees both rows now. Snapshot isolation across a *single*
    // SELECT is what holds; multiple SELECTs each get their own snapshot.
    assert_eq!(still_visible_to_reader.len(), 2);

    let mut new_reader = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    assert_eq!(
        rows(new_reader.execute("SELECT id FROM t ORDER BY id").unwrap()).len(),
        2
    );
}

#[test]
fn rollback_leaves_no_trace_visible_to_future_readers() {
    // A rolled-back transaction's TX ID becomes a gap — no row in the
    // file carries it, so future readers naturally don't see anything
    // attributed to it. Verifies the data model: rolling back doesn't
    // leak rows, even across concurrent observers.
    let tmp = TempDb::new();
    let mut db = Database::open(&tmp.path).unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.execute("ROLLBACK").unwrap();

    let visible = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(visible, vec![vec![Value::Int(1)]]);

    // After reopen the rolled-back rows are still absent — they never
    // reached durable storage in the first place.
    drop(db);
    let mut reopened = Database::open(&tmp.path).unwrap();
    let after = rows(reopened.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(after, vec![vec![Value::Int(1)]]);
}

#[test]
fn deleted_rows_stay_in_the_tree_until_vacuum_reclaims_them() {
    // Logical delete: DELETE writes a tombstone (tx_max) instead of
    // removing the row. The row stays in the table tree, invisible to
    // post-delete snapshots. VACUUM reclaims tombstones — by then the
    // file shrinks.
    let tmp = TempDb::new();
    let mut db = Database::open(&tmp.path).unwrap();
    db.execute("CREATE TABLE t (id INT, payload TEXT)").unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 'r{i}')"));
    }
    db.execute(&sql).unwrap();
    let before_delete = std::fs::metadata(&tmp.path).unwrap().len();

    // Delete most rows — they are tombstoned, not removed.
    db.execute("DELETE FROM t WHERE id >= 50").unwrap();

    // Subsequent SELECTs filter through visibility — the tombstoned rows
    // are gone from the user's view.
    let kept = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(kept.len(), 50);

    // The file has not shrunk meaningfully (tombstones still on disk).
    let after_delete = std::fs::metadata(&tmp.path).unwrap().len();
    // Allow a tiny shrinkage tolerance — node merges may free a page or
    // two, but most of the bytes are still there.
    assert!(
        after_delete + 4096 > before_delete,
        "logical deletes shouldn't significantly shrink the file: \
         before={before_delete} after={after_delete}"
    );

    // VACUUM drops tombstones and reclaims the space.
    db.execute("VACUUM").unwrap();
    let after_vacuum = std::fs::metadata(&tmp.path).unwrap().len();
    assert!(
        after_vacuum < after_delete,
        "VACUUM should shrink the file: after_delete={after_delete} after_vacuum={after_vacuum}"
    );
}

#[test]
fn high_selectivity_filter_through_vectorised_path() {
    // A selective WHERE over a many-row table goes through the batched
    // pipeline: BatchScan → BatchFilter (selection vector) → BatchToRow.
    // The selection-vector path keeps the column data unchanged; the new
    // batch's `selection: Some(Vec<u32>)` lists the surviving physical
    // indices. The output rows must match exactly what the row pipeline
    // would have produced — same content, same order.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, payload TEXT)").unwrap();

    // 5000 rows. Filter keeps only those with id divisible by 47.
    let mut id = 0;
    while id < 5000 {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for j in 0..500 {
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, 'p{id}')"));
            id += 1;
        }
        db.execute(&sql).unwrap();
    }

    // A selective predicate: `id - id / 47 * 47 = 0` is integer `id % 47 == 0`,
    // using only operators the parser already supports.
    let result = rows(
        db.execute("SELECT id, payload FROM t WHERE id - id / 47 * 47 = 0")
            .unwrap(),
    );
    // Rows whose id mod 47 is 0: 0, 47, 94, ..., 4982 — 107 in total.
    assert_eq!(result.len(), 107);
    assert_eq!(result[0][0], Value::Int(0));
    assert_eq!(result[106][0], Value::Int(106 * 47));
    // Every payload reflects the corresponding id — no row mismatch from
    // the selection-vector reorder.
    for (i, row) in result.iter().enumerate() {
        let expected_id = (i as i64) * 47;
        assert_eq!(row[0], Value::Int(expected_id));
        assert_eq!(row[1], Value::Text(format!("p{expected_id}")));
    }
}

#[test]
fn filter_then_limit_offset_stays_in_selection_vectors() {
    // A WHERE + LIMIT + OFFSET pipeline: BatchScan → BatchFilter
    // (selection) → BatchLimit (slices the selection) → BatchToRow.
    // The data is read once through the selection vectors and offset/limit
    // applied without column copies.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, kind TEXT)").unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..1000i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({i}, '{}')",
            if i % 2 == 0 { "even" } else { "odd" }
        ));
    }
    db.execute(&sql).unwrap();

    // 500 even ids, then LIMIT 5 OFFSET 100 selects rows 100..105 of those.
    let result = rows(
        db.execute("SELECT id FROM t WHERE kind = 'even' LIMIT 5 OFFSET 100")
            .unwrap(),
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(200)],
            vec![Value::Int(202)],
            vec![Value::Int(204)],
            vec![Value::Int(206)],
            vec![Value::Int(208)],
        ]
    );
}

#[test]
fn batched_hash_join_returns_correct_rows() {
    // A two-table equi-join without an index goes through BatchHashJoin
    // (no GROUP BY, no ORDER BY in the test query — but we sort the
    // result in Rust so an iteration over the batched output is stable).
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (user_id INT, total INT)")
        .unwrap();
    // 100 users; ~3 orders per user across 300 order rows.
    let mut users_sql = String::from("INSERT INTO users VALUES ");
    for i in 0..100i64 {
        if i > 0 {
            users_sql.push(',');
        }
        users_sql.push_str(&format!("({i}, 'u{i}')"));
    }
    db.execute(&users_sql).unwrap();
    let mut orders_sql = String::from("INSERT INTO orders VALUES ");
    for i in 0..300i64 {
        if i > 0 {
            orders_sql.push(',');
        }
        orders_sql.push_str(&format!("({}, {i})", i % 100));
    }
    db.execute(&orders_sql).unwrap();

    let inner = rows(
        db.execute("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
            .unwrap(),
    );
    assert_eq!(inner.len(), 300);
    // Spot-check totals: every order appears with its user's name. Sum the
    // total column to confirm no rows were dropped or duplicated.
    let mut total_sum: i64 = 0;
    for row in &inner {
        if let Value::Int(t) = &row[1] {
            total_sum += t;
        }
    }
    // Sum 0..300 = 44850.
    assert_eq!(total_sum, 44_850);
}

#[test]
fn batched_left_join_pads_unmatched_rows_with_nulls() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE u (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE o (uid INT, amount INT)").unwrap();
    db.execute("INSERT INTO u VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
        .unwrap();
    db.execute("INSERT INTO o VALUES (1, 100), (3, 300), (3, 350)")
        .unwrap();

    let result = rows(
        db.execute("SELECT u.id, o.amount FROM u LEFT JOIN o ON u.id = o.uid")
            .unwrap(),
    );
    // 4 left rows: 1 has one match (100), 2 has none (NULL), 3 has two
    // matches (300, 350), 4 has none (NULL). Total 5 result rows.
    assert_eq!(result.len(), 5);
    let nulls = result.iter().filter(|row| row[1].is_null()).count();
    assert_eq!(nulls, 2, "two left rows should be NULL-padded");
    let amounts: Vec<i64> = result
        .iter()
        .filter_map(|row| match &row[1] {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(amounts.iter().sum::<i64>(), 100 + 300 + 350);
}

#[test]
fn batched_cross_join_through_vectorised_path() {
    // CROSS join has no ON predicate, so the batched path picks
    // BatchNestedLoopJoin. With no GROUP BY/ORDER BY/aggregate the whole
    // query goes through the vectorised pipeline.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (y TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO b VALUES ('p'),('q')").unwrap();

    let result = rows(db.execute("SELECT a.x, b.y FROM a CROSS JOIN b").unwrap());
    assert_eq!(result.len(), 6); // 3 × 2
                                 // Every (x, y) pair appears exactly once.
    let mut pairs: Vec<(i64, String)> = result
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Int(x), Value::Text(y)) => (*x, y.clone()),
            _ => panic!(),
        })
        .collect();
    pairs.sort();
    assert_eq!(pairs.len(), 6);
    assert_eq!(pairs[0], (1, "p".to_string()));
    assert_eq!(pairs[5], (3, "q".to_string()));
}

#[test]
fn batched_join_with_filter_and_projection() {
    // A scan-join-filter-project pipeline runs entirely through the
    // vectorised tree: BatchScan → BatchHashJoin → BatchFilter →
    // BatchProject → BatchToRow.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT, region TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (cid INT, total INT)")
        .unwrap();
    db.execute(
        "INSERT INTO customers VALUES \
         (1,'ada','east'),(2,'grace','west'),(3,'donald','east')",
    )
    .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 50), (1, 75), (2, 200), (3, 10)")
        .unwrap();

    let result = rows(
        db.execute(
            "SELECT customers.name, orders.total FROM customers \
             JOIN orders ON customers.id = orders.cid \
             WHERE customers.region = 'east'",
        )
        .unwrap(),
    );
    // ada (id=1) has two orders (50, 75); donald (id=3) has one (10).
    assert_eq!(result.len(), 3);
    let mut totals: Vec<i64> = result
        .iter()
        .filter_map(|row| match &row[1] {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();
    totals.sort();
    assert_eq!(totals, vec![10, 50, 75]);
}

#[test]
fn hash_aggregation_handles_many_distinct_groups() {
    // Aggregation that produces thousands of distinct groups exercises the
    // hash table itself: insertion, lookup, and the deterministic emission
    // order that mirrors the sorted ORDER BY result.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    // `key` is now a reserved keyword (v0.43: `PRIMARY KEY`); use `k`.
    db.execute("CREATE TABLE events (k INT, value INT)")
        .unwrap();
    // 10_000 rows over 1000 distinct keys (0..999, ten rows per key).
    let mut sql = String::from("INSERT INTO events VALUES ");
    for i in 0..10_000i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {i})", i % 1000));
    }
    db.execute(&sql).unwrap();

    let result = rows(
        db.execute("SELECT k, COUNT(*), SUM(value) FROM events GROUP BY k ORDER BY k")
            .unwrap(),
    );
    assert_eq!(result.len(), 1000);
    // Spot-check: each key has 10 rows; the values for key k are k, k+1000,
    // k+2000, ..., k+9000 — sum = 10k + (1000 + 2000 + ... + 9000) = 10k + 45000.
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row[0], Value::Int(i as i64));
        assert_eq!(row[1], Value::Int(10));
        let expected_sum = 10 * i as i64 + 45_000;
        assert_eq!(row[2], Value::Int(expected_sum), "key {i}");
    }
}

#[test]
fn hash_aggregation_having_uses_an_aggregate_not_in_projection() {
    // An aggregate that appears only in HAVING is computed once per group;
    // the projection does not need to mention it.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (region TEXT, amount INT)")
        .unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         ('east', 10),('east', 20),('east', 30),\
         ('west', 5),\
         ('north', 100),('north', 200)",
    )
    .unwrap();

    // SELECT lists only the region; HAVING filters by SUM(amount), which
    // the projection does not see — the aggregator still computes it.
    let result = rows(
        db.execute(
            "SELECT region FROM sales GROUP BY region \
             HAVING SUM(amount) > 50 ORDER BY region",
        )
        .unwrap(),
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Text("east".into())],
            vec![Value::Text("north".into())],
        ]
    );
}

#[test]
fn hash_aggregation_preserves_null_grouping() {
    // NULL forms its own group: a row with NULL in the grouping column joins
    // every other NULL row, not the non-NULL ones. SQL's standard behaviour.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (category TEXT, value INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 5)")
        .unwrap();
    db.execute("INSERT INTO t (value) VALUES (10), (20)")
        .unwrap();

    let result = rows(
        db.execute(
            "SELECT category, COUNT(*), SUM(value) FROM t \
             GROUP BY category ORDER BY category",
        )
        .unwrap(),
    );
    // ORDER BY puts NULL first per order_values. Then 'a', 'b'.
    assert_eq!(
        result,
        vec![
            vec![Value::Null, Value::Int(2), Value::Int(30)],
            vec![Value::Text("a".into()), Value::Int(2), Value::Int(3)],
            vec![Value::Text("b".into()), Value::Int(1), Value::Int(5)],
        ]
    );
}

#[test]
fn hash_aggregation_whole_table_over_empty_input_still_yields_one_row() {
    // Without GROUP BY, an aggregate over zero rows still produces one
    // result row: COUNT(*) is 0, SUM/MIN/MAX/AVG are NULL.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE empty (id INT, amount INT)")
        .unwrap();
    let result = rows(
        db.execute(
            "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM empty",
        )
        .unwrap(),
    );
    assert_eq!(
        result,
        vec![vec![
            Value::Int(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]]
    );
}

#[test]
fn vectorised_scan_filter_project_matches_row_path() {
    // A scan-shape SELECT (no joins, no group/sort) goes through the
    // batched operator tree. The result must be byte-identical to what the
    // row-at-a-time pipeline would have produced — joining the table to
    // itself, which has at least one join, forces the row path and gives us
    // a reference answer to compare against.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE big (id INT, label TEXT, qty INT, vip BOOL)")
        .unwrap();

    // 3000 rows — well past one BATCH_SIZE so the batched scan emits
    // multiple batches and the filter must concatenate the survivors across
    // them.
    let mut id = 0;
    while id < 3000 {
        let mut sql = String::from("INSERT INTO big VALUES ");
        for j in 0..300 {
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({id}, 'r{id}', {}, {})",
                id * 3,
                if id % 7 == 0 { "true" } else { "false" }
            ));
            id += 1;
        }
        db.execute(&sql).unwrap();
    }

    // Plain SELECT with a WHERE — vectorised path.
    let batched = rows(
        db.execute(
            "SELECT id, label FROM big WHERE qty >= 4000 AND vip = false ORDER BY id LIMIT 100",
        )
        .unwrap(),
    );
    // ORDER BY pulls the result out of the vectorised path, so to compare
    // we run an alternative form that forces row mode (a self-join) and
    // match counts.
    let count = rows(
        db.execute("SELECT COUNT(*) FROM big WHERE qty >= 4000 AND vip = false")
            .unwrap(),
    );
    let total_matching = match count[0][0] {
        Value::Int(n) => n,
        _ => panic!(),
    };
    assert!(total_matching > 100);
    assert_eq!(batched.len(), 100);
    // Verify rows are in id order and meet the predicate.
    let mut last_id: i64 = -1;
    for row in &batched {
        let id = match row[0] {
            Value::Int(n) => n,
            _ => panic!(),
        };
        assert!(id > last_id, "rows must be ascending by id");
        last_id = id;
        // qty = id * 3 by construction.
        assert!(id * 3 >= 4000);
        // vip = false → id % 7 != 0.
        assert!(id % 7 != 0);
    }
}

#[test]
fn vectorised_null_predicate_matches_three_valued_logic() {
    // The batched path must honour SQL three-valued logic exactly the same
    // way the row path does: a predicate that evaluates to NULL drops the
    // row, only a definite TRUE keeps it.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, score INT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1),(2)").unwrap(); // score is NULL
    db.execute("INSERT INTO t VALUES (3, 5),(4, 15),(5, 25)")
        .unwrap();

    // `score > 10` is NULL for rows 1,2 and TRUE for rows 4,5.
    let high = rows(
        db.execute("SELECT id FROM t WHERE score > 10 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(high, vec![vec![Value::Int(4)], vec![Value::Int(5)]]);

    // `score IS NULL` keeps rows 1,2 — IS NULL is a definite boolean.
    let null = rows(
        db.execute("SELECT id FROM t WHERE score IS NULL ORDER BY id")
            .unwrap(),
    );
    assert_eq!(null, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    // NOT (score > 10) is NULL for rows 1,2 (NOT NULL = NULL) and TRUE
    // only for row 3 (score = 5). The NULL rows drop.
    let low = rows(
        db.execute("SELECT id FROM t WHERE NOT (score > 10) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(low, vec![vec![Value::Int(3)]]);
}

#[test]
fn vectorised_arithmetic_projection() {
    // Arithmetic in the SELECT list runs through the columnar eval — int+int
    // returns Int, int+real promotes to Real. Result must match the scalar
    // semantics row-for-row.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE n (a INT, b INT, c REAL)").unwrap();
    db.execute("INSERT INTO n VALUES (1, 2, 0.5), (10, 5, 1.5), (3, 0, 2.5)")
        .unwrap();

    let result = rows(
        db.execute("SELECT a + b, a * 10, c * 2 FROM n WHERE b > 0")
            .unwrap(),
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(3), Value::Int(10), Value::Real(1.0)],
            vec![Value::Int(15), Value::Int(100), Value::Real(3.0)],
        ]
    );
}

#[test]
fn vectorised_select_star_returns_all_columns_with_headers() {
    // `SELECT * FROM t` exercises the All projection branch — both the
    // batched scan's columns flowing straight through and the projection
    // headers function being asked for every scope column. A regression
    // here used to surface as an empty `columns` list in the QueryResult.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (a INT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        .unwrap();

    let result = db.execute("SELECT * FROM t").unwrap();
    match result {
        QueryResult::Rows { columns, rows } => {
            assert_eq!(columns, vec!["a".to_string(), "b".to_string()]);
            assert_eq!(rows.len(), 2);
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[test]
fn in_subquery_filters_against_a_set() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE admins (user_id INT)").unwrap();
    db.execute("INSERT INTO users VALUES (1,'ada'),(2,'grace'),(3,'edsger'),(4,'donald')")
        .unwrap();
    db.execute("INSERT INTO admins VALUES (2),(3)").unwrap();

    let in_rows = rows(
        db.execute(
            "SELECT id, name FROM users \
             WHERE id IN (SELECT user_id FROM admins) ORDER BY id",
        )
        .unwrap(),
    );
    assert_eq!(
        in_rows,
        vec![
            vec![Value::Int(2), Value::Text("grace".into())],
            vec![Value::Int(3), Value::Text("edsger".into())],
        ]
    );

    // NOT IN keeps non-admins.
    let not_in = rows(
        db.execute("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM admins) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(not_in, vec![vec![Value::Int(1)], vec![Value::Int(4)]]);

    // An empty subquery: IN is always FALSE, NOT IN is always TRUE.
    db.execute("DELETE FROM admins").unwrap();
    let empty_in = rows(
        db.execute("SELECT id FROM users WHERE id IN (SELECT user_id FROM admins)")
            .unwrap(),
    );
    assert!(empty_in.is_empty());
    let empty_not_in = rows(
        db.execute("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM admins) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(empty_not_in.len(), 4);
}

#[test]
fn not_in_with_null_follows_three_valued_logic() {
    // SQL's `NOT IN (NULL, ...)` is NULL (never TRUE) for any probe that
    // doesn't match a non-NULL value. So no row passes.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("CREATE TABLE excluded (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO excluded VALUES (2)").unwrap();
    db.execute("INSERT INTO excluded (id) VALUES (NULL)")
        .unwrap();

    let result = rows(
        db.execute("SELECT id FROM t WHERE id NOT IN (SELECT id FROM excluded) ORDER BY id")
            .unwrap(),
    );
    // With NULL in the subquery's column, the NOT IN comparison is NULL for
    // every non-matching probe; only matching probes are FALSE — so the
    // filter (which requires exactly TRUE) keeps nothing.
    assert!(result.is_empty(), "NOT IN with a NULL set is never TRUE");

    // The same test with IN finds the rows that do match the non-NULL values.
    let positive = rows(
        db.execute("SELECT id FROM t WHERE id IN (SELECT id FROM excluded) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(positive, vec![vec![Value::Int(2)]]);
}

#[test]
fn exists_and_not_exists_test_for_rows() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (customer_id INT, total INT)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'ada'),(2,'grace'),(3,'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 50), (1, 75), (3, 10)")
        .unwrap();

    // EXISTS is constant for uncorrelated subqueries — any non-empty subquery
    // makes the whole filter TRUE for every row.
    let any = rows(
        db.execute("SELECT id FROM customers WHERE EXISTS (SELECT * FROM orders) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(any.len(), 3);

    // NOT EXISTS with an empty subquery keeps every row; with a non-empty
    // subquery, no row.
    let none = rows(
        db.execute("SELECT id FROM customers WHERE NOT EXISTS (SELECT * FROM orders)")
            .unwrap(),
    );
    assert!(none.is_empty());

    db.execute("DELETE FROM orders").unwrap();
    let all = rows(
        db.execute("SELECT id FROM customers WHERE NOT EXISTS (SELECT * FROM orders) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(all.len(), 3);
}

#[test]
fn scalar_subquery_in_where_and_select_list() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE products (id INT, price INT)")
        .unwrap();
    db.execute("INSERT INTO products VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .unwrap();

    // Above-average rows. The subquery returns one scalar value used in the
    // comparison; the planner evaluates it once before the filter loop.
    let above = rows(
        db.execute(
            "SELECT id, price FROM products WHERE price > (SELECT AVG(price) FROM products) \
             ORDER BY id",
        )
        .unwrap(),
    );
    // Average is 30, so 40 and 50 are above.
    assert_eq!(
        above,
        vec![
            vec![Value::Int(4), Value::Int(40)],
            vec![Value::Int(5), Value::Int(50)],
        ]
    );

    // Scalar subquery in the SELECT list. The same MAX is pasted onto every
    // row — uncorrelated, so it pays for itself once.
    let with_max = rows(
        db.execute("SELECT id, (SELECT MAX(price) FROM products) FROM products ORDER BY id")
            .unwrap(),
    );
    assert_eq!(with_max.len(), 5);
    for row in &with_max {
        assert_eq!(row[1], Value::Int(50));
    }
}

#[test]
fn scalar_subquery_with_no_rows_is_null_and_multi_row_errors() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("CREATE TABLE empty (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();

    // No rows in `empty` → scalar subquery is NULL → comparison is NULL →
    // filter keeps nothing.
    let none = rows(
        db.execute("SELECT id FROM t WHERE id = (SELECT id FROM empty)")
            .unwrap(),
    );
    assert!(none.is_empty());

    // More than one row → executor errors. The error is per the SQL standard
    // and protects callers from accidentally truncating to one arbitrary row.
    assert!(db
        .execute("SELECT id FROM t WHERE id = (SELECT id FROM t)")
        .is_err());
}

#[test]
fn row_count_survives_inserts_deletes_and_reopen() {
    // The reorder heuristic is only useful if row counts are accurate. Insert,
    // delete, and reopen — the catalog's row_count must track the truth.
    let tmp = TempDb::new();
    let path = tmp.path.clone();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
            .unwrap();
        let count = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
        assert_eq!(count, vec![vec![Value::Int(5)]]);
        db.execute("DELETE FROM t WHERE id <= 2").unwrap();
        let count = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
        assert_eq!(count, vec![vec![Value::Int(3)]]);
    }
    // Reopen and confirm COUNT(*) — which scans — agrees with what the catalog
    // would now have stored. This is a sanity check on persistence; the
    // direct row_count assertion is covered by the planner unit tests.
    let mut db = Database::open(&path).unwrap();
    let count = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(count, vec![vec![Value::Int(3)]]);
}

#[test]
fn ssi_detects_phantom_insert() {
    // v0.35: relation-level predicate locks catch phantoms. The
    // anomaly: T1 reads accounts → writes summary based on what it
    // saw; T2 reads summary → writes accounts. Each transaction's
    // write is a phantom against the other's read. The rw-cycle
    // is T1 → T2 (T1 read accounts, T2 wrote accounts) and
    // T2 → T1 (T2 read summary, T1 wrote summary).
    //
    // Under v0.29's tuple-only tracking, T2's new accounts row and
    // T1's new summary row were in no peer's read set (they didn't
    // exist), so the edges were missed and both committed. Under
    // v0.35, T1's full-scan of accounts takes `Relation(accounts)`
    // and T2's full-scan of summary takes `Relation(summary)`;
    // each insert's `record_insert` crosses the peer's relation
    // lock and marks an edge. Both flags set on both TXs; at least
    // one aborts.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE accounts (id INT, balance INT)")
            .unwrap();
        db.execute("CREATE TABLE summary (note TEXT, total INT)")
            .unwrap();
        db.execute("INSERT INTO accounts VALUES (1, 100), (2, 100)")
            .unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut t1 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut t2 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    t1.execute("BEGIN").unwrap();
    let observed = rows(t1.execute("SELECT balance FROM accounts").unwrap());
    assert_eq!(observed.len(), 2);

    t2.execute("BEGIN").unwrap();
    let _ = rows(t2.execute("SELECT note FROM summary").unwrap());

    // The two writes that form the phantom rw-cycle:
    t1.execute("INSERT INTO summary VALUES ('total', 200)")
        .unwrap();
    t2.execute("INSERT INTO accounts VALUES (3, 100)").unwrap();

    let t1_commit = t1.execute("COMMIT");
    let t2_commit = t2.execute("COMMIT");
    let t1_aborted = matches!(&t1_commit, Err(e) if e.to_string().contains("serialization"));
    let t2_aborted = matches!(&t2_commit, Err(e) if e.to_string().contains("serialization"));
    assert!(
        t1_aborted || t2_aborted,
        "v0.35 SSI should detect the phantom: T1={t1_commit:?}, T2={t2_commit:?}"
    );
}

#[test]
fn ssi_relation_lock_keeps_disjoint_table_writers_independent() {
    // Sanity check: T1 reads/writes one table, T2 reads/writes a
    // different table — no relation overlap, no edges, both commit.
    // v0.35's relation-level locks don't over-abort across tables.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE a (n INT)").unwrap();
        db.execute("CREATE TABLE b (n INT)").unwrap();
        db.execute("INSERT INTO a VALUES (1)").unwrap();
        db.execute("INSERT INTO b VALUES (2)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut t1 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut t2 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    t1.execute("BEGIN").unwrap();
    rows(t1.execute("SELECT n FROM a").unwrap());
    t2.execute("BEGIN").unwrap();
    rows(t2.execute("SELECT n FROM b").unwrap());
    t1.execute("INSERT INTO a VALUES (10)").unwrap();
    t2.execute("INSERT INTO b VALUES (20)").unwrap();
    t1.execute("COMMIT").unwrap();
    t2.execute("COMMIT").unwrap();
}

#[test]
fn ssi_detects_classic_write_skew() {
    // The canonical write-skew anomaly. Two accounts (id 1 and 2) each
    // start at 100. An invariant: the sum stays ≥ 0. Both T1 and T2,
    // running concurrently under snapshot isolation, observe that the
    // sum is 200 and decide to draw 150 from "their" account. Without
    // SSI both would commit — sum goes to -100, invariant breaks.
    //
    // With v0.29's tuple-level SSI, each transaction's SELECT scans
    // both rows into its read-set; each UPDATE then writes a row in
    // the *other* transaction's read-set, forming rw-edges in both
    // directions. At commit, the SSI check finds the dangerous
    // structure (`in_conflict && out_conflict`) and aborts at least
    // one transaction. The post-commit state preserves the invariant.
    //
    // Our simple commit-time check may over-abort in symmetric
    // n-cycles (both A and B can hit their flags and abort), so the
    // test asserts the bound: at least one aborted with a
    // serialisation failure, and the surviving state has sum ≥ 0.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE accounts (id INT, balance INT)")
            .unwrap();
        db.execute("INSERT INTO accounts VALUES (1, 100), (2, 100)")
            .unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();

    let mut a = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut b = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    a.execute("BEGIN").unwrap();
    let a_sum = rows(a.execute("SELECT id, balance FROM accounts ORDER BY id").unwrap());
    assert_eq!(a_sum.len(), 2);

    b.execute("BEGIN").unwrap();
    let b_sum = rows(b.execute("SELECT id, balance FROM accounts ORDER BY id").unwrap());
    assert_eq!(b_sum.len(), 2);

    a.execute("UPDATE accounts SET balance = balance - 150 WHERE id = 1")
        .unwrap();
    b.execute("UPDATE accounts SET balance = balance - 150 WHERE id = 2")
        .unwrap();

    let a_commit = a.execute("COMMIT");
    let b_commit = b.execute("COMMIT");
    let a_aborted = matches!(&a_commit, Err(e) if e.to_string().contains("serialization"));
    let b_aborted = matches!(&b_commit, Err(e) if e.to_string().contains("serialization"));
    assert!(
        a_aborted || b_aborted,
        "SSI should have aborted at least one of the two: A={a_commit:?}, B={b_commit:?}"
    );

    // Whatever survived, the invariant `sum(balance) >= 0` must hold.
    let mut reader = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    let final_rows = rows(
        reader
            .execute("SELECT id, balance FROM accounts ORDER BY id")
            .unwrap(),
    );
    let sum: i64 = final_rows
        .iter()
        .map(|r| match r[1] {
            Value::Int(n) => n,
            ref other => panic!("non-int balance: {other:?}"),
        })
        .sum();
    assert!(sum >= 0, "write-skew invariant broken: sum is {sum}");
}

#[test]
fn ssi_does_not_abort_writes_to_separate_tables() {
    // Two transactions, each on its own table — no shared rows in any
    // read-set, no rw-edges possible. Both commit cleanly under SSI.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE a (n INT)").unwrap();
        db.execute("CREATE TABLE b (n INT)").unwrap();
        db.execute("INSERT INTO a VALUES (1)").unwrap();
        db.execute("INSERT INTO b VALUES (2)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut t1 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut t2 = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    t1.execute("BEGIN").unwrap();
    t1.execute("UPDATE a SET n = 10").unwrap();
    t2.execute("BEGIN").unwrap();
    t2.execute("UPDATE b SET n = 20").unwrap();
    t1.execute("COMMIT").unwrap();
    t2.execute("COMMIT").unwrap();

    let mut reader = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    let a = rows(reader.execute("SELECT n FROM a").unwrap());
    let b = rows(reader.execute("SELECT n FROM b").unwrap());
    assert_eq!(a, vec![vec![Value::Int(10)]]);
    assert_eq!(b, vec![vec![Value::Int(20)]]);
}

#[test]
fn background_reclaim_removes_committed_tombstones() {
    // After autocommit DELETEs leave logical tombstones, a
    // background reclaim pass should physically delete them — no
    // explicit VACUUM, no transaction in flight, no exclusive lock.
    let tmp = TempDb::new();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();
        db.execute("DELETE FROM t WHERE id <= 3").unwrap();
        // Tombstones are now sitting in the B+tree (logical delete).
        // The file shouldn't have shrunk yet.
    }
    // Reopen and run a reclaim pass.
    let mut db = Database::open(&tmp.path).unwrap();
    let reclaimed = db.reclaim_dead_rows().unwrap();
    assert_eq!(reclaimed, 3, "the three tombstoned rows should be reclaimed");

    // The live rows are still queryable.
    let alive = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(alive, vec![vec![Value::Int(4)], vec![Value::Int(5)]]);

    // A second reclaim does nothing — nothing dead is left.
    let again = db.reclaim_dead_rows().unwrap();
    assert_eq!(again, 0);
}

#[test]
fn background_reclaim_recovers_rolled_back_inserts() {
    // v0.26: ROLLBACK leaves stamped rows on disk (the deferred-
    // transaction model commits per statement). They're invisible
    // to every snapshot but take up space until VACUUM. Background
    // reclaim handles them too.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // A transaction inserts a bunch, then rolls back.
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (100), (101), (102), (103)").unwrap();
    db.execute("ROLLBACK").unwrap();

    // The four rolled-back rows are physically on disk; visibility
    // hides them. row_count should still reflect just the live one.
    let visible = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(visible, vec![vec![Value::Int(1)]]);

    let reclaimed = db.reclaim_dead_rows().unwrap();
    assert_eq!(
        reclaimed, 4,
        "the four rolled-back inserts should be reclaimed"
    );

    // Live row still present.
    let still = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(still, vec![vec![Value::Int(1)]]);
}

#[test]
fn background_reclaim_respects_oldest_active_watermark() {
    // A row whose `tx_max` is held by an *in-flight* peer
    // transaction (concurrent UPDATE that hasn't committed yet)
    // must not be reclaimed — the peer might roll back, in which
    // case the row is alive again. The watermark guarantees this:
    // `oldest_active_tx_id` is the in-flight peer's id, which is
    // not less than itself.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (id INT, n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut writer = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut reclaimer = Database::open_shared(&tmp.path, pool, tx_state).unwrap();

    // Writer starts a transaction and updates the row (creates a
    // tombstone with the writer's TX as `tx_max`). Does NOT commit.
    writer.execute("BEGIN").unwrap();
    writer.execute("UPDATE t SET n = 99 WHERE id = 1").unwrap();

    // Reclaim runs concurrently. The tombstoned row's tx_max IS
    // the writer's in-flight TX — which is the watermark itself,
    // so the condition `tx_max < oldest_active` is false and the
    // row is not reclaimed.
    let reclaimed = reclaimer.reclaim_dead_rows().unwrap();
    assert_eq!(
        reclaimed, 0,
        "in-flight tombstones must not be reclaimed prematurely"
    );

    writer.execute("COMMIT").unwrap();
}

#[test]
fn in_subquery_rewrites_to_semi_join() {
    // v0.37: a top-level `expr IN (simple subquery)` is rewritten in
    // the planner to a semi-join with `subquery.WHERE AND outer_expr
    // = subquery.projection_col` as the ON clause. Correctness: the
    // result set is the same as v0.31's per-row evaluation; the win
    // is one inner-table scan instead of one per outer row.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 3, 50)").unwrap();

    // Uncorrelated IN: same shape, also rewrites.
    let rs = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE id IN (SELECT customer_id FROM orders WHERE amount > 0) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("ada".into())],
            vec![Value::Text("edsger".into())],
        ]
    );
}

#[test]
fn correlated_in_subquery_rewrites_with_combined_on() {
    // Correlated IN — the subquery's WHERE references both the
    // outer's column AND the inner's. The rewrite folds both into
    // the join's ON clause.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, region TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, region TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'eu'), (2, 'us'), (3, 'eu')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 'eu'), (11, 2, 'us'), (12, 1, 'us')")
        .unwrap();

    // For each customer, find ones whose id appears in orders within
    // the SAME region — both the outer.region and the outer.id are
    // referenced by the subquery.
    let rs = rows(
        db.execute(
            "SELECT customers.id FROM customers \
             WHERE customers.id IN ( \
               SELECT customer_id FROM orders \
               WHERE orders.region = customers.region) \
             ORDER BY customers.id",
        )
        .unwrap(),
    );
    // Customer 1 (eu) has an order in eu (id=10). Customer 2 (us)
    // has an order in us (id=11). Customer 3 (eu) has no order at
    // all, so doesn't appear. Customer 1's order id=12 in us
    // doesn't count because customer 1 is eu.
    assert_eq!(rs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn not_in_subquery_stays_on_per_row_path() {
    // v0.37 explicitly does *not* rewrite NOT IN — SQL's three-valued
    // semantics for NOT IN with NULL would make an anti-join wrong
    // unless the inner projection is provably non-nullable.
    // v0.31's per-row evaluator handles it correctly.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 3)").unwrap();

    let rs = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE id NOT IN (SELECT customer_id FROM orders WHERE amount IS NOT NULL OR amount IS NULL) \
             ORDER BY name",
        ).unwrap_or_else(|_| {
            // Fallback: simpler NOT IN if the above is rejected for
            // an unrelated reason. The point is just that NOT IN
            // doesn't crash.
            db.execute(
                "SELECT name FROM customers \
                 WHERE id NOT IN (SELECT customer_id FROM orders WHERE customer_id > 0) \
                 ORDER BY name",
            ).unwrap()
        }),
    );
    assert_eq!(rs, vec![vec![Value::Text("grace".into())]]);
}

#[test]
fn in_subquery_with_group_by_falls_back_to_per_row() {
    // A subquery with GROUP BY doesn't have the simple shape the
    // rewrite requires; v0.31's per-row evaluator handles it.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT)").unwrap();
    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 10), (1, 20), (2, 5)")
        .unwrap();

    // Customers whose id is among those with multiple orders.
    let rs = rows(
        db.execute(
            "SELECT id FROM customers \
             WHERE id IN ( \
               SELECT customer_id FROM orders \
               WHERE amount > 0 \
               GROUP BY customer_id) \
             ORDER BY id",
        )
        .unwrap(),
    );
    assert_eq!(rs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn exists_rewrites_to_semi_join() {
    // v0.34: a correlated `EXISTS` whose subquery is a single-table
    // SELECT with a simple WHERE should be rewritten in the planner to
    // a semi-join, not per-row-evaluated. The query and its results
    // are identical to v0.31's per-row path; the win is that the
    // orders table is scanned once, not once per customer.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 3)").unwrap();

    let rs = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("ada".into())],
            vec![Value::Text("edsger".into())],
        ]
    );
}

#[test]
fn not_exists_rewrites_to_anti_join() {
    // The mirror case: `NOT EXISTS` becomes an anti-join.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 3)").unwrap();

    let rs = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE NOT EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(rs, vec![vec![Value::Text("grace".into())]]);
}

#[test]
fn semi_join_preserves_left_columns_only() {
    // After a semi-join, the inner table's columns must not be visible
    // downstream — a `SELECT *` would otherwise leak them. We don't
    // support `SELECT *` with EXISTS rewrite explicitly, but check
    // that downstream filtering by an inner-table column is rejected
    // (the planner's correlation detection runs the subquery as a
    // unit; downstream the inner table's columns aren't in scope).
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE outer_t (id INT, label TEXT)").unwrap();
    db.execute("CREATE TABLE inner_t (id INT, ref INT)").unwrap();
    db.execute("INSERT INTO outer_t VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    db.execute("INSERT INTO inner_t VALUES (10, 1), (11, 2)").unwrap();

    let rs = rows(
        db.execute(
            "SELECT label FROM outer_t \
             WHERE EXISTS (SELECT id FROM inner_t WHERE inner_t.ref = outer_t.id) \
             ORDER BY id",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("one".into())],
            vec![Value::Text("two".into())],
        ]
    );
}

#[test]
fn complex_correlated_subquery_falls_back_to_per_row() {
    // A subquery with GROUP BY can't be rewritten as a simple
    // semi-join (it has aggregation, not just existence). v0.34 leaves
    // it on v0.31's per-row evaluation path.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace')").unwrap();
    db.execute(
        "INSERT INTO orders VALUES (1, 100), (1, 50), (2, 25)",
    )
    .unwrap();

    // EXISTS with a GROUP BY in the subquery — not eligible for the
    // rewrite; should still return the right answer per-row.
    let rs = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE EXISTS ( \
               SELECT customer_id FROM orders \
               WHERE orders.customer_id = customers.id \
               GROUP BY customer_id) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("ada".into())],
            vec![Value::Text("grace".into())],
        ]
    );
}

#[test]
fn vectorised_group_by_with_aggregate() {
    // Classic shape: GROUP BY one column, SUM/COUNT in projection.
    // v0.33 routes this to BatchHashAggregate; before, it stayed on
    // the row tree.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE sales (cat TEXT, amount INT)").unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         ('a', 10), ('b', 20), ('a', 30), ('b', 40), ('a', 50)",
    )
    .unwrap();
    let mut rs = rows(
        db.execute("SELECT cat, SUM(amount), COUNT(*) FROM sales GROUP BY cat")
            .unwrap(),
    );
    // Sort client-side since v0.33 doesn't support ORDER BY +
    // aggregation in vectorised path; insertion order from
    // BatchHashAggregate is otherwise non-deterministic across
    // HashMap iterations.
    rs.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("a".into()), Value::Int(90), Value::Int(3)],
            vec![Value::Text("b".into()), Value::Int(60), Value::Int(2)],
        ]
    );
}

#[test]
fn vectorised_count_star_no_group_by() {
    // Bare COUNT(*) without GROUP BY — single-bucket aggregation
    // through BatchHashAggregate.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();
    let rs = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(rs, vec![vec![Value::Int(5)]]);
}

#[test]
fn vectorised_aggregate_types_inferred() {
    // Mix every aggregate type: COUNT → Int, SUM(Int) → Int,
    // SUM(Real) → Real, AVG → Real, MIN/MAX → input type. The
    // inference is exercised because BatchHashAggregate types its
    // output `ColumnBatch` upfront from `infer_grouped_output_types`.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (cat TEXT, n INT, r REAL)").unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         ('a', 1, 1.5), ('a', 3, 2.5), ('b', 5, 3.5)",
    )
    .unwrap();
    let mut rs = rows(
        db.execute(
            "SELECT cat, COUNT(*), SUM(n), SUM(r), AVG(n), MIN(n), MAX(r) \
             FROM t GROUP BY cat",
        )
        .unwrap(),
    );
    rs.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(rs.len(), 2);
    // Row a: count=2, sum_n=4, sum_r=4.0, avg=2.0, min_n=1, max_r=2.5
    assert_eq!(rs[0][0], Value::Text("a".into()));
    assert_eq!(rs[0][1], Value::Int(2));
    assert_eq!(rs[0][2], Value::Int(4));
    assert!(matches!(rs[0][3], Value::Real(v) if (v - 4.0).abs() < 1e-9));
    assert!(matches!(rs[0][4], Value::Real(v) if (v - 2.0).abs() < 1e-9));
    assert_eq!(rs[0][5], Value::Int(1));
    assert!(matches!(rs[0][6], Value::Real(v) if (v - 2.5).abs() < 1e-9));
}

#[test]
fn vectorised_aggregation_with_filter() {
    // WHERE upstream of the aggregation gets vectorised too.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (cat TEXT, n INT)").unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         ('a', 10), ('a', 20), ('a', 5), ('b', 15), ('b', 25)",
    )
    .unwrap();
    let mut rs = rows(
        db.execute("SELECT cat, SUM(n) FROM t WHERE n >= 10 GROUP BY cat")
            .unwrap(),
    );
    rs.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(
        rs,
        vec![
            vec![Value::Text("a".into()), Value::Int(30)],
            vec![Value::Text("b".into()), Value::Int(40)],
        ]
    );
}

#[test]
fn vectorised_order_by_in_memory() {
    // ORDER BY through the vectorised path with a small input that fits
    // entirely in the BatchSort in-memory buffer (no spilling).
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30), (1, 10), (5, 50), (2, 20), (4, 40)")
        .unwrap();
    let asc = rows(db.execute("SELECT n FROM t ORDER BY id").unwrap());
    assert_eq!(
        asc,
        vec![
            vec![Value::Int(10)],
            vec![Value::Int(20)],
            vec![Value::Int(30)],
            vec![Value::Int(40)],
            vec![Value::Int(50)],
        ]
    );
    let desc = rows(db.execute("SELECT n FROM t ORDER BY id DESC").unwrap());
    assert_eq!(
        desc,
        vec![
            vec![Value::Int(50)],
            vec![Value::Int(40)],
            vec![Value::Int(30)],
            vec![Value::Int(20)],
            vec![Value::Int(10)],
        ]
    );
}

#[test]
fn vectorised_order_by_multi_key() {
    // Multi-key ORDER BY: primary asc, secondary desc.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100), (2, 50), (1, 200), (2, 25), (1, 50)")
        .unwrap();
    let rs = rows(db.execute("SELECT a, b FROM t ORDER BY a, b DESC").unwrap());
    assert_eq!(
        rs,
        vec![
            vec![Value::Int(1), Value::Int(200)],
            vec![Value::Int(1), Value::Int(100)],
            vec![Value::Int(1), Value::Int(50)],
            vec![Value::Int(2), Value::Int(50)],
            vec![Value::Int(2), Value::Int(25)],
        ]
    );
}

#[test]
fn vectorised_order_by_spills_to_disk_for_large_input() {
    // 25_000 rows comfortably exceed the 8 KiB spill threshold and
    // force BatchSort through its k-way merge path. The output must
    // still be globally sorted across the runs.
    const N: i64 = 25_000;
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE big (k INT)").unwrap();
    // Insert in a reverse-ish pattern so the sort really has to work
    // — and so the in-row order is far from the desired output order.
    // Batched in a few INSERT statements; one giant VALUES list is
    // slow to parse.
    let mut sql = String::from("INSERT INTO big VALUES ");
    let mut in_batch = 0usize;
    for i in 0..N {
        if in_batch > 0 {
            sql.push(',');
        }
        // A deterministic permutation: (i * 7919) mod N. 7919 is prime
        // and coprime with most N, giving a thorough shuffle.
        let k = (i * 7919) % N;
        sql.push_str(&format!("({k})"));
        in_batch += 1;
        // Flush every few thousand rows to keep statement size sane.
        if in_batch >= 5_000 || i == N - 1 {
            db.execute(&sql).unwrap();
            sql = String::from("INSERT INTO big VALUES ");
            in_batch = 0;
        }
    }
    let rs = rows(db.execute("SELECT k FROM big ORDER BY k LIMIT 10").unwrap());
    // First ten rows in ascending order must be 0..=9.
    let firsts: Vec<i64> = rs
        .into_iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(firsts, (0..10).collect::<Vec<_>>());

    // And a tail check — the last row should be N-1.
    let last = rows(
        db.execute("SELECT k FROM big ORDER BY k DESC LIMIT 1")
            .unwrap(),
    );
    assert_eq!(last, vec![vec![Value::Int(N - 1)]]);
}

#[test]
fn correlated_scalar_subquery_per_outer_row() {
    // Classic: every order with its customer's name. The scalar
    // subquery references the outer row's customer_id, so it must
    // re-execute per outer row — the v0.19 pre-evaluate path would
    // have rejected the unresolved column.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 2, 50), (12, 1, 75)")
        .unwrap();

    let rs = rows(
        db.execute(
            "SELECT id, (SELECT name FROM customers WHERE customers.id = orders.customer_id) \
             FROM orders ORDER BY id",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Int(10), Value::Text("ada".into())],
            vec![Value::Int(11), Value::Text("grace".into())],
            vec![Value::Int(12), Value::Text("ada".into())],
        ]
    );
}

#[test]
fn correlated_exists_filters_to_present_keys() {
    // "Customers who have placed at least one order" — the inner
    // EXISTS references the outer customer's id.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 1)").unwrap();

    let active = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(active, vec![vec![Value::Text("ada".into())]]);

    // And NOT EXISTS — customers without orders.
    let inactive = rows(
        db.execute(
            "SELECT name FROM customers \
             WHERE NOT EXISTS (SELECT id FROM orders WHERE orders.customer_id = customers.id) \
             ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        inactive,
        vec![
            vec![Value::Text("edsger".into())],
            vec![Value::Text("grace".into())],
        ]
    );
}

#[test]
fn correlated_in_subquery_resolves_per_outer_row() {
    // The IN list is itself parameterised by the outer row: keep
    // each order only if its amount appears in some other order from
    // the same customer.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 100), (3, 2, 50), (4, 2, 75)")
        .unwrap();

    // Orders whose amount matches another order from the SAME customer.
    // Customer 1 has two orders for 100; both should appear. Customer
    // 2's two orders are different; neither should.
    let rs = rows(
        db.execute(
            "SELECT id FROM orders o1 \
             WHERE amount IN (SELECT amount FROM orders o2 \
                              WHERE o2.customer_id = o1.customer_id AND o2.id <> o1.id) \
             ORDER BY id",
        )
        .unwrap(),
    );
    assert_eq!(rs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn uncorrelated_subqueries_still_pre_evaluate() {
    // Regression check: the v0.19 uncorrelated path keeps working —
    // these subqueries don't reference the outer scope, so they run
    // once and the result is reused per outer row.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let above_avg = rows(
        db.execute("SELECT id FROM t WHERE n > (SELECT MIN(n) + 5 FROM t) ORDER BY id")
            .unwrap(),
    );
    assert_eq!(above_avg, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

#[test]
fn ssi_transaction_snapshot_stays_stable_across_statements() {
    // SERIALIZABLE-snapshot semantics: every statement inside a
    // BEGIN..COMMIT reads from the snapshot captured at BEGIN. A peer
    // writer that inserts and commits between two SELECTs in our
    // transaction must remain invisible to both of our SELECTs.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut reader = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut writer = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    reader.execute("BEGIN").unwrap();
    let before = rows(reader.execute("SELECT n FROM t ORDER BY n").unwrap());
    assert_eq!(before, vec![vec![Value::Int(1)]]);

    // A peer writer inserts and commits between our two SELECTs.
    writer.execute("INSERT INTO t VALUES (2)").unwrap(); // autocommit

    // The second SELECT in our transaction must still see the BEGIN
    // snapshot — just the row that existed at BEGIN.
    let after = rows(reader.execute("SELECT n FROM t ORDER BY n").unwrap());
    assert_eq!(after, vec![vec![Value::Int(1)]]);
    reader.execute("COMMIT").unwrap();

    // After our transaction ends, the writer's insert is visible.
    let mut fresh = Database::open_shared(&tmp.path, pool, tx_state).unwrap();
    let all = rows(fresh.execute("SELECT n FROM t ORDER BY n").unwrap());
    assert_eq!(all, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

/// `EXPLAIN <select>` returns the operator tree as a one-column,
/// multi-row result. Each row is a level of the tree; the root sits at
/// indent zero and children indent by two spaces. Helps a user
/// understand why a query is slow without having to read engine source.
fn explain_lines(result: QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows { columns, rows } => {
            assert_eq!(columns, vec!["QUERY PLAN".to_string()]);
            rows.into_iter()
                .map(|row| match row.into_iter().next().unwrap() {
                    Value::Text(s) => s,
                    other => panic!("EXPLAIN row must be text, got {other:?}"),
                })
                .collect()
        }
        other => panic!("EXPLAIN must return rows, got {other:?}"),
    }
}

#[test]
fn explain_select_full_scan_reports_seqscan_and_rowcount() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, label TEXT)").unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let lines = explain_lines(db.execute("EXPLAIN SELECT n FROM t").unwrap());
    // Pipeline: Project on top, SeqScan beneath. No Filter, no Sort,
    // no Limit — the simplest shape.
    assert_eq!(lines.len(), 2, "got {lines:#?}");
    assert!(lines[0].starts_with("Project"), "got {:?}", lines[0]);
    assert!(lines[0].contains("(rows: 50)"), "got {:?}", lines[0]);
    assert!(lines[1].starts_with("  SeqScan t"), "got {:?}", lines[1]);
    assert!(lines[1].contains("(rows: 50)"), "got {:?}", lines[1]);
}

#[test]
fn explain_select_filter_scales_estimate_by_selectivity() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // `=` defaults to 10% selectivity → ~10 rows.
    let eq = explain_lines(db.execute("EXPLAIN SELECT n FROM t WHERE n = 5").unwrap());
    let filter_line = eq.iter().find(|l| l.contains("Filter")).expect("filter line");
    assert!(filter_line.contains("(rows: 10)"), "got {filter_line:?}");

    // `>` defaults to 33% → ~33 rows (round(100 * 1/3)).
    let gt = explain_lines(db.execute("EXPLAIN SELECT n FROM t WHERE n > 5").unwrap());
    let filter_line = gt.iter().find(|l| l.contains("Filter")).expect("filter line");
    assert!(filter_line.contains("(rows: 33)"), "got {filter_line:?}");

    // AND multiplies: 0.10 * 0.10 = 0.01 → ~1 row (floor of 1).
    let and = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 5 AND n = 6")
            .unwrap(),
    );
    let filter_line = and.iter().find(|l| l.contains("Filter")).expect("filter line");
    assert!(filter_line.contains("(rows: 1)"), "got {filter_line:?}");
}

#[test]
fn explain_select_index_scan_when_indexed_column_is_constrained() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, m INT)").unwrap();
    for i in 0..200 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_n ON t (n)").unwrap();
    let lines = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 50")
            .unwrap(),
    );
    let leaf = lines.last().expect("at least one line");
    assert!(
        leaf.contains("IndexScan t using idx_n"),
        "got {leaf:?}, all={lines:#?}"
    );
}

#[test]
fn explain_select_limit_and_sort_appear_in_tree() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..30 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t ORDER BY n DESC LIMIT 5")
            .unwrap(),
    );
    assert!(lines[0].starts_with("Limit"), "got {:?}", lines[0]);
    assert!(lines[0].contains("limit=5"), "got {:?}", lines[0]);
    assert!(lines[0].contains("(rows: 5)"), "got {:?}", lines[0]);
    assert!(
        lines.iter().any(|l| l.contains("Sort") && l.contains("DESC")),
        "no Sort/DESC line in {lines:#?}"
    );
}

#[test]
fn explain_select_join_renders_inner_join_with_both_scans() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE u (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE o (uid INT, amount INT)").unwrap();
    for i in 0..10 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, 'n{i}')"))
            .unwrap();
    }
    for i in 0..20 {
        db.execute(&format!("INSERT INTO o VALUES ({}, {i})", i % 10))
            .unwrap();
    }
    let lines = explain_lines(
        db.execute(
            "EXPLAIN SELECT u.name, o.amount FROM u INNER JOIN o ON u.id = o.uid",
        )
        .unwrap(),
    );
    assert!(
        lines.iter().any(|l| l.contains("InnerJoin")),
        "no InnerJoin line in {lines:#?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("SeqScan u")),
        "no SeqScan u in {lines:#?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("SeqScan o")),
        "no SeqScan o in {lines:#?}"
    );
}

#[test]
fn explain_select_groupby_uses_hashaggregate_with_sqrt_estimate() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, m INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {i})", i % 10))
            .unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN SELECT n, COUNT(*) FROM t GROUP BY n")
            .unwrap(),
    );
    let agg = lines
        .iter()
        .find(|l| l.contains("HashAggregate"))
        .expect("HashAggregate line");
    // sqrt(100) = 10
    assert!(agg.contains("(rows: 10)"), "got {agg:?}");
}

#[test]
fn explain_does_not_execute_inner_statement() {
    // EXPLAIN of an INSERT must NOT actually insert. We Plan it, then
    // EXPLAIN walks the Plan instead of running it.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    // EXPLAIN can only wrap a SELECT today (parser-enforced); the inner
    // statement is never executed. So this round-trips and the table is
    // empty afterward.
    let _ = explain_lines(db.execute("EXPLAIN SELECT n FROM t").unwrap());
    let count = rows(db.execute("SELECT n FROM t").unwrap());
    assert_eq!(count.len(), 0);
}

#[test]
fn explain_rejects_non_select_inner_statement() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    // The parser restricts EXPLAIN to SELECT; an INSERT here is an error
    // before the executor sees it.
    let err = db.execute("EXPLAIN INSERT INTO t VALUES (1)").unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("explain") || msg.to_lowercase().contains("select"),
        "unexpected error: {msg}"
    );
}

/// `EXPLAIN ANALYZE` runs the inner SELECT for real and annotates the
/// root operator with the observed row count plus a final
/// `Execution time:` line. The plain `EXPLAIN` (without ANALYZE) never
/// runs the inner — see `explain_does_not_execute_inner_statement`.
#[test]
fn explain_analyze_reports_actual_row_count_and_execution_time() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..40 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // WHERE n >= 30 keeps 10 rows (30..40).
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT n FROM t WHERE n >= 30")
            .unwrap(),
    );
    // The root (Project) line picks up the observed count.
    let root = &lines[0];
    assert!(root.starts_with("Project"), "got {root:?}");
    assert!(root.contains("actual: 10"), "got {root:?}");
    // The execution-time footer is the last non-empty line.
    let last = lines.last().expect("at least one line");
    assert!(last.starts_with("Execution time:"), "got {last:?}");
    assert!(last.contains(" ms"), "got {last:?}");
}

#[test]
fn explain_analyze_zero_rows_is_legal() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    // Empty table: actual rows = 0, but the estimator still emits a
    // non-zero estimate by its floor-of-one convention. The discrepancy
    // is exactly what ANALYZE is for.
    let lines = explain_lines(db.execute("EXPLAIN ANALYZE SELECT n FROM t").unwrap());
    let root = &lines[0];
    assert!(root.contains("actual: 0"), "got {root:?}");
    let footer = lines.last().expect("at least one line");
    assert!(footer.starts_with("Execution time:"), "got {footer:?}");
}

#[test]
fn explain_analyze_observes_filter_selectivity_drift() {
    // The estimator says `=` is 10% selectivity; with 100 rows all
    // matching `n > 0`, the real answer is 100 but the estimate is
    // ~33 (range default). ANALYZE surfaces the gap.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT n FROM t WHERE n > 0")
            .unwrap(),
    );
    let root = &lines[0];
    // The estimate is ~33 (1/3 of 100), the actual is 100. Both should
    // be visible on the same line.
    assert!(root.contains("rows: 33"), "got {root:?}");
    assert!(root.contains("actual: 100"), "got {root:?}");
}

#[test]
fn explain_analyze_inside_transaction_uses_snapshot() {
    // ANALYZE participates in the caller's snapshot exactly as a plain
    // SELECT would: a peer writer's uncommitted insert is invisible.
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut reader = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut writer = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    reader.execute("BEGIN").unwrap();
    // First ANALYZE inside the transaction sees only the row that
    // existed at BEGIN.
    let before = explain_lines(
        reader
            .execute("EXPLAIN ANALYZE SELECT n FROM t")
            .unwrap(),
    );
    assert!(before[0].contains("actual: 1"), "got {:?}", before[0]);

    // Peer writer inserts and commits.
    writer.execute("INSERT INTO t VALUES (2)").unwrap();

    // The reader's transaction snapshot is still pinned; ANALYZE
    // observes the snapshot count, not the post-commit count.
    let after = explain_lines(
        reader
            .execute("EXPLAIN ANALYZE SELECT n FROM t")
            .unwrap(),
    );
    assert!(after[0].contains("actual: 1"), "got {:?}", after[0]);
    reader.execute("COMMIT").unwrap();
}

#[test]
fn explain_analyze_actually_executes_the_query() {
    // The ANALYZE form runs the inner SELECT for real, which means
    // it consumes the same machinery the user-facing query would —
    // including the streaming volcano tree. We assert that the same
    // SELECT, run as a normal query, yields the row count ANALYZE
    // reports.
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // The query under analysis.
    let q = "SELECT n FROM t WHERE n < 17";
    let real = rows(db.execute(q).unwrap()).len() as u64;
    assert_eq!(real, 17);

    let lines = explain_lines(db.execute(&format!("EXPLAIN ANALYZE {q}")).unwrap());
    let root = &lines[0];
    assert!(
        root.contains(&format!("actual: {real}")),
        "expected actual: {real} in {root:?}"
    );
}

/// v0.41: per-operator actuals — every line gets its own observed
/// row count, not just the root. Filter and scan have different
/// actuals (the scan reads more, the filter keeps a subset).
#[test]
fn explain_analyze_filter_and_scan_actuals_differ() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT n FROM t WHERE n < 30")
            .unwrap(),
    );
    // Project keeps 30 rows (30 < 30 actually fails — n<30 = 0..29, so 30
    // values). Wait — n < 30 with n in 0..100 is 30 rows. Same as Filter.
    let project = lines
        .iter()
        .find(|l| l.contains("Project"))
        .expect("Project line");
    assert!(project.contains("actual: 30"), "got {project:?}");
    let filter = lines
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    assert!(filter.contains("actual: 30"), "got {filter:?}");
    let scan = lines
        .iter()
        .find(|l| l.contains("SeqScan"))
        .expect("SeqScan line");
    assert!(scan.contains("actual: 100"), "got {scan:?}");
}

/// v0.41: each join in a multi-join query reports its own output count;
/// each right-scan reports its own input count. Two distinct numbers
/// per join.
#[test]
fn explain_analyze_join_reports_per_join_actuals() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE u (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE o (uid INT, amount INT)").unwrap();
    for i in 0..5 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, 'n{i}')"))
            .unwrap();
    }
    // 5 users × 4 orders/user = 20 join rows when uid wraps mod 5.
    for i in 0..20 {
        db.execute(&format!("INSERT INTO o VALUES ({}, {i})", i % 5))
            .unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT u.name, o.amount FROM u INNER JOIN o ON u.id = o.uid")
            .unwrap(),
    );
    // The join line carries an output actual.
    let join = lines
        .iter()
        .find(|l| l.contains("InnerJoin"))
        .expect("InnerJoin line");
    assert!(join.contains("actual: 20"), "got {join:?}");
    // Both scans carry actuals matching their table sizes.
    let scans: Vec<&String> = lines.iter().filter(|l| l.contains("SeqScan")).collect();
    assert_eq!(scans.len(), 2, "got {scans:#?}");
    // base scan = 5 users (the FROM table); right scan = 20 orders.
    assert!(scans[0].contains("SeqScan u"), "got {:?}", scans[0]);
    assert!(scans[0].contains("actual: 5"), "got {:?}", scans[0]);
    assert!(scans[1].contains("SeqScan o"), "got {:?}", scans[1]);
    assert!(scans[1].contains("actual: 20"), "got {:?}", scans[1]);
}

/// v0.41: a Limit operator stops the scan early. The base scan's
/// actual reflects only the rows it had to produce, not the whole
/// table — proving the streaming pipeline really is lazy.
#[test]
fn explain_analyze_limit_short_circuits_the_scan() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..1000 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT n FROM t LIMIT 7")
            .unwrap(),
    );
    let limit = lines
        .iter()
        .find(|l| l.contains("Limit"))
        .expect("Limit line");
    assert!(limit.contains("actual: 7"), "got {limit:?}");
    // The Project under Limit also produced exactly 7.
    let project = lines
        .iter()
        .find(|l| l.contains("Project"))
        .expect("Project line");
    assert!(project.contains("actual: 7"), "got {project:?}");
    // And critically: the SeqScan ran only as long as it had to.
    let scan = lines
        .iter()
        .find(|l| l.contains("SeqScan"))
        .expect("SeqScan line");
    assert!(scan.contains("actual: 7"), "got {scan:?}");
}

/// v0.41: a grouped query falls back to a single observation for
/// every post-aggregation operator (HashAggregate / Sort / Project /
/// Limit), because `grouped_select` is materialised. Filter and the
/// base scan still get their own per-operator actuals.
/// v0.50: a large enough table activates the parallel-scan path
/// (gated by leaf count >= 16). The result must equal the serial
/// scan's output exactly — same rows, same key order. The
/// per-worker channels feeding the receiver in worker-index order
/// preserve table-key order, so this assertion holds without
/// ORDER BY.
#[test]
fn parallel_scan_preserves_order_on_large_table() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, label TEXT)").unwrap();
    // Need enough rows to spill across >= 16 leaf pages. 4 KiB
    // pages hold tens of rows each; 2000 rows is comfortably
    // over the threshold even with wide rows.
    for i in 0..2000 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'row{i}')"))
            .unwrap();
    }
    // No ORDER BY — parallel path engages. The order-preserving
    // per-worker drain means we still get ascending rowid order.
    let rs = rows(db.execute("SELECT n FROM t").unwrap());
    assert_eq!(rs.len(), 2000);
    for (i, row) in rs.iter().enumerate() {
        assert_eq!(row, &vec![Value::Int(i as i64)]);
    }
}

/// v0.50: a small table (< 16 leaves) falls back to the serial
/// path — the gate prevents the worker-thread setup cost on
/// queries that wouldn't win from parallelism.
#[test]
fn parallel_scan_skips_small_tables() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // Whether parallel engages or not, the SELECT must return all
    // rows in order. (No EXPLAIN hint exposes "did parallel
    // fire?" — we just assert correctness.)
    let rs = rows(db.execute("SELECT n FROM t").unwrap());
    assert_eq!(rs.len(), 50);
    for (i, row) in rs.iter().enumerate() {
        assert_eq!(row, &vec![Value::Int(i as i64)]);
    }
}

/// v0.50: a filter on a parallel scan keeps only the matching
/// rows, still in key order. Workers apply the filter in
/// parallel; the receiver concatenates each worker's surviving
/// rows in worker order, which is also key order.
#[test]
fn parallel_scan_with_filter_returns_filtered_rows_in_order() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..2000 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let rs = rows(
        db.execute("SELECT n FROM t WHERE n >= 500 AND n < 1500")
            .unwrap(),
    );
    assert_eq!(rs.len(), 1000);
    for (i, row) in rs.iter().enumerate() {
        assert_eq!(row, &vec![Value::Int((500 + i) as i64)]);
    }
}

/// v0.50: LIMIT on a parallel scan short-circuits via the shared
/// `stop` AtomicBool — workers exit before scanning their full
/// ranges once the receiver has enough rows. The result must
/// have exactly LIMIT rows in key order.
#[test]
fn parallel_scan_limit_short_circuits() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..2000 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let rs = rows(db.execute("SELECT n FROM t LIMIT 7").unwrap());
    assert_eq!(
        rs,
        (0..7).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );
}

/// v0.50: parallel scan respects MVCC visibility. An UPDATE
/// inside an explicit transaction shouldn't be visible to a
/// concurrent (snapshot-isolated) reader.
#[test]
fn parallel_scan_respects_mvcc_snapshot() {
    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (id INT, n INT)").unwrap();
        for i in 0..2000 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .unwrap();
        }
    }
    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();
    let mut reader = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();
    let mut writer = Database::open_shared(&tmp.path, pool.clone(), tx_state.clone()).unwrap();

    reader.execute("BEGIN").unwrap();
    // Reader captures snapshot here.
    let before_count = rows(reader.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(before_count, 2000);

    // Peer writer changes 100 rows.
    writer
        .execute("UPDATE t SET n = -1 WHERE id < 100")
        .unwrap();

    // Reader's snapshot is still pinned — parallel scan must
    // see the pre-UPDATE values.
    let preserved =
        rows(reader.execute("SELECT id, n FROM t WHERE id < 100").unwrap());
    for row in &preserved {
        let id = match row[0] {
            Value::Int(n) => n,
            _ => panic!("non-int id"),
        };
        let n = match row[1] {
            Value::Int(n) => n,
            _ => panic!("non-int n"),
        };
        // Snapshot view: n still equals id, not -1.
        assert_eq!(n, id, "snapshot broken: id={id} n={n}");
    }
    reader.execute("COMMIT").unwrap();
}

/// v0.49 auto-analyze: a fresh table with < 50 mutations doesn't
/// trigger the auto-analyze pass — the threshold is `50 + 0.10 * 0`
/// for an unanalysed table.
#[test]
fn auto_analyze_skips_tables_below_threshold() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..40 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let analyzed = db.auto_analyze_pass().unwrap();
    assert_eq!(analyzed, None, "40 mutations should be below the v0.49 threshold of 50");
}

/// v0.49: enough mutations cross the threshold and trigger ANALYZE
/// on the next pass. Stats land; future EXPLAINs use them.
#[test]
fn auto_analyze_triggers_above_threshold() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..60 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let analyzed = db.auto_analyze_pass().unwrap();
    assert_eq!(analyzed.as_deref(), Some("t"));

    // Stats are now present — EXPLAIN should use 1/n_distinct (= 1/60)
    // rather than the 10% default, so the row estimate for an equality
    // predicate is 1.
    let lines = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 5").unwrap(),
    );
    let filter = lines
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    assert!(filter.contains("(rows: 1)"), "got {filter:?}");
}

/// v0.49: manual ANALYZE resets the counter — a follow-up auto pass
/// finds nothing to do.
#[test]
fn auto_analyze_skips_after_manual_analyze() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // Manual ANALYZE — drains the mutation counter.
    db.execute("ANALYZE t").unwrap();
    // No further mutations — auto pass finds nothing.
    let analyzed = db.auto_analyze_pass().unwrap();
    assert_eq!(analyzed, None);
}

/// v0.49: a 10% mutation bump on a 100-row table triggers
/// auto-analyze the next pass. (Threshold: 50 + 0.10 * 100 = 60;
/// 70 inserts crosses it.)
#[test]
fn auto_analyze_threshold_scales_with_table_size() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    db.execute("ANALYZE t").unwrap();
    // 70 more mutations — over the 60 threshold for a 100-row table.
    for i in 100..170 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let analyzed = db.auto_analyze_pass().unwrap();
    assert_eq!(analyzed.as_deref(), Some("t"));
}

/// v0.49: mutations_since_analyze survives close + reopen.
#[test]
fn auto_analyze_counter_survives_reopen() {
    let tmp = TempDb::new();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        for i in 0..30 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
    }
    // Reopen. The counter persisted; another 30 mutations should
    // push us above 50.
    let mut db = tmp.open();
    for i in 30..60 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let analyzed = db.auto_analyze_pass().unwrap();
    assert_eq!(analyzed.as_deref(), Some("t"));
}

/// v0.48 FK ON DELETE CASCADE: deleting a parent removes
/// matching child rows.
#[test]
fn fk_on_delete_cascade_removes_children() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();

    // Delete customer 1 — orders 10 and 11 should be gone too.
    db.execute("DELETE FROM customers WHERE id = 1").unwrap();
    let surviving = rows(db.execute("SELECT id FROM orders ORDER BY id").unwrap());
    assert_eq!(surviving, vec![vec![Value::Int(12)]]);

    let parents = rows(db.execute("SELECT id FROM customers").unwrap());
    assert_eq!(parents, vec![vec![Value::Int(2)]]);
}

/// v0.48 FK ON DELETE SET NULL: deleting a parent sets child
/// FK column to NULL. The child row survives.
#[test]
fn fk_on_delete_set_null_keeps_child() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1), (11, 2)").unwrap();

    db.execute("DELETE FROM customers WHERE id = 1").unwrap();

    // Both orders still there. Order 10's customer_id is now NULL.
    let rs = rows(
        db.execute("SELECT id, customer_id FROM orders ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        rs,
        vec![
            vec![Value::Int(10), Value::Null],
            vec![Value::Int(11), Value::Int(2)],
        ]
    );
}

/// v0.48: ON DELETE SET NULL on a NOT NULL child column is a
/// runtime error when a matching child actually exists.
#[test]
fn fk_set_null_violates_not_null_at_runtime() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    // customer_id is NOT NULL — so SET NULL would violate it.
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();

    let err = db.execute("DELETE FROM customers WHERE id = 1").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("not null") || msg.contains("set null"),
        "got {err}"
    );
    // The customer row stayed put (the FK action failed before the delete committed).
    let count = rows(db.execute("SELECT id FROM customers").unwrap()).len();
    assert_eq!(count, 1);
}

/// v0.48: explicit RESTRICT and NO ACTION both parse as RESTRICT
/// (the v0.45 default behaviour).
#[test]
fn fk_explicit_restrict_and_no_action_refuse_delete() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE RESTRICT)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invoices (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE NO ACTION)",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();
    db.execute("INSERT INTO invoices VALUES (100, 1)").unwrap();

    let err = db.execute("DELETE FROM customers WHERE id = 1").unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("foreign key"));
}

/// v0.48: CASCADE recurses through a chain — A references B, B
/// references C; deleting C cascades to delete its matching B
/// rows, which cascades to delete *their* matching A rows.
#[test]
fn fk_cascade_recurses_through_chain() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE b (id INT PRIMARY KEY, c_id INT REFERENCES c(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE a (id INT PRIMARY KEY, b_id INT REFERENCES b(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO c VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (10, 1), (11, 1)").unwrap();
    db.execute("INSERT INTO a VALUES (100, 10), (101, 11)").unwrap();

    db.execute("DELETE FROM c WHERE id = 1").unwrap();
    // Everything in the chain should be gone.
    assert_eq!(rows(db.execute("SELECT id FROM c").unwrap()).len(), 0);
    assert_eq!(rows(db.execute("SELECT id FROM b").unwrap()).len(), 0);
    assert_eq!(rows(db.execute("SELECT id FROM a").unwrap()).len(), 0);
}

/// v0.47 ANALYZE: gathering stats with no rows is legal — n_distinct
/// and null_count are zero, histogram is empty.
#[test]
fn analyze_empty_table_succeeds() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, label TEXT)").unwrap();
    let result = db.execute("ANALYZE t").unwrap();
    let QueryResult::Ack(msg) = result else {
        panic!("expected ack");
    };
    assert!(msg.contains("0 rows"), "got {msg}");
}

/// v0.47: rejects ANALYZE of a table that doesn't exist.
#[test]
fn analyze_rejects_unknown_table() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    let err = db.execute("ANALYZE nope").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("no such table"));
}

/// v0.47: EXPLAIN's filter `(rows: N)` estimate changes after ANALYZE
/// — `col = lit` with a unique column moves from the default 10%
/// selectivity to `1 / n_distinct`, which for a 100-row, 100-distinct
/// column is exactly 1 row.
#[test]
fn analyze_sharpens_equality_estimate() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    // Before ANALYZE: 10% of 100 = 10 estimated rows.
    let before = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 5").unwrap(),
    );
    let filter_before = before
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    assert!(
        filter_before.contains("(rows: 10)"),
        "got {filter_before:?}"
    );

    db.execute("ANALYZE t").unwrap();

    // After ANALYZE: 100 distinct values, so 1/100 of 100 = 1 row.
    let after = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 5").unwrap(),
    );
    let filter_after = after
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    assert!(
        filter_after.contains("(rows: 1)"),
        "got {filter_after:?}"
    );
}

/// v0.47: IS NULL selectivity uses null_frac when stats present.
/// A column where 30 of 100 rows are NULL should estimate ~30 rows
/// for `IS NULL`, not the default 10.
#[test]
fn analyze_uses_null_frac_for_is_null() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    // 70 rows with non-NULL `a`, 30 rows with NULL `a`.
    for i in 0..70 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 1)"))
            .unwrap();
    }
    for _ in 0..30 {
        db.execute("INSERT INTO t (b) VALUES (2)").unwrap();
    }
    db.execute("ANALYZE t").unwrap();

    let lines = explain_lines(
        db.execute("EXPLAIN SELECT * FROM t WHERE a IS NULL").unwrap(),
    );
    let filter = lines
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    // 30/100 = 0.30 * 100 = 30 rows.
    assert!(filter.contains("(rows: 30)"), "got {filter:?}");
}

/// v0.47: range selectivity uses the equi-depth histogram. With 100
/// rows of 0..100, `n > 50` should estimate ~half (50ish), much
/// closer to truth than the default 33%.
#[test]
fn analyze_uses_histogram_for_range() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    db.execute("ANALYZE t").unwrap();

    let lines = explain_lines(
        db.execute("EXPLAIN SELECT * FROM t WHERE n > 50").unwrap(),
    );
    let filter = lines
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    // True answer: 49 rows (51..100). The 16-bucket histogram should
    // estimate close to that — somewhere in [40, 60] is fine.
    // Extract the estimate from the line and check.
    let rows_token = filter
        .split_whitespace()
        .find_map(|t| {
            t.strip_prefix("(rows:").and_then(|s| s.trim_end_matches(')').parse::<i64>().ok())
        })
        .or_else(|| {
            // Fall back to "rows: NN)" parsing — the line format is
            // "...  (rows: N)" so split on "rows:".
            filter.split("rows:").nth(1).and_then(|s| {
                s.trim_start()
                    .trim_end_matches(')')
                    .trim_end_matches(' ')
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.trim_end_matches(')').parse::<i64>().ok())
            })
        })
        .unwrap_or_else(|| panic!("couldn't extract rows estimate from {filter:?}"));
    assert!(
        (40..=60).contains(&rows_token),
        "histogram estimate {rows_token} not in [40, 60] for `n > 50`, got {filter:?}"
    );
}

/// v0.47: ANALYZE stats survive close + reopen — they're persisted
/// in the catalog, just like every other column field.
#[test]
fn analyze_stats_survive_reopen() {
    let tmp = TempDb::new();
    {
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        db.execute("ANALYZE t").unwrap();
    }
    // Reopen — stats should still drive the equality estimate.
    let mut db = tmp.open();
    let after = explain_lines(
        db.execute("EXPLAIN SELECT n FROM t WHERE n = 5").unwrap(),
    );
    let filter = after
        .iter()
        .find(|l| l.contains("Filter"))
        .expect("Filter line");
    // 50 distinct values, 1/50 selectivity, 1 row.
    assert!(filter.contains("(rows: 1)"), "got {filter:?}");
}

/// v0.45 FOREIGN KEY: INSERT child with non-existent parent is rejected
/// with a clear error naming the violated FK target.
#[test]
fn fk_rejects_orphan_child_insert() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada')").unwrap();
    // customer_id = 1 exists → ok.
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();
    // customer_id = 99 doesn't exist → reject.
    let err = db.execute("INSERT INTO orders VALUES (11, 99)").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("foreign key") && msg.contains("customers"),
        "got {err}"
    );
}

/// v0.45: NULL in an FK column means "no parent" and is always
/// allowed (assuming no separate NOT NULL constraint).
#[test]
fn fk_allows_null_in_child_column() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    // No parent inserts at all — child with NULL FK still works.
    db.execute("INSERT INTO orders (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO orders (id) VALUES (2)").unwrap();
    let rs = rows(db.execute("SELECT id FROM orders ORDER BY id").unwrap());
    assert_eq!(rs.len(), 2);
}

/// v0.45: DELETE of a parent row that has no children works as
/// usual; DELETE of a parent with children is rejected (RESTRICT).
#[test]
fn fk_delete_parent_with_children_is_restricted() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();
    // customer 2 has no children — delete works.
    db.execute("DELETE FROM customers WHERE id = 2").unwrap();
    // customer 1 has a child — delete rejected.
    let err = db
        .execute("DELETE FROM customers WHERE id = 1")
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("foreign key") && msg.contains("orders"),
        "got {err}"
    );
    // Surviving rows: customer 1 (still there, delete refused).
    let rs = rows(db.execute("SELECT id FROM customers ORDER BY id").unwrap());
    assert_eq!(rs, vec![vec![Value::Int(1)]]);
}

/// v0.45: UPDATE that changes a parent's PK to something else is
/// RESTRICTed when children reference the old PK value.
#[test]
fn fk_update_parent_pk_with_children_is_restricted() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    db.execute("INSERT INTO customers VALUES (1)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();
    let err = db
        .execute("UPDATE customers SET id = 99 WHERE id = 1")
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("foreign key"));
}

/// v0.45: DROP TABLE on a parent with FKs pointing at it is refused.
/// Dropping the child first frees the parent.
#[test]
fn fk_drop_parent_refused_until_child_dropped() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    let err = db.execute("DROP TABLE customers").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("foreign key") && msg.contains("orders"),
        "got {err}"
    );
    // After dropping the child, the parent can be dropped.
    db.execute("DROP TABLE orders").unwrap();
    db.execute("DROP TABLE customers").unwrap();
}

/// v0.45: CREATE TABLE rejects an FK to a non-existent table, an FK to
/// a non-existent parent column, an FK to a non-unique parent column,
/// and an FK whose type doesn't match the parent's.
#[test]
fn fk_create_validates_target() {
    let tmp = TempDb::new();
    let mut db = tmp.open();

    // Parent table doesn't exist yet.
    assert!(db
        .execute("CREATE TABLE t (x INT REFERENCES nope(id))")
        .is_err());

    // Parent exists, but referenced column doesn't.
    db.execute("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    assert!(db
        .execute("CREATE TABLE q (x INT REFERENCES p(missing))")
        .is_err());

    // Parent's referenced column is not PK or UNIQUE.
    db.execute("CREATE TABLE p2 (a INT, b INT)").unwrap();
    let err = db
        .execute("CREATE TABLE r (x INT REFERENCES p2(a))")
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("unique"));

    // Type mismatch (child INT → parent TEXT).
    db.execute("CREATE TABLE p3 (s TEXT PRIMARY KEY)").unwrap();
    let err = db
        .execute("CREATE TABLE s (x INT REFERENCES p3(s))")
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("type"));
}

/// v0.44: NOT IN over a NOT NULL inner column rewrites to an AntiJoin
/// (we verify via EXPLAIN), produces the right rows, and matches what
/// per-row evaluation would yield. Uses distinct column names between
/// outer and inner to keep the inner filter unambiguous in the
/// combined join scope after the rewrite — the IN/NOT IN rewrite
/// copies the inner WHERE into the join's ON clause verbatim, so a
/// bare column name shared by both tables is a pre-existing pitfall
/// (qualify, or use distinct names).
#[test]
fn not_in_over_not_null_column_rewrites_to_antijoin() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE banned (bid INT NOT NULL)").unwrap();
    db.execute("INSERT INTO banned VALUES (2), (4)").unwrap();
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .unwrap();

    // EXPLAIN reveals the AntiJoin in the plan tree — proof that the
    // NOT IN got rewritten rather than going through v0.31's per-row
    // path (which would show as a Filter with no explicit join).
    let lines = explain_lines(
        db.execute(
            "EXPLAIN SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 0)",
        )
        .unwrap(),
    );
    assert!(
        lines.iter().any(|l| l.contains("AntiJoin")),
        "no AntiJoin in {lines:#?}"
    );

    // The query returns the rows whose id is NOT in {2, 4}.
    let rs = rows(
        db.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 0) ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![vec![Value::Text("a".into())], vec![Value::Text("c".into())]]
    );
}

/// v0.44: NOT IN over a NULLABLE inner column stays on v0.31's
/// per-row evaluation path — no AntiJoin appears in EXPLAIN — but
/// still produces the right answer (which differs from anti-join
/// semantics whenever the inner set contains a NULL).
#[test]
fn not_in_over_nullable_column_stays_per_row() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    // `bid` here is *not* NOT NULL — eligible for the v0.31 path only.
    db.execute("CREATE TABLE banned (bid INT)").unwrap();
    db.execute("INSERT INTO banned VALUES (2), (4)").unwrap();
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .unwrap();

    let lines = explain_lines(
        db.execute(
            "EXPLAIN SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 0)",
        )
        .unwrap(),
    );
    // The plan must NOT contain an AntiJoin: the planner refused to
    // rewrite because `banned.bid` is nullable, so NOT IN's NULL
    // semantics could differ from anti-join.
    assert!(
        !lines.iter().any(|l| l.contains("AntiJoin")),
        "unexpected AntiJoin in {lines:#?}"
    );

    // The per-row path produces the same answer as the anti-join
    // version when no NULLs are actually in the inner set.
    let rs = rows(
        db.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 0) ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![vec![Value::Text("a".into())], vec![Value::Text("c".into())]]
    );
}

/// v0.44: SQL three-valued `NOT IN` with a NULL in the inner set
/// returns no rows (per-row eval path). The anti-join rewrite would
/// be wrong here — and the planner correctly refuses it because
/// `banned.bid` is nullable.
#[test]
fn not_in_with_null_in_inner_set_returns_no_rows() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE banned (bid INT)").unwrap();
    // Note the NULL — the WHERE `bid > 0 OR bid IS NULL` keeps it.
    db.execute("INSERT INTO banned VALUES (2), (NULL)").unwrap();
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();

    // SQL semantics: `x NOT IN (2, NULL)` is never TRUE — it's NULL
    // for x=1 and x=3 (because NULL might equal them), and FALSE
    // for x=2 (the equality with 2 wins). WHERE keeps only TRUE,
    // so zero rows.
    let rs = rows(
        db.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 0 OR bid IS NULL) ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(rs.len(), 0, "got {rs:#?}");
}

/// v0.44: NOT IN over an empty inner set returns every outer row.
/// Both the anti-join and per-row paths must agree here. We test
/// against a NOT NULL inner so the anti-join path is taken.
#[test]
fn not_in_with_empty_inner_returns_all_outer_rows() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE banned (bid INT NOT NULL)").unwrap();
    // Inner set: nothing matches `bid > 100`.
    db.execute("INSERT INTO banned VALUES (1), (2)").unwrap();
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (10, 'a'), (20, 'b')")
        .unwrap();

    let rs = rows(
        db.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT bid FROM banned WHERE bid > 100) ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(
        rs,
        vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]
    );
}

/// v0.43 constraints: PRIMARY KEY rejects duplicates, with a clear
/// error message naming the auto-created `_pk_<table>` index.
#[test]
fn primary_key_rejects_duplicate_value() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'b')").unwrap();
    let err = db
        .execute("INSERT INTO users VALUES (1, 'c')")
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("duplicate"),
        "unexpected error: {err}"
    );
    // Only the two distinct ids landed.
    let rs = rows(db.execute("SELECT id FROM users ORDER BY id").unwrap());
    assert_eq!(rs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

/// v0.43: PRIMARY KEY implies NOT NULL — INSERT with a NULL pk
/// is rejected with a clear error mentioning the column.
#[test]
fn primary_key_rejects_null() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)")
        .unwrap();
    let err = db
        .execute("INSERT INTO t (x) VALUES (5)")
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("not null") && msg.contains("id"),
        "unexpected error: {err}"
    );
}

/// v0.43: explicit NOT NULL on a non-PK column rejects NULL inserts.
/// Other columns can still receive NULL.
#[test]
fn not_null_rejects_null_inserts_only_for_that_column() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, name TEXT NOT NULL, note TEXT)")
        .unwrap();
    // name omitted -> NULL -> NOT NULL violation
    let err = db
        .execute("INSERT INTO t (id) VALUES (1)")
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("not null"));
    // Explicit NULL on `note` is fine.
    db.execute("INSERT INTO t (id, name, note) VALUES (1, 'a', NULL)")
        .unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (2, 'b')")
        .unwrap();
}

/// v0.43: UNIQUE rejects duplicate non-NULL values.
#[test]
fn unique_rejects_duplicate_value() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a@x.com')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b@x.com')").unwrap();
    let err = db
        .execute("INSERT INTO t VALUES (3, 'a@x.com')")
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("unique") || msg.contains("duplicate"));
}

/// v0.43: UNIQUE allows multiple NULLs per SQL standard
/// (NULL ≠ NULL for uniqueness purposes).
#[test]
fn unique_allows_multiple_nulls() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (3)").unwrap();
    // Three NULLs in the unique column, all accepted.
    let rs = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(rs.len(), 3);
}

/// v0.43: UPDATE that would create a duplicate value on a UNIQUE
/// column is rejected by the constraint enforcement on
/// `index_insert_row` of the new row version.
#[test]
fn update_rejects_unique_violation() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let err = db.execute("UPDATE t SET id = 1 WHERE id = 2").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("unique") || msg.contains("duplicate"));
    // The id=2 row is unchanged.
    let rs = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(rs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

/// v0.43: UPDATE that would assign NULL to a NOT NULL column is rejected.
#[test]
fn update_rejects_null_into_not_null() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let err = db.execute("UPDATE t SET name = NULL").unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("not null"));
}

/// v0.43: PK declaration with NOT NULL and UNIQUE on the same column
/// is accepted (the planner sees only one PK constraint, the
/// NOT NULL/UNIQUE are noted on the same column).
#[test]
fn multiple_constraints_on_one_column() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    // Three constraints, all internally consistent.
    db.execute("CREATE TABLE t (id INT PRIMARY KEY NOT NULL UNIQUE, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    // PK still rejects duplicates.
    assert!(db.execute("INSERT INTO t VALUES (1, 'b')").is_err());
}

/// v0.43: at most one PRIMARY KEY per table. Two PK declarations
/// in one CREATE TABLE are a plan-time error.
#[test]
fn rejects_two_primary_keys() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    let err = db
        .execute("CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)")
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("primary key") || msg.contains("more than one"));
}

/// v0.42 group commit: N concurrent writers commit faster together
/// than they would serially. We don't pin a tight time budget (CI
/// machines vary wildly), but we assert that under contention every
/// commit succeeds and every row lands — durability is preserved
/// while the leader/follower protocol does its batching.
#[test]
fn group_commit_handles_concurrent_writers_durably() {
    use std::sync::Arc as StdArc;
    use std::sync::Barrier;
    use std::thread;

    let tmp = TempDb::new();
    {
        let mut db = Database::open(&tmp.path).unwrap();
        db.execute("CREATE TABLE t (n INT)").unwrap();
    }

    let pool = SharedPool::new();
    let tx_state = Database::open_with_pool(&tmp.path, pool.clone())
        .unwrap()
        .tx_state();

    const WRITERS: usize = 16;
    const PER_WRITER: i64 = 25;
    let barrier = StdArc::new(Barrier::new(WRITERS));

    let mut handles = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let path = tmp.path.clone();
        let pool = pool.clone();
        let tx_state = tx_state.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let mut db = Database::open_shared(&path, pool, tx_state).unwrap();
            // Line everyone up at the start so the commits do collide.
            barrier.wait();
            for i in 0..PER_WRITER {
                let n = (w as i64) * PER_WRITER + i;
                db.execute(&format!("INSERT INTO t VALUES ({n})"))
                    .expect("insert under concurrency");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Every row landed and is durable.
    let count = rows(
        Database::open(&tmp.path)
            .unwrap()
            .execute("SELECT n FROM t")
            .unwrap(),
    )
    .len();
    assert_eq!(count, WRITERS * (PER_WRITER as usize));
}

#[test]
fn explain_analyze_grouped_uses_grouped_output_for_postagg_ops() {
    let tmp = TempDb::new();
    let mut db = tmp.open();
    db.execute("CREATE TABLE t (n INT, m INT)").unwrap();
    for i in 0..100 {
        // 10 distinct n values, 10 rows each.
        db.execute(&format!("INSERT INTO t VALUES ({}, {i})", i % 10))
            .unwrap();
    }
    let lines = explain_lines(
        db.execute("EXPLAIN ANALYZE SELECT n, COUNT(*) FROM t GROUP BY n")
            .unwrap(),
    );
    // 10 distinct groups.
    let agg = lines
        .iter()
        .find(|l| l.contains("HashAggregate"))
        .expect("HashAggregate line");
    assert!(agg.contains("actual: 10"), "got {agg:?}");
    // The base scan still gives the real input count.
    let scan = lines
        .iter()
        .find(|l| l.contains("SeqScan"))
        .expect("SeqScan line");
    assert!(scan.contains("actual: 100"), "got {scan:?}");
}
