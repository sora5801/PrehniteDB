# PrehniteDB

A relational database written from scratch in Rust — a page-based B+tree
storage engine, a write-ahead log, a SQL frontend, and a network server — with
**zero external dependencies**. Only the Rust standard library.

PrehniteDB is small but genuinely works end to end: start the server, connect
with the CLI, create tables, and run `INSERT` / `SELECT` / `UPDATE` / `DELETE`
with `WHERE` clauses. Data is encoded into pages, indexed in a B+tree, and
committed through a CRC-checked WAL, so it is durable and survives a crash.

> **Status: v0.1.** A complete vertical slice — every layer is real and tested,
> but the SQL surface is deliberately small. See [Limitations](#limitations).

## Highlights

- **No dependencies.** Storage engine, SQL parser, executor, wire protocol, and
  server are all built on `std` alone. `cargo build` fetches nothing.
- **Real durability.** Every statement is its own transaction. A write-ahead
  log of CRC-checked full-page images makes each commit atomic and crash-safe;
  a half-written commit is discarded cleanly on the next open.
- **A real storage engine.** 4 KiB slotted pages, a file-backed pager with
  buffered writes, and a B+tree — with page splits and leaf chaining — that
  stores both table data and the catalog.
- **Client / server.** A thread-per-connection TCP server (`prehnited`) and an
  interactive client (`prehnite`) speak a compact length-prefixed binary
  protocol.

## Architecture

The crate is a stack of layers; each one knows only about the layer below it.

```
            SQL text in,  result rows out
                     │
   ┌─────────────────▼─────────────────┐
   │ protocol   length-prefixed framing │   client <─wire─> server
   ├───────────────────────────────────┤
   │ engine     catalog · planner ·     │   gives bytes meaning as
   │            executor · value model │   tables, rows, typed values
   ├───────────────────────────────────┤
   │ sql        lexer · parser · AST    │   text  ->  Statement
   ├───────────────────────────────────┤
   │ storage    pager · WAL · B+tree    │   pages, byte-string keys/values
   └─────────────────▲─────────────────┘
                     │
              one database file
```

A query's life: the **parser** turns SQL text into a `Statement`; the
**planner** lowers and validates it into a `Plan`; the **executor** runs the
plan against the **catalog** and the **B+tree**; the **pager** stages every
page it touches and commits them as one transaction through the **WAL**.

## Project layout

```
crates/
  prehnitedb/      the library — storage, sql, engine, protocol
  prehnited/       the server daemon          (binary: prehnited)
  prehnite-cli/    the interactive client     (binary: prehnite)
```

## Building

Requires a stable Rust toolchain (1.70+).

```sh
cargo build --release
cargo test --workspace      # 67 tests across every layer
```

This produces `target/release/prehnited` and `target/release/prehnite`.

## Running

Start the server (creates the database file if it does not exist):

```sh
prehnited --db mydata.db --addr 127.0.0.1:7654
```

Connect with the interactive client:

```sh
prehnite --addr 127.0.0.1:7654
```

```
connected to PrehniteDB at 127.0.0.1:7654
type \help for help, \q to quit
prehnite> CREATE TABLE users (id INT, name TEXT, active BOOL);
table 'users' created
prehnite> INSERT INTO users VALUES (1, 'ada', true), (2, 'grace', false);
2 row(s) inserted
prehnite> SELECT name FROM users WHERE active = true;
name
----
ada
(1 row)
prehnite> \q
```

## Embedding it as a library

The server is a thin wrapper over the `prehnitedb` crate, which can be used
directly:

```rust
use prehnitedb::Database;

let mut db = Database::open("example.db")?;
db.execute("CREATE TABLE users (id INT, name TEXT)")?;
db.execute("INSERT INTO users VALUES (1, 'ada')")?;
let result = db.execute("SELECT name FROM users WHERE id = 1")?;
println!("{result}");
```

## SQL reference

PrehniteDB v0.1 understands one statement at a time:

| Statement | Form |
|-----------|------|
| Create    | `CREATE TABLE name (col TYPE, ...)` |
| Drop      | `DROP TABLE name` |
| Insert    | `INSERT INTO name [(cols)] VALUES (...), (...)` |
| Select    | `SELECT * \| col, ... FROM name [WHERE expr]` |
| Update    | `UPDATE name SET col = expr, ... [WHERE expr]` |
| Delete    | `DELETE FROM name [WHERE expr]` |

**Types:** `INT`/`INTEGER`, `REAL`/`FLOAT`, `TEXT`, `BOOL`/`BOOLEAN`.

**Expressions:** integer / real / string / `TRUE` / `FALSE` / `NULL` literals,
column references, arithmetic (`+ - * /`), comparisons (`= != <> < <= > >=`),
`AND` / `OR` / `NOT`, `IS [NOT] NULL`, parentheses, and unary `-`.

`NULL` follows SQL three-valued logic: it propagates through arithmetic and
comparisons, and a `WHERE` clause keeps a row only when the predicate is
exactly `TRUE`. Identifiers are case-sensitive. `--` starts a line comment.

## How it works

### Pages and the pager

The database file is a sequence of fixed 4 KiB pages. Page 0 is the header
(magic, page count, free-list head, catalog root). Every other page is a
*slotted page*: a slot array grows up from the header while variable-length
cells grow down from the end. The **pager** owns the file, hands out pages by
number, recycles freed pages through a free list, and buffers all writes in
memory until commit.

### The write-ahead log

A statement's writes are staged, not applied. On commit the pager (1) writes a
full image of every dirty page to the WAL, each with a CRC-32, followed by a
commit marker, and fsyncs it; (2) writes those pages into the database file and
fsyncs that; (3) truncates the WAL. A crash between (1) and (3) is repaired on
the next open: a transaction with an intact commit marker is replayed, and one
without is discarded. The database file is never left half-updated.

### The B+tree

Table data and the catalog are both B+trees keyed by byte strings. Interior
nodes only route; all key/value pairs live in leaves, which are chained
left-to-right so an ordered scan is one walk. A node that overflows splits, and
the split can cascade to the root — but the root keeps a *fixed page number*
for its whole life, so the catalog can refer to a table by a number that never
moves.

### Transactions

Each call to `execute` is one transaction. It succeeds and commits as a unit,
or fails and rolls back completely — a rejected statement never leaves a
partial effect. The server serializes statements behind a single mutex, so v0.1
is single-writer.

## Limitations

v0.1 is a foundation, not a finished database. It intentionally omits:

- joins, `ORDER BY`, `GROUP BY`, aggregates, and subqueries;
- secondary indexes — every query is a full table scan;
- `ALTER TABLE`;
- overflow pages — a row must fit in roughly 2 KiB;
- B+tree node merging on delete — space is reclaimed only by `DROP TABLE`;
- concurrent writers, and any authentication on the network protocol.

## Roadmap

Natural next steps, roughly in order: secondary (B+tree) indexes and an index
chooser in the planner; `ORDER BY` and aggregates; overflow pages for large
values; node merging and a `VACUUM`; a buffer pool with eviction; and
multi-statement transactions with concurrent readers.

## License

MIT — see [LICENSE](LICENSE).
