# PrehniteDB

A relational database written from scratch in Rust — a page-based B+tree
storage engine, a write-ahead log, a SQL frontend, and a network server — with
**zero external dependencies**. Only the Rust standard library.

PrehniteDB is small but genuinely works end to end: start the server, connect
with the CLI, create tables and indexes, and run `INSERT` / `UPDATE` / `DELETE`
and `SELECT` queries — with joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`,
`LIMIT`, and aggregates. Large values spill across overflow pages, data is
indexed in B+trees, every commit goes through a CRC-checked WAL — so it is
durable and survives a crash — and `BEGIN` / `COMMIT` / `ROLLBACK` group
statements into transactions.

> **Status: v0.29.** Every layer is real and tested; v0.29 adds
> **Serialisable Snapshot Isolation (SSI)** on top of v0.26's
> first-updater-wins. Each explicit `BEGIN..COMMIT` pins its snapshot
> at start (so reads stay stable across statements), tracks every
> tuple it observes, and detects rw-dependency cycles with peer
> transactions at commit time — the canonical write-skew anomaly
> (T1 and T2 each read both halves of an invariant, write to
> opposite halves) is now caught and one transaction aborts with a
> `Serialization` error. See [Limitations](#limitations).

## Highlights

- **No dependencies.** Storage engine, SQL parser, executor, wire protocol, and
  server are all built on `std` alone. `cargo build` fetches nothing.
- **Real durability.** A write-ahead log of CRC-checked full-page images makes
  every commit atomic and crash-safe; a half-written commit is discarded
  cleanly on the next open.
- **Parallel writers on different tables.** Multiple write transactions
  can be in flight at once *and* execute in parallel when they touch
  different tables. The server takes a per-table mutex (from `TxState`),
  not a global writer lock, so two clients each running
  `INSERT INTO different_table` proceed concurrently — through B-tree
  traversal, page mutation, and commit — and contend only briefly inside
  the catalog when both have to bump their own table's `next_rowid`.
  Each statement's writes are physically committed when it runs, stamped
  with the writer's TX ID, and the logical `COMMIT` just appends a
  *committed* record to a persistent **commit log** (`.db-clog`) that
  future snapshots consult. A `ROLLBACK` writes a *rolled-back* record
  instead; `VACUUM` reclaims those rows.
- **Serialisable isolation.** Each explicit `BEGIN..COMMIT` pins its
  snapshot at start (the substrate SSI requires) and tracks every
  tuple it observes. Two kinds of conflict catch concurrent writers:
  **first-updater-wins** aborts the second writer to claim the same
  row (`Conflict`), and **Serialisable Snapshot Isolation** (the
  Cahill algorithm, the same one Postgres adopted) tracks rw-edges
  between transactions and aborts the pivot of any dangerous cycle
  at commit (`Serialization`). The canonical write-skew anomaly —
  two transactions each reading an invariant, each writing one half
  — is caught.
- **A real storage engine.** 4 KiB slotted pages, a file-backed pager, and a
  B+tree — with page splits and leaf chaining — that stores both table data and
  the catalog.
- **Bounded memory.** Pages pass through a fixed-size buffer pool with CLOCK
  eviction. A statement whose working set overflows the pool spills dirty pages
  to the WAL instead of to memory, so even a `VACUUM` of a huge database runs in
  constant RAM. A cached page is read without copying — the pool lends it out
  as a pinned, reference-counted handle, and a page in use is never evicted.
- **Secondary indexes.** `CREATE INDEX` builds a B+tree over one or more
  columns. The planner turns an equality or range `WHERE` clause — including the
  leftmost prefix of a composite index — into a bounded index scan instead of a
  full table scan, and every index is kept in step with `INSERT` / `UPDATE` /
  `DELETE`.
- **Queries.** `SELECT` supports `WHERE`, multi-key `ORDER BY` (which an index
  scan can satisfy for free), the `COUNT` / `SUM` / `AVG` / `MIN` / `MAX`
  aggregates, `GROUP BY` to aggregate per group, `HAVING` to filter those
  groups by their aggregates, and `LIMIT` / `OFFSET`.
- **Joins.** `INNER`, `LEFT`, and `CROSS` joins relate tables on an `ON`
  predicate; columns are disambiguated by a `table.column` qualifier or a table
  alias. An equi-join whose inner column is indexed becomes an index
  nested-loop join — a lookup per left row instead of a full rescan — and an
  un-indexed equi-join becomes a *grace* hash join: O(left + inner), and
  bounded-memory by partitioning both sides to disk and joining a partition at
  a time, so an inner table that does not fit in memory still joins.
- **Cost-based planning.** Every table carries a live row count in the
  catalog, maintained by `INSERT` and `DELETE`. A chain of `INNER JOIN`s is
  reordered to minimise a coarse sum-of-intermediate-sizes estimate — small
  tables move to the front, the big one joins last — and orderings that would
  produce a cross product (a join step with no connecting predicate) are
  penalised. `LEFT` and `CROSS` joins, which are not commutative, stay
  exactly where the user wrote them.
- **Subqueries.** `IN (SELECT ...)`, `NOT IN`, `EXISTS`, `NOT EXISTS`, and
  scalar `(SELECT ...)` are all parsed and executed. They are *uncorrelated*:
  the subquery runs once before the outer query's row loop and its result is
  reused. Standard SQL three-valued logic for `IN`/`NOT IN` with `NULL` — the
  well-known surprise that `x NOT IN (a, NULL)` is never `TRUE` — is honoured
  exactly.
- **Streaming execution.** A `SELECT` runs as a volcano tree of pull-based
  operators over a streaming B+tree cursor, and the server streams each row
  onto the wire as the tree yields it — so a `SELECT` of any size costs the
  server only one row of memory. A `LIMIT` query stops scanning the moment it
  has enough rows.
- **Vectorised pipeline.** A "scan-shape" SELECT — no joins, no GROUP BY,
  no ORDER BY — runs through a *columnar* batched operator tree instead:
  each batch holds 1024 rows in struct-of-arrays layout (typed value array +
  null bitmap per output column), and filter and projection evaluate one
  tight loop per column rather than one loop per row through every
  operator. The Apache Arrow memory layout, on the analytic query shape
  where it actually pays off.
- **No value-size limit.** A value too large for a page spills, transparently,
  into a chain of overflow pages — a single row may be megabytes long.
- **Space reclamation.** A delete merges under-full B+tree nodes and collapses
  the tree's height; `VACUUM` rewrites the whole database into a fresh, densely
  packed file in one crash-safe commit.
- **Client / server.** A thread-per-connection TCP server (`prehnited`) and an
  interactive client (`prehnite`) speak a compact length-prefixed binary
  protocol; a result set is streamed across it as a frame per row.
- **Concurrent reads.** The server guards the database with a reader-writer
  lock: a write takes it exclusively, while any number of read-only `SELECT`s
  take it shared and run in parallel. Every pager — the writer's and the
  readers' — shares one bounded buffer pool, so a reader runs against a warm
  cache, and the server's page cache stays one fixed size however many clients
  connect. The pool itself is split into 16 shards (each its own mutex), so
  readers touching different pages no longer queue behind one lock.

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
**planner** validates it and — consulting the catalog for both schemas and
row counts — picks an access path (a full scan or a bounded index scan) and,
for a chain of inner joins, an evaluation order to produce a `Plan`; the
**executor** runs that plan against the **catalog** and the **B+trees**; the
**pager** stages every page it touches and commits them as one transaction
through the **WAL**.

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
cargo test --workspace      # 193 tests across every layer
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
prehnite> CREATE INDEX by_name ON users (name);
index 'by_name' created on users(name)
prehnite> SELECT id FROM users WHERE name = 'ada';
id
--
1
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

PrehniteDB understands one statement at a time:

| Statement    | Form |
|--------------|------|
| Create table | `CREATE TABLE name (col TYPE, ...)` |
| Drop table   | `DROP TABLE name` |
| Create index | `CREATE INDEX name ON table (col, ...)` |
| Drop index   | `DROP INDEX name` |
| Insert       | `INSERT INTO name [(cols)] VALUES (...), (...)` |
| Select       | `SELECT items FROM table [JOIN table ON p ...] [WHERE p] [GROUP BY col, ...] [HAVING p] [ORDER BY key, ...] [LIMIT n [OFFSET m]]` |
| Update       | `UPDATE name SET col = expr, ... [WHERE expr]` |
| Delete       | `DELETE FROM name [WHERE expr]` |
| Vacuum       | `VACUUM` |
| Transaction  | `BEGIN` / `COMMIT` / `ROLLBACK` |

**Types:** `INT`/`INTEGER`, `REAL`/`FLOAT`, `TEXT`, `BOOL`/`BOOLEAN`.

**Select items** are `*`, plain columns, or aggregates — `COUNT(*)`,
`COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`. With `GROUP BY` an aggregate is
computed per group, and a plain column may be selected only if it is a grouping
column; without `GROUP BY`, aggregates fold the whole filtered table into one
row. `HAVING` filters those groups by a predicate over their aggregates — it is
to groups what `WHERE` is to rows. `ORDER BY` on a grouped query sorts the
groups by their grouping columns. `LIMIT` caps how many rows come back, and
`OFFSET` skips that many before the first.

**Joins.** A `FROM` clause may chain `INNER JOIN`, `LEFT JOIN`, and `CROSS
JOIN`, each (except `CROSS`) carrying an `ON` predicate. A table may take an
alias — `FROM users u` or `FROM users AS u` — and a column reference may be
qualified, `users.id` or `u.id`, which a multi-table query needs wherever a
bare name would match two tables.

**Expressions:** integer / real / string / `TRUE` / `FALSE` / `NULL` literals,
column references, arithmetic (`+ - * /`), comparisons (`= != <> < <= > >=`),
`AND` / `OR` / `NOT`, `IS [NOT] NULL`, parentheses, and unary `-`.

**Subqueries.** `expr [NOT] IN (SELECT ...)` tests set membership against the
subquery's single column. `[NOT] EXISTS (SELECT ...)` tests whether the
subquery has any rows. A `(SELECT ...)` in any expression position is a
**scalar subquery**: it must return one row of one column (or none — that
yields `NULL`), and its value is used in place. All subqueries are
*uncorrelated* in v0.19 — they execute once per outer query, before its row
loop, and their result is reused. `NULL` in an `IN`/`NOT IN` set is handled
per the SQL standard's three-valued logic.

`NULL` follows SQL three-valued logic: it propagates through arithmetic and
comparisons, and a `WHERE` clause keeps a row only when the predicate is
exactly `TRUE`. Identifiers are case-sensitive. `--` starts a line comment.

## How it works

### Pages and the pager

The database file is a sequence of fixed 4 KiB pages. Page 0 is the header
(magic, page count, free-list head, catalog root). Every other page is a
*slotted page*: a slot array grows up from the header while variable-length
cells grow down from the end. The **pager** owns the file, hands out pages by
number, and recycles freed pages through a free list.

Pages pass through a **buffer pool** — a fixed-size cache (1024 frames, 4 MiB)
that bounds the pager's memory however large a statement grows. When the pool
is full it evicts a page under the CLOCK policy: each frame has a "recently
used" bit, a hand sweeps the frames, and the first frame whose bit is already
clear is the victim. A *clean* victim — one still matching the database file —
is simply dropped; a *dirty* one, an uncommitted write, is **spilled** to the
WAL first. `read_page` lends the page out copy-free, as a `PageRef` — a
reference-counted handle onto the frame — so a frame a caller still holds is
*pinned*, and the CLOCK hand steps over a pinned frame rather than evicting it.
A frame leaves the cache only once every `PageRef` to it is gone.

### The write-ahead log

A statement's writes never reach the database file directly; they go to the WAL
first. A page image — full, CRC-32-checked — is appended to the log either when
the buffer pool spills it or, for whatever is still resident, at commit time.
`commit` then appends a commit marker and fsyncs, copies every logged page into
the database file, fsyncs that, and truncates the log. A crash before the
marker is durable leaves a markerless log, which the next open discards — the
database file, untouched until the marker exists, is left pristine. A crash
after the marker replays the log. The database file is never half-updated.

Recovery streams the log one record at a time — never holding more than a
single page — so replaying even a transaction larger than memory is safe. And
because `commit` and crash recovery both finish by copying a sealed log into
the database file, they share the very same routine: a commit is just recovery
of a log the pager wrote on purpose.

### The B+tree

Table data and the catalog are both B+trees keyed by byte strings. Interior
nodes only route; all key/value pairs live in leaves, which are chained
left-to-right so an ordered scan is one walk. A node that overflows splits, and
the split can cascade to the root — but the root keeps a *fixed page number*
for its whole life, so the catalog can refer to a table by a number that never
moves.

### Secondary indexes

`CREATE INDEX` builds a second B+tree over one or more columns. A key is the
*order-preserving* encoding of each indexed value, concatenated, followed by the
row's rowid. Order preservation means the tree's byte order is tuple order, so
equality *and* range lookups are both plain key-range scans; the encoding is
also self-delimiting, which is what lets several columns share one key. Every
`INSERT`, `UPDATE`, and `DELETE` maintains every index in the same transaction
that changes the table, so the two can never disagree.

The planner classifies each `AND` conjunct of a `WHERE` clause as an equality or
a range on one column, then walks an index's columns left to right: equality
predicates extend a pinned key prefix, and the first non-equality column may add
one range bound — the standard "leftmost prefix" rule. The result is a `[lower,
upper)` key range; the executor scans it, fetches those rows, and *still*
applies the whole `WHERE` clause. An index only narrows the search — it never
changes an answer.

### Streaming execution

A `SELECT` runs as a *volcano* tree of operators — a scan at the leaves, then
`Filter`, `Sort`, `Project`, and `Limit` stacked above — each a pull-based
iterator whose `next` draws a single row from the operator below it. Rows
stream through the pipeline one at a time instead of being gathered into an
intermediate buffer, and the B+tree scan is itself a cursor that holds only the
current leaf, so a query never materializes the whole table just to walk it.

The clearest payoff is `LIMIT`: the `Limit` operator stops pulling the instant
it has its quota, so `SELECT ... LIMIT 10` reads about ten rows out of the tree
and no further — memory proportional to the limit, not the table. The lone
exceptions are the operators that must see all of their input before they can
emit anything — `Sort`, and the `GROUP BY` pass — which buffer; everything
downstream of them still streams.

That pull model now reaches past the executor to the wire. `Database::execute`
still drains the tree into a `QueryResult` for an embedder who wants the whole
answer in hand; but the server pulls one row, frames it, and writes it to the
socket before pulling the next — a `RowsBegin` carrying the column names, then
a `Row` per row, then a `RowsEnd`. A `SELECT *` of a million-row table thus
costs the server one row of memory rather than a million. The price is that a
streaming reader holds its lock for the whole reply — the pager is pulled from
throughout — where a buffered reply could have released it first.

### Joins

A join is one more operator in the volcano tree. `NestedLoopJoin` streams its
left input and, for each left row, scans a buffered copy of its right input,
emitting the concatenations whose `ON` predicate holds — and, for a `LEFT`
join, any left row that matched nothing, padded with `NULL`s. Chaining
`a JOIN b JOIN c` simply stacks two of these, the outer one taking the inner as
its left input.

When the `ON` clause is an equality whose inner side is the leading column of
an index, the join becomes an `IndexNestedLoopJoin` instead: it never buffers
the inner table at all — each left row evaluates the join key, looks it up in
the index, and fetches only the matching rows. That turns the join's cost from
O(left × inner) into O(left × log inner). The full `ON` predicate is still
re-applied to each pair, so the index only ever narrows the search.

Without an index, an equi-join takes the *grace hash join* path: both inputs
are partitioned to disk by the hash of their join key — equal keys hash to
the same partition, so matching rows are confined to one partition pair — and
each pair is then joined in memory by an ordinary hash join. The per-partition
join builds a table on the inner side, probes per left row, and re-applies
the full `ON` predicate to each pair, so the hash only narrows. The cost
profile against the nested loop is O(left + inner) rather than O(left ×
inner); the win against a purely in-memory hash join is bounded memory — only
one partition's hash table is live at a time, regardless of the inner table's
size. The price is that the left side no longer streams: it is drained into
its partition files before the join phase can begin. `NULL` join keys never
match — an inner row whose key is `NULL` is dropped during partitioning, and
a `NULL`-keyed left row is sent to one fixed partition and probes nothing
there.

The executor is no longer single-table: it builds a *scope* — the columns of
every joined table, each tagged with its table's name or alias — and a column
reference resolves against it, a qualified one by table *and* name, a bare one
by name alone (rejected as ambiguous if two tables offer it). A join with no
equi-join condition at all — a `CROSS JOIN`, or `ON a.x <> b.y` — falls back to
the buffered nested-loop join over a full scan, which is the general fallback:
it is correct for any predicate.

### Planning

The planner does the bind-and-plan pass between parser and executor. It
validates a statement against the catalog, lowers parser types into the
engine's, and — for a query that has more than one place it could go — picks
which one to go to.

Two such choices are open. The first is *access-path selection* for a
single-table `WHERE`, described above under
[Secondary indexes](#secondary-indexes): the leftmost-prefix rule turns a
predicate into a bounded index scan, or falls back to a full scan when no
index helps.

The second is *join reordering*. `INNER JOIN` is commutative and associative,
so the query's left-to-right order is the planner's call. Every table carries
a row count in its catalog entry, kept current by `INSERT` and `DELETE`;
when a `FROM` is a chain of `INNER JOIN`s, the planner enumerates every
ordering (capped at eight tables — 8! permutations is ~40k, still tens of
microseconds) and scores each by a simple sum-of-intermediates estimate:
at each join step the intermediate is `max(prev, new_table_rows)` when a
predicate ties the new table to the existing set, and `prev * new_rows` when
none does. The cheapest ordering wins; ties keep the user's order. ON
predicates ride along — each one re-attaches to the first join step where
every table it mentions is in the joined set, ANDed with any others landing
there.

The product penalty matters: with three tables `a`, `hub`, `b` where `a`
and `b` only join through `hub`, putting `a` and `b` adjacent has no
predicate connecting them — a cross product. The product scoring makes the
chain that goes through `hub` first the obvious winner without any
special-case logic.

A `LEFT` or `CROSS` join anywhere in the chain freezes the layout — those
joins are not commutative, so the user's order is the answer. Likewise a
chain whose ON predicates use unresolvable column names (an ambiguous bare
reference, an unknown qualifier) is left alone rather than risk misplacing a
predicate. The reorder is opportunistic; correctness never depends on it.

### Subqueries

A `WHERE`, `HAVING`, or `SELECT` list may contain a `SELECT` of its own as a
subquery, in three syntactic positions:

- `expr IN (SELECT ...)` or `expr NOT IN (SELECT ...)` — set membership
  against the subquery's single column. The subquery must return one column.
- `EXISTS (SELECT ...)` or `NOT EXISTS (SELECT ...)` — whether the subquery
  yields any row. The subquery's columns and values are ignored.
- `(SELECT ...)` in any expression position — a *scalar subquery*. It must
  yield one row of one column (zero rows is `NULL`; more than one row is an
  error), and the value substitutes for the subquery.

In v0.19 every subquery is **uncorrelated**: it cannot reference columns from
the outer query, and a column it does mention has to resolve inside its own
`FROM`. Uncorrelation has a payoff: the subquery executes *once*, before the
outer row loop starts, and the executor rewrites the subquery node in-place
with its materialised result — an `Expr::ScalarSubquery` becomes a literal,
an `Expr::Exists` becomes `Expr::Bool(true_or_false)`, and an
`Expr::InSubquery` becomes an `Expr::InList` carrying the collected values
plus a `has_null` flag. The per-row `eval` only sees pre-resolved nodes.

`NULL` in an `IN` set follows the SQL standard's three-valued logic: `x IN
(set)` is `TRUE` if `x` matches a value, `FALSE` if it matches none and the
set has no `NULL`, and `NULL` if it matches none but the set holds a `NULL`
(or `x` itself is `NULL`). `NOT IN` is the boolean negation, so `x NOT IN
(a, NULL)` is `NULL` for any non-matching `x` — a `WHERE` clause keeps no
such row. This is the standard well-known surprise; PrehniteDB reproduces it
exactly.

The rewrite-in-place machinery keeps the rest of the executor unaware that
subqueries exist: the volcano operator tree, the per-row evaluator, the
filter and projection operators all see only the existing `Expr` shapes
they already handle. The cost: the subquery must materialise in full into
memory (or onto whatever the executor itself spills through, for joins) —
fine for the small lookup tables `IN (SELECT ...)` typically targets, less
so for `IN (huge subquery)`. The cure, future work, is to leave the
subquery as a streaming source for the IN check rather than materialise it.

A correlated subquery — one that references the outer row's columns — is
not yet supported. The shape would be the same on the parser side, but the
executor would need to plan and re-execute the subquery per outer row
(with a scope that spans both queries), which is a substantial separate
pass; v0.19 deliberately stops short.

### Vectorised pipeline

The volcano operator tree described above moves one row at a time through
the pipeline. That is the right shape for joins, sort, and grouping, all of
which need full row tuples to do their work. It is *not* the right shape
for the analytic-query majority — scan, filter on some predicate, project
some columns, optionally limit. Each operator pays a per-row dispatch cost
the work itself does not justify; each predicate evaluation hops between
column types row by row.

v0.21 adds a second operator tree, alongside the existing one, that the
planner uses when the query qualifies — no joins, no `GROUP BY`/`HAVING`/
aggregates, no `ORDER BY`. Otherwise the row-at-a-time tree runs unchanged.

#### Columnar batches

The data unit is a [`ColumnBatch`](crates/prehnitedb/src/engine/batch.rs):
up to 1024 rows in **struct-of-arrays** layout — one typed value array
*per output column*, each paired with a packed null bitmap (one bit per
row, `1` = valid, `0` = `NULL`, in `Vec<u64>` words). At a null position
the underlying typed slot holds whatever the column's zero is; it is
never read. This is the layout Arrow, DuckDB, Polars all use: a
columnwise scan touches a contiguous slice of one type, the loop has no
type-dispatch branches, and a 1024-row mask fits in 128 bytes — well
within L1 alongside the value arrays.

A batch may also carry a **selection vector** — a `Vec<u32>` of physical
row indices into the underlying columns. When present, the batch's
logical rows are the indices in the selection, in that order; the
columns themselves are unchanged. A filter that survives 100 of 1024
rows produces a batch holding the same 1024-row columns and a 100-entry
selection, instead of copying 100 rows out into fresh columns. Operators
above read through `row_at(logical)` which maps through the selection
transparently.

#### Batched operators

- `BatchScan` decodes up to 1024 rows from one `cursor.next` loop into a
  `ColumnBatch`. The B+tree leaf is touched once per batch instead of
  once per row.
- `BatchFilter` evaluates the predicate columnwise to produce a Bool
  column, then walks the mask to build a fresh `Vec<u32>` of physical
  row indices for the surviving rows. The column data passes through
  untouched; the next operator sees a batch whose `selection: Some(...)`
  carries the new row set. SQL three-valued logic is exact: `NULL` and
  `FALSE` both drop the row; only `Bool(true)` is kept.
- `BatchProject` evaluates each output expression columnwise: column
  references clone the input column straight through, arithmetic and
  comparisons run tight per-element loops with null propagation, AND/OR
  follow three-valued logic.
- `BatchLimit` counts rows across batches, slicing the last one
  partially when the quota lands mid-batch (a slice of the selection,
  not the column data) and stops pulling — the scan ends early on a
  small `LIMIT`.
- `BatchToRow` is the adapter that exposes a `BatchOperator` tree as the
  `Operator` (`fn next() -> Option<Vec<Value>>`) interface the rest of
  the executor consumes. The streaming protocol upstream is unchanged.

#### Columnar `eval`

A second evaluator runs alongside the per-row one: `eval_batch` recurses
through an `Expr` and returns a `Column` of `n_rows`. Literals broadcast
to a full column; column references clone the matching input column;
arithmetic and comparisons run element-wise loops with overflow checks;
logical AND/OR/NOT walk three-valued tables. `IS NULL` is a definite
boolean and a one-bit-per-row test. `IN`/`InList` falls back to per-row
within the columnar shell — fast columnar paths for set membership are a
future optimisation. Aggregates are not allowed in the vectorised path;
the planner steers any aggregate-bearing query back to the row
pipeline.

#### When the vectorised path is used

The planner picks the batched tree whenever:

- the query has no `GROUP BY`, `HAVING`, or aggregate in the projection,
- there is no `ORDER BY`,
- and no join would prefer an index nested-loop (the row pipeline keeps
  that optimisation; the batched path covers everything else).

Joins go through `BatchHashJoin` (equi-joins — build a `HashMap<key,
rows>` from the inner side, probe per left row, reapply the full ON
predicate, emit one row per match) or `BatchNestedLoopJoin` (everything
else — drain the right side once, scan it per left row, evaluate the ON
predicate per pair). Both produce `ColumnBatch`es up to 1024 rows;
`LEFT` joins pad unmatched left rows with `NULL` columns from the
right side. State persists between `next_batch` calls so an output
batch can split mid-left-row.

Anything that needs `ORDER BY`, grouping, or an index nested-loop keeps
the row-at-a-time pipeline. The row tree still handles every operator
the batched tree handles plus those — correctness is preserved across
the choice.

### Sorting, grouping, and aggregates

`ORDER BY` sorts the matched rows with a stable, total comparator (`NULL`s sort
first) before they are projected — unless the index scan the planner already
chose happens to yield them in the requested order, in which case it flags the
plan *presorted* and the executor skips the sort.

A `GROUP BY` (or a bare aggregate, which is just "group by nothing") runs the
matched rows through a **hash aggregator**: one pass over the input, one
hash-map bucket per distinct grouping-column tuple, and per-row updates to the
bucket's running aggregate state. `COUNT` is a `u64`; `SUM` and `AVG` carry a
running total (and count, for `AVG`); `MIN` and `MAX` hold the current best
value seen. Each aggregate runs in `O(1)` per row — no scan over the group's
rows at finalisation time. Memory is `O(distinct groups)` rather than
`O(input rows)`; the old sort-then-partition pass needed the whole input
materialised and then sorted.

The hash key is the tuple of grouping-column values, with a custom `Eq`/`Hash`
over [`Value`] — `Real` is bit-compared via `to_bits` (every `NaN` lands in
one bucket, `-0` and `0` stay distinct) and `NULL` forms its own group, both
following SQL convention. The same `Aggregate` AST node appearing in both the
projection and the `HAVING` clause is recognised as one slot — it is computed
once per group, not once per call site.

A `HAVING` clause is then evaluated against each finalised group, with column
references resolving to the group's value for that grouping column and
aggregate calls looking up their precomputed slot. Groups whose predicate is
not exactly `TRUE` are dropped — `WHERE` applies to rows, `HAVING` applies to
groups.

### Overflow pages

A value that does not fit in a page spills into a linked chain of *overflow
pages*; the B+tree leaf cell then holds just a one-byte tag and a pointer to the
chain. This is invisible above the storage layer — `insert`, `search`, and
`scan` reassemble spilled values transparently — and invisible below it: the
slotted-page code never learns a value can live elsewhere. An overflow cell is
tiny, so every leaf cell still fits `MAX_CELL` and the split proof is
undisturbed. Keys are never spilled.

### Reclaiming space

A run of deletes would otherwise leave a B+tree sparse — many half-empty nodes,
the same height as ever. So after a key is removed, PrehniteDB checks whether
the node and one of its siblings together fit in a single page; if they do it
merges them, drops the now-redundant separator from the parent, and frees the
emptied page. Merges cascade upward exactly as splits do, and when the root is
left with a single child the tree loses a level. A tree that has seen many
deletes stays shallow and dense.

Merging returns pages to the free list, but the file itself never shrinks —
freed pages are reused, not removed. `VACUUM` is what shrinks it: it rewrites
every table and index into a fresh, densely packed file with no free space,
then swaps that image in atomically. The rebuilt pages are staged through the
same WAL as any other commit, so a crash mid-`VACUUM` simply leaves the
original database intact.

### Transactions and MVCC

By default each statement auto-commits — it runs as its own transaction,
committed on success and rolled back on failure, so a rejected statement never
leaves a partial effect. `BEGIN` opens an explicit transaction instead, but
v0.26 changes what "open" means: each statement inside the transaction is
*physically* committed when it runs — stamped with the writer's TX ID and
sealed to the WAL — and the logical `COMMIT` just appends a `committed`
record to the **commit log** (`.db-clog`), at which point every snapshot
starts seeing the rows. `ROLLBACK` appends a `rolled-back` record instead;
the rows the writer stamped stay in the file but become invisible to every
snapshot, and `VACUUM` eventually reclaims them.

PrehniteDB runs under **MVCC snapshot isolation**. Every row carries two
extra 8-byte fields: `tx_min`, the transaction ID that created it, and
`tx_max`, the transaction ID that logically deleted it (0 = still live).
Each statement takes a *snapshot* at start — the current next-TX counter
plus the **set** of every in-flight write transaction at that instant —
and a scan filters every row against that snapshot: visible iff `tx_min`
is *committed* per the clog and committed-before-us, and `tx_max` is
either zero, future-to-us, in-flight, or rolled back. `DELETE` is
*logical* — the row stays in the B+tree with `tx_max` set, and the table
size grows until `VACUUM` reclaims the tombstones. `UPDATE` is
delete-plus-insert: the old version is tombstoned and a new row is
inserted at a fresh rowid, both stamped with the writer's TX ID.

The commit log is a per-database, append-only file of fixed 9-byte
records (8-byte TX ID + 1 status byte), fsynced on every append. On
open it is read into an in-memory `HashMap<u64, Status>` so visibility
lookups are O(1). A TX ID below the persisted `next_tx_id` that has no
clog entry is treated as **rolled back** — the crash-recovery rule —
so a writer that died mid-transaction leaves no rows visible to anyone.

### Concurrent writers and conflict detection

Multiple writers can have transactions open at the same time, and from
v0.28 they can execute *truly in parallel* when they touch different
tables. Each writer takes its own TX ID at the first writing
statement, the shared `TxState` tracks the *set* of in-flight IDs, and
every snapshot captures that set at its start — a row stamped with any
in-flight ID is invisible to readers other than the writer itself
(`own_tx` is the visibility override that lets a writer see its own
work).

The server gives each TCP connection its own `Database` handle —
sharing the buffer pool, the `TxState`, the commit log, and a
**`SharedMeta`** that holds the database header — but with its own
per-pager catalog cache and transaction state. Writes serialise on
**per-table mutexes** (stored on `TxState`, looked up by parsed table
name): two writers on different tables proceed in parallel; same-table
writes serialise on one mutex. Page allocation goes through
`SharedMeta` so concurrent allocators never hand out the same page
number, and `Catalog::put` (the schema-update path every `INSERT` /
`UPDATE` / `DELETE` walks) takes a brief engine-internal lock so two
writers' read-modify-write cycles on a shared catalog leaf can't lose
each other's `next_rowid` bumps.

Each pager tracks its **own** dirty pages (`dirty_pages: HashSet<u32>`)
instead of trusting the shared pool's per-frame dirty bit. At commit,
the pager flushes only its own writes; a peer pager's in-flight pages
stay put for its own commit. Each pager also writes to its **own** WAL
file — `<db>-wal-<id>`, where `id` is a monotonic counter in
`SharedMeta` — so two concurrent `commit`s never share a WAL cursor.
The legacy single-WAL path (`<db>-wal`, used by v0.27 and earlier) is
recovered at first open for backwards compatibility.

Rollback in this model does **not** revert the shared header. A
writer that aborts after allocating pages would otherwise wind back
counters that a peer writer has already advanced past. Instead, the
rolling-back pager stashes its allocated pages in a per-pager
`pending_freelist` for reuse on its next allocation; pages that
escape that reuse (the connection drops) are reclaimed by `VACUUM`.

### Serialisable Snapshot Isolation

Snapshot isolation has a famous gap: **write-skew**. Two transactions
each read both halves of an invariant (say, two account balances
with the constraint that their sum stays non-negative), each decides
based on the snapshot to draw down "their" half, and each writes the
other half no one else is writing — so they pass first-updater-wins.
Both commit; the invariant breaks. SI sees no conflict because the
overlap is between one transaction's *reads* and the other's
*writes*, not between their writes.

PrehniteDB v0.29 closes this with **Serialisable Snapshot
Isolation** — the Cahill algorithm, the same one Postgres adopted
for SERIALIZABLE. The substrate is two things:

1. **Transaction-wide snapshot.** `BEGIN` reserves a TX ID and
   pins one snapshot; every statement inside the transaction reads
   against that snapshot. v0.25–v0.28 captured a fresh snapshot per
   statement (REPEATABLE-READ-ish); v0.29 needs one snapshot per
   transaction to make read-set tracking meaningful.

2. **Read-set tracking.** Every tuple this transaction observes —
   in any scan, including the scans inside `UPDATE` and `DELETE`
   that look for candidates — is added to a per-TX read-set
   indexed by `(table_root, rowid)`.

With those in place, the engine detects **rw-dependency edges**
between in-flight transactions:

- When transaction `R` reads a tuple whose `tx_max` is in-flight by
  some writer `W`, that's an edge `R → W` — `R` read what `W` is
  mid-modifying. Record on `R.out_conflict = true`, `W.in_conflict
  = true`.
- When transaction `W` writes (tombstones) a tuple, walk every
  in-flight peer `R`'s read-set; for each match, record the same
  edge.

A transaction with **both** `in_conflict` and `out_conflict` set is
the *pivot* of a dangerous structure (Cahill's simplification of
"cycle of rw-edges"). At commit time, the pivot transaction aborts
with `Error::Serialization`, breaking the cycle.

The tradeoff is honest: tuple-level tracking is **pessimistic**.
Two transactions whose `WHERE` clauses target disjoint rows but
whose scans walk overlapping ranges (because there's no index) will
form spurious rw-edges and may both abort. Postgres mitigates this
with `SIREAD` locks at page and relation granularity; PrehniteDB
v0.29 stops at tuple level. The classic write-skew on distinct
rows is still caught, and writes to entirely separate tables stay
edge-free.

When two writers both try to mutate the same row, the second one
detects the **write-write conflict** at write time and aborts under
*first-updater-wins*: when collecting candidate rows for an UPDATE or
DELETE, the executor inspects each row's `tx_max`. A non-zero `tx_max`
belonging to another in-flight transaction means a peer has already
claimed this row as a tombstone — and the conflicting statement
returns `Error::Conflict`, which aborts the transaction. A `tx_max`
belonging to a committed transaction means the row was already
deleted before our snapshot; we skip it as not-a-candidate. A
rolled-back `tx_max` we ignore — the tombstone never took effect.

`VACUUM` is the MVCC garbage collector. It rewrites every table and
index into a fresh densely packed file, but now skips two kinds of
physically present but dead row: tombstones whose `tx_max` is
committed per the clog, *and* rows whose `tx_min` is rolled back per
the clog. Both are gone from every snapshot's view; both can be
permanently discarded.

### Concurrent readers — and concurrent reader + writer

A read in PrehniteDB mutates: `read_page` admits pages into the buffer pool and
turns CLOCK bits, so even a `SELECT` needs `&mut` access to the pager. Until
v0.12 that forced every statement to take the database lock exclusively, and a
`SELECT` waited behind every other connection.

v0.25 closed that gap for readers — they took snapshots and ran lock-free
alongside the single writer. v0.26 extended the same MVCC machinery to
writers at the *engine* layer: the `in_flight` set became a
`HashSet<u64>` (multiple TXs may be live at once), every snapshot
captures that whole set, and the persistent commit log answers the
visibility question — *was this TX committed at the instant the
snapshot was taken?* — for any TX, any time. v0.27 carries that all
the way to the wire: each TCP connection has its own `Database` handle
(sharing the pool, `TxState`, and clog), and the server's writer
mutex shrinks from "held across `BEGIN..COMMIT`" to "held across one
statement". Two clients can have transactions open simultaneously,
their statements interleave at the lock, and FUW aborts a conflicting
overlap with a `conflict:` error frame.

What a reader's pager does *not* own is its buffer pool. Every pager the server
opens — the writer's and each reader's — shares one `SharedPool`, a bounded
cache split into **16 independent shards**, so a reader opens against a warm
cache instead of filling a cold private one, and the server's page cache stays
one fixed size however many clients connect. Each shard is its own CLOCK
cache with its own mutex. The buffer pool may now carry an in-flight
writer's dirty pages alongside committed ones — that's the price of
concurrent reader+writer — and the reader's visibility check on each row
filters out anything the snapshot can't see. `read_page` still lends pages
out copy-free as reference-counted handles, so the shard lock is held only
long enough to find a frame, never while one is being read.

A `SELECT` inside an open writer transaction is the exception: it must see the
transaction's own uncommitted writes, so it runs on that connection's pager.
The visibility rules give it the override — `tx_min == own_tx` is visible
to self.

## Limitations

PrehniteDB is young; it still omits:

- *correlated* subqueries (a subquery that references the outer row),
  `RIGHT` / `FULL OUTER` joins, derived tables (`FROM (SELECT ...) AS s`),
  CTEs (`WITH`), and `ANY` / `ALL`;
- `ALTER TABLE`;
- index keys larger than ~2 KiB — large *values* spill to overflow pages, but
  indexing a column of large values is still rejected;
- **predicate locks** for SSI — v0.29 tracks reads at tuple
  granularity. Postgres's `SIREAD` lock escalation to page or
  relation granularity catches conflicts on logically-disjoint
  predicates that happen to read the same rows; PrehniteDB doesn't
  have it yet, so SSI is **pessimistic** (some valid serial
  schedules abort that wouldn't need to), and **incomplete**
  against phantoms (a transaction whose `SELECT * FROM t` is
  followed by a peer's `INSERT INTO t` doesn't catch the phantom);
- the SSI **cycle detection** is the simple commit-time
  `in_conflict && out_conflict` check. An n-cycle (n ≥ 2) of
  symmetric writers may abort more than the strict minimum (one)
  before the cycle breaks;
- **unbounded** per-TX read-set memory — long-running read-write
  transactions accumulate every observed `(table, rowid)` pair.
  Postgres caps with lock escalation; PrehniteDB v0.29 does not;
- automatic background VACUUM — MVCC garbage (tombstones, rolled-back
  rows) currently waits for an explicit `VACUUM`. `VACUUM` is also
  not safe with concurrent writers — the engine assumes no other
  writes are in flight when it rebuilds the file;
- same-table parallel writes — two writers on the *same* table still
  serialise on its per-table mutex. Finer granularity (per-page
  latching on the B+tree) is its own bigger piece of work;
- DROP TABLE racing with concurrent writers on that same table — the
  catalog drop and the table writes don't share a lock today, so
  applications must coordinate this at the SQL layer;
- any authentication on the network protocol.

It is also pre-1.0: the on-disk format is not yet stable, so a database file
written by an earlier version will not open.

## Roadmap

Natural next steps, roughly in order: **predicate locks** for SSI,
escalating from tuple to page to relation granularity so logically-
disjoint reads stop forming spurious rw-edges and phantoms (inserts a
read might have observed) get caught; **B+tree latch crabbing** so
two writers on the *same* table can interleave their inserts at the
page level instead of serialising on the table mutex; automatic
background **VACUUM** of MVCC tombstones and rolled-back rows, driven
by an oldest-active-TX watermark, and made safe under concurrent
writers; **clog truncation** so the commit log doesn't grow
unboundedly; extending the vectorised tree to `ORDER BY` and feeding
it into the hash aggregator; *correlated* subqueries, with scope
propagation and per-outer-row re-execution (often optimised to
semi-joins); column statistics (distinct-value counts, small
histograms) to give the planner real selectivity instead of just
table cardinalities.

## Engineering notes

[`DEEP_DIVE.md`](DEEP_DIVE.md) is a per-session engineering log — the
architecture, algorithms, and design decisions behind each version.

## License

MIT — see [LICENSE](LICENSE).
