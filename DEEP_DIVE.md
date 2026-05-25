# PrehniteDB — Technical Deep Dives

A per-session engineering log: the architecture, algorithms, and design
decisions behind each version of PrehniteDB, written at a depth meant for
someone who works on systems software. Each section covers one development
session and the version it produced.

---

## Session 1 — PrehniteDB v0.1: a SQL database from nothing

**Shape.** A 3-crate Cargo workspace, **zero external dependencies** (only
`std`). The library is a strict layer stack — `storage < sql < engine <
protocol` — where each layer knows only the one beneath it.

### Storage: the page

The file is a sequence of 4 KiB pages. Page 0 is the header (magic, page size,
page count, free-list head, catalog root). Every other page is a *slotted
page*: a 16-byte header, then a slot array growing **up** from the header while
variable-length cells grow **down** from the page end — free space is the gap.
Two cell shapes: leaf `[key_len u16][val_len u32][key][val]`, interior
`[child u32][key_len u16][key]`.

The key decision is **materialize-modify-rebuild**: every mutation reads *all*
cells into a `Vec`, edits the `Vec`, and rebuilds the page from scratch. The
payoff is that pages are *never fragmented* — there is no in-page free list, no
compaction code, ever. The cost is an O(page) rewrite per operation, but
in-place slotted-page surgery is also O(page) (slot-array shifts), so it is the
same big-O with one extra allocation. Correctness bought cheaply.

### Storage: the split-correctness proof

The most interesting bit of the whole engine. When a node overflows it must
split into two pages that **each fit** — and with variable-length cells that is
not automatically true. The counterexample: page capacity 100, cells
`[50, 100, 50]` — total 200, but *no* contiguous 2-way split leaves both halves
≤ 100. And it is reachable: a valid pre-insert leaf `[50, 50]` (=100) plus a
100-byte cell inserted in the middle.

The fix is the cap `MAX_CELL = USABLE/2 − 2` (~2 KiB). Then: pre-insert page ≤
`USABLE`, new cell ≤ `USABLE/2`, so total ≤ 1.5·`USABLE`, and a greedy "largest
prefix that fits" split *provably* yields two sides each ≤ `USABLE`.
`split_index` tries a balanced cut first and falls back to that proven-correct
greedy cut. This is why rows are capped at ~2 KiB — it is not arbitrary, it is
the precondition that makes splitting total.

### Storage: pager, WAL, transactions

The pager buffers writes — `write_page` stages a page in a `dirty` map,
`read_page` checks `dirty` first (read-your-writes) — and nothing hits disk
until `commit`. Rollback is subtle because `alloc_page` mutates
`page_count`/`freelist_head` in memory, so the pager keeps two `Meta`
snapshots, `meta` (working) and `committed`; rollback restores one from the
other.

Durability is a **write-ahead log** of full-page redo images. `commit` is three
ordered steps: (1) write every dirty page + CRC-32 to the WAL, then a commit
marker, **fsync**; (2) write the pages into the database file, **fsync**; (3)
truncate the WAL. The ordering *is* the correctness argument: a crash in (1)
leaves no commit marker → recovery discards → the transaction never happened; a
crash in (2) leaves a complete WAL → recovery replays it → the database file is
completed; a crash in (3) replays harmlessly (idempotent). The database file is
never observed half-updated. The CRC catches torn writes.

### Storage: the B+tree

Both table data and the catalog are B+trees over byte-string keys; leaves are
chained so a scan is one walk. The notable trick: **the root page number is
immortal**. A root split would normally move the root, so instead the old
root's content is copied to a fresh page and the root *page* is rebuilt in
place as the new 2-child node — the catalog can therefore store a table's root
as a number that never changes. Table keys are 8-byte **big-endian** rowids, so
the tree's byte order is numeric order.

### Engine

The catalog is itself a B+tree (table name → encoded `Schema`). The rowid
decision is worth noting: "rowid = max key + 1" was rejected because deleting
the highest rows makes the max regress and a new insert would collide with a
live row — so `next_rowid` is stored in the schema, monotonic, never reused.
The executor implements **SQL three-valued logic** — `NULL` propagates through
arithmetic and comparison, and `AND`/`OR` let a definite `FALSE`/`TRUE` win
even against `NULL` — and a `WHERE` keeps a row only on exactly `Bool(true)`.
Every `execute` is one transaction: commit on success, roll back on any error.

The server is thread-per-connection over `Arc<Mutex<Database>>`, holding the
lock only during `execute` (not during socket I/O), so a slow client never
blocks another's query.

---

## Session 2 — Secondary indexes (v0.2)

### The index B+tree and order-preserving keys

A secondary index is its own B+tree with key = `order-preserving-encoding(value)
++ 8-byte rowid`, empty value. The rowid lives *in the key* because a non-unique
index has many rows per value and a B+tree maps each key to one value — the
rowid suffix makes every entry distinct, and an equality lookup becomes a range
scan over all keys sharing the value prefix.

The encoding must make **byte order equal value order**:

- **Int** — flip the sign bit, big-endian: two's-complement order becomes
  unsigned byte order.
- **Real** — remap IEEE-754 bits (sign set → flip all; else flip the sign bit)
  so the bit pattern sorts numerically.
- **Text** — the hard case: a variable-length value followed by a rowid needs
  an unambiguous boundary. Escape `0x00 → 0x00 0x01`, then append a `0x00 0x00`
  terminator. `00 00` sorts before any escaped content and — because it can
  never appear *inside* escaped content — is an unambiguous delimiter. The
  encoding is both order-preserving *and* self-delimiting (which is exactly what
  makes multi-column indexes possible in v0.3).

### The planner and a key subtlety

The planner gained catalog access: it flattens the `WHERE` clause's top-level
`AND` conjuncts and looks for `col = literal` on an indexed column. The
subtlety: it must **coerce the literal to the column's type first**, because
index keys are built from *stored* (coerced) values — a `REAL` column queried
`WHERE r = 5` has literal `Int(5)` but the index holds `Real(5.0)`, which encode
to different type tags. And `WHERE col = NULL` never uses an index (it is never
`TRUE`).

The governing principle in the executor: **the index narrows, the filter
decides.** An index lookup yields *candidate* rowids; the executor fetches those
rows and still applies the *complete* `WHERE`. So an index is purely an
optimization — it may over-include but never changes an answer. Every
`INSERT`/`UPDATE`/`DELETE` maintains every index inside the same transaction as
the table change, so the WAL commits them atomically together and table and
index cannot diverge.

---

## Session 3 — Range and composite index scans (v0.3)

### Generalizing the access path

v0.2's `AccessPath` had `IndexEq { index_root, value }` — a point lookup only.
v0.3 replaced it with `IndexScan { index_root, lower, upper }` — a raw byte-key
range, `upper = None` meaning open-ended. The consequence is a clean shift of
responsibility: **the planner now owns all index-key reasoning** and emits raw
byte bounds; the executor's lookup path is dumb — `scan_range(lower, upper)`,
take the rowid suffix of each key, fetch, filter.

### Why range scans were almost free

The order-preserving encoding from v0.2 already makes the index B+tree sorted by
value, and `BTree::scan_range` already existed. So a range query is just a
key-range scan — all the new work landed in the *planner*. The bound
construction reuses v0.2's `prefix_upper_bound` for the strict/non-strict
boundary: `col >= v` → lower `enc(v)`; `col > v` → lower
`prefix_upper_bound(enc(v))` (step past every key whose column equals `v`);
`col < v` → upper `enc(v)`; `col <= v` → upper `prefix_upper_bound(enc(v))`.

### Multi-column indexes — the self-delimiting payoff

A composite index over `(c1, …, cn)` has key `enc(v1) ++ … ++ enc(vn) ++
rowid`. This works **only because the v0.2 encoding is self-delimiting** —
fixed-width for `INT`/`REAL`/`BOOL`, terminator-delimited for `TEXT`.
Concatenating self-delimiting, order-preserving encodings yields an encoding
that is itself order-preserving on *tuples* (plain lexicographic order). That
property was designed in v0.2 for an unrelated reason — separating value from
rowid — and turned out to be exactly what composite keys require.

### The leftmost-prefix rule, and why only one range column

`build_index_scan` walks an index's columns left to right. Equality predicates
on consecutive leading columns extend a *pinned prefix* `P`; the first column
after the run may contribute **one** range bound; columns past it are ignored
(the filter handles them).

The "only one range column" rule is not a simplification — it is the actual
shape of what a one-dimensional sorted tree can answer. `region = 'east' AND
year >= 2022` is a *contiguous* key range; `region > 'd' AND year = 2022` is
not — the `year = 2022` rows are scattered across every region past `'d'`. Once
you range on a column, the columns after it stop being contiguous in key space.
A query that constrains only a *non-leading* column cannot use the index at all
— the planner falls back to `FullScan`.

### Predicate classification and selection

`choose_access` flattens the `WHERE` clause's `AND` conjuncts; `classify` turns
each into an equality/lower/upper predicate, orienting `5 = id` into `id = 5` by
flipping the operator. Index selection scores each usable index by
`(pinned_columns, has_range)` and picks the max — a real, if simple, cost
heuristic with no statistics yet.

### Correctness and the format break

The bounds the planner computes are a *superset* of the true matches; the
executor applies the full `WHERE` to every candidate, so the planner only has to
produce a conservative bound, never an exact one. The schema encoding changed
(single-column → length-prefixed columns), so the file magic was bumped
`PREHNDB1` → `PREHNDB2` — an older file is cleanly rejected rather than
mis-read.

---

## Session 4 — ORDER BY and aggregates (v0.4)

### `ORDER BY`: the explicit sort, and the free one

`ORDER BY` is implemented two ways, and the planner picks. **Explicit sort:** the
executor collects the filtered rows as full `Vec<Value>` rows, stable-sorts
them, *then* projects — sorting the full row is what lets `SELECT a FROM t ORDER
BY b` work. **Free sort:** when the `WHERE` clause already drove an index scan,
the rows arrive in index-key order; if that matches the `ORDER BY`, the planner
flags the plan `presorted` and the executor skips the sort.

### The total comparator

`sort_by` demands an infallible comparator, but the executor's general `compare`
returns `Result` (it errors on incompatible types). Within one column that
cannot happen, but the type system does not know that. So `order_values` is a
dedicated **total** order: `NULL` sorts before everything; same-type values
compare naturally (`f64::total_cmp`, so even NaN has a place); mismatched types
fall back to a per-variant rank. Total and infallible by construction — and
reused for `MIN`/`MAX`.

### Detecting index-provided order

An index scan walks the tree in `(c1, …, cn, rowid)` order. But
`build_index_scan` may have **equality-pinned** the first `k` columns — constant
across every result row. So the *effective* order is `(c_{k+1}, …, cn)`. `ORDER
BY` is free iff its keys (all `ASC`) form a prefix of that. The leftmost-prefix
reasoning from v0.3 applied to *output order* instead of *search bounds*.

### Aggregates reshape the `SELECT`

A normal `SELECT` is row-in, row-out; an aggregate `SELECT` is set-in,
*one*-row-out. The difference is pushed into the `Projection` type. Aggregate
function names (`COUNT`, `SUM`, …) are deliberately **not keywords** — they are
recognized contextually, as an identifier immediately followed by `(`, so
`count` stays a usable column name.

### Aggregate semantics

`COUNT(*)` counts rows; `COUNT(col)` counts non-`NULL` values. `SUM`/`AVG`/
`MIN`/`MAX` skip `NULL`s; over an empty input they yield `NULL`, but `COUNT`
yields `0` — a real asymmetry in the SQL spec, faithfully reproduced. `SUM` over
an `INT` column accumulates in a **checked** `i64` (overflow is an error, never
a wrap); `AVG` always accumulates in `f64`.

---

## Session 5 — GROUP BY and overflow pages (v0.5)

### `GROUP BY` reshapes the projection

v0.4's `Projection` was `All | Columns | Aggregates` — a `SELECT` was *either*
plain columns or *all* aggregates, never mixed. `GROUP BY` breaks that: `SELECT
region, COUNT(*) … GROUP BY region` mixes a column and an aggregate. So
`Projection` became `All | Items(Vec<SelectItem>)`, where a `SelectItem` is a
`Column` or an `Aggregate`. The parser no longer rejects a mix — mixing is now
syntactically legal; whether it is *meaningful* (every bare column must be
grouped) is a semantic rule the executor enforces.

The unifying realization: whole-table aggregation is just `GROUP BY ()` — one
group containing every row. v0.4's separate "aggregate result" path collapsed
into the grouped path. `SELECT COUNT(*) FROM t` and `SELECT region, COUNT(*)
FROM t GROUP BY region` run the exact same code, the former with zero grouping
columns.

### Grouping by sorting

Grouping must collect rows with equal group-key tuples. A hash map keyed on
`Vec<Value>` is the obvious move — but `Value` holds an `f64`, which is not
`Hash` or `Eq`. Rather than fight that, `partition` *sorts* the rows by the
grouping columns (reusing `order_values`, the total comparator built for `ORDER
BY` in v0.4), then a single linear pass cuts the sorted run into groups wherever
the key changes. O(n log n), no hashing, and it reuses machinery that already
existed.

The empty-input asymmetry is handled by construction: with no grouping columns
`partition` returns one group of all rows *even when there are none*, so `SELECT
COUNT(*)` over an empty table still yields one row (`0`); with grouping columns
and no rows, it returns zero groups, so `SELECT region, COUNT(*) … GROUP BY
region` over an empty table correctly yields nothing.

### Validation and ORDER BY

The SQL rule "a bare column in the `SELECT` list must appear in `GROUP BY`" is
enforced in the executor — its value would otherwise not be well-defined for
the group. With no `GROUP BY`, that means a whole-table aggregate may select
*no* bare columns, which is exactly v0.4's old rule, now falling out of the
general one.

`ORDER BY` on a grouped query orders the *groups*, not table rows, so v0.4's
"an index scan already provides the order" optimization cannot apply — the
planner forces `presorted` false whenever `GROUP BY` is present. `ORDER BY` keys
must name `GROUP BY` columns; the executor sorts the groups by a representative
row before projecting.

### Overflow pages: making the B+tree value-size-agnostic

Until v0.5 a row had to fit in ~2 KiB — the `MAX_CELL` cap. Overflow pages lift
that: a value too big to inline is spilled into a linked chain of pages, and the
leaf cell keeps only a pointer.

The cleanest home for this turned out to be *inside the B+tree*, not the engine:
`BTree::insert`/`search`/`scan`/`delete` now transparently handle values of any
size, so the engine layer did not change at all — it still just calls
`tree.insert(rowid, encoded_row)` with a possibly-huge row.

The design keeps `page.rs` — the slotted-page layer — completely untouched. The
trick: the B+tree prefixes *every* stored value with a one-byte **tag**. Tag `0`
means "the rest is the value, inline." Tag `1` means "the next 4 bytes are the
first page of an overflow chain." The page layer still stores opaque
`(key, bytes)` pairs; it has no idea some of those bytes mean "look elsewhere."
A flag *had* to live somewhere — an overflow cell cannot be told from an inline
one by content alone — and a one-byte value prefix was far less invasive than
threading a flag through every leaf-cell operation.

### The overflow chain

An overflow page is `[next: u32][chunk_len: u32][chunk bytes]`. `write_overflow`
slices the value into page-sized chunks and writes them **back to front**, so
each page can record the number of the page after it — the chain is built
tail-first. `read_overflow` walks `next` pointers reassembling chunks;
`free_overflow` walks them returning pages to the free list.

Every mutation keeps the chain consistent: replacing a key frees the old value's
chain before storing the new one; deleting a key frees its chain; dropping a
table frees the chains behind every leaf cell. All of it happens inside the
statement's single transaction, so the WAL commits the chain changes atomically
with the tree change — a crash cannot leave a half-written or orphaned chain.

### Why the split proof still holds

v0.1's B+tree correctness rested on every cell being ≤ `MAX_CELL`, which made an
overflowing node always splittable into two that fit. Overflow could have broken
that — except an overflow cell is *tiny*: a tag byte plus a 4-byte pointer,
about 11 bytes with the key. Inline cells are still capped at `MAX_CELL` by the
inline/spill decision. So every leaf cell is still ≤ `MAX_CELL`, and the
original split proof carries over untouched. The one remaining size limit is on
*keys* — never spilled — so a row can now be arbitrarily large, though indexing
a column of huge values still is not supported.

### The format break

The value tag prefix changes the on-disk format of every value, so the file
magic was bumped `PREHNDB2` → `PREHNDB3` — a v0.4 file is cleanly rejected
rather than mis-read. Third magic bump in three versions: pre-1.0 the format is
explicitly not stable, and a clear "incompatible version" error beats silent
corruption every time.

## Session 6 — HAVING, node merging, and VACUUM (v0.6)

### Aggregates become expressions

Through v0.5 an aggregate could appear in exactly one place: a top-level item of
the `SELECT` list. `HAVING SUM(amount) > 100` needs more — an aggregate *nested
inside* an expression tree. So `Expr` gained an `Aggregate` variant, and
`COUNT` / `SUM` / `AVG` / `MIN` / `MAX` are now first-class expression leaves,
evaluable anywhere an expression can appear.

They are deliberately *not* keywords. Reserving `count` would forbid a column
named `count`, and the lexer — which has no grammar context — would be the one
forced to make that call. Instead the parser recognizes an aggregate by
*shape*: an identifier immediately followed by `(`. `count` alone is still an
ordinary column reference; `count(*)` is always an aggregate call. One helper,
`parse_aggregate_call`, handles the `name(arg)` form and is shared by the two
sites that can introduce an aggregate — the projection list and `primary()`,
the expression-leaf parser that a `HAVING` clause flows through.

### Two evaluation contexts

`WHERE` and `HAVING` look identical — a predicate kept when it is `TRUE` — but
they evaluate against different things. A `WHERE` predicate is judged per *row*;
a `HAVING` predicate per *group*. The row evaluator, `eval`, resolves
`Expr::Column` to a cell of the current row and has no notion of a group.
Rather than retrofit it with a scope parameter and risk the working query path,
v0.6 adds a parallel `eval_having` that walks the same `Expr` tree with
group-aware leaves: `Expr::Aggregate` folds over the group's rows via
`compute_aggregate`, and `Expr::Column` resolves to the group's (constant) value
for that column — and only if it is a `GROUP BY` column, since any other column
has no single value across the group. Every compound node (`Binary`, `Unary`,
`IsNull`) just recurses, reusing `eval_binary` / `eval_unary` unchanged.

`eval` itself gained exactly one arm: `Expr::Aggregate => Err`. An aggregate in
a row context — `WHERE COUNT(*) > 0`, `SET x = SUM(y)` — is meaningless, and
that one arm rejects it.

`HAVING` slots into the grouped path right after partitioning and before
`ORDER BY`: each group is run through `eval_having` and kept only when the
verdict is exactly `Bool(true)` — the same three-valued rule `WHERE` applies to
rows, so a `NULL`/unknown verdict drops the group. Because v0.5 already unified
whole-table aggregation as "group by nothing — one group," `HAVING` needed no
special case for it: `SELECT COUNT(*) FROM t HAVING COUNT(*) > 99` filters that
single group and correctly returns zero rows.

### Delete learns to merge

Through v0.5, `delete` removed a key from its leaf, rewrote the leaf, and
stopped. A delete-heavy table was left structurally intact but sparse —
half-empty leaves, the same tree height as ever — and the only way to reclaim a
page was to `DROP` the whole table.

v0.6 makes delete rebalance. The recursion now mirrors insert's: `delete_from`
descends to the leaf, removes the key, and on the way back up — *after* each
child returns — calls `merge_child` on the parent. Insert splits propagate
upward on the return path; delete now merges upward on the same path. The merge
policy is a single test: read the just-touched child and a sibling, concatenate
their entry lists, and if the combined footprint fits one page's `USABLE`
budget, write the union into the left page and free the right. If it does not
fit, nothing happens — the tree is left slightly under-full, which is never
*wrong*, only less dense.

This is cheap precisely because of the B+tree's materialize-rebuild style:
every node operation already reads the whole node into a `Vec` of entries and
rebuilds the page from scratch. Merging two nodes is therefore `left.extend(
right)` followed by the same `build_leaf` / `build_internal` the splitter uses
— no slot-array surgery, no in-place compaction. The classic refinement —
*borrowing* a single key from an over-full sibling when a full merge will not
fit — is deliberately skipped: merge-or-nothing keeps the surviving tree correct
and the code a single branch.

Leaves and interior nodes differ in one detail. A merged *leaf* must inherit the
right leaf's forward chain link — the left leaf's old `right_link` pointed at
the leaf now being absorbed, so without the fix-up an ordered scan would walk
into a freed page. Interior nodes carry no such chain, so their merge is a plain
concatenation.

### Collapsing the root

Merging shrinks a level's node count, and that can leave the root with a single
child — a wasted level of indirection. After the recursive delete, `delete`
loops: while the root is an interior node with one child, it copies that child's
page contents up into the root and frees the child. The loop matters because
collapsing one level can expose another one-child root beneath it.

The crucial constraint is that the root keeps its *fixed page number*. The
catalog identifies a table by its root's page number; that number must never
move. So the root cannot simply *become* its child — the child's bytes are
copied into the root's existing page, and the child page is freed. This is the
exact mirror of v0.1's split trick, where a splitting root also kept its number
by pushing a new level *below* itself. Insert grows the tree a level at the root
and keeps the root's number; delete now removes one and keeps it too.

Overflow chains are untouched by all of this. A merge concatenates leaf *cells*
— and a spilled value's cell is just a tag byte plus a page pointer — so the
chain travels with its cell for free. `free_if_overflow` runs only on an actual
key removal, never on a merge; a merge that freed a chain whose value is still
live would be a disaster, and the code structure makes that impossible.

### VACUUM: compaction by rebuild

Merging returns pages to the free list, but the *file* never shrinks — a
database that grew to a gigabyte and then deleted nine-tenths of its rows is
still a gigabyte on disk, just with a long free list. `VACUUM` is the pass that
actually reclaims it.

It does not compact in place. In-place compaction means sliding live pages down
over free holes, and every page number that moves has to be chased through every
parent pointer, every index root, and the catalog — order-dependent, fiddly, and
hard to make crash-safe mid-move. v0.6 sidesteps all of it: it *rebuilds*. A
second `Pager` is opened on a temp file beside the database; for every table,
`VACUUM` creates a fresh B+tree, scans the old one, and inserts each row into the
new one — then does the same for every index. The rebuilt trees have entirely
new page numbers, but every pointer is internally consistent because the tree
was just constructed from nothing, densely, with no free space. Because the copy
goes through `BTree::scan` and `BTree::insert`, spilled values are reassembled
and re-spilled transparently — the compact image even gets fresh, contiguous
overflow chains at no extra code.

### The swap rides the WAL

The interesting part is committing the rebuilt image without a window in which a
crash loses the database. `Pager::replace_with` does it without juggling file
handles or renaming files. It reads every page of the temp image into the
*live* pager's dirty-page buffer, adopts the temp file's header as the new
`Meta`, and then calls the ordinary `commit()`. The whole compact image is thus
written to the WAL — CRC-stamped, commit-marked, fsynced — and only then applied
to the real database file. VACUUM inherited crash-safety for free: a crash at
any instant is repaired by the same WAL replay every other commit relies on,
leaving either the old database whole or the new one whole, never a mix. After
the commit, a single `set_len` drops the now-unreachable tail pages of the old,
larger file; a crash before that truncate is harmless, because the committed
header already records the smaller page count.

VACUUM is also the one statement the executor never sees. The executor works
*through* one pager on one open file — it cannot rewrite the file or reopen the
catalog. So `Database::execute` intercepts `Plan::Vacuum` before dispatching,
runs the rebuild-and-swap itself, and reopens the catalog afterward (its root
page moved along with everything else). The executor's `Plan::Vacuum` arm is
simply `unreachable!`. This session changes nothing on disk — the `PREHNDB3`
format is untouched, so a v0.5 database opens unchanged in v0.6, and a vacuumed
file is byte-for-byte an ordinary database, just a denser one.

## Session 7 — A bounded buffer pool with steal eviction (v0.7)

### The ceiling: a transaction had to fit in RAM

Until v0.7 the pager held every page a statement touched in one `HashMap` —
`dirty` — and kept it there until `commit`. Nothing was ever evicted; the map
only grew. For most statements that is a few dozen pages and no one notices.
But the design had a hard ceiling: a statement that writes more pages than
there is memory simply runs the process out of it. And such statements are not
exotic — a `VACUUM` of a large database builds the entire compact image in
memory before committing it, and inserting a single multi-megabyte value
spreads it across thousands of overflow pages, every one of them dirty at once.
v0.7 replaces the unbounded map with a fixed-size **buffer pool**.

### A bounded pool, evicted by CLOCK

The pool is a `Vec` of at most `POOL_CAPACITY` frames (1024 — a 4 MiB cap at
4 KiB a page) plus a `page number → slot` index. When a page must become
resident and every frame is taken, one is evicted. The policy is **CLOCK**:
each frame carries a one-bit `referenced` flag, set whenever the page is used;
a "hand" sweeps the frames in a ring, and at each frame it either clears a set
bit and moves on, or evicts a frame whose bit is already clear. It is a cheap
approximation of LRU — a page used since the hand last passed earns a second
chance — without LRU's per-access list surgery: a lookup just sets a bit. The
sweep is guaranteed to finish within two passes, because the first clears every
bit it finds.

### Steal: a dirty page is spilled, not dropped

Eviction has to reckon with *what* it is throwing out. A **clean** page — one
identical to its image in the database file — can simply be dropped; reading it
again just re-reads the file. A **dirty** page cannot: it is an uncommitted
write that exists nowhere else, and dropping it would lose data. So a dirty
victim is **spilled** — its image is appended to the WAL — and only then is its
frame reused. This is the discipline a database textbook calls *steal*: the
buffer manager may "steal" a frame from an uncommitted transaction, because the
transaction's work is safe in the log.

Steal forced the WAL to change shape. It used to be written in one burst at
commit — a single call dumped every dirty page and the marker together. Now
pages trickle in: each eviction appends one page record, and `commit` appends
whatever dirty pages are still resident, then the marker. The log accumulates
the transaction *as it happens* rather than all at the end. The crash contract
is unchanged — the database file is untouched until a marker is durably
fsync'd — so a crash before commit still discards a markerless log and leaves
the database pristine.

### Why there are no pin counts

A buffer pool usually needs *pin counts*: a page being read or written by some
caller must not be evicted out from under it, so callers pin a page and the
pool refuses to evict a pinned frame. PrehniteDB needs none of this, and the
reason is an old, almost accidental decision: `read_page` returns an **owned
copy** of the page, not a reference into the pool. A caller mutates its own
`Box<[u8; PAGE_SIZE]>` and hands it back through `write_page`. Because no caller
ever holds a reference *into* the pool, there is never a frame that is unsafe to
evict — the pool may evict anything, any time, between calls, and the only rule
is "spill if dirty." The cost is a memory copy per page access; the payoff is
that the entire pin/unpin apparatus, and the whole class of bugs that comes with
forgetting to unpin, simply does not exist.

### Reading an evicted page back

A page that has been spilled is in an awkward place: not in the pool, and not —
in its current form — in the database file, which still holds the stale
committed version. Its only good copy is in the WAL. So the pager keeps a
second small map, `wal_index`, from page number to the byte offset of that
page's latest image in the log. `read_page` consults three places in order: the
pool, then `wal_index` (reading the image back from the WAL), then the database
file. The point that matters for the memory bound is that `wal_index` holds
*offsets*, not pages — a few bytes an entry. The page *data* is capped at
`POOL_CAPACITY` frames; the only thing that grows with a giant transaction is a
map of small integers. That is the difference between "bounded" and "bounded
except for the part that isn't."

### Streaming recovery, and why commit shares it

Reusing the WAL for spills exposed a flaw in recovery. The old `recover` read
the entire log into a `Vec<u8>`, validated it, then replayed it — fine when the
log was one small transaction. But `commit` now needs to copy the sealed log
into the database file, and had it done so by calling the old `recover`,
committing a transaction of many gigabytes would read those gigabytes back into
memory. The OOM would just move from the staging map to the commit step.

So recovery was rewritten to **stream**. It is two passes, each holding a single
record at a time. Pass one (`scan`) walks the log confirming every page
record's CRC and that it ends in a valid commit marker — answering only "is
this a complete transaction?" Pass two (`apply`) walks it again, writing each
page image straight into the database file. `commit` and crash-recovery now
share pass two exactly: `commit` seals the log and calls `apply`; crash-recovery
runs `scan` first — it does not trust a log it did not just write — and then
`apply`. One streaming routine, O(1) memory, drives both the normal path and
the repair path.

### Discarding a transaction without touching the disk

A rolled-back statement has spilled pages sitting in the WAL that must not
survive. The tidy move would be to truncate the log — but truncation is a
syscall that can fail, which would make `rollback` fallible and ripple through
the engine. Instead `rollback` calls `discard`, which just resets the WAL's
in-memory write cursor to zero; the stale bytes are left on disk, and the next
transaction overwrites them from the start. This is safe for two reasons that
must both hold: an abandoned transaction was never sealed, so it has no commit
marker and `scan` can never accept it as committed; and page records are a
fixed size, so a later transaction's records land exactly aligned over the old
ones, never leaving a half-record that might parse as garbage. Rollback does no
I/O at all.

### The cost: RAM traded for I/O

A buffer pool with steal is not free; it is a *trade*. Before, staging a page
touched only memory. Now, under memory pressure, evicting a dirty page writes it
to the WAL — which is why staging a page is itself a fallible operation in v0.7,
its `?` threaded through every B+tree write — and asking for that page again
reads it back. The WAL grows to hold the whole transaction, so peak *disk* use
rises even as peak *memory* use falls. That is the bargain every real database
makes: bounded, predictable memory, paid for with I/O that only materializes
when the working set genuinely exceeds the pool. A statement that fits in 1024
pages — nearly all of them — touches the disk exactly as it did in v0.6. A
statement that does not now finishes instead of dying. And `VACUUM`, once the
worst offender, streams its rebuilt image page-by-page through the WAL: the
compaction that reclaims a huge database no longer needs to hold one in memory.
The on-disk format is untouched — a v0.6 database opens unchanged.

## Session 8 — A streaming, iterator-model executor (v0.8)

### Three copies of every row

The v0.7 executor was a *materializing* one. Running a `SELECT` meant
`collect_candidates` walked the access path and built a `Vec` of every row it
found; the filter loop copied the survivors into a second `Vec`, `matched`; and
projection built a third, the output. A query over a million rows held a
million rows in memory — up to three times over — before a single row was
returned. The buffer pool had just bounded the *pager's* memory; the executor
was now the layer with no bound at all. v0.8 rebuilds the `SELECT` executor on
the **volcano model**: a tree of operators, each a pull-based iterator, with
rows drawn through it one at a time.

### The B+tree learns to stream

Streaming has to start at the bottom. `BTree::scan` and `scan_range` built a
`Vec` of the whole tree by walking the leaf chain — the very materialization
the executor sat on. v0.8 adds a **`Cursor`**: it holds one leaf's cells, hands
them out one `next` at a time, and when that leaf is spent follows the leaf's
`right_link` to load the next. Memory is one leaf — about 4 KiB — no matter how
large the tree. A spilled overflow value is reassembled inside `next`, for the
single row being yielded, so even a table of megabyte values is walked a row at
a time.

`scan` and `scan_range` did not go away — they are now three-line wrappers that
open a cursor and drain it. So `VACUUM`, `CREATE INDEX`, and the other callers
that genuinely *want* every row in a `Vec` are unchanged; only the new executor
reaches for the cursor directly.

### A tree of operators

A `SELECT`'s pipeline is built from small operators, each implementing one
trait:

```rust
trait Operator {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>>;
}
```

`TableScan` and `IndexScan` sit at the leaves, wrapping a cursor. Above them
stack `Filter` (drops rows failing the `WHERE` predicate), `Sort`, `Project`
(narrows a row to the selected columns), and `Limit`. `select` assembles
whichever of these the query needs into a `Box<dyn Operator>` and pulls the
root until it runs dry. A row enters at a leaf, is pulled upward through each
operator, and emerges at the top — and the next row does not start until the
consumer asks. Each pull is a virtual call through the `dyn Operator`: the
per-row dispatch overhead the volcano model is known for, which production
engines eventually trade away for vectorized or compiled execution — at
PrehniteDB's scale it costs nothing worth reclaiming.

### Threading the pager, not borrowing it

Every operator's `next` needs the pager — to read the next leaf, to chase an
overflow chain. The tempting design is for each operator to *hold* a
`&mut Pager`. It is also a dead end: the tree would be full of references into
one mutably-borrowed pager, an aliasing knot the borrow checker rightly
refuses, and a `Cursor` that stored a `&mut Pager` could never coexist with the
pager being used anywhere else.

The fix is to pass the pager *as an argument to `next`*, down the tree, on
every call. An operator borrows the pager only for the instant it is running;
the borrow is released the moment `next` returns. The tree itself owns nothing
but its own children and a little state. This is the standard volcano answer —
the execution context travels with the call — and in Rust it is the difference
between a tree that compiles and one that cannot.

### Pipeline breakers

Not every operator can stream. `ORDER BY` cannot yield its first row until it
has seen its last — the smallest row might arrive at the very end. `Sort` is
therefore a *pipeline breaker*: its first `next` drains the entire input into a
buffer, sorts it, and only then yields; later calls just hand out the sorted
buffer. `GROUP BY` is the same — a group is not complete until the input is
exhausted.

This is not a flaw in the model; it is the model being honest. A breaker
buffers because its operation genuinely needs every row, and *only* that
operator buffers — everything downstream of it still streams a row at a time.
v0.8 keeps the grouped path as the proven `grouped_select` pass rather than
dressing it as an operator: `GROUP BY` is blocking either way, and reusing
working code beat re-deriving it. `Sort`, which the plain `SELECT` path needs,
did become an operator.

### LIMIT, and what streaming buys

The model earns its keep with `LIMIT`. The `Limit` operator counts the rows it
has passed along and, the instant it has enough, returns `None` *without
pulling its input again*. That `None` propagates: the `Project` below it stops,
the `Filter` below that stops, the scan stops, and the B+tree cursor stops
walking leaves. `SELECT ... LIMIT 10` from a billion-row table reads about ten
rows off the disk and halts — memory and I/O proportional to the limit, not the
table. Under the old executor the same query built the billion-row `Vec` first
and then threw all but ten of it away.

v0.8's scope is the executor and the B+tree: `execute` still gathers the
finished rows into a `QueryResult`, and the protocol, server, and public API
are untouched. So a `LIMIT`-less `SELECT *` of a huge table is still as large
as its result — what is now bounded is everything *before* the result, and any
query carrying a `LIMIT`. Pushing the stream the rest of the way — out through
the wire protocol, so even an unbounded result need not be buffered — is the
natural next step, and the operator tree built here is exactly what it will
pull from.

### Why UPDATE and DELETE still gather first

`SELECT` became streaming; `UPDATE` and `DELETE` deliberately did not. They walk
a set of rows and *mutate the tree as they go* — and a cursor halfway through a
tree cannot survive an insert or delete beneath it, because a split or a merge
moves the very leaves it is pointing at. So `UPDATE` and `DELETE` still gather
every matching row up front, then mutate; the materialization there is a
correctness requirement, not an oversight. The volcano executor is for reading
— writing still looks before it leaps.

## Session 9 — Joins (v0.9)

### A query outgrows one table

Every `SELECT` before v0.9 was single-table to its bones. The AST's
`Statement::Select` held a `table: String`. The planner resolved that one
table; the executor resolved one `Schema`; expression evaluation matched a
column name against that schema's columns. A join breaks every link in that
chain at once: a joined row spans two or more tables, so a column reference can
no longer be "the i-th column of *the* table" — it has to name *which* table.
v0.9 makes the executor multi-table from the FROM clause up.

### Qualified columns: a `ColumnRef` everywhere

`a JOIN b ON a.id = b.a_id` is unusable without `a.id` — qualified column
references are not optional for joins. So `Expr::Column` stopped being a bare
`String` and became a `ColumnRef { table: Option<String>, name: String }`, and
the same change rippled to every other place a column is named: the `SELECT`
list, `ORDER BY` keys, `GROUP BY` columns, aggregate arguments. The dotted name
could instead have been smuggled inside a single `String` and split on `.` at
resolution time — less code to touch — but a struct makes the qualifier a thing
the compiler can see. Every site that consumes a column reference had to be
updated, and the compiler named each one; a stringly-typed qualifier would have
let a missed site fail silently at runtime instead.

### The scope

The unit that replaces "one `Schema`" is a **scope**: the columns of every
table in the `FROM` clause, concatenated, each tagged with the qualifier its
table is reached by — its alias if it has one, otherwise its name. A joined row
is the matching concatenation of values, and a `ColumnRef` resolves to a
position in it: a qualified reference must match table *and* name; a bare one
matches by name alone and is an error if two tables both offer it. A
single-table query is just the one-table case of the same machinery — its
scope has one table, every bare name resolves, and nothing about it changed.
`UPDATE` and `DELETE`, which never join, build a one-table scope and reuse the
identical evaluator.

### A join is just another operator

The volcano tree from v0.8 made joins almost anticlimactic: a join is one more
operator. `NestedLoopJoin` streams its **left** input and, on its first pull,
drains its **right** input into a buffer; then, for each left row, it walks
that buffer, concatenating left with right and keeping the pairs whose `ON`
predicate holds. The predicate is evaluated exactly like a `WHERE` clause —
against the combined row, through the join's scope. Buffering the inner side is
the price of a pull-based model: the right input cannot be rewound, so it is
materialized once and rescanned from memory. The left side still streams, and
everything above the join — `WHERE`, `ORDER BY`, `GROUP BY`, `LIMIT` — streams
from it unchanged, because the join hands up ordinary rows.

### Inner, left, cross — and chains

One operator covers three join kinds. `CROSS JOIN` has no `ON` and keeps every
pair. `INNER JOIN` keeps the pairs whose `ON` is `TRUE`. `LEFT JOIN` does the
same but remembers whether the current left row matched anything, and when its
scan of the right finds nothing, emits that left row once more, padded with
`NULL`s for the right side's columns. A multi-way `a JOIN b JOIN c` is left-deep
— `NestedLoopJoin(NestedLoopJoin(a, b), c)` — so the outer join's left input is
itself a join, and the scope each join evaluates its `ON` against spans exactly
the tables to its left plus its own right. Because tables are reached by their
alias, `FROM emp e JOIN emp m ON e.manager = m.id` — a self-join — works with
no special case: `e` and `m` are simply two qualifiers over the same B+tree,
walked by two independent cursors.

### The cost, and what is deferred

A nested-loop join is O(rows × inner-rows): every left row rescans the whole
buffered inner table. That is the honest, correct, any-predicate baseline — it
works for `ON a.x <> b.y` as readily as for an equality — but it is not fast
for two large tables. The planner reflects this: a single-table query still
gets its index access-path selection, but a joined query full-scans every
table. The obvious next step is the *index-driven join* — when the inner
table has an index on the `ON` column, look the match up instead of scanning —
which turns O(n × m) into O(n × log m). The nested-loop operator is the
floor to build that on. `RIGHT` and `FULL OUTER` joins are deferred too; a
`RIGHT` join is a `LEFT` join with the inputs swapped, and both are rarer than
the three v0.9 ships. The on-disk format is unchanged — a v0.8 database opens
unchanged in v0.9.

## Session 10 — Index-driven joins (v0.10)

### The rescan tax

v0.9's join was honest but slow. `NestedLoopJoin` buffers the inner table once
and, for *every* left row, walks that whole buffer looking for matches: a join
of an n-row table to an m-row table is O(n × m). For `users JOIN orders` —
each user paired against all 200,000 orders to find their three — that quadratic
is the difference between instant and unusable. v0.10 fixes the common case.
When the inner table has an index on the join column, a left row need not scan
the inner table at all: it can *look its matches up*. That is the **index
nested-loop join**, and it turns O(n × m) into O(n × log m).

### Recognizing an index join

The opportunity lives in the `ON` clause. `users u JOIN orders o ON u.id =
o.user_id` is an equi-join: a left column equals an inner column. If `o.user_id`
is the leading column of an index on `orders`, then for each user row the join
can encode that user's `id` and range-scan the index for it — exactly the
lookup the planner already does for an indexed `WHERE`.

The recognizer walks the `ON` predicate's top-level `AND` conjuncts and looks
for an equality `Column = Column` where one side resolves to a *left* column
and the other to a column of the *inner* table — and where that inner column is
the leading column of one of the table's indexes. Telling left from inner is
free: the join scope is the left tables' columns followed by the inner table's,
so a column reference that resolves below the left/inner boundary is a left
column and one at or above it is an inner column. There is one more condition —
the two sides must have the *same* type, so the left value encodes to the key
the inner column was actually indexed under; an `INT = REAL` equi-join falls
back to the buffered join, whose value comparison handles the cross-type case
correctly.

### Where the decision is made

This recognition runs in `build_from` — the executor function that assembles
the join pipeline — not in the planner. That is a deliberate break from "the
planner picks access paths." The reason is the *scope*: deciding which side of
the `ON` equality is the inner column needs the same multi-table resolver the
executor already builds as it walks the `FROM` clause. Putting the decision in
the planner would mean reconstructing the scope there — duplicating the join
resolution wholesale. `build_from` already has every joined table's schema (so
every joined table's indexes) and the scope in hand; the decision costs only a
look at the `ON` predicate it is already holding.

### The operator

`IndexNestedLoopJoin` is, structurally, a `NestedLoopJoin` with its inner side
replaced. It still streams the left input; but where the plain join buffers the
whole inner table, the index join buffers *nothing*. For each left row it
evaluates the join-key expression, encodes the result, range-scans the inner
table's index for that key, and follows the matched index entries' rowids back
to the inner rows — a fresh, small set per left row. From there the two
operators are identical: concatenate left with each matched inner row, re-apply
the *full* `ON` predicate (the index only narrows — a compound `ON ... AND ...`
still needs its other conjuncts checked), and, for a `LEFT` join, pad an
unmatched left row with `NULL`s. A `NULL` join key matches nothing, since
`NULL = anything` is never `TRUE` — so the lookup for it simply returns no rows.

### When it does not apply

The index join is an optimization layered cleanly over a correct floor. A
`CROSS` join has no `ON`; a non-equality `ON a.x <> b.y`, an equi-join on a
non-leading or unindexed column, a type mismatch — any of these, and
`build_from` builds the v0.9 buffered `NestedLoopJoin` instead. That fallback
is not a lesser path so much as the general one: it joins on *any* predicate,
where the index join only accelerates the equi-join-on-an-indexed-column shape
that happens to be overwhelmingly the common one. The on-disk format is
untouched — a v0.9 database opens unchanged in v0.10.

## Session 11 — Multi-statement transactions (v0.11)

### Statements that auto-committed

Through v0.10 every statement was its own transaction. `Database::execute` ran
one statement, and on success called `pager.commit()`; on failure,
`pager.rollback()`. There was no way to say "these three `INSERT`s land
together or not at all" — no `BEGIN`, no `COMMIT`, no `ROLLBACK`. v0.11 adds
them, and the surprising part is how little had to move.

### The pager already knew how

A transaction *is* a unit of staged work that commits or discards as a whole —
and that is exactly what the pager has always been. Within a single statement
the executor writes page after page; the pager accumulates them in the buffer
pool, spilling to the WAL under memory pressure, and `commit` seals the lot
while `rollback` throws it away. A multi-statement transaction is nothing more
than *more statements staging into that same buffer before `commit` is called*.
The pager did not change at all. Even a transaction larger than memory already
works: the v0.7 steal path spills its dirty pages to the WAL exactly as a
single oversized statement does. The whole feature lives one layer up, in
`Database` — in *when* `commit` is called, not in what it does.

### A three-state machine

`Database` gained a `TxnState`: `None`, `Open`, or `Aborted`. In `None` — the
default — each statement auto-commits, exactly as before. `BEGIN` moves to
`Open`; now `execute` runs statements but does *not* commit them — their pages
just stage. `COMMIT` calls `pager.commit()` and returns to `None`; `ROLLBACK`
calls `pager.rollback()` and returns to `None`.

`BEGIN` / `COMMIT` / `ROLLBACK` are parsed as ordinary `Statement`s but never
reach the planner or executor — they carry no rows and choose no access path.
`Database::execute` matches them off the top and handles them directly, the
same interception `VACUUM` gets. The planner and executor are untouched; a
transaction is purely a question of how `Database` brackets the calls.

### When a statement fails mid-transaction

The `Aborted` state exists because of a real limitation: the pager has no
savepoints. It can stage and it can discard, but it cannot undo *one* statement
out of several. So when a statement fails inside an open transaction — a type
error, a missing column — there is no way to roll back just that statement and
keep the rest. The honest response is to roll the whole transaction back and
mark it `Aborted`. From there only `ROLLBACK` is accepted; every other
statement is refused until the transaction is explicitly closed. This is the
behaviour Postgres has — an error poisons the transaction — and here it falls
out of the pager's shape rather than being designed in.

### The lock is the transaction's, for its whole span

The server is where transactions meet concurrency. One pager has one staged
buffer, so two transactions cannot be in flight at once. The server enforces
that with the database lock itself: a connection holds the `Mutex` for exactly
the span of its open transaction. A statement outside a transaction locks,
runs, and unlocks per request, as before; but the moment a connection runs
`BEGIN`, it keeps the guard — across request after request — until `COMMIT` or
`ROLLBACK` hands it back. An open transaction therefore excludes every other
connection outright, and transactions can never interleave. A connection that
drops with a transaction still open has its staged writes rolled back, so the
next writer starts clean.

The cost is plain: a connection sitting on an open transaction blocks everyone
else. That is the single-writer, lock-as-isolation model — the same one
SQLite's rollback-journal mode uses — and it is correct, if not concurrent.

### What is still single

v0.11 delivers atomic multi-statement transactions; it does not yet deliver
*concurrent readers*. A reader still cannot run alongside a writer, because a
read in PrehniteDB mutates the buffer pool — `read_page` admits pages and turns
CLOCK bits — so it needs exclusive access to the pager. Letting readers run in
parallel means reworking that read path so a read no longer requires `&mut`,
which is its own session. The transaction layer built here is the foundation
that rework will stand on. The on-disk format is unchanged — a v0.10 database
opens unchanged in v0.11.

## Session 12 — Concurrent readers (v0.12)

### The read that was secretly a write

Session 11 closed on an admission: a reader could not run beside another
connection. The reason is in the pager. `read_page` takes `&mut self` — it
admits the page into the buffer pool, turns the frame's CLOCK bit, and on a miss
reads from the file. A `SELECT` that touches a thousand pages mutates the pool a
thousand times. A read, in other words, is a write to the buffer pool — and the
server held one lock, taken exclusively by every statement alike. A query waited
behind every other connection, `SELECT` behind `SELECT` included.

### Two ways to make a read shareable

There are two ways out, and they are very different sizes.

The first is *interior mutability*: change `read_page` to take `&self` and move
the mutation behind a lock or atomics *inside* the buffer pool. The cache
becomes shared — every connection reads through the same frames — and
concurrency becomes a property of the pool itself. This is what a mature
database does. It is also a rework of the single hottest path in the system,
spread across six files, and it trades the server's one coarse lock for a finer
lock inside the pool that every page touch now contends on. The risk is concrete
and the payoff is subtle.

The second is to not share the cache at all. A reader that needs a mutable pager
can simply *have its own*: open a second `Database` on the same file, with its
own pager and its own buffer pool, and let it turn its own CLOCK bits in
private. Nothing is shared, so nothing inside needs a lock. v0.12 takes this
path — and the engine does not change by a single line. The whole feature lives
in the server.

### A reader-writer lock, and a pager per reader

The server's `Mutex<Database>` becomes an `RwLock<Database>`. A write — anything
that is not a plain `SELECT` — takes the lock exclusively and runs on the shared
`Database`, exactly as before. A read-only statement takes the lock *shared*,
then does the new thing: it opens its own private `Database` on the same path
and runs the query against that. Two readers hold the lock shared at once, each
on its own pager, each turning its own CLOCK bits — they share no frame, so
neither can block the other.

The shared lock is held for the span of the query — the `Database::open` and the
execution — and released before the response goes back to the socket. The lock
guards data access; the slow part, the network reply, runs outside it. That is
the discipline v0.11 already used to drop the write lock before replying.

### Why a private pager is safe

Two independent caches over one file sounds alarming, and without the lock it
would be. The lock is the whole proof.

A reader holds the `RwLock` shared; a writer needs it exclusive; the two are
mutually exclusive. So *no commit is ever in flight while a reader is open*. The
bytes a reader sees do not shift underneath it — header, catalog root, B+tree
pages all sit exactly as the last committed write left them. The reader's
`Database::open` reads a consistent snapshot because nothing is allowed to write
during it.

And that `open`, on a database that already exists, only reads. It loads the
header and catalog and runs WAL recovery — but the WAL is empty. A commit
truncates the WAL *before* the writer releases the write lock, so by the time
any reader can take the lock shared, there is nothing to recover and recovery is
a no-op. A reader never writes the file. N readers opening at once are just N
handles reading the same stable bytes, which the operating system allows without
complaint. The writer's exclusive lock closes the other direction: no writer
ever runs while a reader holds the lock.

### Classification has to be exact

The scheme rests on one judgement: is this statement a read or a write? Misjudge
it one way and a write runs on a *throwaway* pager — the private `Database` the
reader opened — so its commit lands in a file dropped the instant the query
returns. The write is silently lost.

So the classifier does not guess by eye. `is_read_only` parses the statement and
is true for exactly one thing: a well-formed `SELECT`. Not a string that merely
begins with `select` — case, leading whitespace, and comments would each need
handling, and a malformed `SELECT …` the parser would reject must not slip
through as a read. Every other statement is a write; *anything that fails to
parse at all* is a write. The error is built to fall the safe way — misjudging a
read as a write costs only a little concurrency, while the reverse corrupts. The
classifier lives in the library beside `Database`, because what counts as
read-only is a fact about the SQL, not about the server.

### A transaction is exclusive — and so is a SELECT inside one

One case breaks the rule that every `SELECT` is a concurrent read, and it must.
A `SELECT` *inside an open transaction* has to see that transaction's own
uncommitted writes. Run it on a fresh private pager and it would open the last
*committed* state and miss everything the transaction has staged.

So the server tests for an open transaction first. A connection that has run
`BEGIN` holds the write lock — an `RwLockWriteGuard` kept across requests, just
as v0.11 kept a `MutexGuard` — and every statement on that connection runs on
the held guard, read or write alike. Only with no transaction open does the
server consult `is_read_only` and consider the shared-lock fast path. The order
is the rule: inside a transaction, on the writer's own pager; outside one, a
`SELECT` goes parallel.

### The cost, and the next step

A private pager is not free. A reader opens cold — it re-reads the header and
catalog, and fills its buffer pool from nothing, sharing not one cached page
with any other connection. A workload of many small reads pays that startup cost
again and again.

That is the honest trade v0.12 makes: a cold cache, bought with an engine that
did not change and a concurrency model small enough to be obviously correct. The
shared-cache pager — the interior-mutability design — is the real destination,
and it is a smaller step now than it was. The server already has a reader-writer
boundary and a precise notion of which statements are reads; a later session can
move the cache behind that boundary instead of having to invent the boundary as
well. The on-disk format is untouched — a v0.11 database opens in v0.12
unchanged.

## Session 13 — A shared buffer pool (v0.13)

### The debt v0.12 named

v0.12 made readers concurrent by giving each its own `Database`: its own pager,
and its own buffer pool. It worked, and it was honest about the price — a reader
opened *cold*, re-reading the header and catalog and filling a private 4 MiB
pool from nothing, sharing not one cached page with any other connection. Ten
readers meant ten pools, ten separate copies of whatever was hot. v0.13 pays
that debt down: one buffer pool, shared by the writer and every reader.

### A rework smaller than it was billed

v0.12's own deep dive predicted how this would go. The shared-cache pager, it
said, was "the interior-mutability design": make `read_page` take `&self`, move
the mutation behind a lock inside the pool, and let the change ripple up through
the B+tree and executor as the lifetimes shift. A big, multi-file rework.

It needed one piece of that, and not the rest — a lock inside the pool, yes; the
`&self` read path and its cascade, no. The prediction had conflated two
separable things: sharing the *cache*, and reworking the read path. v0.13 shares
the cache and leaves the read path exactly as it was.

Each reader still has its own `Pager` — its own file handle, its own WAL handle,
its own metadata — exactly as in v0.12. The single structural change is that a
`Pager` no longer *owns* its `BufferPool`; it holds a `SharedPool`, a handle to
one pool that every pager on the file shares. `SharedPool` is
`Arc<Mutex<BufferPool>>` and nothing more. The `BufferPool` itself — its frames,
its page index, its CLOCK hand — was not redesigned; it simply moved inside a
mutex. The server builds one `SharedPool` at startup and hands a clone to the
writer and to every reader; cloning it is an `Arc` bump, so every clone is the
same pool.

### Why `read_page` did not have to take `&self`

Here is why the read path could stay still. `read_page` is `&mut self`, and that
remains correct: two reader threads call it concurrently, but on *different*
`Pager` objects, each thread owning its own `&mut`. There is no aliasing to
forbid. The one thing the two pagers genuinely share is the `SharedPool`, and a
`SharedPool` is reached through `&self` — a `Mutex` behind an `Arc` needs no
`&mut` — so the pager's exclusive borrow and the pool's shared borrow never
collide. They are borrows of different objects.

So the `&self`-cascade v0.12 feared never begins. The executor, the planner, the
B+tree, the catalog — every layer that runs a query — takes `&mut Pager` exactly
as before, untouched, because each still runs against a pager no other thread
can see. Beyond the pool, v0.13 adds only a constructor: `Pager` and `Database`
each gain an `open_with_pool` that accepts a shared pool instead of building a
private one, and the server calls it. That is the entire change.

### A pool that, to a reader, is always clean

Sharing a cache among readers is the easy half. Sharing it with the *writer* is
the question — because the writer's pool holds dirty pages, uncommitted writes
staged until `commit`, and a reader that saw one would be reading a transaction
that has not happened.

It cannot, and the reason is the v0.12 reader-writer lock. A writer dirties pages
only while it holds that lock *exclusively*; a reader runs only while it holds it
*shared*; the two are mutually exclusive. The windows never overlap. Every
instant a reader is touching the shared pool, the writer is not — and the last
thing the writer did before it released the lock was either `commit`, which
flushes the dirty pages and marks every frame clean, or `rollback`, which drops
the dirty frames outright. From any reader's vantage the shared pool holds
nothing but clean, committed pages.

That turns a frightening-sounding arrangement — many readers and a writer on one
cache — into a safe one, and the pool itself knows nothing about why. It has no
notion of "reader" or "writer", no notion of a transaction; the invariant that
it looks clean to readers is *imposed from above*, by a lock in the server. A
reader's eviction therefore never meets a dirty frame, never spills, never
touches a WAL. v0.13's correctness rests squarely on v0.12's — which is the real
reason the two had to land in that order.

### The lock never wraps a syscall

A mutex around a cache is only as good as its critical sections are short, and
the dangerous case is a cache *miss*: a miss needs a `read` from the file, and
holding the pool lock across a syscall would funnel every reader through one
reader's disk I/O.

So `read_page` does not. It locks the pool, looks the page up, copies it out,
and unlocks — all in memory. Only on a miss, with the lock already *released*,
does it read the file; then it locks again to admit the page. Two readers that
miss the same page both read it from disk, redundantly — but the bytes are
identical, since no writer is active to change them, and admitting an
already-resident page merely refreshes the frame. A little wasted I/O buys never
serializing readers on each other's syscalls. The lone place the lock spans I/O
is `commit`, which appends dirty pages to the WAL while holding the pool — but a
commit is the writer's act, the writer runs alone, and a lock contended by no
one costs nothing.

### The cost, and what is still deferred

One mutex guards the whole pool, so concurrent readers do contend — briefly, in
memory, but they contend. The honest next step is to shard it: partition the
frames by page number, give each shard its own lock, and two readers touching
different pages never meet. v0.13 does not; one mutex is enough to be correct
and to make the shared cache real, and sharding is now a tuning change the new
boundary invites rather than demands.

The grander destination v0.12 imagined — `read_page` lending a borrowed, pinned
frame, with no copy at all — is still out there and still unbuilt. v0.13 shows it
was never on the path to a *shared* cache: it is a separate optimization, the
removal of the per-read copy, and it is precisely that copy v0.13 leans on to
keep eviction free of pin counts. The on-disk format is untouched — a v0.12
database opens in v0.13 unchanged.

## Session 14 — Copy-free page reads (v0.14)

### The copy v0.13 leaned on

Every session so far rested on one quiet cost. `read_page` returned an *owned*
`Box<[u8; PAGE_SIZE]>` — a fresh 4 KiB copy of the cached frame, handed to the
caller. A B+tree search of a three-level tree copied 12 KiB to inspect three
pages; a scan copied every leaf it walked. v0.13's deep dive pointed straight at
it: the per-read copy was "precisely that copy v0.13 leans on" to keep eviction
simple. v0.14 takes the copy out.

### "Borrowed" cannot be a lifetime

The instinct, in Rust, is to return a reference: `read_page` lending a
`&[u8; PAGE_SIZE]` straight into the cached frame. It does not work here, and
the obstacle is the pool's `Mutex`. A `&` into a frame is valid only as long as
the `MutexGuard` that produced it — so to lend one out, `read_page` would have
to hold the pool locked for as long as the B+tree walks the page, serializing
every reader behind whoever holds a page. The alternative — thread a `'pool`
lifetime up through `BTree`, `Cursor`, and the executor — is the pervasive
rewrite v0.13 dreaded.

The way out is to make the pin a *reference count*, not a Rust lifetime.
`read_page` returns a `PageRef`: an `Arc<Frame>`, a counted handle onto the
frame. Cloning the `Arc` is the pin; dropping it is the unpin. An `Arc` owns its
contents — it borrows nothing — so a `PageRef` carries no lifetime parameter,
and neither does anything built on one. The cascade never starts.

### A frame that cannot change, and a slot that can

For an `Arc<Frame>` to be shared freely — for a `PageRef` to hand out
`&[u8; PAGE_SIZE]` to any number of readers at once — the `Frame` has to be
immutable. But the pool mutates: it sets a dirty bit on a write, flips CLOCK
reference bits as the hand sweeps. Those cannot live on a shared, immutable
frame.

So the frame split in two. `Frame` is now just `{ no, page }` — a page number
and its bytes, never mutated once admitted, the thing inside the `Arc`. The
mutable bookkeeping moved into a new `Slot { frame, dirty, referenced }`, which
the pool owns and mutates under its `Mutex`. The shared object is pure data; the
bits that change stay with the pool, behind the lock that already serialized
them. A `PageRef` lending out `&[u8; PAGE_SIZE]` is then plainly sound — the
bytes it points at are immutable for the frame's whole life.

### The pin is the count

A frame is pinned exactly when a `PageRef` to it is alive — and that is exactly
when its `Arc` strong count exceeds one. The pool's slot holds one reference;
every outstanding `PageRef` holds another. So eviction needs no separate pin
counter: the CLOCK sweep simply skips any slot whose `Arc::strong_count` is
greater than one. Drop the last `PageRef` and the count falls back to one, the
frame evictable again — with no bookkeeping call, because `Arc`'s own `Drop`
did it.

This brings a failure mode the copy-out pool never had: if *every* frame is
pinned, there is nowhere to admit a new page. The old CLOCK loop spun until it
found an unreferenced frame, which two sweeps always guaranteed; a pool of
all-pinned frames would now spin forever. So the sweep is bounded — at most two
passes — and returns `None` if it never lands, which `read_page` surfaces as an
error. In practice it is unreachable: the live pin set is a root-to-leaf path,
three to five frames against a pool of 1024. But it is reported honestly rather
than assumed away.

### Why the rework stopped at the pager

Changing `read_page`'s return type sounds like it should ripple everywhere. It
does not, for one reason: outside the pager itself, `read_page` has a single
caller — the B+tree. The executor, the planner, the catalog never touch a raw
page; `BTree`'s methods hand them owned `Vec`s. So the change is contained to
three files: `pager.rs` (the pool), `page.rs` (a `Page` now wraps either an
owned `Box` or a borrowed `PageRef`), and `btree.rs` (the call sites — a
mechanical swap of one constructor for another). The query engine above did not
change by a line, because a `PageRef` owns through its `Arc` and borrows
nothing, so the B+tree's borrow structure is exactly as it was.

The write path needed nothing at all. The B+tree never edits a page in place: it
reads the cells out, edits a `Vec`, and rebuilds the page with `build_leaf` /
`build_internal`. Writes already construct a fresh buffer, so `write_page` is
untouched — only the *read* copy disappeared. The two spots that read a page and
wrote it back verbatim — a root split, a root collapse — now take one explicit
copy each, the only copies left anywhere near a read.

### What it buys, and what it does not

A B+tree descent now inspects each frame in place: a three-level search that
copied 12 KiB copies nothing. Every page access in the system — every descent,
every leaf a scan loads — loses its 4 KiB memcpy, for the price of one atomic
increment to pin and one to unpin.

The honest limit: the streaming `Cursor` still calls `leaf_entries()`, which
materializes a leaf's cells into owned `Vec`s. v0.14 removed the pool-to-`Box`
copy that sat *under* that; the leaf-to-`Vec` copy above it is a separate matter
— the cursor yields owned `(key, value)` pairs, so a row's bytes are copied into
the caller's hands somewhere regardless. What v0.14 eliminated is the copy every
`read_page` made unconditionally, whether the caller wanted an owned page or
merely glanced at it. The on-disk format is untouched — a v0.13 database opens
in v0.14 unchanged.

## Session 15 — Streaming results to the wire (v0.15)

### The last thing that did not stream

PrehniteDB streams almost everywhere. v0.8 made the executor a *volcano* — a
tree of pull-based operators, each `next` call drawing one row. The B+tree
cursor holds only its current leaf. WAL recovery replays one page at a time.
v0.14 made page reads copy-free. And yet `Database::execute` ended a `SELECT` by
*draining* the whole volcano tree into a `QueryResult::Rows { rows: Vec<_> }`,
and the server sent that `Vec` as a single wire frame. A `SELECT *` of a
million-row table built a million-row `Vec` in server memory before one byte
reached the client. v0.15 removes that last buffer.

### The obstacle that wasn't there

A streamed result has to be something the server *pulls* — pull a row, write
it, pull the next. The obvious Rust worry is lifetimes: a row iterator that
borrows the `Database`, its borrow threaded up through the server's send loop.

It never arises, because the v0.8 volcano tree was already built for it. Every
operator's method is `next(&mut self, pager: &mut Pager)` — the pager is
*threaded through the call*, not held by the operator. So a `Box<dyn Operator>`
owns its whole subtree and borrows nothing. Streaming a `SELECT` is, almost
exactly, *not* calling the `drain()` that used to collect the tree: hand the
`Box<dyn Operator>` back instead, wrapped in a `RowStream`, and let the caller
pull `next` against a pager it supplies. No lifetime parameter appears anywhere
— not on `RowStream`, not on the server's hold of it. The volcano model from
v0.8 was built, perhaps without knowing it, for exactly this.

### Two kinds of row source

A `RowStream` carries one of two `RowSource`s. `Volcano(Box<dyn Operator>)` is a
plain `SELECT`: the operator tree, pulled live, a row materialized only as it is
asked for. `Buffered(vec::IntoIter)` is the grouped path — `GROUP BY`, `HAVING`,
and bare aggregates are pipeline breakers, since grouping must see every row
before it can fold the first — so that result is materialized no matter what,
and the `RowStream` simply hands the finished rows out one at a time. One `next`
interface over both.

The materializing `Database::execute` did not go away — an embedder linking the
library usually wants the whole answer in hand. It is now *defined* in terms of
the streaming path: build the `RowStream`, drain it into a `QueryResult`. One
executor path, with `execute` a thin collector on top of it.

### The protocol grew a vocabulary

The wire spoke one `Response` per request: `Ack`, `Error`, or a single `Rows`
frame carrying the entire result. A streamed result is not one message but a
*sequence*, so `Response` became a per-frame enum: `RowsBegin` with the column
names, a `Row` per row, and `RowsEnd`. The server writes that sequence as it
pulls; the client reads frames in a loop until the end.

The sequence also has to carry *failure*. A `SELECT` can fault partway through —
a corrupt overflow chain that `unwrap_value` cannot reassemble, a B+tree page
that will not read — and by then some `Row` frames are already on the wire. So
an `Error` frame may stand in for `RowsEnd`: rows, rows, rows, error. The
client, mid-result-set, reports the error and drops the partial set rather than
rendering a misleading half-table.

### The lock it costs

v0.12 was deliberate about one thing: a reader releases its lock *before* the
network reply is written, so a slow client never holds a writer up. v0.15 gives
that up — and must. The server pulls the volcano tree, which pulls the pager,
*throughout* the send; the pager has to stay valid and the file stable for the
whole streamed reply, so the reader's shared lock is held from `RowsBegin` to
`RowsEnd`. A slow client draining a large result now delays writers for exactly
that long. Readers still never block each other — the lock is shared — but a
writer waits.

That cost is also why the streaming stops at the server. The client — the
interactive CLI — still buffers the streamed frames and renders one aligned
table, because aligning columns needs every row's width, which needs the whole
set. That is the right place to stop: the CLI is one person's process showing
one human-sized answer, while the server is the shared, long-lived process that
a `SELECT *` must not be able to topple. v0.15 bounds the memory of the process
that matters.

### What still buffers, and what changed underneath

The honest scope: `Sort` and the `GROUP BY` pass are pipeline breakers and still
buffer their input, so an `ORDER BY` query keeps a result's worth of rows live
*inside the executor* — sorting cannot yield its first row until it has seen the
last. v0.15 does not change that; nothing can. What it changes is the common
path: a plain, filtered, or `LIMIT`ed `SELECT` now streams from B+tree leaf to
socket without ever being collected.

The on-disk format is untouched — this is an executor and protocol change, no
storage change. The *wire* format did change: a v0.14 client and a v0.15 server
no longer understand each other. Pre-1.0 that is allowed; past 1.0 it would need
a negotiated protocol version.

## Session 16 — Hash joins (v0.16)

### The third textbook join

v0.9 added the nested-loop join — buffers the inner table once, rescans it per
left row, correct for any `ON` predicate, O(left × inner). v0.10 added the
index nested-loop join — the inner side is a B+tree index, looked up per left
row instead of rescanned, O(left × log inner). v0.16 adds the third: a hash
join, for an equi-join whose inner table has no usable index. Build a hash
table on the inner side once, then probe it per left row — O(left + inner). It
is the standard answer for the case the other two leave: an equi-join with no
index. (Sort-merge is the fourth textbook algorithm; with hash joins in, it
adds nothing PrehniteDB needs.)

### The shape was already there

A hash join slots into `build_from` as a third path. That function already
tried an *index* nested-loop join first — pattern-matching the `ON` clause for
an equality between a left column and an indexed inner column, walking
through `AND`s — and fell through to the buffered nested loop. The hash-join
path is the same pattern *minus the index requirement*: `find_equi_join` is
`find_index_join` with the index lookup removed. If that finds a column-to-
column equality, the join builds a `HashJoin` instead of a `NestedLoopJoin`;
otherwise (a `CROSS JOIN`, or an `ON a.x <> b.y`) the nested loop is still
correct and still the fallback.

### Build, probe, re-check

A `HashJoin` carries the left operator, the inner operator, the column index
of the join key on each side, the full `ON` predicate, and a hash table built
on the first `next` call. Build: drain the inner side into a `HashMap<Vec<u8>,
Vec<Vec<Value>>>`, keyed by the encoded join-key value. Probe: per left row,
encode its key, look up the bucket, walk it, re-apply the full `ON` predicate
to each pair, emit matches. `LEFT` joins `NULL`-pad an unmatched left row, the
same way the other two joins do.

The hash key is `codec::encode_index_value` — the same per-value byte encoding
the indexes already use. Reusing it is more than convenient: indexes must
store equal values as equal bytes (else an index lookup would miss), so that
encoding is *the* canonical definition of value equality in PrehniteDB. The
hash join inherits it for free, including whatever the encoding does about
edge cases like `-0.0` and `0.0`.

The full `ON` is re-applied because the hash key is a *necessary* condition,
not a sufficient one. An `ON` may carry more — further AND-chained equalities,
range tests — and a bucket match only proves the one equality the hash
narrows on. So matching rows are in the same bucket (correct), most
non-matching ones are in different buckets (the hash filters), and the
re-check rejects the rest. The pattern is exactly the one
`IndexNestedLoopJoin` uses for the same reason.

### NULLs match nothing

SQL three-valued logic: `NULL = anything` is `NULL`, never `TRUE`, so a join
`ON a.x = b.x` never matches a row whose `x` is `NULL` on either side. The
full `ON` re-check would reject those pairs anyway — `passes_filter` keeps
only rows whose predicate is exactly `TRUE` — but the hash join handles
`NULL`s up front. An inner row with a `NULL` build key is dropped at build
time, so the table holds no unreachable entries; a left row with a `NULL`
probe key skips the lookup and never matches. A `LEFT` join still pads it
with `NULL`s on the right, since it matched nothing.

### What didn't have to change

Adding a new join algorithm took one operator, one helper to detect the
equi-join condition, and one branch in `build_from`. The wire format, the
storage engine, the planner, and the rest of the executor are untouched. The
existing join tests — `inner_join_relates_two_tables`, `left_and_cross_joins`,
`multi_way_and_self_joins`, and the index-vs-plain equivalence test — were
written against the nested-loop fallback; with v0.16 they all now exercise the
hash join (their `ON` clauses are equi-joins, and they do not index the inner
side), which is free correctness coverage. The
`index_driven_join_matches_a_plain_join` test changed character in particular:
it used to compare nested-loop with index nested-loop; it now compares hash
join with index nested-loop, a stricter equivalence check.

### What's in memory, and what stays bounded for later

This is an *in-memory* hash join: the inner side and the hash table both sit
in RAM. That is the same memory profile as the nested-loop fallback it
replaces — which already buffered the whole inner table — just much faster. A
*grace* hash join, partitioning both sides to disk so memory stays bounded
however large the inner table, is a real next step: it could reuse v0.7's
spill machinery in spirit, but the pager spills *pages*, not row batches, so
grace hashing would need a new row-batch spill path. That is a separate
session. The on-disk format is untouched, and the wire format is unchanged —
hash join is purely an executor change, so a v0.15 client still talks to a
v0.16 server.

## Session 17 — A grace hash join (v0.17)

### The v0.16 hash join, bounded only by memory

v0.16's hash join was strictly faster than the nested-loop fallback — O(left +
inner) instead of O(left × inner) — but it kept the nested loop's *memory*
profile: the inner side was buffered whole, and the hash table sat on top of
it. For an inner table that fits, that is exactly right. For one that does
not, the join would simply run out of room. v0.17 fixes the bound — bounded
memory regardless of inner size — without giving up the algorithmic win.

### Partition both sides, then join a partition at a time

The trick is the textbook one: equal join keys hash to equal values, so *any*
hash function used to partition both sides puts matching rows in the same
partition. So:

1. Pick a fixed N (16). Hash every inner row's join key into one of N
   buckets; spill it to that partition's file. Same for every left row.
2. For each i in 0..N: read inner partition i, build an in-memory hash table
   on it; read left partition i, probe per row; emit matches. Drop the hash
   table.

Memory is bounded by the largest partition — *not* the inner table. With N=16
and a hash function that distributes evenly, that is roughly the inner table
over 16. (If a partition itself is too big, the textbook answer is to
re-partition it recursively. v0.17 skips that — the fixed-N case is plenty
for the workloads PrehniteDB targets.)

### Spill files, cleaned on drop

The spill mechanism is deliberately small: a `SpillFile` holds a single OS
temp file (in `std::env::temp_dir()`), opened read+write, with a process-local
atomic counter for uniqueness. Each row is written length-prefixed — a `u32`
length followed by `codec::encode_row`'d bytes — so reading back is just
`read_exact` of four bytes and then `read_exact` of that many. `Drop` removes
the file, so a panic or early return cleans up after itself; living in the OS
temp dir means anything that *does* leak (a kill -9 mid-run) gets swept by
the OS eventually.

The encoded form is the same one the B+tree uses for stored values —
`codec::encode_row` / `codec::decode_row` — so the spill files inherit
whatever encoding decisions the storage engine already made. No new encoder,
and the round-trip is the one tested across every existing data path.

### Each partition is just an ordinary HashJoin

Once both sides are partitioned, joining partition `i` is *exactly* the v0.16
hash join over two inputs that happen to be `SpillReader`s instead of table
scans. `SpillReader` is a small `Operator` that decodes one row at a time
from a `SpillFile` and hands it up. The per-partition join builds a hash
table on the inner-partition reader, probes per left-partition row,
re-applies the full `ON` predicate, and `NULL`-pads `LEFT` misses — code
v0.16 already wrote.

So `GraceHashJoin` is mostly *orchestration*: drain both inputs into
partitions, then for each partition pair, spin up a fresh `HashJoin` over the
spill readers, drain it, drop it, advance. The clever bit is what isn't
reinvented.

### The cost: left stops streaming

v0.16's hash join streamed the left side — pull a row, probe, emit. Grace
can't: the left has to be drained into partition files before the first
partition's join can run, because the per-partition join needs *only* the
left rows whose key hashes to that partition, which means knowing the
partition for every left row up front. So a `SELECT ... LIMIT 10` over a
giant join no longer stops scanning after ten rows — it scans both inputs
fully, partitions them, then begins to emit. The price of bounded memory,
paid in latency-to-first-row.

This is also why the streaming protocol from v0.15 — which holds the reader's
lock for the whole streamed reply — does *more* work now for a grace-path
query: the reader's lock now spans the partition phase too. Both costs come
from the same place: the left input is no longer free to flow row by row
from B+tree leaf to socket.

### What's bounded, and what isn't

What v0.17 bounds: the *memory* a hash join uses. The largest per-partition
hash table, not the inner table. With N=16 partitions and an evenly-
distributed hash function, that is the inner table size divided by ~16.

What it does *not* bound is the worst case. A pathologically skewed key
distribution — say, every inner row sharing one join key — sends every row
to the same partition; the per-partition hash table is the whole inner
table, and memory is unbounded again. The textbook answer is recursive
re-partitioning: when a partition turns out too big, re-partition *it* with a
different hash. v0.17 leaves that for later.

Disk usage is bounded by the size of both inputs (each is written exactly
once across the partition files). Spill files live in the OS temp dir for
the join's lifetime and are removed on drop. The on-disk database format is
unchanged, and the wire format is unchanged — a v0.16 client still talks to
a v0.17 server.

## Session 18 — Cost-based planner: row-count statistics and INNER-join reorder

Until v0.18 the planner was *cardinality-blind*: it knew which tables a
query touched, but not how big any of them were. So when the user wrote
`FROM big INNER JOIN mid ON ... INNER JOIN tiny ON ...`, the executor built
the join tree exactly as written — big on the bottom, tiny on top — and a
500-row table joined to a 5-row table walked five hundred thousand pairs
where fifty would have done.

`INNER JOIN` is commutative and associative; the order is the planner's
choice, not the user's. v0.18 makes that choice cost-aware.

### A single new field: `row_count` on `Schema`

The planner needs one thing to reason about cost: a *count of rows in
each table*. Nothing more sophisticated than that. So `Schema` grows a
single `row_count: u64` field, maintained by the executor's INSERT and
DELETE handlers and persisted in the catalog. Two writes, one new column;
the rest of the catalog encoding is identical.

```rust
pub struct Schema {
    pub name: String,
    pub columns: Vec<Column>,
    pub root: u32,
    pub next_rowid: u64,
    pub row_count: u64,   // ← new
    pub indexes: Vec<Index>,
}
```

The encoding appends `row_count` as a trailing little-endian u64 after the
index section. Two changes follow:

- INSERT: `schema.row_count += inserted` after the loop, then
  `catalog.put`. (The same call already persisted `next_rowid`; the new
  field rides along free.)
- DELETE: `schema.row_count = schema.row_count.saturating_sub(deleted)`,
  conditionally calling `catalog.put` only if any rows were actually
  removed. `saturating_sub` is belt-and-braces: a future miscount should
  not corrupt stats below zero.

VACUUM, which rewrites the catalog from scratch, copies `row_count` across
to the new file like every other Schema field.

### MAGIC: PREHNDB3 → PREHNDB4

A new field at the tail of the encoded Schema would, in principle, decode
fine in *both* directions: the existing decode path even had a "schemas
written before v0.2 had no index section" branch that yielded an empty
index list for short inputs. But row counts are *cumulative state* — a
zero would silently mean "we haven't started counting yet", which would
quietly defeat the entire reorder pass on any database carried forward
from v0.17. Better to refuse to open it: bump the magic, and let the user
know explicitly.

The MAGIC bump also lets the decode path drop its v0.2-compat branch: the
file's magic now guarantees the format matches the code, so an unexpected
EOF mid-decode is a corruption error, not a feature-detection
opportunity.

### Reorder: enumerate, score, attach

The reorder pass — `reorder_inner_chain` — sits in the planner's Select
branch, ahead of the existing access-path selection. It handles a single
shape: a `FROM` whose joins are *all* `INNER`, with at most eight tables.
LEFT and CROSS joins are not commutative, so anywhere one appears in the
chain the layout freezes. The eight-table cap exists because the
enumeration is brute-force (8! = 40320 orderings is roughly a tenth of a
millisecond; nine is ten times that).

For a chain that qualifies, the pass:

1. **Collects** each table's row count from the catalog and each ON
   expression. It builds two indexes: qualifier → table position (so a
   `t.col` reference resolves to a table) and column name → list of tables
   (so a bare `col` reference can be checked for ambiguity).
2. **Analyses each predicate's references** as a bitmask. A `t.col`
   reference contributes `1 << t_index`; a bare `col` contributes only if
   exactly one table has it. If anything is ambiguous (a bare reference
   mentioning a column two tables share) the pass bails entirely and the
   user's order survives — the analysis is *opt-in*: a chain it cannot
   reason about cleanly is left untouched.
3. **Enumerates every permutation** of `0..n` via a textbook recursive
   swap. The first permutation visited is the identity, which together
   with a strict-less-than cost compare lets the user's order win every
   tie.
4. **Scores each ordering** with a sum-of-intermediates estimate:

   ```
   intermediate₀ = max(1, rows[ord[0]])
   for step k in 1..n:
       new = ord[k]
       connected = ∃ predicate whose refs touch joined ∧ touch new
                                                ∧ refs ⊆ joined ∪ {new}
       intermediate_k = if connected: max(intermediate_{k-1}, rows[new])
                        else:         intermediate_{k-1} * rows[new]
   cost = Σ intermediate_k
   ```

   The product penalty for a disconnected step is what stops a naïve
   "smallest-first" sort from picking a cross product. With three tables
   `a`, `hub`, `b` where `a` and `b` only join through `hub`, the
   orderings `[a, hub, b]` and `[hub, a, b]` connect at every step;
   `[a, b, hub]` and `[b, a, hub]` don't (no predicate ties `a` and `b`
   directly) and pay `|a|*|b|` at step one. The product blows the
   disconnected ordering past every connected one without any special-case
   logic in the algorithm.

5. **Re-attaches predicates** to the chosen ordering. Each predicate
   lands on the *earliest* step whose joined set covers every table it
   references. A step with no predicate becomes `ON TRUE` (kept INNER so
   the executor's join-algorithm picker still sees it), and multiple
   predicates landing on one step are ANDed together. The output is a
   reshaped `FromClause` with the same semantics as the input — only the
   order has changed.

The cost model is intentionally weak. `max(left, right)` is a poor
estimate of the actual join cardinality (which depends on selectivity, key
overlap, NULLs); it does *not* distinguish the two orderings of a
two-table connected join (max is commutative); and it ignores per-tuple
join cost differences between nested-loop and hash. What it *does* do
well, given only table cardinalities, is push the largest table to the end
of a chain — which is the headline win, since the largest table appears
in every subsequent intermediate the running max touches.

### Why the algorithm choice stays in the executor

A real cost-based planner picks both the *order* of joins and the
*algorithm* for each one — nested-loop, index-nested-loop, hash. v0.18
splits these: the planner picks the order, the executor (unchanged) picks
the algorithm per step.

This is a deliberate scope decision. Algorithm choice already works well
in v0.17's executor: an equi-join whose inner column is indexed becomes an
index nested-loop join; an equi-join without an index becomes a grace hash
join; everything else falls back to a nested loop. The detector runs on
each join step as the executor builds the tree, so it already adapts to
whatever order the planner hands down. Moving the choice into the planner
would mean teaching the planner about indexes, hash-table sizes, and
spill thresholds — a much larger change for marginal gain in v0.18.

### The two-table tie

A subtle property of the scoring: for a two-table connected join the
estimate is `max(left, right)`, which is identical in either direction.
The planner does not reorder a two-table join — every test asserting one
was rewritten to expect the user's order preserved. This is a *correct*
behaviour of the heuristic, not a bug: there is no cost difference at the
planner's level of resolution, so the planner declines to make a choice it
cannot justify. The actual asymmetry — which side of a nested-loop or
hash join is cheaper as outer vs inner — lives in the executor and is the
natural target of a future pass.

### Predicates that the planner cannot resolve

The reorder pass bails — falling back to the user's order — in three
distinct cases:

1. **Ambiguous bare reference.** `... ON id = id` where both tables have
   an `id` column. Without a way to attribute the column to a specific
   table, the predicate's reference bitmask cannot be built, so the cost
   estimate would be wrong and the re-attachment step might place the
   predicate on the wrong join.
2. **Unknown qualifier.** A column reference whose `t.col` qualifier does
   not match any table in the FROM. This is almost always a query error
   that the executor will catch; until then the planner just leaves it
   alone.
3. **Aggregate in an ON.** An aggregate is invalid in an ON clause and
   the executor will reject it; the planner declines to reason about it
   first.

In all three the contract is the same: the reorder is *opportunistic*. A
chain it cannot reason about cleanly produces exactly the plan the user
asked for, which is what the executor would have run anyway in v0.17.
Correctness never depends on the reorder.

### Tests

Seven unit tests in `planner.rs`, two integration tests in
`integration.rs`. The unit tests use the existing `fixture()` to stand up
a Pager + Catalog, then `catalog.put` schemas with specific `row_count`
values to drive the cost. They cover:

- two-table no-op (the tie),
- three-table largest-pushed-to-end (the headline win),
- LEFT and CROSS keep user order (anchored joins),
- ambiguous bare reference punts (the bail path),
- cross-product avoidance (the product penalty in the cost),
- predicate re-attachment correctness (no orphans, references the new
  table).

The integration test runs the worst-order three-table query against a
real database, comparing the result row-by-row to the hand-written
best-order query. They must match. A second integration test confirms
that `row_count` is maintained by `INSERT`, `DELETE`, *and* a reopen — the
reorder is only useful if its inputs are accurate.

### What v0.18 does not give the planner

A short list of things a real cost-based planner does that v0.18 leaves to
later:

- Selectivity. Without column-value histograms or distinct-count stats
  the join intermediate is the max of inputs, not the join cardinality
  estimate proper. A many-to-one foreign key gets the same estimate as a
  many-to-many.
- Index information. The planner does not consult `Schema.indexes` when
  scoring orderings — an index nested-loop join on a per-row lookup is
  much cheaper per left row than a hash join with a large build side, but
  the heuristic does not see that.
- Algorithm choice. The executor still picks per step. A planner that
  enumerated `(order, algorithm)` pairs together could co-optimise — for
  instance, pick the order that lets the smallest table become the hash
  table's build side.
- LEFT join reorder. A LEFT join's *right* side is sometimes
  reorderable; v0.18 freezes the whole chain when any LEFT is present.
- Multi-join algorithm graphs that aren't left-deep. Bushy plans (joining
  two intermediate results) can be cheaper than any left-deep tree, but
  v0.18 only enumerates left-deep orderings.

The on-disk format changes (PREHNDB4) and the wire format is unchanged —
a v0.17 client still talks to a v0.18 server, but a v0.17 database file
will not open.

## Session 19 — Subqueries

Until v0.19 PrehniteDB's parser had a flat expression grammar that
recognised only "ordinary" SQL expressions: literals, columns, arithmetic,
comparisons, `IS [NOT] NULL`. The headline SQL feature missing was
**subqueries** — a `SELECT` inside an `Expr`. v0.19 adds three forms, all
uncorrelated:

- `expr [NOT] IN (SELECT ...)` — set membership.
- `[NOT] EXISTS (SELECT ...)` — row presence.
- `(SELECT ...)` in any expression position — a *scalar subquery*.

Each is opt-in syntactic sugar that turns into something the executor's
existing per-row evaluator can handle, so the bulk of the work was *not*
making the executor's loop subquery-aware — it was making sure the loop
never sees a subquery node at all.

### AST: four new `Expr` variants

```rust
Expr::InSubquery   { expr: Box<Expr>, subquery: Box<Statement>, negated: bool }
Expr::Exists       (Box<Statement>)
Expr::ScalarSubquery(Box<Statement>)
Expr::InList       { expr: Box<Expr>, values: Vec<Expr>, has_null: bool, negated: bool }
```

The first three are what the parser emits. The fourth is the *resolved*
form of `InSubquery` — the subquery has run, its rows are collected, and
the IN node now holds the values directly. `Exists` and `ScalarSubquery`
don't need their own resolved variants because they collapse cleanly to
existing literal forms (`Expr::Bool(b)` and `Expr::Integer/Real/Str/...`).

The `Box<Statement>` in three of the variants creates a mutual cycle
through the AST: `Expr` → `Statement` → `Expr` again (a subquery's
`Statement::Select` has its own `filter: Option<Expr>`). Box handles the
sizing; the cycle is finite per query because the user's text is.

Adding `Expr` inside `SelectItem` (so `SELECT (SELECT MAX(x) FROM t)`
parses) forced one downstream change: `f64` doesn't implement `Eq`, so
`Expr` is only `PartialEq` — which means `SelectItem` and (transitively)
`Projection` both had to drop their `Eq` derives. No call site cared.

### Parser: three small additions, one big one

The expression grammar's precedence ladder stays the same:

```
OR < AND < NOT < comparison < + - < * / < unary - < primary
```

Three of the new shapes slot in cleanly:

- `[NOT] IN (SELECT ...)` sits at the **comparison** level — it's a
  postfix on the left operand, the same precedence slot as `=`. The
  parser, after parsing the left side, peeks for `IN` or `NOT IN` and
  recurses into `statement()` for the subquery body. Right-paren closes
  it.
- `EXISTS (SELECT ...)` is a new **primary**. The `EXISTS` keyword
  triggers `(`, `statement()`, `)`, and the parser emits `Expr::Exists`.
  `NOT EXISTS` rides the existing unary-`NOT` machinery — it falls out
  for free.
- `(SELECT ...)` as a scalar subquery is a disambiguation in **primary**.
  After consuming `(`, peek: if the next token is the `SELECT` keyword,
  it's a subquery; otherwise it's an ordinary parenthesised expression.

The big addition is `SELECT (SELECT ...) FROM ...` — a scalar subquery in
the projection. The old `projection()` parser was bespoke: it knew about
columns, qualified references, and aggregate calls and produced
`SelectItem::Column` or `SelectItem::Aggregate` directly. The new version
just calls `self.expr()` and then lowers:

```rust
items.push(match expr {
    Expr::Column(c)    => SelectItem::Column(c),
    Expr::Aggregate(a) => SelectItem::Aggregate(a),
    other              => SelectItem::Expr(other),
});
```

That "lower if recognisable, wrap if not" is the entire change. It also
admits arithmetic in select lists for free — `SELECT a + 1 FROM t` now
parses, which we got asked-for ages ago and never built.

### Executor: rewrite-in-place, not memoise

Because `eval` takes no pager and no catalog (just an `Expr` and a row
context), subqueries cannot execute during eval — by the time the
per-row loop runs, every subquery in the filter has to have been
resolved. The clean way to do it: **walk the expression once, before the
loop starts, executing each subquery and rewriting its node**.

`prepare_subqueries(expr, pager, catalog)` does the walk. It recurses
into the children of each operator node and, on the way back up, matches
on the three parser variants:

- `Expr::InSubquery` runs the subquery, splits the column into a `Vec`
  of values and a `has_null` boolean, and rewrites the node as
  `Expr::InList` carrying both. `std::mem::replace` lifts the inner LHS
  expression out before swapping.
- `Expr::Exists` runs the subquery and rewrites the node as
  `Expr::Bool(any_rows)`.
- `Expr::ScalarSubquery` runs the subquery, expects ≤1 row × 1 column
  (NULL for 0 rows; error for more), and rewrites the node as the
  matching literal `Expr` variant (Integer, Real, Str, Bool, or Null).

Each subquery runs through the *normal* executor — `planner::plan` then
`executor::execute` — so a nested subquery (a subquery whose filter
contains another subquery) is resolved bottom-up by the recursive walk.
A pager and catalog are threaded down because planning and execution
both need them.

Calling `execute()` recursively inside `select()`/`update()`/`delete()`
works because pager and catalog are `&mut` and `&`, and Rust is happy to
nest the borrows: we own the call stack and there's no aliasing.

`prepare_subqueries` is called at four entry points:

- `select()` — for the filter, having, and each projection item that is
  `SelectItem::Expr`.
- `update()` — for each assignment's value expression and the filter.
- `delete()` — for the filter.

After the walk, the filter, having, and assignments contain only
"normal" expression nodes — no subquery shapes — so eval, the existing
per-row evaluator, doesn't need to know subqueries exist. The four new
`Expr` variants in eval are all error arms: an unprepared subquery is a
"corruption" error (a planner/executor bug), not a user-facing one.

### IN with NULL: standard SQL three-valued logic

`x IN (a, b, c)` is `TRUE` if `x` matches any value, `FALSE` if it
matches none. But what about `NULL`?

- `NULL IN (anything)` is `NULL`. Every comparison against `NULL` is
  `NULL`; the OR of `NULL`s is `NULL`.
- `x IN (a, NULL, b)` for `x` not matching `a` or `b` is `NULL`, not
  `FALSE`. The reasoning: `x = NULL` is `NULL`, and `FALSE OR NULL` is
  `NULL`.
- `x IN (a, b)` for `x` not matching either is `FALSE` — no `NULL` was
  ever introduced.

`NOT IN` is the logical negation, so the same `NULL` poison propagates:
`x NOT IN (a, NULL)` is `NULL` for any non-matching `x`, never `TRUE`.
A `WHERE` clause filters for `Bool(true)` exactly, so a `NULL` predicate
drops the row — meaning `NOT IN` against a set with `NULL` returns
*nothing*. That is the standard SQL surprise, and PrehniteDB now
reproduces it. An integration test asserts it explicitly so a future
refactor cannot quietly break the semantics.

The `has_null` flag on `Expr::InList` carries this out: the IN match
checks the values list first, and if no equality matches, looks at
`has_null` to decide between `FALSE` and `NULL`.

### Projection's new "Expr" item: a small operator change

The plain (non-grouped) projection used to be a `Vec<usize>` of column
indices and a `Project` operator that copied them out per row. With
`SelectItem::Expr` now possible — a scalar subquery, arithmetic, a
literal — `Project` has a richer item kind:

```rust
enum PlainItem {
    Column(usize),
    Expr(Expr),
}
```

The operator clones a column directly when it can; otherwise it calls
`eval` against the row. Scope is carried only when at least one item is
an expression — pure-column projections still avoid the allocation.

The grouped path (`GROUP BY`, `HAVING`, or any aggregate) is *not*
extended to handle `SelectItem::Expr` in v0.19. The grouped path's
projection logic is more involved: a non-aggregate item must be a
grouping column, the per-group projection re-evaluates aggregates over
the group's rows, and threading expression evaluation through that means
handling references to grouping columns *and* aggregates inside an
expression. A `SelectItem::Expr` in a grouped query is an explicit error
for now.

### Tests: parser, executor, and the NULL surprise

Six parser tests for shapes (IN/NOT IN/EXISTS/NOT EXISTS/scalar
in-where/scalar in-select), one for arithmetic-in-select-list as a side
benefit of the refactor. Five integration tests:

1. `in_subquery_filters_against_a_set` — IN, NOT IN, empty subquery.
2. `not_in_with_null_follows_three_valued_logic` — the surprise.
3. `exists_and_not_exists_test_for_rows` — including the empty case.
4. `scalar_subquery_in_where_and_select_list` — both positions.
5. `scalar_subquery_with_no_rows_is_null_and_multi_row_errors` — the
   two corner cases of the scalar form.

144 tests total. The smoke test exercises IN, EXISTS, and a scalar
subquery against the live server, end to end through the wire protocol.

### What v0.19 leaves to a future session

- **Correlated subqueries.** A subquery that references the outer
  query's columns. Implementing them requires propagating the outer
  scope down to the subquery's planner and re-executing the subquery
  per outer row (or, much better, rewriting it to a semi-join). The
  re-execution model alone is a session; the optimiser path is more.
- **Derived tables.** `FROM (SELECT ...) AS s` — a subquery in the
  FROM clause. Parser change is small; executor needs an operator that
  streams from a sub-plan.
- **CTEs.** `WITH x AS (...) SELECT ... FROM x` — named scopes for a
  subquery, often recursive.
- **`ANY` / `ALL`.** `x = ANY (subquery)` (equivalent to IN), `x > ALL
  (subquery)`. Different shape; modest extension.
- **Streaming the IN set.** Right now the IN subquery materialises into
  a Vec; for a million-row IN subquery the lookup is O(n) per probe.
  A HashSet on hashable values, or even a sorted Vec with binary search,
  is the obvious next step. The bottleneck is not in production
  workloads yet.

The on-disk format is unchanged (still PREHNDB4) and the wire format is
unchanged — a v0.18 client still talks to a v0.19 server, and a v0.18
database file opens cleanly.

## Session 20 — Sharding the buffer pool

A buffer pool the whole server shares behind a single mutex is fine while
one writer holds the database lock. It is also fine while one reader is
running. It is *not* fine the moment two readers run at the same time on
different pages: every `read_page` takes the pool's mutex to look up the
frame, and two readers serialise on it exactly as if the pool itself were
single-threaded. v0.13 made the pool sharable; v0.20 makes it actually
share well.

### Sixteen CLOCK caches, one routing function

The change is internal. `SharedPool` used to wrap one `Mutex<BufferPool>`;
now it wraps `Arc<[Mutex<BufferPool>]>`. Each shard is the same
`BufferPool` as before — same slot array, same `HashMap<u32, usize>`
index, same CLOCK hand — with a fraction of the total capacity. The
default 1024-frame pool becomes 16 shards of 64 frames each.

A page is routed to its shard by `(page_no as usize) % shard_count`. The
modulo compiles to a single `AND` instruction when the shard count is a
power of two (and `POOL_SHARDS = 16` always is). The lookup function the
pager calls — `get(no)`, `put(frame, dirty)` — locks only the relevant
shard. Two reads on pages that hash to different shards take different
mutexes and run in parallel, full stop.

```rust
fn shard(&self, no: u32) -> MutexGuard<'_, BufferPool> {
    let idx = (no as usize) % self.shards.len();
    self.shards[idx].lock().expect("...")
}

fn get(&self, no: u32) -> Option<Arc<Frame>> {
    self.shard(no).lookup(no)
}
```

`POOL_SHARDS = 16` is the sweet spot the conventional wisdom (and PG's
`NBuffers` / `num_partitions = 128`, MySQL's
`innodb_buffer_pool_instances` defaulting to 8, Cassandra's row cache
shards) cluster around. A workload uniformly distributed across pages
contends on each lock one-sixteenth as often as a single-mutex pool. Too
many shards and lock-array indirection plus per-shard
under-utilisation costs more than it saves; too few and reader fanout
hits the mutex faster than it can be released. 16 is the right answer
for a single-writer / many-reader system on a typical host.

### Capacity arithmetic: small pools clamp

Tests deliberately use tiny pools (4 frames, 16 frames) to force
eviction. With 16 shards and a 4-frame pool we'd get 0.25 frames per
shard — meaningless. The implementation clamps:

```rust
let shard_count = capacity.min(POOL_SHARDS);
let per_shard   = capacity / shard_count;
let remainder   = capacity % shard_count;
```

So a 4-frame pool gets 4 shards of 1 frame each. The total capacity is
still exactly what the caller asked for: the remainder is distributed
one frame at a time across the leading shards so the totals always add
up to `capacity`. A test asserts this.

This clamp matters: it preserves the v0.13 bounded-memory property
exactly. The pool never holds more frames than its `capacity`. A
sharded pool that rounded up per shard (so 4 → 16 → 64 frames) would
have inflated the small-pool tests' working set sixteenfold, broken the
eviction tests, and inflated production memory by a tenth of the
default capacity.

### Evicting under a shard

CLOCK eviction now runs per-shard, on a sweep that touches only that
shard's slot array. The eviction outcome is the same it always was — a
clean victim is dropped, a dirty one is returned to the pager so it can
spill the page to the WAL — but the contention story changes:

- A shard's CLOCK sweep no longer competes with another shard's reads.
- A pinned frame in shard 0 cannot stall an admission to shard 1.
- A pathologically narrow workload (every page hashed to one shard) can
  still saturate that shard's eviction. That's the trade-off: we
  reduce common-case contention at the cost of accepting a degenerate
  worst case. Production workloads with broad page distributions, which
  is most of them, hit the common case.

The `pinned_pages_block_eviction` test extended to a sharding-aware
variant in v0.20: pin a page in shard 0, watch the admission to another
shard-0 page fail (correct: shard 0 has one frame, pinned), and watch
the admission to a shard-1 page succeed (correct: the shards are
isolated). The trick that made this clean — predicting which shard a
page would land in — is just `page_no % 16`.

### Iteration: commit, rollback, clear

A few pool operations naturally walk every frame:
`for_each_dirty` (commit flushes every dirty page to WAL),
`has_dirty` (commit's fast-path skip when nothing changed),
`mark_all_clean` (after commit), `drop_dirty` (rollback),
`clear` (VACUUM). Each used to lock the pool once and iterate;
now each walks the shards in order, locking and releasing each in
turn.

This costs more lock acquisitions per commit — 16 instead of 1 — but
each acquire is uncontended (commit holds the database-wide write
lock, so no reader is racing) and amortised over thousands of normal
reads between commits. The same pattern Postgres uses for its
`partition_lock` array. Negligible at scale.

The order matters only for `for_each_dirty`'s WAL append, and even
there only in that pages are appended in a per-shard-then-per-slot
order rather than insertion order. WAL apply replays records in WAL
order regardless, and the database file's atomicity at commit doesn't
care which order pages reach disk in.

### What stays the same

- The public `SharedPool` API is unchanged: `new`, `with_capacity`
  (still internal), `clone` (still an `Arc` bump). Every caller
  outside `pager.rs` compiles untouched.
- `PageRef` still pins by `Arc::strong_count > 1`. The pin lives on
  the `Frame`, which is inside a shard's slot; the shard's CLOCK
  sweep checks the same Arc count it always has.
- The `wal_index` on `Pager` is per-pager and routes by page number,
  not by shard. Spilled-page recovery is unaffected.
- The on-disk format is unchanged (still `PREHNDB4`); the wire
  protocol is unchanged. A v0.19 client and database both work with
  v0.20.

### What v0.20 does not give the pool

A short list of next-level pool work, in rough increasing
sophistication:

- **Lock-free or RCU lookup.** The shard mutex serialises within a
  shard. A lock-free hashmap (or even an `RwLock` per shard) would
  let parallel reads of the *same* shard run in parallel too, for an
  N-fold improvement on small working sets that fit in one shard.
  Substantial design work.
- **Per-thread cache layer.** A small thread-local cache in front of
  the shared pool would cut even shard-mutex traffic when reads
  repeat. Standard CPU caches do this implicitly; database pools
  can do it explicitly.
- **Dynamic shard count.** The static 16 ignores the host's actual
  core count. Choosing N at startup from `available_parallelism()`
  is straightforward, but the win is marginal — 16 covers most
  shapes.
- **Better eviction.** CLOCK is the simplest reasonable policy.
  LRU-K, GCLOCK, 2Q, or ARC would catch some workloads CLOCK loses
  to. Each is a paper of its own.

The on-disk format is unchanged (still PREHNDB4) and the wire format
is unchanged — a v0.19 client still talks to a v0.20 server, and a
v0.19 database file opens cleanly.

## Session 21 — Vectorised pipeline

The volcano operator tree of v0.7 onwards is a beautiful abstraction and
the right shape for a database whose hard queries are joins and group-by.
It is also a bad fit for the *easy* queries — scan, filter, project,
maybe limit — that make up most analytic workloads. Every operator pays
the same per-row dispatch cost; every predicate evaluation visits one row
of mixed-type cells, with branches on type per cell and one `Vec<Value>`
allocated per row passing through the pipeline. v0.21 adds a second,
columnar operator tree alongside the existing one, used when the query
shape qualifies for it.

### Columnar batches: SoA + null bitmap

The unit of work is a [`ColumnBatch`](crates/prehnitedb/src/engine/batch.rs):
up to 1024 rows in **struct-of-arrays** layout. Each output column is its
own typed value array — `Vec<i64>`, `Vec<f64>`, `Vec<String>`, `Vec<bool>` —
paired with a packed null bitmap of one bit per row. The bitmap is a
`Vec<u64>` with `1` meaning valid (the typed slot holds a real value) and
`0` meaning `NULL` (the typed slot is unused). 1024 rows is 16 u64 words,
128 bytes — well within L1 alongside the value array.

This is the Apache Arrow layout. The win it gives is not directly that
of SIMD instructions (although a future pass can add those); it is that
a columnar inner loop visits a contiguous slice of one type, with no
type-switch per element and the predictable branch pattern modern CPUs
get right. The null mask is checked separately so the value loop itself
never branches on nullability.

`Column` is a typed enum (`Int`/`Real`/`Text`/`Bool`), each variant
holding a `Vec` of that type plus the mask. Pushing a `Value::Null`
appends a sentinel value and clears the mask bit; pushing a typed value
appends it and sets the mask bit. Reconstructing a row visits one slot
per column, indexing into both the values vec and the mask.

### A parallel `BatchOperator` tree

The new operators live in `executor.rs` alongside the row ones. Five
operators, plus an adapter:

- `BatchScan` opens a `Cursor` over either the table B+tree or a
  secondary index (`IndexScan` ranges chase the rowid suffix back to the
  table). Each `next_batch` pulls up to 1024 rows, decoding each into a
  `Vec<Value>` and pushing into the batch's columns. A B+tree leaf is
  read once per batch instead of once per row — every read past the
  first touches an already-cached buffer-pool page.
- `BatchFilter` evaluates its predicate columnwise into a Bool column,
  then materialises a new batch holding only the rows where the mask is
  exactly `Bool(true)`. SQL's three-valued logic is exact: `NULL` and
  `FALSE` both drop the row, only a definite TRUE keeps it. A batch
  that filters to zero rows is invisible above; the operator pulls
  again until something survives or the input ends.
- `BatchProject` evaluates each output expression columnwise: column
  references clone the matching input column straight through (one
  `Vec`/`String` clone, the values pass through unchanged); arithmetic,
  comparisons, and logic each run a tight element-wise loop.
- `BatchLimit` counts rows across batches. The last batch is partially
  sliced when the quota lands mid-batch; once empty, the operator stops
  pulling and the scan ends early.
- `BatchToRow` is the adapter that exposes a `BatchOperator` tree as the
  row-at-a-time `Operator` interface. It keeps a cursor into the current
  batch and pulls a new one when exhausted. Everything upstream — the
  streaming protocol, the buffered embedder path, the `LIMIT`
  short-circuit — works unchanged.

The trait itself is trivially:

```rust
trait BatchOperator {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>>;
}
```

`None` is end of stream; an empty batch is forbidden (a filtered-down
batch is dropped and the operator retries).

### Columnar `eval`

The scalar evaluator returns one `Value`. Its columnar twin —
`eval_batch(expr, batch, scope)` — recurses through the `Expr` tree and
returns a `Column` of exactly `batch.n_rows` rows. Literals broadcast
to a full column (`Expr::Integer(5)` over a 1024-row batch becomes
`Column::Int { values: vec![5; 1024], nulls: all_valid }`); column
references clone the matching input column (one `Vec` clone, no
per-cell work); arithmetic and comparisons run element-wise loops with
null propagation; logical AND/OR/NOT walk SQL's three-valued tables.

```rust
fn eval_batch(expr: &Expr, batch: &ColumnBatch, scope: &Scope) -> Result<Column> {
    match expr {
        Expr::Null    => broadcast_null(batch.n_rows),
        Expr::Integer(v) => broadcast_int(*v, batch.n_rows),
        Expr::Column(c)  => batch.columns[scope.resolve(c)?].clone(),
        Expr::Binary { op, left, right } => {
            let l = eval_batch(left, batch, scope)?;
            let r = eval_batch(right, batch, scope)?;
            binary_columnar(*op, l, r)
        }
        // …
    }
}
```

The arithmetic paths split by operand types: Int+Int stays in `i64`
with the same `checked_add`/`checked_sub`/`checked_div` overflow
checks the scalar evaluator uses. Mixed Int/Real promotes to `f64`,
matching the row-at-a-time `arithmetic` function. Comparisons walk
through `Value` for cross-type ordering (Int vs Real, Text-Text,
Bool-Bool) — a future columnar fast path could specialise the
same-type cases without the `Value` round-trip.

Three-valued logic is exact:

```rust
BinaryOp::And => match (l_valid, r_valid) {
    (true, true)  => (lv[i] && rv[i], true),
    (true, false) if !lv[i] => (false, true),   // FALSE AND NULL = FALSE
    (false, true) if !rv[i] => (false, true),   // NULL AND FALSE = FALSE
    _             => (false, false),             // anything-with-NULL = NULL
},
```

The dominance rule (a definite FALSE/TRUE wins against a NULL operand)
is implemented row-by-row in the same loop — branching on the validity
bits, never on the values' contents.

`IS NULL` becomes a single one-bit-per-row test against the input
column's mask. `IN`/`InList` falls back to per-row inside the columnar
shell — a hash-set fast path is a worthwhile future optimisation but
not required for correctness.

### When the vectorised path is used

The planner enters the batched tree at the top of `select()` when:

- the `FROM` is a single table (no joins),
- there is no `GROUP BY`, `HAVING`, or aggregate in the projection,
- there is no `ORDER BY`.

Anything else falls through to the existing row-at-a-time pipeline,
which still handles all the operators (join, sort, group, aggregate)
the batched tree does not. The decision is structural — at the planner's
level it does not depend on data — so a query is either batched or not,
deterministically, by its shape alone.

The `SELECT *` case skips the `BatchProject` step entirely: `BatchScan`
already produces a batch typed for the schema's columns, so the project
would be the identity transformation. A `SELECT col_a, col_b` (or any
explicit projection) constructs a `BatchProject` with one `PlainItem`
per output position; column refs clone, expressions evaluate columnwise.

### A pre-existing `SELECT *` bug, found en route

Wiring up `projection_headers` for the new path turned up a v0.19
regression: the row-at-a-time call site passed `&[]` for `projected`,
which made `SELECT *` produce an empty `columns` list in the result.
No test had caught it because none of the integration tests do
`SELECT * FROM table` at the top level — they all use explicit item
lists. The fix is straightforward: `projection_headers(&projection,
&scope)` uses every column the scope owns when `Projection::All`,
without needing the caller to pass a parallel index array. A new test
asserts the headers explicitly so the case stays covered.

### Concurrent-pool test got more contended

A second, smaller fallout: v0.20's `concurrent_pagers_share_one_pool`
ran against a 16-frame shared pool, which v0.20 turned into 16 shards
of 1 frame each. Eight threads in tight `read_page` loops sometimes
saturate one shard's single frame — every concurrent pin on that shard
fails. The test passed in v0.20 by timing luck; v0.21 happens to lose
that race deterministically. The fix: bump the test's pool to 64
frames (4 per shard) so contention has somewhere to evict to. The
test's intent — exercise eviction under contention — is preserved.

### What v0.21 leaves to a future session

Vectorisation here is *partial* on purpose. A short list of next-level
work, in rough increasing complexity:

- **Vectorised aggregation.** Hash aggregation in particular composes
  naturally with the SoA layout: the hash-table key is the
  concatenation of one slot from each grouping column, and per-group
  state updates can run columnwise.
- **Vectorised sort.** Run generation (each batch sorted locally, then
  merged) is the conventional shape; pdqsort over a single batch is
  fast enough that the merge dominates.
- **Vectorised joins.** Selection vectors (a `Vec<u32>` of kept rows,
  not a fully materialised batch) often outperform materialisation
  for the build/probe loop. The join algorithms (nested-loop,
  index-nested-loop, hash, grace hash) all need rework to consume
  batches.
- **SIMD intrinsics.** The columnar loops are already cache-friendly;
  explicit SIMD (auto-vectorisation already finds some of it) is a
  meaningful next win for Int and Bool arithmetic and Int/Bool
  comparison.
- **Columnar `IN`/`InList`** with a hashed set instead of a per-row
  linear scan.
- **Direct decode into columns.** `BatchScan` currently goes through
  `Vec<Value>` to push into columns; a direct decode that writes the
  typed slots without materialising `Value` would save a Vec
  allocation per row.

The on-disk format is unchanged (still `PREHNDB4`) and the wire format
is unchanged — a v0.20 client still talks to a v0.21 server, and a
v0.20 database file opens cleanly.

## Session 22 — Hash aggregation

The original `GROUP BY` path, all the way back to v0.7, was sort-then-group:
materialise every matched row, sort by the grouping-column tuple, walk the
sorted run to split it into per-group buckets, then for each bucket
re-scan its rows once per aggregate call. Correct, simple, *slow* —
`O(N log N)` for the sort, `O(K × G)` for `K` aggregates over `G` rows
total, every aggregate computed by an independent pass. v0.22 replaces
the whole path with a single-pass hash aggregator.

### One pass, one bucket per group

The shape: a `HashMap<GroupKey, Vec<AggregateState>>`. The key is the
tuple of values at the grouping columns; the value is a `Vec` parallel
to the query's distinct aggregates, holding their running state. Per
input row: compute the key, find or insert the bucket, update each
aggregate in place.

```rust
for row in &matched {
    let key = GroupKey {
        values: group_cols.iter().map(|&i| row[i].clone()).collect(),
    };
    let states = buckets.entry(key)
        .or_insert_with(|| template.clone());
    for (state, slot) in states.iter_mut().zip(&registry.slots) {
        state.update(slot, row)?;
    }
}
```

`template` is the initial `Vec<AggregateState>` — `Count(0)` /
`SumInt { 0, false }` / `AvgReal { 0.0, 0 }` / `Extreme { None, want }`
per slot — cloned into each new bucket. Memory is `O(G)` distinct
groups times `O(K)` aggregates, not `O(N)` input rows. Time is `O(N × K)`
total instead of `O(N log N + N × K)`. The sort vanishes.

### `GroupKey`: hashing a `Vec<Value>`

`Value` does not implement `Hash` — `Real(f64)` is the obstacle, since
`f64` has no `Hash` impl. The wrapper supplies one by hand:

```rust
impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values { hash_value(v, state); }
    }
}
fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    match v {
        Value::Null    => 0u8.hash(state),
        Value::Int(n)  => { 1u8.hash(state); n.hash(state); }
        Value::Real(r) => { 2u8.hash(state); r.to_bits().hash(state); }
        Value::Text(s) => { 3u8.hash(state); s.hash(state); }
        Value::Bool(b) => { 4u8.hash(state); b.hash(state); }
    }
}
```

A discriminant byte first, so `Bool(false)` and `Null` cannot collide by
accident. `Real` hashes by its `to_bits` representation, and a parallel
`value_eq` compares by `to_bits` equality — every `NaN` lands in one
bucket and `-0` stays distinct from `0`, matching SQL convention.
`Null` is its own group: two `Null`s compare equal, a `Null` and any
non-null compare unequal. Standard SQL `GROUP BY` semantics, made
explicit.

### Aggregate de-duplication

A query that says `SUM(amount)` in both the projection *and* the HAVING
should compute the sum once per group, not twice. The
`AggregateRegistry` walks the projection items and the HAVING expression
once before the hash pass, calls `intern(aggregate)` on every
`Expr::Aggregate` it finds, and deduplicates by the AST node's own
`Eq`/`Hash` (added in this session — `Aggregate`, `AggregateArg`,
`ColumnRef`, `AggregateFunc` all derive `Hash`). The registry hands back
a `Vec<AggregateSlot>` of distinct calls plus a `HashMap<Aggregate,
usize>` for lookup during emit.

A `SUM(amount)` mentioned three times maps to one slot. A `SUM(amount)`
and a `SUM(other)` map to two distinct slots. A `COUNT(*)` and a
`COUNT(amount)` are different aggregates and map to two slots, since
the `arg` is different.

### `AggregateState`: running per-row state

Each aggregate type carries the minimal state to update incrementally:

```rust
enum AggregateState {
    Count(u64),
    SumInt  { total: i64, seen: bool },
    SumReal { total: f64, seen: bool },
    AvgReal { total: f64, count: u64 },
    Extreme { best: Option<Value>, want: Ordering },
}
```

`Count` is just a `u64`. `SumInt` keeps a `seen` flag so an all-`NULL`
column still finalises to `NULL`, not `0` — SQL's distinction. `SumInt`
uses `checked_add` and errors on overflow, matching the scalar path.
`AvgReal` keeps total in `f64` regardless of column type, so an `AVG`
of `INT` values returns `REAL`. `Extreme` carries the current best plus
the comparison direction (`Less` for `MIN`, `Greater` for `MAX`),
walking `order_values` per non-null row.

`update(slot, row)` matches on `(slot.func, slot.column, &mut state)` —
the column index lives on the slot, not in the AST, so the inner loop
never re-resolves a name. `finalize(self)` collapses the running state
to one `Value`.

### `eval_group_expr`: HAVING and projection share one evaluator

The old code had two evaluators: `eval_having` for HAVING, and inline
match arms in `grouped_select` for the projection items. With the hash
aggregator's precomputed aggregates, both reduce to the same shape: an
expression to evaluate against a finalised group, where column refs
mean "the group's value at this grouping column" and aggregate calls
mean "look up the precomputed slot". `eval_group_expr` does both:

```rust
fn eval_group_expr(
    expr: &Expr,
    group_cols: &[usize],
    key: &GroupKey,
    aggregates: &[Value],
    registry: &AggregateRegistry,
    scope: &Scope,
) -> Result<Value> {
    match expr {
        Expr::Aggregate(a) => Ok(aggregates[registry.lookup(a).unwrap()].clone()),
        Expr::Column(c) => {
            let column = scope.resolve(c)?;
            let pos = group_cols.iter().position(|&i| i == column).ok_or_else(...)?;
            Ok(key.values[pos].clone())
        }
        // … arithmetic, comparisons, IS NULL, InList — same shapes as eval()
    }
}
```

One side-benefit: `SelectItem::Expr` (arithmetic and the like) now works
in grouped queries, where it previously errored. The bound is that
column refs must resolve to grouping columns — same rule as bare
columns, naturally enforced by `eval_group_expr`'s `Column` arm.

### The empty whole-table aggregate

`SELECT COUNT(*) FROM empty_table` must return `0`, not zero rows. The
buckets `HashMap` is empty after the hash pass — no row, no
`or_insert_with` call. The fix: when `group_cols` is empty *and* the
hash map is empty, manually insert one bucket with the template state.
The finalisation pass then emits one row with `Count(0)` → `Int(0)` and
the other aggregates at their `NULL` start. `SELECT COUNT(*),
SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM empty_table`
yields `(0, NULL, NULL, NULL, NULL)`, matching SQL standard exactly.

### Deterministic output order without a sort

`HashMap` does not preserve insertion order — successive runs would
emit groups in arbitrary, hash-dependent order. ORDER BY hides this,
but a query without ORDER BY would have non-deterministic output, which
makes tests fragile and CLI behaviour weird. Easy fix: a parallel
`Vec<GroupKey>` records insertion order alongside the hash map. The
emit pass walks this Vec, draining the map by key, so the output rows
appear in the order their first row was encountered — which equals the
input row order, which equals B+tree rowid order, which is stable
across runs.

### Three pre-existing tests that exercise nothing changed

All ten `grouped_select`-touching tests pass unchanged. The contract is
preserved exactly: same correctness, same ORDER BY semantics, same
NULL handling, same error messages. The only visible change is *which*
order groups appear in when no ORDER BY is given — and the existing
tests either include ORDER BY or assert on a row count, so neither
sees the difference.

### What v0.22 leaves to a future session

A short list of follow-on work:

- **Vectorised hash aggregation.** The hash table is row-at-a-time
  today — `update(slot, row)` walks one row's `Vec<Value>`. The
  natural pairing with v0.21's `ColumnBatch` is per-column updates
  (`SUM` reads the typed column slice directly), with the hash-key
  computation taking a batch worth of group keys at once. Big win for
  high-cardinality groupings.
- **Spill to disk.** The hash table is `O(distinct groups)` in
  memory; a truly large grouping that does not fit needs partitioning
  to disk, much like v0.17's grace hash join does for joins. Out of
  scope here.
- **Median, percentile, string-concat aggregates.** Easy slot additions;
  v0.22 keeps the existing five (`COUNT` / `SUM` / `AVG` / `MIN` / `MAX`).
- **Distinct aggregation.** `COUNT(DISTINCT col)` needs a per-group
  hash set of values; not yet supported.
- **Aggregate functions in HAVING that reference columns not in the
  projection.** Already works — that is the case the registry was
  designed for, and there is now a test for it.

The on-disk format is unchanged (still `PREHNDB4`) and the wire format
is unchanged — a v0.21 client still talks to a v0.22 server, and a
v0.21 database file opens cleanly.

## Session 23 — Vectorised joins

v0.21 added the columnar pipeline; v0.22 added hash aggregation on top
of it; v0.23 closes the last big gap in the batched executor — joins.
With this session, a scan-filter-join-project query moves through the
columnar tree end-to-end, batched scan to batched filter to batched
join to batched project, with the row-at-a-time pipeline now reserved
for `ORDER BY`, `GROUP BY`/aggregates, and index nested-loop.

### Two new operators

The shape mirrors the row-at-a-time joins; the difference is in the
data type and the iteration model.

**`BatchNestedLoopJoin`** handles INNER / LEFT / CROSS. The left
batches stream; the right input is drained once into a
`Vec<Vec<Value>>` and rescanned per left row. For each left batch
row, the operator pairs against every buffered right row, evaluating
the `ON` predicate with the scalar evaluator over the combined row
(or unconditionally for CROSS). Matches go to the output batch; a
LEFT row that matched nothing is padded with `NULL`s.

```rust
struct BatchNestedLoopJoin {
    left: Box<dyn BatchOperator>,
    right_input: Option<Box<dyn BatchOperator>>,
    right_rows: Option<Vec<Vec<Value>>>,
    output_types: Vec<Type>,
    on: Option<Expr>,
    kind: JoinKind,
    scope: Scope,
    right_width: usize,
    // iteration state kept across next_batch calls:
    current_left: Option<ColumnBatch>,
    left_pos: usize,
    right_pos: usize,
    matched_current: bool,
}
```

**`BatchHashJoin`** handles INNER and LEFT equi-joins. On first
`next_batch` it drains the inner side into a
`HashMap<Vec<u8>, Vec<Vec<Value>>>` keyed by the encoded build column
value. Per left row, it encodes the probe column, looks up the
bucket, reapplies the full `ON` predicate to every (left, inner) pair
(the hash key only narrows; the predicate decides), and emits a row
per match. A `NULL` probe key matches nothing — exactly the SQL rule
the row-path `HashJoin` enforces. The build phase drops `NULL`-keyed
inner rows for the same reason.

Both operators bound their output to `BATCH_SIZE` rows. When an
output batch fills mid-left-row, the operator stores its iteration
state (the current left batch, position within it, and join-specific
cursor) and returns; the next call resumes where it stopped. This is
critical for `LIMIT`: a query like `... JOIN ... LIMIT 10` reads only
as many left rows as it must, even when an inner side is huge.

### Wiring: `joins_vectorisable` + `build_batched_scan`

The qualification check in `select` was simply
`from.joins.is_empty()` before v0.23. Now it consults
`joins_vectorisable`, which walks every join the way `build_from` does
— building a fresh `Scope` per step, looking up the inner schema —
and returns false if any join would prefer an index nested-loop (that
is, `find_index_join` returns `Some` for its `ON`). For those queries
the index NL is faster than a hash join, so we keep the row path
exclusively.

`select_vectorised` then walks the joins itself, using the same
`find_equi_join` helper the row path does to pick `BatchHashJoin`
(equi-join) vs `BatchNestedLoopJoin` (everything else — CROSS, non-
equi, or queries the equi-join detector cannot crack). Each inner
side becomes its own `BatchScan` via the new `build_batched_scan`
helper — same one the base table uses.

### Same algorithm choice, same semantics

The batched joins share the row joins' detection helpers
(`find_equi_join`, `find_index_join`) and the row joins' SQL
semantics (NULL keys never match in an equi-join; LEFT pads
unmatched left rows with NULLs; the full `ON` predicate is reapplied
even after a hash key match). Every join integration test from
v0.9–v0.17 — INNER, LEFT, CROSS, multi-way, self-join, hash-join
with duplicate and NULL keys, the grace-hash 2,752-row LEFT — passes
without changes. The path the query takes is just different; the
contract is identical.

### Output materialisation: the obvious next optimisation

The batched joins build their output by pushing combined rows into a
new `ColumnBatch` one at a time. That defeats some of the
vectorisation win: a 1024-row left batch joining to a 1024-row inner
side might produce a million combined rows, each materialised
column-by-column with a per-cell `Value::push` into the output batch.
The conventional vectorised-execution answer is *selection vectors*:
keep the underlying column data laid down once, and let each
operator pass a `Vec<u32>` of surviving row indices alongside the
batch. Filter and join produce selection vectors; sort and the wire
boundary materialise.

v0.23 keeps materialisation throughout, accepting the cost for the
simpler implementation. Selection vectors compose well with what is
already here — the `BatchColumn` data lives unchanged behind an
`Arc<Vec<…>>` or similar, with operators carrying selections — but
it is a substantial refactor of `eval_batch` (which currently clones
columns for column refs and produces fresh ones for arithmetic).
That is its own session.

### What v0.23 leaves to a future session

The short list of next-level work:

- **Selection vectors.** The headline output-side optimisation. A
  `Vec<u32>` of surviving row indices per batch, threaded through
  filter, join, and project, with materialisation only at sort or
  the wire boundary.
- **Vectorised index nested-loop.** Today the index NL kicks the
  whole query back to the row pipeline. A batched variant would
  let queries that join a large table to a small indexed inner
  side stay columnar throughout.
- **Vectorised grace hash join.** The row-path GraceHashJoin
  partitions both sides to disk when the inner side does not fit
  in memory. A batched version would let the vectorised pipeline
  handle joins of arbitrary size, not just ones whose inner fits
  in memory.
- **SIMD intrinsics** for the columnar inner loops. Auto-
  vectorisation finds some of it; explicit SIMD is the next step.

The on-disk format is unchanged (still `PREHNDB4`) and the wire
format is unchanged — a v0.22 client still talks to a v0.23 server,
and a v0.22 database file opens cleanly.

## Session 24 — Selection vectors

The vectorised pipeline since v0.21 has had one persistent inefficiency:
a filter that keeps 100 of 1024 rows still allocates 100-row columns
for every output column, copying the surviving cells out of the input.
For a `WHERE id < 5` over a thousand-column-wide table the column copy
dominates the filter cost — and the copy is sometimes thrown away one
operator later, when a `LIMIT` trims it further. The conventional
vectorised-execution answer is **selection vectors**: instead of
copying surviving rows, return the same column data with a small
`Vec<u32>` listing which physical rows are still in play.

### `ColumnBatch.selection`

`ColumnBatch` gains a third field:

```rust
pub struct ColumnBatch {
    pub columns: Vec<Column>,
    pub n_rows: usize,
    pub selection: Option<Vec<u32>>,
}
```

`selection` carries the logical-row → physical-row mapping. When
`None` (the post-`BatchScan` form), logical row `i` is at physical row
`i` and `n_rows` equals every column's length. When `Some(sel)`, the
batch's logical rows are exactly `sel[0..sel.len()]` of the underlying
columns; `n_rows == sel.len()`. The column data is unchanged — only the
selection vector tells consumers which rows survive.

A new helper `physical_for(logical) -> usize` does the lookup
(branchless when `None`, one indirection when `Some`), and `row_at`
goes through it transparently. The eight existing tests that pushed
materialised rows into batches still work — `push_row` requires
`selection.is_none()`, which is the default for `with_types`, and the
join operators that build output via `push_row` already create their
output batches that way.

A new unit test exercises the selection logic explicitly: build a
five-row batch, attach `selection = Some(vec![4, 1, 3])` with `n_rows
= 3`, and assert that `row_at(0..3)` returns the values at physical
positions 4, 1, 3 respectively.

### `BatchFilter`: build a selection, don't materialise

The old filter wrote a fresh `ColumnBatch` row by row. The new filter
walks the predicate's mask, maps each surviving logical row through
the input's selection to get a physical index, and emits the input
batch with a new selection of those physical indices:

```rust
let mask = eval_batch(&self.predicate, &input, &self.scope)?;
let selection = build_selection(&input, &mask)?;
if !selection.is_empty() {
    let n_rows = selection.len();
    return Ok(Some(ColumnBatch {
        columns: input.columns,        // ← unchanged
        n_rows,
        selection: Some(selection),
    }));
}
```

`build_selection` is the only loop that touches the surviving rows,
and even there it writes `u32`s into a tight `Vec`. The columns pass
through. For a high-selectivity filter (1% surviving), the new path
is dominated by the predicate eval; the row-data copy that used to
sit alongside is gone.

`build_selection` is also the place that maps logical rows back to
physical: when the input *already* carries a selection (a chained
filter, conceivably, although today's planner ANDs them), the
surviving physical index is `input.physical_for(logical)`, not just
`logical`. Selections compose naturally — each layer maps from its
logical rows to the underlying physical rows.

### `eval_batch`: gather column refs through the selection

The columnar evaluator's `Expr::Column` arm used to clone the input
column straight through. With a selection present, that clone returns
the wrong shape — its length is the full column's, not `batch.n_rows`.
The new arm calls `materialise_column(&col, batch.selection.as_deref())`,
which gathers the selected physical rows into a fresh `Column` of
exactly `n_rows` length:

```rust
fn gather_column(col: &BatchColumn, selection: &[u32]) -> BatchColumn {
    // For each variant: walk `selection`, push the selected typed value
    // and null bit into a fresh, contiguous Column.
}
```

This is the cost the selection-vector path concedes: at every column
read inside `eval_batch`, the column is gathered into a logical-row-
aligned `Vec<T>`. For arithmetic over two columns, that's two gathers
plus one elementwise loop — versus the old design's no gather plus
one elementwise loop (over physical rows the predicate has already
admitted). The saving is in the columns *not* referenced — arithmetic
on `a + b` produces a result column of `n_rows` cells, but columns
`c`, `d`, ... that the predicate doesn't mention are never gathered.

For `WHERE a < 100` over a 50-column-wide table, the old filter
copied 50 columns. The new filter materialises 0 (the predicate
gathers `a` into a 1024-row column, evaluates `a < 100`, builds a
selection; the other 49 columns pass through without a touch). The
gather cost only kicks in for columns the projection or a downstream
operator actually consumes.

### `BatchProject` materialises through the input selection

The projection is the natural point where the selection vector
gets "flushed". For column-ref items, `materialise_column` gathers
through the input's selection. For expression items, `eval_batch`
returns a column already aligned to logical rows. The output batch
has `selection: None` — it's fully materialised at logical-row
count.

This makes sense: projection often reorders or renames columns, and
once the output has different columns from the input there is no
point keeping the input's selection. The selection vector earned its
keep upstream; downstream gets the materialised result.

### `BatchLimit`: slice the selection

The old `BatchLimit` called `select_rows(&batch, &indices)` which
materialised a new batch holding rows at the given indices. The new
`BatchLimit` calls `slice_logical_rows(batch, range)`, which either
sub-slices the existing selection (if any) or builds a fresh
selection covering the kept range. Either way, the column data
flows through unchanged.

So a `WHERE ... LIMIT 10` chain reads exactly 10 rows out of the
B+tree (via the existing scan early-stop), evaluates the predicate
on each batch, materialises 0 columns at the filter, and slices the
selection at the limit — until `BatchToRow` reads the 10 rows out
one at a time via `row_at`.

### Joins keep materialising

`BatchHashJoin` and `BatchNestedLoopJoin` consume their input via
`row_at` (which already honours selection), so the change is
free for them on the input side. On the output side, the join
constructs a new batch by `push_row` per match — fully materialised,
no selection. The cross-product nature of a join makes selection-
vector output awkward (which physical row would the selection point
at?), so for v0.24 the joins continue to materialise.

The natural future step: pair selection vectors with **row-id
columns** so the join's output can carry "row index in the left
batch" and "row index in the right buffer" as two selection vectors
into the join's two inputs. DuckDB and Velox do this. Out of scope
here.

### Test count and verification

Two new integration tests: a 5,000-row high-selectivity filter
(`id % 47 == 0`, 107 of 5,000 surviving) confirms the data flows
correctly through the selection-vector path; a filter + LIMIT +
OFFSET test exercises the selection-slice path on `BatchLimit`.
Existing tests all pass — the contract is preserved exactly; the
path through the executor is different.

165 → 167 tests; smoke-tested with a 3,000-row filter via the live
server. Clippy clean. Wire format and on-disk format unchanged —
a v0.23 client and database both work with v0.24.

### What v0.24 leaves to a future session

A short list of next-level work:

- **Join output as selection vectors over the inputs.** Instead of
  materialising combined rows, carry two selections — one per join
  input — for the matched pairs. Halves the output-side memcpy in
  join-heavy queries.
- **Compose selection vectors across multiple filters.** Today the
  planner ANDs adjacent filters; if it didn't, two `BatchFilter`s
  in a row would each gather through the previous selection. A
  future fast path could compose selections directly without
  re-materialising.
- **SIMD over the columnar inner loops.** Auto-vectorisation gets
  some of it; explicit SIMD intrinsics on the elementwise paths
  (Int+Int, Int<Int) would pay off where the columns are large.

The on-disk format is unchanged (still `PREHNDB4`) and the wire
format is unchanged — a v0.23 client still talks to a v0.24 server,
and a v0.23 database file opens cleanly.

## Session 25 — MVCC with snapshot isolation

For 24 sessions the database has been single-cursor: at any moment
exactly one statement could mutate it, and reads ran in turn, never
alongside a writer. v0.25 changes that. Every row in the storage
layer now carries its own MVCC visibility metadata, every reader takes
a *snapshot* at statement start, and readers no longer take any lock
at all — they run alongside the single in-flight writer, filtered to
just the data their snapshot can see.

### Row format: `tx_min`, `tx_max`

Every encoded row gains a 16-byte prefix: `tx_min` (the transaction
that created the row, u64 little-endian) and `tx_max` (the transaction
that logically deleted it, `0` if it is still live). The values follow
in the existing tag-prefixed format. Index keys are unchanged —
visibility is checked on the table side, after the index has located a
rowid.

The MAGIC bump is `PREHNDB4 → PREHNDB5`; a v0.24 database does not
open. Adding the prefix to every row in place would have required a
content rewrite, so the cleaner answer is to refuse to load older
files.

### The `next_tx_id` counter

Page 0 of the database holds a new 8-byte field at offset 24:
`next_tx_id`, the smallest TX ID never yet handed out. Each writer
reserves the current value at `BEGIN` and increments the in-memory
counter; the increment is persisted as part of the commit. A
**rollback** leaves the in-memory counter advanced — the reserved ID
is "wasted" — so a TX ID is never reused even when the transaction
itself never commits. Wasted IDs become gaps; no row in the file
carries them.

### `Snapshot` and the visibility rule

A snapshot has three fields:

```rust
pub struct Snapshot {
    pub next_tx: u64,
    pub in_flight: Option<u64>,
    pub own_tx: Option<u64>,
}
```

`next_tx` is the snapshot's upper bound. `in_flight` is the single
write transaction (if any) active at snapshot time — its writes are
*not* yet visible. `own_tx` is the writer's own TX when the reader is
itself writing (or running inside a BEGIN..COMMIT that has done
writes); own writes are visible to the writer via an override.

The visibility check for a row with `(tx_min, tx_max)`:

```rust
let created = (tx_min < self.next_tx && Some(tx_min) != self.in_flight)
    || Some(tx_min) == self.own_tx;
let not_deleted = tx_max == 0
    || (Some(tx_max) != self.own_tx
        && (tx_max >= self.next_tx || Some(tx_max) == self.in_flight));
created && not_deleted
```

Six unit tests in `engine::transaction` walk every branch — TX before
next_tx, TX in flight, own writes, future deletes, own deletes, etc.

### `TxState`: the shared coordinator

`TxState` is the process-wide MVCC coordinator. It wraps an
`Arc<Mutex<{ next_tx_id, in_flight }>>` so every `Database` open on
one file sees the same authoritative state. The server constructs one
at startup and clones it into every connection.

```rust
impl TxState {
    pub fn snapshot(&self, own_tx: Option<u64>) -> Snapshot { ... }
    pub fn begin_write(&self) -> u64 { ... }   // reserve + set in_flight
    pub fn end_write(&self) { ... }            // clear in_flight (commit or rollback)
}
```

A `Database` holds a `TxState` plus its own `current_tx: Option<u64>`
— the TX ID it is writing under, when it is writing. The single-writer
contract is enforced not by `TxState` itself but by the server's
writer mutex; `TxState` happily hands out multiple TX IDs and would
allow concurrent writes today.

### Logical deletes and update-as-insert

`DELETE` no longer removes rows. Instead it rewrites each candidate
row in place with `tx_max = current_tx`:

```rust
table_tree.insert(
    pager,
    &rowid_key,
    &codec::encode_row(record.tx_min, tx_id, &record.values),
)?;
```

Index entries are *not* deleted — they still point at the rowid, and
the row is still in the tree. The visibility check on the table side
filters out tombstoned rows for snapshots after the delete commits.

`UPDATE` is delete-plus-insert: the old version is tombstoned with
`tx_max = current_tx`, and a new row is inserted at a fresh rowid
with `tx_min = current_tx, tx_max = 0`. Old index entries point at
the old (tombstoned) row; new entries point at the new row. Readers
get consistent snapshots: an old snapshot sees the original via the
old index entries; a new snapshot sees the updated row via the new
index entries. Index scans dedupe by rowid so each row is decoded
once per scan.

### Visibility threaded through every operator

`execute` and `execute_streaming` now take `&Snapshot`. From there
the snapshot reaches:

- `TableScan` and `IndexScan` (row pipeline) — decode, check
  `snapshot.visible(tx_min, tx_max)`, skip if not.
- `BatchScan` (vectorised pipeline) — same check before pushing into
  the output batch.
- `IndexNestedLoopJoin::lookup` — the per-row index probe on the
  inner table side filters too; an index entry pointing at a
  tombstoned row is silently dropped.
- `collect_candidates` for UPDATE/DELETE — the writer only sees rows
  visible to its own snapshot, so a row already tombstoned by an
  earlier transaction won't be tombstoned again.

Subqueries inherit their outer query's snapshot, so an
uncorrelated `(SELECT MAX(x) FROM t)` sees the same data the outer
statement does.

### Lock relaxation: readers run free

The server used to wrap the database in `Arc<RwLock<Database>>` —
writes took the lock exclusively, reads took it shared, and they
never overlapped. v0.25 replaces the `RwLock` with a `Mutex` (writers
only) and lets readers open their own `Database` against the shared
pool and shared `TxState`, taking **no lock at all**. The reader's
snapshot keeps it consistent: writes that commit during the read are
invisible (their TX ID was either past `snapshot.next_tx` or equal
to `snapshot.in_flight`).

The shared buffer pool may now hold the writer's uncommitted dirty
pages alongside committed ones. The reader's pager reads those dirty
pages (they're in the pool, freshly admitted by the writer), but the
visibility check on each row filters out the uncommitted ones. The
writer's `tx_min` is in the reader's `snapshot.in_flight`; the
visibility rule rejects.

A `SELECT` inside an open writer transaction (`BEGIN; INSERT; SELECT;
COMMIT`) is the exception: it must see the writer's own uncommitted
inserts. It runs on the writer's pager, under the writer mutex, with
`own_tx = current_tx`. The `tx_min == own_tx` override admits the
own writes.

### VACUUM reclaims tombstones

The MVCC data model means the table tree only grows. `DELETE` and
`UPDATE` add new entries (tombstones, new versions) without
reclaiming old ones. Eventually `VACUUM` runs and cleans up:

- For each table, walk every row and copy only the live ones
  (`tx_max == 0`) into the new compact image.
- Track the surviving rowids; for each index, copy only entries whose
  rowid is in the surviving set.

VACUUM is safe in v0.25 because it takes the writer mutex — by the
time it runs, no other writes are in flight, and readers can be
safely "as of the moment vacuum started" (they still hold their
snapshots from before; they see whatever the new file holds).

In a future session with concurrent writers and longer-lived snapshots,
VACUUM will need an "oldest active snapshot" cutoff and only reclaim
rows whose `tx_max < cutoff`. For v0.25 the single-writer + brief-
snapshot model makes the simpler design correct.

### What v0.25 leaves to a future session

- **Concurrent writers** with write-write conflict detection.
  Multiple writers in flight at once, each with its own TX ID; at
  commit time, check whether any other committed write touched the
  same rows since this writer started, and abort if so. Substantial
  scope on its own.
- **Background VACUUM** that runs continuously with an "oldest
  active snapshot" cutoff, rather than the user-triggered batch
  `VACUUM` of v0.25.
- **Index tombstones**. Today index entries are left behind by
  DELETE/UPDATE and only swept by VACUUM. A future optimisation
  would tombstone the index entry inline, so a scan can skip it
  without chasing back to the table.
- **Serialisable isolation** on top of snapshot isolation, via SSI
  (serialisable snapshot isolation, the algorithm Postgres adopted).

The on-disk format is now `PREHNDB5` — a v0.24 database file does not
open. The wire format is unchanged.

## Session 26 — Concurrent writers with FUW conflict detection (v0.26)

v0.25 put the database under MVCC snapshot isolation, but only one
write transaction could be in flight at a time. The `in_flight` set
on the shared `TxState` was an `Option<u64>`, the writer mutex was
held across `BEGIN..COMMIT`, and a writer that crashed left no
persistent record of what its TX ID resolved to.

v0.26 turns each of those into a plural. `in_flight` is now a
`HashSet<u64>`. A persistent **commit log** records every TX's final
outcome durably, so visibility no longer depends on whether the
writer is still in memory. Transactions are **deferred**: each
statement's writes are physically committed when the statement runs,
stamped with the writer's TX ID, and the logical `COMMIT` is just a
clog append — so two writers can have transactions open at once and
their statements interleave at the engine layer. When they collide on
a row, **first-updater-wins** detects the conflict and aborts the
second writer cleanly.

The scope was carefully cut to one session. Full concurrent network
writers — rewriting `prehnited` around per-connection `Database`
handles and a per-statement writer lock — is honest follow-up work
the engine now supports but the server has not yet adopted. The
v0.26 integration tests demonstrate two `Database` handles sharing a
pool + `TxState` running fully interleaved transactions; the engine
layer is real.

### The commit log

The visibility question for v0.25 had two parts: *is `tx_min`
committed?* and *did it commit before our snapshot?* The second
reduces to `tx_min < snapshot.next_tx && !snapshot.in_flight.contains(&tx_min)`.
The first, in v0.25, was implicit: a writer kept its TX ID in
memory in `in_flight` while it ran, removed it on commit, and a
row whose `tx_min` was anywhere below `next_tx` was assumed
committed. That works if the only thing that can hide a TX is the
in-flight set — but it breaks the moment a writer can roll back, or
crash, and leave its rows on disk under an ID that no snapshot can
distinguish from a committed one.

The fix is a real, durable record of every TX's outcome. A new
`Clog` (`crates/prehnitedb/src/engine/clog.rs`) maintains a
per-database `.db-clog` file of fixed 9-byte records:

```
[ tx_id : u64 LE ][ status : u8 ]   // status: 1 = committed, 2 = rolled back
```

`Clog::record_commit(tx)` appends a record and `fsync`s. So does
`record_rollback(tx)`. On open, the whole file is streamed into an
in-memory `HashMap<u64, Status>` so lookups are O(1); the file is
positioned at the end so future appends go in the right place.

Visibility now consults the clog directly:

```rust
let created = if Some(tx_min) == self.own_tx {
    true
} else if !self.clog.is_committed(tx_min) {
    false                                  // rolled back, in flight, or unknown
} else {
    tx_min < self.next_tx && !self.in_flight.contains(&tx_min)
};
```

The `is_committed(tx_min)` check is what makes a rolled-back row
invisible to *every* snapshot — even snapshots taken after the
rollback. A row stamped with a rolled-back TX stays in the B+tree
(rollback doesn't undo writes) but the clog answer kills it.

`Snapshot` now carries a cheap `Clog` handle (Arc-backed, cloneable)
alongside its `next_tx` and `in_flight` set; `TxState` owns the
single instance and clones it into every snapshot at capture.
`Snapshot` lost its `Copy` impl (an `Arc<Mutex<...>>` inside means
the field can't be `Copy`) and gained `Clone` — every call site that
did `*snapshot` to copy was rewritten to `snapshot.clone()`.

### Multi-writer `TxState`

The in-flight bookkeeping went from "the one writer" to "every
writer":

```rust
struct TxStateInner {
    next_tx_id: u64,
    in_flight: HashSet<u64>,   // was: Option<u64>
}
```

`begin_write()` reserves the next ID and inserts it into the set.
The previous `end_write()` is gone — split into `commit_write(id)`,
which calls `clog.record_commit(id)` and *then* removes from
`in_flight`, and `rollback_write(id)`, which calls
`clog.record_rollback(id)` and removes. The clog write fsyncs before
the in-memory remove, so a writer that crashes between the two
leaves the on-disk record authoritative.

A snapshot captures the *entire* set:

```rust
pub fn snapshot(&self, own_tx: Option<u64>) -> Snapshot {
    let inner = self.inner.lock().expect("poisoned tx state");
    Snapshot::new(
        inner.next_tx_id,
        inner.in_flight.clone(),
        own_tx,
        self.clog.clone(),
    )
}
```

A row stamped by any of those IDs is invisible to this snapshot, by
the visibility rule above — exactly as the v0.25 single-flight case,
just generalised to N.

### Deferred transactions

The v0.25 model held `pager.commit()` for the logical commit: every
statement inside an explicit transaction *staged* its writes in the
buffer pool (or spilled to the WAL), and only the final `COMMIT`
sealed them all into the database file. That was fine for a single
writer — but it means a `BEGIN..COMMIT` *blocks the pager*, so a
peer writer can't even take the file-level lock until the current
transaction finishes.

v0.26 flips the model. `run_plan` (in `engine/database.rs`) now
calls `pager.commit()` after *every* successful statement, even
inside an open `BEGIN..COMMIT`:

```rust
match executor::execute(&mut self.pager, &self.catalog, &snapshot, plan) {
    Ok(result) => {
        if writes {
            self.pager.commit()?;       // physical commit, every statement
        }
        if self.txn == TxnState::None && writes {
            let id = self.current_tx.take().expect("write TX reserved above");
            self.tx_state.commit_write(id)?;   // logical commit (autocommit)
        }
        Ok(result)
    }
    ...
}
```

The logical `COMMIT` is now nothing more than `tx_state.commit_write(id)?` —
an append to the clog. The rows are already on disk, stamped with
`tx_min = id`. The clog write is what flips them from "invisible to
every other snapshot" to "visible to every snapshot".

`ROLLBACK` is the mirror: `tx_state.rollback_write(id)?`. Any
statements that ran inside the transaction are physically on disk,
but their `tx_min` is now in the clog as `RolledBack`, and the
visibility check `clog.is_committed(tx_min)` returns false for
every future snapshot. The rows are invisible — they just take up
space until `VACUUM` reclaims them.

This is the deferred-transactions discipline Postgres uses too:
writes are durable as soon as they're written, but their *logical*
visibility is gated by a single small atomic action (the clog
append) at the very end. The benefit is that the writer mutex (or
the file-level lock) is held only for the statement's duration, not
the whole transaction.

### First-updater-wins (FUW) conflict detection

Two writers can now race for the same row. The model says the first
writer to claim it wins; the second must abort cleanly so it can
retry on a fresh snapshot.

The detection happens in `collect_candidates` in the executor —
the function that gathers the rows an `UPDATE` or `DELETE` will
touch, before any tombstones are written. As each candidate is read
from the table, its `tx_max` is inspected:

```rust
if record.tx_max != 0 && Some(record.tx_max) != snapshot.own_tx {
    match snapshot.clog.status(record.tx_max) {
        Some(Status::RolledBack) => {
            // The other writer's delete didn't take. Treat as live.
        }
        Some(Status::Committed) => {
            // Already deleted before our snapshot. Skip this row.
            return Ok(());
        }
        None => {
            // tx_max is in flight — another writer is mid-modify.
            return Err(Error::conflict(format!(
                "write-write conflict on a row stamped by in-flight transaction {}",
                record.tx_max
            )));
        }
    }
}
```

A non-zero `tx_max` is a tombstone, and the clog has the
authoritative answer for what it means. *Rolled back* — the
tombstone never took; treat the row as live. *Committed* — the row
is dead per a previous transaction, regardless of what our snapshot
shows; skip it. *Neither* — the writer that stamped it is still in
flight, and we're the second to touch it. We abort with
`Error::Conflict`, which propagates up through `run_plan` and
aborts the transaction (`TxnState::Aborted`).

The "first updater" is the writer whose statement reaches the row
first under the writer mutex (still one writer physically at a
time in the engine layer, though the engine itself is now ready
for finer-grained locking). The "wins" half means that writer's
tombstone is in place by the time the second writer's
`collect_candidates` runs.

Conflict is a normal `Error` variant, displayed as `"conflict:
..."`. The client (or library user) can catch it, retry on a
fresh transaction, or surface it.

### VACUUM reclaims rolled-back rows

The v0.25 VACUUM dropped rows whose `tx_max` was set (tombstones).
v0.26 extends the discard rule to include rows whose `tx_min` is
recorded as rolled-back in the clog:

```rust
let clog = self.tx_state.clog();
// ... for each row in the table:
if record.tx_min != 0
    && matches!(clog.status(record.tx_min), Some(Status::RolledBack)) {
    continue;   // skip — invisible to every snapshot
}
if record.tx_max != 0
    && matches!(clog.status(record.tx_max), Some(Status::Committed)) {
    continue;   // skip — tombstoned and the tombstone is durable
}
// otherwise: copy into the compact image
```

This matters because the deferred-transaction model bloats the file
with rolled-back inserts: a `BEGIN; INSERT 500 rows; ROLLBACK`
leaves 500 physically-present rows that no snapshot can ever see.
The v0.26 integration test `rolled_back_inserts_are_reclaimed_by_vacuum`
seeds 500 rolled-back rows, confirms the file didn't shrink at
rollback, and confirms VACUUM finishes by shrinking it.

VACUUM is still safe because it takes the writer mutex —
no transaction is in flight while VACUUM runs, so every TX has a
final clog status to decide on.

### Crash recovery

The crash-recovery rule is the same as before, but the
`status_or_rolled_back` helper on `Clog` codifies it:

```rust
pub fn status_or_rolled_back(&self, tx_id: u64, oldest_active: u64)
    -> Option<Status>
{
    match self.map.get(&tx_id) {
        Some(&status) => Some(status),
        None if tx_id < oldest_active => Some(Status::RolledBack),
        None => None,
    }
}
```

A TX ID below the watermark with no clog entry means a writer
*started* it (reserved it via `begin_write`, persisted the bumped
`next_tx_id` in the pager header) but *crashed before recording
the outcome*. The rule: treat it as rolled back. Its rows are then
invisible to every snapshot, exactly as if the writer had cleanly
rolled back.

The crashed writer's rows are still on disk; the next VACUUM
reclaims them.

### What v0.26 leaves to a future session

- **Per-connection server `Database`** with a per-statement writer
  lock, so concurrent transactions through the network are real
  and not just engine-layer. The integration tests prove the
  engine handles it; the server's mutex pattern is the bottleneck.
- **Predicate (range) conflict detection** for serialisable
  isolation — v0.26's FUW is row-level, which is snapshot
  isolation. Write-skew, the classic SI anomaly, is still
  possible. SSI on top of v0.26 is a natural next step.
- **Background VACUUM**, driven by an oldest-active-TX watermark,
  rather than the user-triggered batch we have today.
- **Clog truncation**. The clog grows unboundedly. Once every
  TX below a watermark is irrelevant (no live snapshot can refer
  to it), the clog's prefix can be compacted into a single
  "everything below N is committed" sentinel.

The on-disk MAGIC bumps to `PREHNDB6`; a v0.25 file does not open.
v0.26's visibility check consults the clog for *every* row's
`tx_min`, and the clog is per-database — a v0.25 file has no clog,
so every existing row's `tx_min` would resolve to "not committed"
and the entire database would appear empty. The clean answer is
to refuse the older format; the alternative — backfilling the
clog on first open by marking `[1, next_tx_id)` as committed —
would silently rewrite the upgrade contract and is left to a
future session that introduces a proper migration path.

The wire protocol is unchanged.

## Session 27 — Concurrent writers at the wire (v0.27)

v0.26 made concurrent writers a property of the *engine*. The
integration tests demonstrated two `Database` handles, sharing a
pool and a `TxState`, with interleaved `BEGIN..COMMIT`s and FUW
detection. But the server still lived in v0.25's shape: one
`Arc<Mutex<Database>>` for writers, held across `BEGIN..COMMIT`,
so two TCP clients trying to open transactions still serialised
at the connection level. The infrastructure was real; the wire
didn't carry it.

v0.27 finishes the story. Each TCP connection now opens its own
`Database` via `open_shared`, the writer mutex shrinks from
"held across a transaction" to "held across one statement", and
each connection's pager re-reads the database header from disk
the moment it takes the lock — so a peer writer's page
allocations are visible before this writer's next allocation can
collide with them. The wire-level integration tests boot the
server in-process, open multiple TCP connections, and verify the
full interleaved-transaction story end-to-end.

### The meta-coherence problem

The shape of the problem first. v0.26's `Database` was designed
for the shared-pool, shared-`TxState` case: every connection can
open its own handle on the same file and they cooperate on page
contents (the pool serves the same bytes to every reader of a
page) and on MVCC bookkeeping (the `TxState` is the single
source of truth for next-TX and in-flight). But each handle's
**`Pager` has its own `Meta`** — its private snapshot of page 0
(page_count, freelist_head, catalog_root, next_tx_id) read at
open or at this connection's own last commit.

Imagine two connections A and B on the same file:

1. A takes the writer lock. A's pager allocates pages 50, 51, 52
   for a fresh table. A's commit writes page 0 with
   `page_count = 53` and flushes pages 50-52 to disk. A releases
   the lock.
2. B takes the writer lock. B's pager still has the old
   `Meta { page_count: 50, ... }` from when B last
   committed (or from open). B's next allocation reads
   `meta.page_count`, takes 50, increments to 51, writes a fresh
   zeroed page at offset 50 — overwriting A's table!

The buffer pool gives us coherent *page contents* (A's page 50
is in the pool; if B reads page 50 it would get A's bytes
through the pool, until B's own write replaces it). But B's
*decision* about where to write is driven by B's local meta,
which is stale.

Two ways to fix this. Share the meta — put `Meta` behind an
`Arc<Mutex<>>` so every pager reads and writes through the same
authoritative copy. Or refresh — give each writer a chance to
sync its meta from the header before it starts allocating. v0.27
takes the refresh path because it is local and minimal: one new
method on `Pager`, one new method on `Database`, and one call
in the server at the top of every write statement.

### `Pager::reload_meta_from_disk`

A new method on `Pager`:

```rust
pub fn reload_meta_from_disk(&mut self) -> Result<()> {
    let page = self.read_page(0)?;
    let meta = decode_header(page.bytes())?;
    drop(page);
    self.meta = meta;
    self.committed = meta;
    Ok(())
}
```

`read_page(0)` consults the shared pool first. After a peer's
commit, page 0 in the pool holds the peer's updated header
(their `commit()` wrote it via `write_page(0, ...)` and then
`mark_all_clean`, leaving the bytes in the pool marked clean).
If page 0 happened to get evicted, `read_page` falls back to
the disk file — also fine, because the peer's commit ran
`wal.apply` and `file.sync_all` before returning.

The `drop(page)` is to release the pin on page 0 before we
overwrite `self.meta`; not strictly necessary (the borrow ends
at end-of-scope) but makes the intent explicit. Setting both
`meta` and `committed` is what makes the refresh idempotent
under a subsequent `rollback()` — which restores `meta =
committed`. Without it, a write statement that rolled back
would snap meta back to *our previous* committed view, not the
peer-updated view we just installed.

### `Database::reload_for_write`

One small wrapper on `Database`:

```rust
pub fn reload_for_write(&mut self) -> Result<()> {
    self.pager.reload_meta_from_disk()?;
    self.catalog = Catalog::open(&mut self.pager)?;
    Ok(())
}
```

Re-opens the catalog too, because the catalog's *root page
number* can move when the catalog B+tree splits at the root.
The catalog root is recorded in `Meta`, so once we have the
fresh meta we can ask `Catalog::open` to find the catalog
afresh. The catalog itself is mostly a wrapper around a tree
root — schemas are always read from the tree on `get`, never
cached on the catalog struct — so this is a cheap pointer
update, not a schema reload.

The server calls this immediately after acquiring the writer
lock for a write statement. Reads don't need it: a snapshot's
visibility check is the source of truth, and a stale
`page_count` only matters when you *allocate*, which reads
never do.

### The new server

`prehnited` was rewritten around the per-connection model. The
core diff is the absence of `Arc<Mutex<Database>>`:

```rust
fn serve_client(
    mut stream: TcpStream,
    db_path: Arc<str>,
    pool: SharedPool,
    tx_state: TxState,
    write_lock: Arc<Mutex<()>>,
) {
    // Each connection has its own Database.
    let mut db = Database::open_shared(&*db_path, pool, tx_state)?;

    loop {
        match read_request(&mut stream)? {
            Some(Request::Query(sql)) => {
                if prehnitedb::is_read_only(&sql) {
                    respond(&mut stream, &mut db, &sql)?;
                } else {
                    let _guard = write_lock.lock().unwrap();
                    db.reload_for_write()?;
                    respond(&mut stream, &mut db, &sql)?;
                }
            }
            None => break,
        }
    }

    if db.in_transaction() {
        let _guard = write_lock.lock().unwrap();
        db.abort_transaction();
    }
}
```

Three things changed from v0.26's server:

1. **No more shared writer Database.** The server bootstraps the
   engine (creating the file and clog if needed), keeps the
   shared `pool` + `tx_state` + `write_lock`, and lets each
   connection open its own `Database`. The bootstrap Database is
   dropped at startup.

2. **Per-statement lock.** The `write_lock` is taken at the
   start of each write statement and released at end-of-scope
   when the response is sent. The lock no longer spans a
   `BEGIN..COMMIT`: between the writer's statements, a peer
   writer's statements can run.

3. **`reload_for_write` at the top.** Inside the lock, before
   running the statement, the connection refreshes its pager
   header — so allocations see the latest `page_count` /
   `freelist_head` / `catalog_root`.

The disconnect path also takes the lock: a client that drops
mid-transaction needs to `abort_transaction`, which writes a
rolled-back record to the clog. That clog write is observable
to other connections (their snapshots' visibility check would
flip on the next statement), so it deserves the lock just like
any other write.

### Library refactor for testability

To exercise the server in-process from integration tests, the
loop logic moved into a `lib.rs` alongside the existing
`main.rs`:

```rust
pub fn serve_on(
    listener: TcpListener,
    db_path: Arc<str>,
    pool: SharedPool,
    tx_state: TxState,
    write_lock: Arc<Mutex<()>>,
);

pub fn bootstrap(db_path: &str)
    -> Result<(SharedPool, TxState, Arc<Mutex<()>>)>;
```

`main.rs` is now a 40-line arg parser that calls `prehnited::run`.
Tests can `TcpListener::bind("127.0.0.1:0")` to get a random
port, then `thread::spawn(move || serve_on(...))` to run the
listener on a background thread — no spawning a binary, no
flaky network setup.

### Wire-level integration tests

Four tests in `crates/prehnited/tests/concurrent_writers.rs`:

- **`two_clients_can_have_transactions_open_simultaneously_over_tcp`**.
  Two TCP connections each run `BEGIN`, then `INSERT` a row,
  then a `SELECT` that confirms own-write visibility, then
  `COMMIT`. A third connection sees both rows. Without per-
  statement locking, B's `BEGIN` would block on A's writer
  mutex (held across A's open transaction); with it, both
  flow through.

- **`wire_level_write_write_conflict_aborts_the_loser`**. Two
  connections both `UPDATE` the same row. The second to take
  the writer lock sees A's in-flight tombstone (via
  `tx_max in TxState.in_flight`), `collect_candidates`
  returns `Error::Conflict`, the server frames it as an
  `Error` response, and the client receives the `"conflict:
  ..."` string.

- **`rolled_back_transaction_over_tcp_leaves_no_visible_rows`**.
  A `BEGIN; INSERT 3 rows; ROLLBACK` over one connection; a
  fresh connection sees zero rows. Confirms the deferred-
  transaction rollback path works through the wire — the
  three rows are physically on disk but the clog's `rolled-back`
  record hides them.

- **`parallel_inserts_from_many_clients_dont_corrupt_pages`**.
  Four real client threads spawned via `thread::spawn`, each
  opening its own TCP connection and running `BEGIN;
  INSERT × 200; COMMIT`. Total 800 inserts spread across
  fresh pages. After all threads join, a fifth connection
  reads back the table: the row count must be exactly 800
  and every `(writer, n)` pair must appear exactly once.
  This is the stress test for `reload_for_write` — without
  it, parallel allocators would step on each other and we'd
  see fewer rows than inserted (the test would fail with a
  page-allocation race).

### What doesn't change

The on-disk format is unchanged (`PREHNDB6`); a v0.26 file opens
cleanly under v0.27. The wire protocol is unchanged. The
`prehnitedb` library API is unchanged except for two new methods
(`Pager::reload_meta_from_disk`, `Database::reload_for_write`)
that callers using a single `Database` will never need.

### What v0.27 leaves to a future session

- **Finer-grained physical locking.** v0.27 still has one
  writer mutating the file at any instant — the per-statement
  lock serialises physical writes. A multi-writer pager (page-
  level latching, MVCC at the page level, or partitioned
  storage so different writers touch different pages without
  coordination) is a separate, big piece of work.
- **Predicate (range) conflict detection** for serialisable
  isolation. v0.26's FUW is row-level; SSI on top is the
  natural next step.
- **Background VACUUM**, driven by an oldest-active-TX
  watermark. Tombstones and rolled-back rows currently wait
  for an explicit `VACUUM`.
- **Clog truncation.** The clog grows unboundedly. Once every
  TX below a watermark is irrelevant (no live snapshot can
  refer to it), the clog's prefix can be compacted into a
  single "everything below N is committed" sentinel.

## Session 28 — Per-table physical concurrency (v0.28)

v0.27 carried v0.26's concurrent transactions to the wire, but the
server still serialised every write statement on one shared
`write_lock`. Concurrent *transactions* were a property of the
engine and the deferred-transaction model; concurrent *execution*
of write statements was not. v0.28 fixes that. Two TCP clients
running `INSERT INTO different_table` now proceed through B-tree
traversal, page mutation, and commit truly in parallel — they
contend only on the brief catalog-page write at the end of each
statement.

The work was four pieces, in dependency order: per-pager dirty
tracking, shared meta, per-pager WAL files, and the per-table
mutex map. Each fixed a specific race the previous server design
hid behind its one big lock.

### Per-pager dirty tracking

v0.27's `Pager` relied on the shared `BufferPool`'s per-frame
dirty bit to know what to flush at commit. With one writer at a
time that's fine — every dirty page belongs to *that* writer.
The moment two writers can dirty pages concurrently, a global
dirty bit lies. A's commit would scan the pool, find both A's and
B's dirty pages, write *both* to A's WAL, and mark them all
clean. B's commit would then find nothing to flush and wrongly
believe its work was durable; or worse, B's writes would have
been silently committed inside A's transaction, with no MVCC
indication that they belonged to B.

The fix: each `Pager` keeps its own `dirty_pages: HashSet<u32>`.
`write_page` inserts; `commit` walks this set (not the pool's
global state) to flush only this pager's pages; `rollback` calls
the new `SharedPool::drop_pages(&self.dirty_pages)` to evict
only its own in-flight pages, leaving a peer's dirty frames
alone. The pool keeps its per-frame dirty bit as a hint for
eviction-time spill decisions, but commit/rollback no longer
consult it.

The old `SharedPool::for_each_dirty`, `mark_all_clean`, and
`drop_dirty` methods went away; in their place is
`mark_clean(&HashSet<u32>)` and `drop_pages(&HashSet<u32>)`,
which operate on a specific pager's set.

### `SharedMeta` for coordinated allocation

v0.27 worked around the meta-coherence problem (each pager had
its own `Meta` from page 0, stale relative to peer commits) by
calling `Pager::reload_meta_from_disk` after acquiring the
writer lock. With per-table locks, every statement on every
table would need that refresh under its own lock — and the
refresh would race with concurrent allocators on other tables.

v0.28 stops working around the problem: meta is now genuinely
shared. `SharedMeta` wraps a `Mutex<Meta>` (plus a counter for
WAL IDs, see below) and every read and write of `page_count`,
`freelist_head`, `catalog_root`, and `next_tx_id` goes through
it. `Pager::alloc_page` holds the lock for the whole
allocation — through the freelist-head read-back if any —
because *two pagers can't be reading the same freelist head and
both advancing it*. The bump-allocation path is one increment
under the lock; the freelist path is one `read_page` under the
lock (the read goes through the pool, which has its own shard
mutexes — different lock, no contention with shared meta).

Rollback no longer reverts the shared meta. A peer writer may
have allocated past us in the interim, and rewinding `page_count`
would risk handing them our (still-bumped) numbers on their next
allocation. Instead, the rolling-back pager stashes its
`allocated_pages` set into a per-pager `pending_freelist`, where
its next allocation reuses them before going back to the shared
meta. Pages that escape that reuse — the connection drops, or
the rolled-back pages get past the per-pager freelist's typical
horizon — are reclaimed by `VACUUM`. The tradeoff is honest: a
small space leak on rollback, in exchange for never stomping a
peer's allocation.

A subtle detail: a peer's allocation may have bumped
`page_count` to N without yet having committed the pages
themselves. The file is therefore shorter than the meta
advertises. We could extend the file at allocation time (one
`set_len` per `alloc_page`, slow, and a rollback would leave
the file extended anyway), but instead `read_page` tolerates
short reads at the file's tail: the buffer is zero-filled before
the read, and a short read just leaves the rest as zeros. In
practice nothing references such a "phantom" page until the
allocator's own commit extends the file, so no one ever sees the
zeros.

The persisted on-disk format is unchanged from v0.27 — meta
still occupies its v0.27 layout on page 0, with the same magic
`PREHNDB6`. The "shared" in `SharedMeta` is purely a runtime
concept.

### Per-pager WAL files

Each `Pager` has its own `Wal` struct with its own `File`
handle, but in v0.27 they all opened the same `<db>-wal` path.
The `Wal` struct tracks its `cursor` (the file offset where the
next record lands) locally — so two pagers writing to the same
path through different `File` handles would each seek to *their
own* cursor and write, colliding on file offsets and corrupting
each other's records.

v0.28 mints a unique WAL path per pager: `<db>-wal-<id>`, where
`id` is a counter on `SharedMeta`. The first pager opened on a
file gets `id=0`; peer pagers via `open_shared_with_meta` get
`1`, `2`, `3`. Each pager's commit is then a sealed apply of
its own WAL into the shared database file. The applies write
to different page offsets (per-table mutex guarantees this for
non-catalog pages; the catalog page is serialised by the
catalog's internal write lock), so the OS-level concurrent
writes are safe.

Crash recovery: on first open after a process death, scan the
directory beside `<db>` for any `<db-stem>-wal-<digits>` files
and recover each in turn. Each WAL holds at most one committed
transaction (any unsealed log is discarded). The legacy
single-WAL path (`<db>-wal`) is also recovered for backwards
compatibility with v0.27 files.

Clean shutdown: `Pager::drop` resets and removes its own WAL
file, so a normal session leaves no WAL behind. A panicked
session leaves files behind for recovery on next open.

### Per-table mutexes in `TxState`

With the engine made safe for concurrent writers, the server
finally drops its global `write_lock`. `TxState` now carries:

```rust
table_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
catalog_lock: Arc<Mutex<()>>,
commit_lock: Arc<Mutex<()>>,
```

Per-table mutexes are minted on first lookup via
`tx_state.table_lock(&name)`. A writer statement parsed as
`INSERT INTO foo` / `UPDATE foo` / `DELETE foo` /
`CREATE INDEX ON foo` takes `table_lock("foo")` for its
duration. Two such statements on `foo` vs `bar` proceed
concurrently. CREATE TABLE, DROP TABLE, VACUUM, and DROP INDEX
fall under `WriteScope::Catalog`; they do *not* take an outer
mutex (would deadlock with the engine-internal lock — see
below) and rely on the engine to serialise the catalog mutation
itself.

A new `lib.rs` function `prehnitedb::write_scope(sql)`
classifies the SQL into one of `Table(String)`, `Catalog`,
`None` (BEGIN/COMMIT/ROLLBACK), or `Unknown` (parse error). The
server's `run_write` dispatches on this.

### The catalog write lock

The remaining race was inside `Catalog::put` itself. Every
`INSERT` / `UPDATE` / `DELETE` calls it to update the target
schema's `next_rowid` / `row_count`. Even when two writers hold
*different* per-table mutexes, their `catalog.put` calls touch
the *same* catalog leaf page when both schemas happen to live
there (which they almost always do for small databases). The
read-modify-write cycle on a shared page is the classic lost-
update bug: both read T0's catalog, both modify in memory, the
second write overwrites the first.

The fix lives inside `Catalog`: a private `write_lock:
Arc<Mutex<()>>`, taken inside `put` and `remove`, released
immediately after the tree write. Brief, only blocks during the
catalog page write itself, not the rest of the INSERT. The
lock comes from `TxState::catalog_lock`, threaded through
`Catalog::open_with_lock`.

The first version of this work also took `catalog_lock` in the
server for `WriteScope::Catalog` statements, intending it as a
"big" catalog-mutation gate. That self-deadlocked: a CREATE
TABLE would take `catalog_lock` in the server, then call
`Catalog::put` inside `db.execute`, which would try to take the
same (non-reentrant) `Mutex` again. The right answer is the
engine alone owns this lock — `WriteScope::Catalog` statements
take no outer mutex.

### Tests

Two new wire-level integration tests in
`crates/prehnited/tests/concurrent_writers.rs`:

- **`writes_to_different_tables_run_in_parallel`** — 4 client
  threads, each on its own table, each running
  `BEGIN; INSERT × 100; COMMIT`. All 400 rows must land,
  distinct, with no losses. The earlier failing version of this
  test caught the catalog race exactly.

- **`one_writers_open_transaction_does_not_block_another_tables_writer`**
  — the defining v0.28 property in isolation. Writer A opens a
  transaction on table `a`, does INSERTs, and holds. Writer B,
  on its own connection, must complete an INSERT on table `b`
  without waiting on A. In v0.27 the server's
  `Mutex<Database>`-across-BEGIN..COMMIT model would have B
  block on A; in v0.28 B takes a different per-table mutex and
  flies through.

The earlier 4 wire tests from v0.27 still pass:
`two_clients_can_have_transactions_open_simultaneously_over_tcp`,
`wire_level_write_write_conflict_aborts_the_loser`,
`rolled_back_transaction_over_tcp_leaves_no_visible_rows`, and
`parallel_inserts_from_many_clients_dont_corrupt_pages` (which
stress-tests same-table parallel writers, now under per-table
mutex serialisation rather than global). And two pager unit
tests that pinned the v0.27 rollback semantics
(`rollback_discards_writes_and_allocations` →
`rollback_recycles_allocated_pages_for_reuse`, plus the spilled
variant) were rewritten to reflect v0.28's shared-meta-non-revert
semantics.

### What v0.28 leaves to a future session

- **Same-table parallel writes.** The per-table mutex
  serialises two writers on the same table. Going finer requires
  B+tree latch crabbing (lock current node, lock child, release
  parent), so two inserts targeting different leaves of the same
  tree run concurrently. Real work, well-documented in textbooks.
- **VACUUM concurrent with writers.** VACUUM's `replace_with`
  rewrites the whole file; v0.28 keeps the "single-writer
  VACUUM" invariant from earlier versions. Making it safe under
  concurrent writers needs either an exclusive global lock (take
  every per-table + catalog mutex), or background-VACUUM
  semantics that don't rewrite the whole file at once.
- **DROP TABLE concurrent with writers on that table.** Catalog
  drops don't currently take per-table locks; an INSERT racing
  with the matching DROP is undefined.
- **Predicate (range) conflict detection** for serialisable
  isolation, on top of v0.26's row-level FUW.

The on-disk format is unchanged from v0.27 (`PREHNDB6`); the
WAL naming changes from `<db>-wal` to `<db>-wal-<id>`, with
the legacy path recovered on first open so a v0.27 database
opens cleanly under v0.28. The wire protocol is unchanged.

## Session 29 — Serialisable Snapshot Isolation (v0.29)

Snapshot isolation has a famous gap. Two transactions can each
observe the same invariant, each take an action that's safe under
their snapshot, each write a row the other one doesn't, and both
commit cleanly under v0.26's first-updater-wins (since FUW is
row-level and they wrote different rows). The invariant breaks.
This is **write-skew**, and v0.29 closes it with **Serialisable
Snapshot Isolation** — the Cahill algorithm Postgres adopted for
its `SERIALIZABLE` isolation level.

The algorithm has a satisfying shape: track rw-dependencies
between in-flight transactions, and at commit time abort any
transaction that sits at the pivot of a "dangerous structure" —
a two-step rw-cycle that means the precedence graph isn't
serialisable.

### The substrate: a transaction-wide snapshot

v0.25–v0.28 captured a fresh snapshot per statement. Read-stable
across one statement, possibly different across two — closer to
`READ COMMITTED` than `REPEATABLE READ`. SSI requires a snapshot
that lasts the whole transaction; otherwise the read-set we'd
track isn't a coherent observation of a single point-in-time.

v0.29 captures the snapshot at `BEGIN` and pins it in
`Database.transaction_snapshot: Option<Snapshot>`. Every
statement inside the transaction reads against it
(`snapshot_for_statement` clones the pinned snapshot and patches
in `own_tx` for own-write visibility). Auto-commit statements
still capture per-statement snapshots, since each is its own
transaction.

`BEGIN` now also reserves the TX ID immediately (previously
lazy, at first write), so SSI's read-set has somewhere to land
even for `SELECT` statements before the first write. A read-only
`BEGIN..COMMIT` therefore now writes one clog `committed`
record at commit — the only durable cost of the reservation.

### Tracking the read-set

A new `SsiTxState` (in `engine/transaction.rs`) holds, per
in-flight TX:

```rust
struct SsiTxState {
    read_set: HashSet<(u32, Vec<u8>)>,   // (table_root, rowid_key)
    out_conflict: bool,                  // we read what a peer wrote
    in_conflict: bool,                   // a peer read what we wrote
}
```

`TxState` carries an `Arc<Mutex<HashMap<u64, SsiTxState>>>`,
keyed by TX ID, created in `begin_write` and removed in
`commit_write`/`rollback_write`. The `Snapshot` itself carries an
`Arc` clone of this map (alongside its existing clog handle), so
the executor can mutate read-sets and check edges without
threading a `TxState` reference all the way down.

`Snapshot::record_read(table_root, rowid_key, tombstone_by)` is
called from every scan path that emits a row:

- `TableScan::next` — full-table scans.
- `IndexScan::next` — bounded index scans, where the index entry
  resolves to a row in the table.
- The `admit` closure inside `collect_candidates` —
  `UPDATE`/`DELETE`'s scan over candidate rows.

The `tombstone_by` argument is the row's `tx_max` (or `None` if
zero). If `tombstone_by` names an in-flight peer writer, that's
an rw-edge `reader → peer` and we mark `reader.out_conflict =
true; peer.in_conflict = true` while we have the read-set lock.

### Tracking the write-set, indirectly

We don't track writes in a separate structure — we walk the
read-sets when writes happen. `Snapshot::record_write(table_root,
rowid_key)` is called from `update` and `delete` after the
`WHERE` filter has decided we'll actually write the row:

```rust
let key = (table_root, rowid_key.to_vec());
let readers: Vec<u64> = ssi.iter()
    .filter(|(&t, _)| t != writer_tx)
    .filter(|(_, s)| s.read_set.contains(&key))
    .map(|(&t, _)| t).collect();
if !readers.is_empty() {
    writer.in_conflict = true;
    for peer in readers { peer.out_conflict = true; }
}
```

This is the rw-edge in the other direction: writer wrote what
peer read.

### A subtle gotcha: FUW belongs after the WHERE clause

Wiring SSI surfaced an existing v0.26 design bug. The first-
updater-wins check inside `collect_candidates` would fire on any
row whose `tx_max` named an in-flight peer — *including rows the
`WHERE` clause would have discarded*. So two transactions
updating *disjoint* rows in the same table could spuriously
conflict if their scans happened to visit the same in-flight
tombstones.

v0.29 moves the FUW check out of `collect_candidates` into the
`update` and `delete` loops, after `passes_filter`. The new
`check_write_write_conflict` helper inspects `record.tx_max`
only for rows we actually intend to write. The integration tests
that previously masked this — they used disjoint UPDATEs but
on a table small enough that both rows share a leaf — exposed
it as soon as SSI's read tracking was wired in: the
non-conflicting test failed with `Conflict` from the misplaced
FUW check, not with `Serialization` as expected.

### Commit-time abort

`commit_transaction` in `Database` now calls
`tx_state.ssi_check_commit(tx_id)` before `commit_write`. The
check is the dangerous-structure test:

```rust
if state.in_conflict && state.out_conflict {
    return Err(Error::serialization(format!(
        "transaction {tx} would close a dangerous rw-dependency cycle"
    )));
}
```

The new `Error::Serialization` variant (display: `"serialization
failure: ..."`) is returned. The application is expected to retry
the transaction on a fresh snapshot.

The same check is folded into the autocommit success paths
(`run_plan`, `run_plan_streaming`), though in practice an
autocommit single-statement transaction can almost never close a
cycle on its own — autocommit writes still flow through the
machinery for uniformity.

### The honest limitations

Tuple-level SSI is **pessimistic**. If transaction A's
`SELECT n FROM t WHERE id = 1` runs as a full scan (no index on
id), it observes all rows of t, not just `id = 1`. A peer's
`UPDATE t SET n = 99 WHERE id = 2` then triggers a write-rw-edge
against A's read-set, even though the two transactions are
logically disjoint. Postgres mitigates this with `SIREAD` locks
at multiple granularities — page-level, relation-level,
sometimes coarser — so the lock's "range" matches the
transaction's actual predicate. PrehniteDB v0.29 doesn't have
this; tests reflect the limit (the cross-table SSI test uses
genuinely separate tables, not separate predicates on one
table).

Tuple-level SSI is **incomplete against phantoms**. A
transaction whose `SELECT * FROM t` is followed by a peer's
`INSERT INTO t` doesn't catch the phantom — our read-set has the
rows that existed at scan time, not the predicate "all rows of
t". A predicate-lock-aware SSI catches this; tuple-level cannot.

The cycle detection is the **simple commit-time check**: if our
flags are both set at commit, abort. An n-cycle of symmetric
writers (n ≥ 2) can have multiple transactions hit
`in_conflict && out_conflict` and all abort. The classic
write-skew test acknowledges this: it asserts "at least one
aborted", not "exactly one aborted". Postgres pre-aborts more
selectively.

Per-TX read-set memory is **unbounded**. A long-running write
transaction accumulates every observed `(table, rowid)` pair.
Postgres caps via lock escalation; PrehniteDB v0.29 does not.

### Tests

Three integration tests in `crates/prehnitedb/tests/integration.rs`:

- **`ssi_detects_classic_write_skew`** — the canonical anomaly:
  two accounts starting at 100, invariant `sum >= 0`, both
  transactions read both, both decrement 150 from "their"
  account. Asserts at least one aborts with `serialization` and
  the final state preserves the invariant.

- **`ssi_does_not_abort_writes_to_separate_tables`** — two
  transactions, each on its own table, both commit cleanly. No
  shared rows in any read-set, no edges possible.

- **`ssi_transaction_snapshot_stays_stable_across_statements`**
  — two `SELECT`s inside one `BEGIN..COMMIT`, with a peer
  writer's autocommit insert in between. Both `SELECT`s must
  see the same rows — the snapshot is pinned. Confirms the
  `SERIALIZABLE`-snapshot substrate works.

The existing 190 tests all still pass — including v0.26's
`write_write_conflict_aborts_the_second_writer`, which still
catches the row-overlap case via FUW (now after-WHERE rather
than during-scan).

### What v0.29 leaves to a future session

- **Predicate locks for SSI** (the `SIREAD` model), to reduce
  over-aborting and catch phantoms. The natural shape:
  page-level locks for full scans, range locks for index scans,
  relation locks for unbounded reads.
- **Per-edge tracking** so n-cycle aborts can be minimal (abort
  one TX per cycle, not n). Postgres tracks "conflict-out" and
  "conflict-in" lists, not bare booleans.
- **Read-set memory bounding** via lock escalation: once a TX's
  read-set crosses a threshold, fold it up to coarser
  granularity (one entry per page or per relation).

The on-disk format is unchanged (`PREHNDB6`); the wire protocol
is unchanged; v0.28 databases open cleanly under v0.29. SSI is
a pure-runtime addition.

## Session 30 — B+tree latch crabbing (v0.30)

v0.28 gave PrehniteDB cross-table write parallelism by replacing
the global writer mutex with per-table mutexes. v0.30 finishes the
concurrency story for *same-table* writes. Two writers
`INSERT INTO same_table` now run truly in parallel, contending
only on the actual leaves they touch — not on the table.

### The pieces

Three things had to compose:

1. **Per-page latches** on every page, with read-coupled crabbing
   in the B+tree descent.
2. **Per-table `RwLock`** (was `Mutex`) — `INSERT`/`UPDATE`/`DELETE`
   take the shared side and rely on the page latches for safety;
   `CREATE INDEX`/`DROP INDEX` keep the exclusive side.
3. **Per-table atomic `next_rowid` counter** so concurrent inserters
   don't all read the same `schema.next_rowid` from their local
   schema copies and collide on the rowid.

### Owning a `std::sync::RwLockReadGuard`

Latch crabbing — release the parent latch *after* taking the child
latch — wants to hold guards across loop iterations and recursive
calls. `std::sync::RwLockReadGuard` is borrowed from its `RwLock`;
the borrow checker rejects guards that outlive the local that owns
the lock.

`OwnedReadLatch` (in `pager.rs`) wraps the Arc and the guard
together, relying on Rust's field-drop-in-declaration-order rule
to release the lock before dropping the Arc:

```rust
pub struct OwnedReadLatch {
    guard: RwLockReadGuard<'static, ()>,   // dropped first
    _lock: Arc<RwLock<()>>,                // dropped second
}

impl OwnedReadLatch {
    pub fn acquire(lock: Arc<RwLock<()>>) -> OwnedReadLatch {
        let guard = lock.read().expect("poisoned page latch");
        // SAFETY: the Arc lives in `_lock` for the guard's whole
        // lifetime. Field-drop order releases the lock first.
        let guard = unsafe { std::mem::transmute(guard) };
        OwnedReadLatch { guard, _lock: lock }
    }
}
```

One `unsafe` per acquire, contained, soundness argued in the doc
comment. `OwnedWriteLatch` is the symmetric write variant. The
latches sit in a `SharedPool::latch(page_no) -> Arc<RwLock<()>>`
lazy-init table keyed by page number — never shrinks (cost ~80
bytes per page, bounded by the file's page count).

### Optimistic descent

`BTree::insert` and `BTree::delete` now try an **optimistic** fast
path first. The descent uses read-coupled shared latches on internal
nodes: at each step, acquire the child latch before releasing the
parent's. At the leaf, drop the leaf's shared latch and acquire the
EX latch — *while still holding the parent's shared latch*, so the
leaf can't be freed or merged in the gap between the shared release
and the EX acquire.

After acquiring leaf EX, re-read it (it might have been modified
under the lock-upgrade gap), re-validate that the key still belongs
in this leaf (it's the rightmost or `key <= last_key`), then check
if the new insert would fit without splitting:

```rust
let footprint_sum: usize = entries.iter()
    .map(|(k, v)| page::leaf_footprint(k, v))
    .sum();
if footprint_sum > USABLE {
    return Ok(OpOutcome::Restart);
}
```

If it fits, write the new leaf and return `Done`. If not, return
`Restart` and the caller falls back to **pessimistic** descent.

### Pessimistic fallback

The pessimistic path takes an EX latch on the **root** (the
tree-wide gate that blocks every other descent — optimistic
readers, optimistic writers, anyone) and runs the existing
recursive `insert_into` / `delete_from`. Those recursive helpers
acquire an EX latch on each child as they descend; the latch lives
in the recursion frame and releases when the frame returns. The
caller-holds-the-current-latch contract is documented at the top
of each helper.

The recursion structure is what makes the borrow checker happy
here: each frame's latch lives in stack-local storage with the
frame, no Vec, no shared lifetime.

### Read paths

`search` descends with shared latches, read-coupled — acquire
child latch, release parent. `Cursor` acquires the leaf's shared
latch, copies the entries into its buffer, then releases (held
only during the copy). Walking the leaf chain via `right_link`
re-acquires per leaf.

### The rowid race

The first time I ran the 4-thread same-table-insert stress test
after wiring all of the above, it failed: 754 rows out of 800
expected. The latching was correct; the data loss came from
elsewhere. Each `INSERT` did:

```rust
let mut schema = catalog.get(...);   // local snapshot of the schema
for row in rows {
    let rowid = schema.next_rowid;
    schema.next_rowid += 1;
    tree.insert(rowid, ...);
}
catalog.put(&schema);   // persist the local next_rowid
```

Two writers each have their own *local* schema snapshot read at
statement start. Both see `next_rowid = 10`. Both assign rowid 10
to their first INSERT. The B+tree treats the second
`tree.insert(10, ...)` as an *update* of the existing key 10,
silently overwriting the first writer's row.

The fix: a shared atomic per-table `next_rowid` counter on
`TxState`, with `fetch_max` + `fetch_add` semantics so even if a
peer writer's catalog.put has advanced the persisted value past
our local schema, the counter catches up:

```rust
pub fn reserve_rowid(&self, table: &str, schema_next_rowid: u64) -> u64 {
    let counter = self.rowid_counters.entry(table).or_insert(AtomicU64::new(schema_next_rowid));
    counter.fetch_max(schema_next_rowid, Ordering::SeqCst);
    counter.fetch_add(1, Ordering::SeqCst)
}
```

The executor's INSERT/UPDATE paths now call
`snapshot.reserve_rowid(&table, schema.next_rowid)` instead of
bumping the local `schema.next_rowid`. At the end of the
statement, `schema.next_rowid = snapshot.current_next_rowid(...)`
captures the latest counter value for `catalog.put`. Concurrent
catalog.put writes may persist the counter at slightly different
moments, but the value is monotonically non-decreasing across
puts, so the persisted state never regresses.

`row_count` has the same race in principle but is already
treated as an approximation (the planner uses it as a heuristic
for join reorder, and VACUUM re-counts). v0.30 leaves it
per-writer-local.

### Per-table `RwLock` and `WriteScope::TableAccess`

`TxState::table_locks` becomes `HashMap<String, Arc<RwLock<()>>>`.
`WriteScope::Table` carries a new `TableAccess` enum:

```rust
pub enum TableAccess {
    Shared,    // INSERT/UPDATE/DELETE — page latches handle safety
    Exclusive, // CREATE INDEX — needs whole-table exclusion
}
```

`write_scope(sql)` returns `Table(name, Shared)` for the data
operations and `Table(name, Exclusive)` for `CREATE INDEX`. The
server's `run_write` takes `.read()` or `.write()` accordingly.

### Tests

The existing `parallel_inserts_from_many_clients_dont_corrupt_pages`
test (originally a v0.27 same-table stress for the writer mutex)
now genuinely tests *parallel* writes: 4 client threads × 200
INSERTs each = 800 rows, every `(writer, n)` pair distinct. With
v0.30's machinery it passes; without the rowid atomic (the bug
the first test run exposed) it loses ~46 of those rows.

Ran the test 5 times in a row after the fix — stable.

### What's still single-threaded

- **Root splits / merges** still take a tree-wide EX latch. Common
  in young trees, rare in steady state.
- **`CREATE INDEX`** still excludes table writes by taking the
  per-table `RwLock` write-side.
- **`VACUUM`** still requires no concurrent writers (the engine
  assumes single-writer when rebuilding the file).
- **`row_count`** is per-writer-local and slightly off under
  concurrent writers — a documented approximation.

The on-disk format is unchanged (`PREHNDB6`); the wire protocol is
unchanged; v0.29 databases open cleanly under v0.30. The latches
are pure runtime structure.

## Session 31 — Correlated subqueries (v0.31)

v0.19 added uncorrelated subqueries — the executor pre-evaluates a
`SELECT` once before the outer row loop and rewrites the subquery
node in place with its materialised result. Anything that
referenced an outer-query column got rejected at planning time as
"no such column", because the subquery's own `FROM` scope didn't
have it.

v0.31 fills that in. A subquery whose `WHERE` mentions a column the
subquery's own scope can't resolve is now treated as **correlated**:
detected at the same `prepare_subqueries` pass, deferred from
pre-evaluation, and resolved per outer row by substituting the
outer column references with the row's values and running the
(now uncorrelated) subquery.

### The detection shape

`prepare_subqueries` walks the outer expression tree looking for
`Exists`, `InSubquery`, and `ScalarSubquery` nodes. For each one,
v0.31 calls a new `subquery_is_correlated` helper that:

1. Builds the subquery's own `FROM` scope (base table + every join).
2. Walks the subquery's `WHERE` expression looking for any `Column`
   reference whose `scope.resolve()` returns `Err`.
3. Returns `true` on the first unresolved reference.

If the subquery is uncorrelated, the existing v0.19 path runs
unchanged — execute once, rewrite the node to a literal / `Bool` /
`InList`. If correlated, the node is rewritten to a new
executor-internal `Expr::CorrelatedExists` / `CorrelatedScalarSubquery`
/ `CorrelatedInSubquery` carrying the original `Statement`.

The detection pass intentionally **does not recurse into nested
subqueries** — each has its own scope and its own correlation
analysis. v0.31 supports single-level correlation only; the shape
extends naturally to nested correlation in a future session.

### Per-row resolution

The `Filter` and `Project` operators are the only operators that
evaluate expressions per row, so they're the ones that need to
handle correlated subqueries. Each grew three fields:

```rust
struct Filter {
    ...
    has_correlated: bool,   // cached at construction
    catalog: Catalog,       // for re-planning the substituted subquery
    snapshot: Snapshot,     // for executing it under the right view
}
```

`has_correlated` is `true` iff any `Correlated*` node lives in the
predicate; cached so the hot path (the common case of no
correlation) doesn't walk the tree on every row. When it's `false`,
`Filter::next` calls `passes_filter(&self.predicate, ...)` as
before.

When it's `true`, each row first goes through `resolve_correlated`:

```rust
fn resolve_correlated(
    expr: &Expr,
    outer_scope: &Scope,
    outer_values: &[Value],
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<Expr>
```

This walks the expression tree and returns a copy where every
`Correlated*` node has been replaced by its per-row result:

- `CorrelatedExists(stmt)` → execute the substituted statement, lift
  the `Ok(any_rows)` to `Expr::Bool(any)`.
- `CorrelatedScalarSubquery(stmt)` → same, but lift the single value
  to a literal `Expr`.
- `CorrelatedInSubquery { expr, subquery, negated }` → execute the
  substituted subquery, collect its values + `has_null`, return
  `Expr::InList { … }`.

The resolved expression is then handed to `eval` as if v0.19
pre-resolution had produced it. `eval` itself never learned about
correlated subqueries.

### Substitution

The interesting bit is `substitute_outer_refs`:

```rust
fn substitute_outer_refs(
    statement: &Statement,
    outer_scope: &Scope,
    outer_values: &[Value],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Statement> {
    let mut cloned = statement.clone();
    let inner_scope = subquery_inner_scope(&cloned, pager, catalog)?;
    substitute_in_statement(&mut cloned, &inner_scope, outer_scope, outer_values)?;
    Ok(cloned)
}
```

We deep-clone the subquery's `Statement` (one allocation per outer
row, cheap for typical subquery sizes), build the subquery's own
inner scope, then walk the cloned statement's expressions. For every
`Column` reference we try the inner scope first. If it resolves
there, leave it alone. If not, try the outer scope. If it resolves
there, replace the `Column` node with `value_to_literal(value)` —
the literal value from the outer row. If neither scope resolves it,
surface the error.

After substitution, the cloned statement is uncorrelated and can be
planned and executed by the regular subquery machinery
(`execute_exists_subquery`, `execute_scalar_subquery`,
`execute_in_subquery`).

### The bug projection caught

The first run of the tests showed `correlated_scalar_subquery_per_outer_row`
failing — a scalar correlated subquery in the **projection** position
threw `corruption: correlated scalar subquery was not resolved
before filter execution`. The fix was wiring `Project` the same way
as `Filter`: a `has_correlated` flag at construction, and per-row
`resolve_correlated` before `eval`. Same pattern, same code path.

### Tests

Four integration tests in
`crates/prehnitedb/tests/integration.rs`:

- **`correlated_scalar_subquery_per_outer_row`** — the canonical
  "join via correlated scalar" pattern (`SELECT id, (SELECT name
  FROM customers WHERE customers.id = orders.customer_id) FROM
  orders`).
- **`correlated_exists_filters_to_present_keys`** — both `EXISTS`
  and `NOT EXISTS` variants, the SQL-equivalent of a semi-join /
  anti-join.
- **`correlated_in_subquery_resolves_per_outer_row`** — the
  `IN`-subquery shape, with the subquery's `WHERE` referencing two
  outer columns.
- **`uncorrelated_subqueries_still_pre_evaluate`** — regression
  check that the v0.19 fast path keeps working.

### What's still missing

- **Nested correlation.** A subquery two levels deep that references
  the outermost query's columns isn't detected — the v0.31 pass
  doesn't recurse into nested subqueries when collecting outer refs.
- **`EXISTS → semi-join` rewrite.** Correlated `EXISTS` and `IN`
  forms with selective predicates are textbook candidates for
  rewriting into a single semi-join in the planner. v0.31 runs the
  straightforward "execute the subquery per outer row" version. For
  large outer cardinalities a semi-join would be much cheaper; the
  rewrite is a natural follow-up.
- **Substitution depth.** `substitute_in_expr` recurses through
  ordinary expression shapes but stops at nested subquery
  boundaries — a correlated grandchild needs its own substitution
  pass once nested correlation is supported.

The on-disk format is unchanged (`PREHNDB6`); the wire protocol is
unchanged; v0.30 databases open cleanly under v0.31. Three new
`Expr` variants live entirely in executor-internal territory —
parsers don't produce them and the format never sees them.

## Session 32 — Vectorised ORDER BY with external sort (v0.32)

`ORDER BY` was the last operator gating the vectorised pipeline.
Any query that ordered its output had to fall back to the
row-at-a-time `Sort`, which buffered everything in memory before
the first row could come out — fine for small results, terrible
for a sort over a million-row scan. v0.32 closes that gap with a
proper external-sort `BatchSort`: bounded memory, runs spill to
temp files, k-way merge at read time.

### The protocol

`BatchSort` lives in `crates/prehnitedb/src/engine/executor.rs`,
alongside the other `Batch*` operators, and threads through a
state machine:

```rust
enum SortState {
    Building { buffer: Vec<Vec<Value>>, runs: Vec<SpilledRun> },
    DrainingMemory(std::vec::IntoIter<Vec<Value>>),
    DrainingMerge {
        runs: Vec<SpilledRun>,
        heap: BinaryHeap<MergeEntry>,
        keys: Arc<[(usize, bool)]>,
    },
}
```

**Building phase.** On the first `next_batch`, `BatchSort::drain_input`
pulls every batch from its input, materialises each batch's rows
into `buffer`. Whenever `buffer.len()` crosses `SORT_SPILL_THRESHOLD`
(8 KiB rows), `spill_sorted_run` sorts the buffer in place under
the ORDER BY keys and writes it out as a fresh temp file. Each row
is encoded as a `u32 LE` length prefix followed by
[`codec::encode_values`] — the same tag-and-bytes format the
storage layer uses, minus the MVCC header that's irrelevant during
sort. `buffer` clears, the run is appended to `runs`, and the
input loop continues.

When input is drained:

- If `runs` is empty (everything fit), sort `buffer` once and
  transition to `DrainingMemory(buffer.into_iter())`.
- Otherwise spill the tail too (so the merge code path is uniform),
  initialise a `BinaryHeap` seeded with one entry per run, and
  transition to `DrainingMerge`.

**Draining phase.** Each `next_batch` calls `next_sorted_row`
`BATCH_SIZE` (1024) times and packs the results into a fresh
`ColumnBatch`. `next_sorted_row` either iterates the in-memory
sorted vector or pops the heap and refills from the consumed
run — classic k-way merge.

### The unsafe-free heap entry

`BinaryHeap` is a max-heap; we want a min-heap, and `std::cmp::Ord`
needs to live on the heap element itself. `MergeEntry` carries the
sort keys via `Arc<[(usize, bool)]>`:

```rust
struct MergeEntry {
    row: Vec<Value>,
    run_id: usize,
    keys: Arc<[(usize, bool)]>,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so the smallest sorted row pops first.
        let row_ord = self.cmp_rows(other).reverse();
        // Break ties by run id for deterministic output.
        row_ord.then(other.run_id.cmp(&self.run_id))
    }
}
```

The `Arc` clone per entry is cheap (one atomic increment), and the
keys vector is bounded by the ORDER BY clause's length — typically 1
to 3 entries. No `unsafe`, no lifetime gymnastics.

### Spill file format

```
[u32 LE length] [encoded values bytes] [u32 LE length] [encoded values bytes] ...
```

`SpilledRun` reads through a `BufReader<File>`. `next_row` returns
`Ok(None)` on `UnexpectedEof` so the merge naturally drains. The
struct also owns the temp file path and removes it in `Drop` —
spilled runs vanish on a clean shutdown, on a panic mid-sort, or on
process abort.

Spill paths are minted by `unique_spill_path()`:

```rust
std::env::temp_dir().join(format!(
    "prehnite-sort-{}-{n}",
    std::process::id()
))
```

`n` is an `AtomicU64` per process, so concurrent sorts (multiple
clients each running a different ORDER BY query) can't collide on
filenames.

### Wiring it in

The dispatch in `select()` previously bailed to the row pipeline
whenever `!order_by.is_empty()`. v0.32 removes that gate — now the
vectorised path is taken regardless of ORDER BY, and
`select_vectorised` inserts `BatchSort` between `BatchFilter` and
`BatchProject` when the keys are non-empty. The keys are resolved
against the **pre-projection** scope (the joined-table scope), so
they can name columns that aren't in the SELECT list — mirroring
the row pipeline's `scan/join → filter → sort → project → limit`
order.

The dispatch also gained a new fence: `query_has_correlated_subquery`
walks both the predicate and the projection items, and if any
subquery is correlated, steers back to the row pipeline. The
vectorised operators don't carry the per-row `resolve_correlated`
substitution machinery v0.31 added to `Filter` and `Project`; this
fence is necessary, and the right place is the dispatch gate
(checking after `joins_vectorisable` and before
`projection_has_aggregate`).

### What v0.32 doesn't do

- **Presorted shortcut.** The row pipeline's `Sort` is skipped when
  the access path already yields rows in the requested order
  (e.g., a leading-column index scan). The vectorised path always
  inserts `BatchSort`. Carrying the `presorted` flag through is a
  small follow-up.
- **Vectorised GROUP BY.** Aggregation still keeps the row tree.
  Adding it to the vectorised pipeline is its own session — the
  hash aggregator's per-row update needs a columnar reformulation.
- **External-merge spill on the merge side.** Our k-way merge holds
  one row per run plus the merge heap; the buffered I/O reader
  takes one batch's worth of memory per run. For very wide runs
  (thousands of files), the merge could itself spill into a
  hierarchical merge — Postgres does this. PrehniteDB's typical
  workloads won't hit this regime soon.

### Tests

Three integration tests in `tests/integration.rs`:

- **`vectorised_order_by_in_memory`** — small input, no spilling,
  both ASC and DESC variants.
- **`vectorised_order_by_multi_key`** — `ORDER BY a, b DESC`
  exercises the multi-key comparator and the per-column descending
  flag.
- **`vectorised_order_by_spills_to_disk_for_large_input`** —
  25 000 rows inserted in a deterministic shuffle (`(i * 7919) %
  N`). Forces multiple spills and the k-way merge. Asserts the
  first ten rows ascending and the last row are exactly what an
  integer sort would produce.

### Numbers

- 200 tests across the workspace, all passing
- Spill threshold: `SORT_SPILL_THRESHOLD = 8192` rows (configurable
  constant in `executor.rs`)
- The on-disk format is unchanged (`PREHNDB6`); the wire protocol
  is unchanged; v0.31 databases open cleanly under v0.32. The
  spill format is a runtime concern — never reaches a clean
  shutdown's on-disk state.

## Session 33 — Vectorised hash aggregation (v0.33)

`ORDER BY` was the last gate on the vectorised pipeline that v0.32
removed. The other one — aggregation — went today. A new
`BatchHashAggregate` operator handles `GROUP BY` and bare
aggregates inside the batched tree, so a `SELECT cat, SUM(amount)
FROM sales GROUP BY cat` no longer falls back to the row pipeline
for the per-bucket loop.

### Reusing what v0.22 built

The row pipeline already had the heavy lifting: `GroupKey`,
`AggregateRegistry`, `AggregateSlot`, `AggregateState`. v0.22's
hash aggregator owns the per-row update logic, with `Int`-typed
`COUNT`, separate `SumInt`/`SumReal` accumulators, an `AvgReal`
running sum + count, and `Extreme { best, want }` for min/max.

v0.33 reuses every one of those types. `BatchHashAggregate` is a
different driver — pulls batches instead of rows — but its hash
table is exactly the same `HashMap<GroupKey, Vec<AggregateState>>`
the row pipeline builds, and the per-row update path goes through
the same `AggregateState::update(slot, &row)`.

The state machine mirrors `BatchSort`'s:

```rust
enum AggregateOpState {
    Building { buckets: HashMap<GroupKey, Vec<AggregateState>>, order: Vec<GroupKey> },
    Draining(IntoIter<Vec<Value>>),
}
```

On the first `next_batch`, `drain_input` pulls every input batch,
calls `batch.row_at(i)` for each logical row, computes the
`GroupKey` from the resolved group columns, finds (or creates) the
bucket, and runs every slot's `update`. When the input is drained,
it finalises every bucket — `aggregates: Vec<Value> = states.into_iter().map(AggregateState::finalize).collect()`
— and builds output rows in insertion order, materialising the
projection items column by column. The state transitions to
`Draining(output_rows.into_iter())` and subsequent `next_batch`
calls pack `BATCH_SIZE` rows at a time into typed `ColumnBatch`es.

### Typing the output

Pre-typing the output batches was the tricky bit. `BatchProject`
emits `ColumnBatch` from `materialise_column` and `eval_batch`,
which each carry their own type. `BatchHashAggregate` builds output
rows from raw `Value`s, so it needs to know the per-column types
**before** the first row is pushed (`ColumnBatch::with_types`).

A new helper does it:

```rust
fn infer_grouped_output_types(items: &[SelectItem], scope: &Scope) -> Result<Vec<Type>>
```

For each projection item:

- `SelectItem::Column(colref)` → `scope.column_type(scope.resolve(colref)?)`.
- `SelectItem::Aggregate(agg)` → `infer_aggregate_type(agg, scope)`:
  - `COUNT` → `Int` (always).
  - `SUM(Int)` → `Int`, `SUM(Real)` → `Real`.
  - `AVG` → `Real` (sum is tracked in `f64`).
  - `MIN`/`MAX` → input column's type.
- `SelectItem::Expr(_)` → `Err` (the dispatch gate steers
  expression items to the row pipeline).

The helper mirrors what `AggregateState::for_slot` does at runtime
when it allocates the right state variant; same logic, run twice —
once to type the output, once to seed the state.

### Dispatch

The vectorised path used to bail to the row pipeline whenever:
- GROUP BY was present, OR
- HAVING was present, OR
- the projection had any aggregate.

v0.33's gate keeps HAVING and `Expr`-item projections on the row
tree, plus the special case of `ORDER BY` *with* aggregation (the
post-agg sort would need a synthetic post-aggregation scope this
v0.33 doesn't build). Everything else — `GROUP BY x`, bare
`COUNT(*)`, `SUM`/`MIN`/`MAX`/`AVG` on a filtered table —
vectorises.

### One bug `projection_headers` surfaced

The first test run after wiring failed all the way back at the
header pass:

```
internal error: entered unreachable code: a plain projection has no aggregates
```

`projection_headers` had assumed any `Aggregate` item routed away
from the vectorised path *before* it computed the headers. With
v0.33 routing aggregation through `select_vectorised`, that
unreachable became reachable. The fix was one line — call
`aggregate_label(agg)` for the `Aggregate` arm, mirroring what
`grouped_select` already did.

A satisfying side effect: every `aggregates_compute_over_the_table`
and `group_by_aggregates_each_group` test in the suite now exercises
the vectorised aggregation path. They didn't change; the dispatch
did.

### Tests

Four new integration tests in `tests/integration.rs`, plus 200
existing tests still green:

- **`vectorised_group_by_with_aggregate`** — the canonical
  `SELECT cat, SUM(amount), COUNT(*) FROM sales GROUP BY cat`.
- **`vectorised_count_star_no_group_by`** — single-bucket
  aggregation, the whole-table shape.
- **`vectorised_aggregate_types_inferred`** — every aggregate flavor
  in one query, asserts the output column types end up right
  (`COUNT` → `Int`, `SUM` stays in its input type, `AVG` → `Real`,
  `MIN`/`MAX` → input type).
- **`vectorised_aggregation_with_filter`** — `WHERE` upstream of
  `BatchHashAggregate`, exercising the typical `BatchFilter →
  BatchHashAggregate` chain.

### What v0.33 leaves to a future session

- **`HAVING`**. Falls back to the row tree. Would need a per-group
  predicate evaluation pass between bucket finalisation and output
  row construction.
- **`Expr` projection items**. Same. Would need a small
  post-aggregation expression evaluator with access to the
  aggregate registry.
- **`ORDER BY` with aggregation**. Would need a post-agg synthetic
  scope (`output_types` + names) so `BatchSort` could resolve
  order keys against output positions.

None of these are deep; each is a small extension. v0.33 ships the
core vectorised aggregation and the type-inference plumbing.

The on-disk format is unchanged (`PREHNDB6`); the wire protocol is
unchanged; v0.32 databases open cleanly under v0.33.

## Session 34 — EXISTS → semi-join rewrite (v0.34)

v0.31 added correlated subqueries, but kept the "obvious"
implementation: re-plan and re-execute the subquery once per
outer row. For `SELECT name FROM customers WHERE EXISTS (SELECT
1 FROM orders WHERE orders.customer_id = customers.id)`, that
means scanning the `orders` table once *per customer*. The
algorithm is correct, but for large outer cardinalities it pays
quadratic time when a single linear join would do.

v0.34 fixes this for the EXISTS / NOT EXISTS shape with a
planner-level rewrite. The query above becomes, in essence,
`SELECT name FROM customers SEMI JOIN orders ON
orders.customer_id = customers.id`. The inner table is scanned
once, the existing `NestedLoopJoin` buffers it, and each
customer is matched against the buffered set — back to linear.

### The two new JoinKinds

```rust
pub enum JoinKind {
    Inner, Left, Cross,
    /// **Semi-join** — each left row at most once, when *some* right row
    /// satisfies the `ON` predicate. Output is left columns only — no
    /// right columns, no `NULL`-padding. Executor-internal: the parser
    /// never emits this; the planner mints it when rewriting a
    /// correlated `EXISTS` subquery into a join.
    Semi,
    /// **Anti-join** — each left row once, when *no* right row satisfies
    /// the `ON` predicate. Planner-only, for `NOT EXISTS` rewrites.
    Anti,
}
```

The parser doesn't recognise `SEMI JOIN` or `ANTI JOIN` syntax (no
SQL standard does). These are executor-internal kinds the planner
synthesises.

### The rewrite

In `planner::plan` for `Statement::Select`, right before the
cost-based reorder, a new pass walks the top-level `WHERE` clause:

```rust
fn rewrite_exists_to_semi_joins(from, filter, pager, catalog)
    -> Result<(FromClause, Option<Expr>)>
```

It flattens the filter into AND-chained conjuncts (`x AND y AND z`
→ `[x, y, z]`), then tries to extract each conjunct as a
semi/anti-join pattern:

- `Expr::Exists(subquery)` with simple shape → `Semi`.
- `Expr::Unary { op: Not, expr: Exists(subquery) }` with simple shape → `Anti`.

"Simple" means the subquery is:
- A `SELECT`.
- `FROM` a single table (no joins).
- Non-empty `WHERE` (otherwise the rewrite degenerates).
- No `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.

Matched conjuncts drop from the filter; the corresponding `Join`
appends to `from.joins`, with the subquery's `WHERE` as the join's
`ON` clause and the subquery's table as the join's table. Remaining
conjuncts stay in the filter.

The reorder pass that runs immediately after doesn't reorder
semi/anti-joins (its existing guard already bails on any non-Inner
join).

### Inside NestedLoopJoin

The operator now has four code paths per match decision:

```rust
if keep {
    self.matched_current = true;
    match self.kind {
        JoinKind::Semi => {
            semi_emit = Some(left.clone());  // emit left only, drop this left
            break;
        }
        JoinKind::Anti => {
            self.right_pos = right_rows.len();  // skip rest of right
            break;
        }
        JoinKind::Inner | JoinKind::Left | JoinKind::Cross => {
            return Ok(Some(combined));
        }
    }
}
```

After the inner loop exhausts:

```rust
match self.kind {
    JoinKind::Left if !self.matched_current => /* NULL-pad and emit */,
    JoinKind::Anti if !self.matched_current => /* emit left */,
    _ => /* advance to next left */,
}
```

Semi-emit stashes the row in a local `Option<Vec<Value>>` because
we want to clear `current_left` before returning — borrow-checker
considerations.

### Scope after a semi-join

The trickiest correctness bit: a semi/anti-join's output is left
columns only, but its `ON` predicate needs the *combined* scope
to evaluate `outer.id = inner.ref`. v0.34 captures two copies of
the pre-join scope in `build_from`: one (`left_scope`) is consumed
by the join branches as their "left scope" field; the other
(`left_scope_for_reset`) is used to **revert the outer-loop scope
variable** after a semi/anti-join, so subsequent operators and
joins see only left columns. The join's own `scope` field still
holds the combined scope for its `ON` evaluation.

### Routing

`joins_vectorisable` returns `false` whenever any join is Semi/Anti
— the batched operators (`BatchHashJoin`, `BatchNestedLoopJoin`)
don't yet teach the new emit rules. Queries that pick up a
semi/anti-join via the rewrite route to the row pipeline. The
buffered nested-loop in the row pipeline is still vastly cheaper
than per-row plan-and-execute; specialised semi-hash and
semi-index-nested-loop joins are future work.

Inside `build_from`'s row-pipeline branch, the index-nested-loop
and grace-hash selectors also skip Semi/Anti (`semi_or_anti` flag),
sending them straight to `NestedLoopJoin`.

### What doesn't qualify (and stays per-row)

- `EXISTS (SELECT customer_id FROM orders GROUP BY customer_id WHERE
  orders.customer_id = customers.id)` — the inner subquery has
  `GROUP BY`, so the rewrite skips.
- `EXISTS (SELECT 1 FROM o1 JOIN o2 ON ... WHERE ...)` — the
  subquery has joins.
- `EXISTS (SELECT 1 FROM orders ORDER BY id LIMIT 1 WHERE ...)` —
  the subquery has paging.
- The classic `IN (correlated subquery)` — v0.34 deliberately
  handles only EXISTS/NOT EXISTS. `IN` would need the same shape
  plus an outer-column equality with the subquery's projection
  column. Natural follow-up.

In every "doesn't qualify" case, the v0.31 per-row evaluation runs
unchanged — same correctness, slower throughput.

### Tests

- **`exists_rewrites_to_semi_join`** — canonical EXISTS shape;
  customers with at least one order. Asserts the rewrite produces
  the same answers as the v0.31 path.
- **`not_exists_rewrites_to_anti_join`** — mirror; customers
  *without* an order.
- **`semi_join_preserves_left_columns_only`** — the rewrite
  doesn't leak right-table columns into the outer scope.
- **`complex_correlated_subquery_falls_back_to_per_row`** — a
  GROUP BY in the subquery disqualifies the rewrite, and v0.31's
  per-row path still produces the right answer.

The v0.31 correlated tests still pass without modification —
they're now exercising the v0.34 rewrite path, demonstrating that
the rewrite is semantics-preserving.

### Numbers

- 208 tests across the workspace, all passing
- Touched: `ast.rs` (+JoinKind variants), `planner.rs` (+rewrite
  pass), `executor.rs` (NestedLoopJoin handles Semi/Anti, gating
  in build_from and joins_vectorisable), `integration.rs` (4 new
  tests)
- The on-disk format is unchanged (`PREHNDB6`); the wire protocol
  is unchanged; v0.33 databases open cleanly under v0.34.

## Session 35 — Predicate locks for SSI (v0.35)

v0.29 added Serialisable Snapshot Isolation with tuple-level read
tracking. Every emitted row went into the transaction's
`HashSet<(table_root, rowid_key)>` read set; every write checked
peers' read sets for matching tuples. That caught **write-skew**
on rows-that-existed-when-you-read, but it missed two things:

- **Phantom inserts.** A transaction's `SELECT *` records every
  visible row, but a peer's `INSERT INTO t` creates a row that
  was never in any read set. The rw-edge from the reader to the
  inserter is never marked; the cycle isn't detected; SSI
  silently lets through a non-serialisable schedule.
- **Memory unbounded.** A full table scan of a 10 M-row table
  records 10 M tuple locks. Long-running transactions accumulate
  proportionally to what they observe.

v0.35 fixes both with a single refinement: a full table scan
takes a **relation-level read lock** — one entry per table, not
per row — and `INSERT` calls a new `record_insert` that walks
peers' relation locks to mark phantom edges.

### `ReadLock` is now an enum

```rust
pub(crate) enum ReadLock {
    /// Specific tuple — table B+tree root + rowid bytes. Index scans.
    Tuple(u32, Vec<u8>),
    /// Whole relation — table B+tree root. Full table scans.
    Relation(u32),
}
```

`SsiTxState.read_set` is now `HashSet<ReadLock>`. Existing
`record_read` continues to add `ReadLock::Tuple` (index scans
benefit from the precision); a new `record_relation_read`
adds `ReadLock::Relation` (full scans pay for one entry total).

### `record_write` checks both granularities

```rust
let tuple_lock = ReadLock::Tuple(table_root, rowid_key.to_vec());
let relation_lock = ReadLock::Relation(table_root);
let readers: Vec<u64> = ssi.iter()
    .filter(|(&t, _)| t != writer_tx)
    .filter(|(_, s)| {
        s.read_set.contains(&tuple_lock) || s.read_set.contains(&relation_lock)
    })
    .map(|(&t, _)| t)
    .collect();
```

An `UPDATE` or `DELETE` of row `R` in table `T` now marks an
edge from any peer holding *either* `Tuple(T, R)` *or*
`Relation(T)`.

### `record_insert` — the phantom catcher

The new row's rowid was minted by `INSERT`. No peer's read set
can name it as `Tuple` (the row didn't exist when the peer
read). The relation lock is what catches it:

```rust
pub fn record_insert(&self, table_root: u32) {
    let Some(writer_tx) = self.own_tx else { return; };
    let mut ssi = self.ssi.lock().expect("poisoned ssi");
    let relation_lock = ReadLock::Relation(table_root);
    let readers: Vec<u64> = ssi.iter()
        .filter(|(&t, _)| t != writer_tx)
        .filter(|(_, s)| s.read_set.contains(&relation_lock))
        .map(|(&t, _)| t)
        .collect();
    if readers.is_empty() { return; }
    if let Some(s) = ssi.get_mut(&writer_tx) { s.in_conflict = true; }
    for peer in readers {
        if let Some(s) = ssi.get_mut(&peer) { s.out_conflict = true; }
    }
}
```

The `insert()` executor function calls this per row inserted —
phantoms become first-class rw-edges in the SSI graph.

### TableScan / BatchScan / collect_candidates

Three scan paths needed wiring:

- **`TableScan` (row pipeline)** — at first `.next()`, calls
  `record_relation_read(table_root)` once; the per-tuple
  `record_read` calls are gone (the relation lock dominates).
- **`BatchScan` (vectorised pipeline)** — same one-shot
  relation lock when `table_for_index` is `None` (full scan);
  for the index-scan branch, per-tuple `record_read` is kept
  (and was missing entirely before v0.35 — the vectorised
  pipeline used to skip SSI tracking, which is how
  `ssi_detects_classic_write_skew` started failing under v0.32's
  ORDER-BY-routes-to-vectorised change before this session
  noticed).
- **`collect_candidates` (UPDATE/DELETE)** — full-scan path
  records a relation lock; index-scan path keeps per-tuple
  `record_read`.

A `relation_read_recorded: bool` on each scan operator makes
the call idempotent — recorded once per scan, regardless of
how many `next_batch`/`next` calls happen.

### Picking the right granularity

Why doesn't an *index* scan also escalate? Because it's already
precise: an index range scan visits exactly the rows the
predicate covers, and the per-tuple read set is naturally
bounded by the range. Escalating to a relation lock would
over-pessimise — an index probe into `id = 5` would falsely
conflict with an insert into `id = 1000`.

But this leaves a gap: a peer's INSERT into a range that was
index-scanned isn't caught as a phantom (the row didn't exist;
no tuple match). Catching it would need **page-level locks**:
the index pages the scan touched, plus a check at insert time
that the new index entry's page is in some peer's read set.
v0.35 documents this gap; closing it is natural follow-up.

### One bug v0.35 surfaced

The vectorised `BatchScan` had never been wired to SSI in v0.29
— that was a row-pipeline-only thing. v0.32 routed
ORDER-BY-bearing queries through the vectorised path, which
silently broke `ssi_detects_classic_write_skew` for queries
that hit the vectorised dispatch. The breakage didn't appear
in v0.32's test suite because the test happened to use the
row path before then.

Wiring `BatchScan` to SSI in this session fixed both v0.35's
phantom test *and* the v0.29 write-skew test — they passed
under v0.29 (row path), broke silently under v0.32 (vectorised
path with no SSI), and pass again under v0.35 (vectorised
path with SSI wired).

### Tests

- **`ssi_detects_phantom_insert`** — the canonical phantom: T1
  reads accounts, writes summary; T2 reads summary, writes
  accounts. The reads take relation locks; each insert crosses
  the peer's relation lock; both rw-edges form; both flags
  set on both TXs; at least one aborts with `Serialization`.
- **`ssi_relation_lock_keeps_disjoint_table_writers_independent`**
  — sanity check: two transactions on different tables
  shouldn't form edges. They don't. Both commit.
- The existing `ssi_detects_classic_write_skew` still passes,
  now via relation locks rather than tuple matches.

### Numbers

- 210 tests across the workspace, all passing
- Touched: `transaction.rs` (+`ReadLock` enum, +`record_relation_read`,
  +`record_insert`; `record_read`/`record_write` updated for the
  new variant), `executor.rs` (TableScan/BatchScan/collect_candidates
  all wired to `record_relation_read`, INSERT calls
  `record_insert`), `integration.rs` (2 new tests).
- The on-disk format is unchanged (`PREHNDB6`); the wire protocol
  is unchanged; v0.34 databases open cleanly under v0.35. The
  predicate locks are runtime-only.

## Session 36 — Background VACUUM (v0.36)

PrehniteDB's MVCC creates garbage as a matter of course. Every
`DELETE` leaves a tombstone (`tx_max != 0`); every `UPDATE` is a
delete-plus-insert pair, so it leaves a tombstoned old version
*and* a fresh new one; every `ROLLBACK` of an explicit
transaction leaves whatever physical writes the transaction made
on disk, stamped with a TX ID the clog later records as
rolled-back. Visibility hides all of this from readers; the
on-demand `VACUUM` is the only way to reclaim the space, and
it's expensive (full file rebuild) and unsafe under concurrent
writers (it assumes nothing's in flight).

v0.36 adds **incremental, in-place, concurrent-safe reclamation**
as a continuously-running background thread. The space doesn't
shrink (we don't rebuild the file), but dead rows stop
accumulating.

### The watermark

The whole algorithm rests on one observation:

```rust
pub fn oldest_active_tx_id(&self) -> u64 {
    let inner = self.inner.lock().expect("poisoned tx state");
    inner.in_flight.iter().min().copied().unwrap_or(inner.next_tx_id)
}
```

The smallest TX ID still in flight is the smallest snapshot any
reader currently holds. Every active snapshot's `next_tx` is
`>= oldest_active`. So:

- A row with `tx_max < oldest_active` and `tx_max` committed
  in the clog: every active snapshot sees the delete as
  already committed, so the row is dead to everyone — safe to
  physically delete.
- A row with `tx_min < oldest_active` and `tx_min` rolled back
  in the clog: every active snapshot sees the insert as
  rolled-back (i.e., never happened), so the row is dead to
  everyone — safe to physically delete.

Anything *above* the watermark might still belong to an active
transaction whose visibility says it's alive. The watermark is
the only synchronisation needed between the reclaimer and the
readers — no read locks, no read-side blocking, no signalling.

### `Database::reclaim_dead_rows`

The reclaim pass walks every table in the catalog. For each:

1. Take the table's per-table write lock (`TxState.table_lock(name).write()`).
   Foreground writers on this table block briefly; everyone else
   keeps going.
2. Scan the B+tree, decoding each row.
3. Apply the watermark test from above. Collect the rowid + row
   values of every dead row.
4. For each dead row: delete its index entries (re-encoding the
   index key from the row's values + indexed columns + rowid
   suffix) and the row itself from the table.
5. Update `schema.row_count` (rolled-back inserts were counted
   at INSERT time and need to come off; committed tombstones
   were already deducted at DELETE time).
6. `pager.commit()` to make the deletions durable.

Two-phase: collect dead first, delete after. Necessary because
the B+tree iterator can't tolerate concurrent mutation of the
tree it's walking.

Per-row index cleanup uses the same `encode_index_key(values,
columns, rowid_key)` helper the original index insert used —
it's deterministic, so we can reproduce the exact key the
row's index entries were stored under.

### The reclaimer thread

The library exposes `Database::reclaim_dead_rows()` synchronously;
the server (`prehnited`) is what makes it "background". At
startup, before `serve_on`:

```rust
fn spawn_reclaimer(db_path: Arc<str>, pool: SharedPool, tx_state: TxState) {
    thread::Builder::new()
        .name("prehnited-reclaimer".into())
        .spawn(move || {
            let mut db = Database::open_shared(&*db_path, pool, tx_state).unwrap();
            loop {
                thread::sleep(RECLAIM_INTERVAL);  // 30s
                match db.reclaim_dead_rows() {
                    Ok(0) => {}
                    Ok(n) => eprintln!("prehnited: reclaimed {n} dead row(s)"),
                    Err(e) => eprintln!("prehnited: reclaim failed: {e}"),
                }
            }
        }).unwrap();
}
```

Daemon — runs forever, no JoinHandle stored, dies with the
process. Errors are logged and the loop continues; one bad
reclamation tick doesn't kill the thread. The reclaimer opens
its own `Database` against the shared pool and tx_state, so
it shows up in `TxState` like any other writer for the per-
table-lock dance.

The library *itself* doesn't spawn the thread. Tests would have
exploded — every `TempDb` would have launched a thread, and
tests run in parallel. Library users who want background
reclamation can spawn the thread themselves; the algorithm
they call is the same `reclaim_dead_rows`.

### Tests

- **`background_reclaim_removes_committed_tombstones`** — after
  autocommit DELETEs (which leave logical tombstones), a manual
  `reclaim_dead_rows` pass returns the right count and the
  table reads back the live rows only.
- **`background_reclaim_recovers_rolled_back_inserts`** — the
  v0.26 rolled-back-insert case: 4 rows physically present
  after a ROLLBACK, invisible to scans, all 4 reclaimed by
  the next pass.
- **`background_reclaim_respects_oldest_active_watermark`** —
  the safety test: a writer has BEGIN open and has tombstoned
  a row (with the writer's in-flight TX as `tx_max`). The
  reclaimer runs concurrently. The watermark IS the writer's
  TX ID, so `tx_max < oldest_active` is false and the row is
  not reclaimed.

### What's still missing

- **Adaptive scheduling.** v0.36's interval is a fixed 30s
  constant. A real autovacuum scales the interval to write
  rate, table size, or tombstone count. Easy follow-up.
- **The on-demand `VACUUM`** still rebuilds the file (and still
  needs exclusive access). It's the only way to *shrink* the
  file — the background reclaimer just clears dead rows in
  place, leaving the freed B+tree pages on the freelist.
  Making `VACUUM` itself concurrent-safe would need page-by-
  page rebuild rather than swap, which is a much bigger
  change.
- **Clog truncation.** The same oldest-active watermark could
  drive clog truncation (everything below the watermark is
  resolved and can be folded into a single "everything below N
  is committed" sentinel). Natural follow-up.

### Numbers

- 213 tests across the workspace, all passing
- Touched: `transaction.rs` (+`oldest_active_tx_id`), `database.rs`
  (+`reclaim_dead_rows`), `prehnited/src/lib.rs` (+`spawn_reclaimer`),
  `integration.rs` (3 new tests).
- The on-disk format is unchanged (`PREHNDB6`); the wire protocol
  is unchanged; v0.35 databases open cleanly under v0.36. The
  reclaimer is pure runtime machinery.

## Session 37 — IN (correlated) → semi-join (v0.37)

v0.34 rewrote correlated `EXISTS` and `NOT EXISTS` patterns into
semi/anti-joins, eliminating v0.31's per-outer-row plan-and-
execute cost for those shapes. The natural sibling — correlated
`IN` — stayed on the per-row path. v0.37 extends the planner
rewrite to cover it.

### The shape

```sql
SELECT name FROM customers c
WHERE c.id IN (SELECT customer_id FROM orders o
               WHERE o.region = c.region)
```

becomes, after the rewrite:

```sql
SELECT name FROM customers c SEMI JOIN orders o
ON o.region = c.region AND c.id = o.customer_id
```

The subquery's `WHERE` (the correlation predicate) and the
implied equality (`outer_expr = subquery.projection`) AND together
to form the join's `ON` clause. The subquery falls out of the
filter; the inner table is scanned once per outer pass instead
of once per outer row.

### `try_extract_in_join`

Mirrors `try_extract_exists_join` from v0.34, with three extra
requirements:

1. **The subquery's projection is a single column reference.** An
   expression-shaped projection (`SELECT amount + 1 FROM ...`)
   would need that expression plumbed through the join's ON; the
   v0.37 rewrite skips and leaves it to per-row eval.
2. **The inner column is qualified.** If the subquery wrote the
   projection bare (`SELECT customer_id FROM orders`), the rewrite
   qualifies it (`orders.customer_id`) so the combined join scope
   resolves it unambiguously when the outer query has a column of
   the same name.
3. **The outer expression is a column reference.** Same
   qualification: if bare, the rewrite qualifies it with the
   outer's base-table qualifier. More complex outer expressions
   (arithmetic, calls) skip the rewrite — re-qualifying bare
   sub-references inside an arbitrary expression is more work
   than v0.37 wants, and the per-row path still produces the
   right answer.

`NOT IN` is **intentionally skipped**. SQL's three-valued
`x NOT IN (set with NULL)` is `NULL` (not `TRUE`), so an
anti-join rewrite would be wrong unless the inner projection is
provably non-nullable — which v0.37 doesn't have the type
information to decide. Postgres handles this with explicit
NOT NULL constraints + nullability inference; v0.37 doesn't.

### The bug the existing tests caught

First run of the full suite after wiring failed in
`correlated_in_subquery_resolves_per_outer_row` (a v0.31 test):

```
column reference 'amount' is ambiguous
```

The test's outer FROM is `orders o1`, the subquery's FROM is
`orders o2`. The outer expression `amount` was a bare column ref;
when lifted into the join's ON, the combined scope (o1 + o2)
saw two `amount` columns. The fix was to qualify the outer
expression with the outer base table's qualifier — `o1.amount`
in the ON — when it's a bare column ref. Otherwise the rewrite
declines and the per-row path takes over.

The fix is a small one; the satisfying part is that the v0.31
test now exercises the v0.37 rewrite path, demonstrating the
rewrite is semantics-preserving.

### Tests

- **`in_subquery_rewrites_to_semi_join`** — uncorrelated IN
  (`WHERE id IN (SELECT customer_id FROM orders WHERE amount >
  0)`) — also rewrites. Same answer as the v0.19 pre-eval path.
- **`correlated_in_subquery_rewrites_with_combined_on`** —
  correlated IN with two outer column references in the
  subquery's `WHERE` and a third in the IN's outer expression.
  The combined ON folds them all together.
- **`not_in_subquery_stays_on_per_row_path`** — `NOT IN` doesn't
  rewrite; v0.31's per-row evaluator handles it. Result is
  correct.
- **`in_subquery_with_group_by_falls_back_to_per_row`** — a
  GROUP BY in the subquery disqualifies the rewrite; the per-row
  path produces the right answer.

The 4 v0.31 correlated tests still pass without modification —
the IN ones now exercise the v0.37 rewrite, the EXISTS ones
continue on the v0.34 rewrite.

### Numbers

- 217 tests across the workspace, all passing
- Touched: `planner.rs` (+`try_extract_in_join`, renamed
  `rewrite_exists_to_semi_joins` → `rewrite_subquery_joins`),
  `integration.rs` (4 new tests).
- The on-disk format is unchanged (`PREHNDB6`); the wire protocol
  is unchanged; v0.36 databases open cleanly under v0.37.

## Session 38 — Crash recovery stress test (v0.38)

PrehniteDB has had real durability since the WAL went in: every
commit appends a sealed log, applies the log to the database
file, and `fsync`s. A crash before the marker discards the log
on next open; a crash after the marker replays it. v0.26's clog
keeps every TX's outcome durable too, with the same fsync
discipline.

That's the *claim*. v0.38 turns it into a *test*. Not a unit
test — a unit test runs in-process and can't simulate a real
process-level crash. A property-based external test: spawn a
worker process, kill it dead at a random point, restart, and
verify the engine's durability promises hold up.

### The worker

`crates/prehnitedb/src/bin/crash_worker.rs` is a tiny binary.
Opens a `Database` at `argv[1]`, creates `t (id INT, n INT)`
idempotently, then loops:

```rust
loop {
    let id = next_id;
    next_id += 1;
    let n = id * 100;
    db.execute(&format!("INSERT INTO t VALUES ({id}, {n})"))?;
    // DB acked — log the id and fsync.
    writeln!(log, "{id}")?;
    log.sync_all()?;
}
```

The log is append-only, one decimal id per line, `fsync`ed
after each ACK. Run forever; the test kills it externally.

There's a deliberate gap: the DB ack happens **before** the log
fsync. If the kill lands between them, the row is on disk but
the log doesn't say so. The test tolerates that — anything not
in the log is unconstrained.

### The harness

`crates/prehnitedb/tests/crash_recovery.rs` is a single
integration test, `acked_inserts_survive_random_kills`, that
runs eight iterations of spawn-kill-verify:

```rust
let mut child = Command::new(worker)
    .arg(&db_path).arg(&log_path)
    .stdout(Stdio::null()).stderr(Stdio::null())
    .spawn()?;
let life = Duration::from_millis(rng.millis_between(150, 500));
std::thread::sleep(life);
child.kill()?;
child.wait()?;

let logged = read_logged_ids(&log_path);
let actual: HashSet<i64> = read_db_ids(&db_path).into_iter().collect();
for id in &logged {
    assert!(actual.contains(id), "logged id {id} missing after restart");
}
```

Three pieces of infrastructure:

1. **Path to the worker binary**. `env!("CARGO_BIN_EXE_crash_worker")`
   gives the integration test the path to the test crate's
   built bin. Cargo handles this automatically for `[[bin]]`
   targets in the same package.
2. **Tiny LCG for kill timings**. The project has no external
   deps; a Numerical Recipes LCG is plenty for picking random
   millisecond intervals. Seeded from wall-clock at first use
   so different test runs land kills at different points.
3. **Sidecar cleanup**. Each iteration uses a fresh DB path
   under `temp_dir()`. The cleanup deletes the `.db`,
   `-clog`, and per-pager `-wal-<id>` files (v0.30) so the
   next iteration starts fresh.

`child.kill()` is `SIGKILL` on Unix, `TerminateProcess` on
Windows — both forms of "process dies right now, no chance to
flush anything". The worker doesn't get a `Drop` chance for
its `Database` (no graceful close) or its log file (no
buffered-data flush). The engine's recovery has to do all of
it from cold disk state.

### Results

Eight iterations, kill times 150–500 ms, run five times in a
row: 5/5 passes. Every logged id (the ones whose log fsync
landed before the kill) survives the restart.

What the test rules out, concretely:
- **Lost commits.** An INSERT whose `db.execute` returned `Ok`
  and whose log line fsync'd must be visible after restart.
  If the engine claimed durability but the row vanished, this
  fails immediately.
- **WAL recovery bugs.** The kill lands in random pipeline
  stages; recovery has to handle each correctly. The v0.30
  per-pager WAL (where each pager opens a unique
  `<db>-wal-<id>` file) goes through this path on every
  iteration.
- **Clog recovery bugs.** v0.26's clog also gets fsync'd per
  commit. Same exposure.
- **Half-applied transactions.** The WAL apply step copies a
  whole transaction's worth of pages into the DB file. A
  kill mid-apply replays on next open; the test would catch a
  missing post-apply page as a missing logged id.

What the test doesn't rule out (yet):
- **Atomicity of explicit transactions.** The workload is
  autocommit-only. A `BEGIN..COMMIT` that's killed mid-statement
  has a more complex correctness property: either all its
  writes survive or none. Testing that needs a richer log
  format (record `BEGIN`/op/op/op/`COMMIT` rather than just
  ids) and a richer property check.
- **Concurrent writers crashing.** The worker is single-
  threaded. v0.28+ has multi-writer concurrency; a future
  test could spawn N worker threads against the same DB, kill,
  and verify.
- **WAL file corruption.** The test kills the process cleanly;
  it doesn't write garbage into the WAL or DB files. A
  fault-injection harness that randomly truncates or corrupts
  files would push the recovery code harder.

These three are natural follow-ups; v0.38 ships the simplest
useful version.

### What surprised me

Nothing. The durability claim has been right since v0.04 (the
WAL went in early); the only thing v0.38 does is *check*. The
test was, in a sense, designed to fail — to find some bug in
the recovery path or the multi-WAL story or the clog dance.
It didn't. The engine's been doing the right thing.

That's not a license to stop testing — it's a license to add
more adversarial properties on top of this scaffolding. The
crash worker + property harness is a pattern that scales: any
durability claim can become a "spawn the worker, do random
things, kill, restart, verify" test.

### Numbers

- 218 tests across the workspace (the crash-recovery test
  itself runs 8 iterations per pass, each iteration spawning
  the worker for 150–500 ms)
- Touched: `crates/prehnitedb/src/bin/crash_worker.rs` (new
  binary), `crates/prehnitedb/tests/crash_recovery.rs` (new
  integration test). No engine changes.
- The on-disk format is unchanged (`PREHNDB6`); the wire
  protocol is unchanged; v0.37 databases open cleanly under
  v0.38. The crash worker is a pure test artifact.

## Session 39 — `EXPLAIN` + cardinality estimator (v0.39)

A planner that costs `INNER JOIN` chains has been in the tree
since v0.18, but the costs have always been hidden: the
catalog carries `Schema::row_count`, `score_ordering` walks
the join permutations, and the cheapest order wins — silently.
A user looking at a slow query had no way to see what the
planner thought was happening, or why a `WHERE id = 5` against
an indexed column was still hitting `SeqScan`.

v0.39 adds `EXPLAIN <select>` and a small selectivity model
behind it. The output is one row per logical operator, indented
two spaces per nesting level, each ending in a
`(rows: N)` cardinality estimate:

```
> EXPLAIN SELECT name FROM users WHERE id = 5 LIMIT 10;
Limit  (limit=10)  (rows: 10)
  Project  (name)  (rows: 10)
    Filter  ((id = 5))  (rows: 10)
      IndexScan users using idx_id  (full)  (rows: 33)
```

Five things had to fall into place.

### 1. The keyword, the AST node, the parser branch

`EXPLAIN` is a fresh keyword (`crates/prehnitedb/src/sql/token.rs`)
and a fresh `Statement` variant: `Statement::Explain(Box<Statement>)`.
The boxing matters — `Statement` is otherwise a flat enum with
no recursion, and the variant carries an inner statement
verbatim. Parsing rejects anything but a `SELECT` inside:

```rust
Some(Token::Keyword(Keyword::Explain)) => {
    self.pos += 1;
    if !matches!(self.peek(), Some(Token::Keyword(Keyword::Select))) {
        return Err(Error::parse("EXPLAIN must be followed by a SELECT"));
    }
    let inner = self.statement()?;
    Ok(Statement::Explain(Box::new(inner)))
}
```

That restriction is a safety choice: the server's
`is_read_only` classifier and `write_scope` get to treat
`EXPLAIN` as read-only without any caveats. There's no
question of an `EXPLAIN INSERT` being asked to take a write
lock or, worse, write a row.

### 2. The planner mints `Plan::Explain` — and plans the inner statement anyway

The planner doesn't just wrap the `Statement::Explain` in an
opaque "render me" node — it *plans the inner statement*, so
the EXPLAIN output reflects the same `AccessPath`, the same
reordered join chain, the same access-path selection the
executor would actually use:

```rust
Statement::Explain(inner) => {
    let inner_plan = plan(*inner, pager, catalog)?;
    Ok(Plan::Explain(Box::new(inner_plan)))
}
```

This is what makes EXPLAIN useful. If `idx_id` isn't being
picked, you see `SeqScan` in the output — because that's what
the planner actually decided, not because EXPLAIN approximated
it. If the join chain got reordered, you see it in left-deep
order with `users` not where you wrote it.

### 3. The selectivity model

`crates/prehnitedb/src/engine/explain.rs` is the new module
holding the estimator. The model is intentionally coarse — the
Postgres defaults, no histograms, no MCV lists, no NDV stats:

| Predicate shape | Selectivity |
|---|---|
| `col = literal` | 0.10 |
| `col <> literal` | 0.90 |
| `<`, `<=`, `>`, `>=` | 0.33 |
| `IS NULL` | 0.10 |
| `AND` | `s₁ × s₂` (independence) |
| `OR` | `1 − (1−s₁)(1−s₂)` |
| `NOT p` | `1 − sel(p)` |
| `IN (a, b, c)` | `min(1.0, n × 0.10)` |
| anything else | 1.0 |

`scale_rows(rows, sel)` multiplies, *rounds to nearest*, then
clamps a non-zero selectivity to at least one row. The round
matters: chained `0.10 × 0.10` in `f64` is
`0.010000000000000002` (one ULP above `0.01`), and an honest
`.ceil()` turns `100 × 0.01 = 1.0` into `2`. The unit test
pins this:

```rust
assert_eq!(scale_rows(100, 0.10 * 0.10), 1);
```

Group cardinality with no NDV stats has to be a guess; the
classic placeholder is `sqrt(input)` — far better than `1` (the
ungrouped collapse) or `input` (no compression). An index
scan's cardinality bias is based on the bound shape: both
bounds → 0.10, one bound or a pinned prefix → 0.33, neither →
1.0.

### 4. The renderer is top-down, the estimates are bottom-up

The renderer emits the operator tree as the executor *runs*
it — `Limit` at the top, `SeqScan` at the bottom — but
cardinalities flow *up* the tree (the scan tells the filter
how many candidates; the filter tells the project; the project
tells the limit). Solving that means computing all the
intermediate sizes first, then emitting top-down with the
right number on each line.

```rust
let after_where = scale_rows(joined, sel(filter));
let after_group = group_rows_estimate(after_where, group_by.len());
let after_having = scale_rows(after_group, sel(having));
let after_limit  = (after_having - offset).min(limit);

// Then emit Limit → Project → Sort → HashAggregate → Filter → joins → scans
```

Joins are recursive — `fmt_joins_recursive` walks the
left-deep chain from outermost (last) to base, emitting the
root `InnerJoin` line first, then recursing on its left child,
then rendering its right scan. The output indentation grows
each step. `JoinKind` controls the row math:

- `Inner` / `Left` — `outer × inner × on_sel`
- `Cross` — `outer × inner`
- `Semi` / `Anti` — `outer × on_sel` (no cardinality blow-up)

### 5. The executor wires it up

The actual `EXPLAIN` execution is anticlimactic:

```rust
fn explain(pager: &mut Pager, catalog: &Catalog, inner: Box<Plan>) -> Result<RowStream> {
    let text = format_plan(pager, catalog, &inner)?;
    let rows: Vec<Vec<Value>> = text.lines()
        .map(|line| vec![Value::Text(line.to_string())])
        .collect();
    Ok(RowStream {
        columns: vec!["QUERY PLAN".to_string()],
        source: RowSource::Buffered(rows.into_iter()),
    })
}
```

The inner `Plan` is *never run*. `format_plan` walks it
structurally — reading `Schema::row_count` from the catalog
for base estimates, the `AccessPath` for bound bias — and
produces lines. Wrap each line in a one-column row and the
result is a normal `RowStream` the rest of the engine and the
streaming wire protocol know how to handle.

### What this enables

`EXPLAIN` flips a debugging dynamic. Before v0.39, "why is
this query slow?" meant `RUST_LOG=trace`, reading executor
source to figure out what `select_vectorised` decided, and
guessing at row counts. After v0.39:

```
> EXPLAIN SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.uid WHERE o.amount > 100;
Project  (u.name)  (rows: 16500)
  InnerJoin  on (u.id = o.uid)  (rows: 16500)
    SeqScan u  (rows: 1000)
    Filter  ((o.amount > 100))  (rows: 16500)
      SeqScan o  (rows: 50000)
```

The user sees immediately: 50K orders feeding a join with
1K users, post-filter only 16.5K survive, and the join then
multiplies. If `idx_uid` would help, it shows up here as
`IndexScan o using idx_uid` instead of `SeqScan o`. The
estimates are not the truth — they're the *planner's belief* —
but that's exactly what's useful: if `(rows: 16500)` says one
thing and `EXPLAIN ANALYZE` (a v0.40+ idea) would say `(actual: 5)`,
that's a signal that the model needs distinct-value
statistics, or the predicate is fooling the independence
assumption.

### What surprised me

The amount of context the EXPLAIN output captures. After
adding the line for `presorted` (the planner notices when an
index scan already yields rows in `ORDER BY` order, sparing
the sort), a query like `EXPLAIN SELECT * FROM t WHERE n > 5
ORDER BY n` shows *no* `Sort` line — the index walk is
implicitly ordered. That fact has been in the planner since
v0.18; EXPLAIN just made it visible to a user for the first
time.

The other thing: how cheap the model is. About 200 lines for
the entire selectivity walk + renderer, no traversal of
statistics tables, no histogram bucket math. The Postgres
defaults aren't right — they're just useful — and for v0.39
that's the whole point. A real cost model belongs to a future
session that adds NDV/MCV statistics; for now the user sees
the planner's reasoning at the resolution the planner uses
internally.

### Numbers

- 230 tests across the workspace (218 → 230: +8 EXPLAIN
  integration tests, +4 selectivity unit tests). 1 second
  added to the test suite.
- Touched: `crates/prehnitedb/src/engine/explain.rs` (new,
  ~480 lines), `crates/prehnitedb/src/engine/mod.rs` (module
  registration), `crates/prehnitedb/src/engine/executor.rs`
  (one new `explain` helper + one match arm),
  `crates/prehnitedb/src/engine/planner.rs` (one new
  `Plan::Explain` variant + one match arm),
  `crates/prehnitedb/src/sql/{ast,parser,token}.rs` (the
  keyword and AST node), `crates/prehnitedb/src/lib.rs`
  (`is_read_only` and `write_scope` recognise `EXPLAIN`),
  `crates/prehnitedb/tests/integration.rs` (the 8 EXPLAIN
  end-to-end tests), `README.md` (Highlights + SQL
  reference). On-disk format is unchanged (`PREHNDB6`).

## Session 40 — `EXPLAIN ANALYZE` (v0.40)

v0.39's `EXPLAIN` shipped a useful but one-sided artifact: it
showed the planner's *beliefs* about how a query would run —
selectivity estimates, join orderings, access-path choices —
but never compared those beliefs to reality. A user staring at
`(rows: 33)` had no way to know whether the actual answer was
30 or 30,000.

v0.40 closes that loop. `EXPLAIN ANALYZE <select>` runs the
inner query for real, drains the row stream, times the run,
and annotates the EXPLAIN output with the observed numbers:

```
> EXPLAIN ANALYZE SELECT n FROM t WHERE n > 0;
Project  (n)  (rows: 33, actual: 100)
  Filter  ((n > 0))  (rows: 33)
    SeqScan t  (rows: 100)
Execution time: 0.482 ms
```

The estimator says "33 rows survive `n > 0`"; the observation
says "all 100 of them did". That gap — visible at a glance —
is the whole point of ANALYZE. The default selectivities are
fine for `WHERE pk = 5` (the prototypical 10% rule), and
clearly wrong for `WHERE n > 0` where every row matches. The
fix is *not* to invent a better default; it's to gather real
statistics. EXPLAIN ANALYZE makes that gap concrete enough to
act on.

### The smallest-possible cut

For v0.40 I deliberately deferred per-operator actuals. The
straightforward way to gather them — wrap every operator in a
`Counting<O>` adapter that increments a `Cell<u64>` on each
`next` — requires plumbing the wrap through every operator
constructor in `select()`, `build_from()`, `scan_operator()`,
the vectorised path. That's mechanically simple but invasive,
and the user value-per-line-of-code curve falls off after the
first actual: showing the root total + the time is most of the
calibration signal.

So v0.40 ships with:
- `actual: N` annotation on **the root operator only**
- `Execution time: X.XXX ms` footer
- Per-operator actuals → v0.41

The implementation is small enough that the trade-off is
visible in the code: about 40 lines of executor change plus
60 lines of formatter change.

### How the parser distinguishes EXPLAIN from EXPLAIN ANALYZE

A new `Keyword::Analyze` (added to `sql/token.rs`'s catalog)
and one branch in the parser:

```rust
Some(Token::Keyword(Keyword::Explain)) => {
    self.pos += 1;
    let analyze = if matches!(self.peek(), Some(Token::Keyword(Keyword::Analyze))) {
        self.pos += 1;
        true
    } else {
        false
    };
    if !matches!(self.peek(), Some(Token::Keyword(Keyword::Select))) {
        return Err(Error::parse(if analyze {
            "EXPLAIN ANALYZE must be followed by a SELECT"
        } else {
            "EXPLAIN must be followed by a SELECT"
        }));
    }
    let inner = self.statement()?;
    Ok(Statement::Explain { inner: Box::new(inner), analyze })
}
```

The AST variant moved from `Explain(Box<Statement>)` (a tuple)
to `Explain { inner, analyze: bool }` (a struct). Same for
`Plan::Explain`. That cascaded match-arm updates in
`lib.rs::is_read_only`, `lib.rs::write_scope`,
`planner::plan`, `executor::execute_streaming`, and
`explain.rs::fmt_plan` — all small and mechanical, all caught
by the compiler. Adding a `bool` to a struct variant is
exactly the kind of refactor Rust's pattern-match exhaustiveness
makes a non-event.

### The execution path

The executor's `explain` helper grew an `analyze` parameter:

```rust
fn explain(
    pager: &mut Pager, catalog: &Catalog, snapshot: &Snapshot,
    inner: Box<Plan>, analyze: bool,
) -> Result<RowStream> {
    let text = if analyze {
        let start = std::time::Instant::now();
        let inner_plan = *inner.clone();
        let exec = execute_streaming(pager, catalog, snapshot, inner_plan)?;
        let actual_rows = match exec {
            Execution::Rows(mut stream) => {
                let mut count = 0u64;
                while stream.next(pager)?.is_some() { count += 1; }
                count
            }
            Execution::Ack(_) => return Err(Error::corruption(...)),
        };
        let elapsed = start.elapsed();
        format_plan_analyzed(pager, catalog, &inner,
            AnalyzeStats { actual_rows, elapsed })?
    } else {
        format_plan(pager, catalog, &inner)?
    };
    // ... wrap text lines into a one-column RowStream as before
}
```

Three things to notice. First, **recursive re-entry into
`execute_streaming`** with the same snapshot — that's how
ANALYZE inherits all of MVCC visibility, all of SSI's
relation locks, all of the per-table RwLock taking. ANALYZE
*is* a SELECT, just one we describe afterward; running it
through the same execute path is the only way to guarantee
those properties without parallel implementations.

Second, **`Instant::now()` not `SystemTime::now()`**. Wall
clock can run backwards under NTP adjustments; the elapsed
time of a query inside one process is a quintessential
monotonic-clock job.

Third, **the `Plan` is cloned** so the formatter still has
access to it after execution consumed `inner_plan`. The
clone is cheap — `Plan` is `Clone` and mostly holds names
and indices — and it lets us decouple the "run" pass from
the "render" pass cleanly.

### Annotating the root

`format_plan_analyzed` calls the v0.39 `format_plan` to get
the multi-line text, then scans the lines for the first that
ends in `(rows: N)` and rewrites it:

```rust
fn annotate_root_with_actual(text: &str, actual: u64) -> String {
    let mut out = String::with_capacity(text.len() + 24);
    let mut annotated = false;
    for line in text.split_inclusive('\n') {
        if annotated { out.push_str(line); continue; }
        if let Some(stripped) = line.strip_suffix(")\n") {
            if stripped.rfind("(rows: ").is_some() {
                out.push_str(stripped);
                out.push_str(&format!(", actual: {actual})\n"));
                annotated = true;
                continue;
            }
        }
        out.push_str(line);
    }
    out
}
```

`split_inclusive('\n')` keeps the trailing newline on each
chunk, so a `strip_suffix(")\n")` cleanly handles the format.
The `rfind("(rows: ")` is unambiguous because every operator
line ends with `(rows: N)` as its last parenthesised group —
the predicate or column list earlier in the line never matches.

The footer is one more `write!`:

```rust
let ms = stats.elapsed.as_secs_f64() * 1000.0;
write!(&mut text, "Execution time: {ms:.3} ms\n");
```

Three-decimal-place precision because typical PrehniteDB
queries on a small dataset run in tens of microseconds; the
extra digits avoid losing the signal to rounding.

### What ANALYZE inherits from the rest of the engine

Because ANALYZE runs the inner SELECT through `execute_streaming`,
every property of a normal SELECT carries over for free:

- **Snapshot isolation.** `EXPLAIN ANALYZE` inside a `BEGIN..COMMIT`
  observes the snapshot pinned at BEGIN — not the freshest
  committed data. The
  `explain_analyze_inside_transaction_uses_snapshot` test
  pins this: peer writer commits between two ANALYZEs inside
  the reader's transaction, both ANALYZEs report `actual: 1`.
- **SSI conflict detection.** The relation lock the scan
  takes is added to the snapshot's read-set the same way a
  plain SELECT would. If this is the read that turns the
  transaction into the pivot of a dangerous cycle, COMMIT
  will abort it. ANALYZE pays the same conflict price as a
  query; that's correct.
- **Streaming.** The volcano tree streams rows one at a
  time; ANALYZE drains it the same way the materialising
  `execute` path does. Memory cost is one row, even for a
  10M-row SELECT being analyzed.

This is the dividend from v0.27+'s consistent MVCC + SSI
plumbing: a new feature that "runs a query" doesn't need any
parallel locking or visibility logic — it just calls
`execute_streaming`.

### What surprised me

How short the diff is. The whole feature is about 100 net
lines across the parser, AST, planner, executor, and
formatter — and 5 integration tests proving it works. The
"run the inner query and capture the count" operation is one
recursive call, one match arm, one tight loop.

That short diff is only possible because v0.39 designed
`format_plan` as a function that takes a `Plan` and emits a
string — not as a function welded into the executor. With the
formatter decoupled, ANALYZE is "run the plan, then re-render
the plan, then mash the actuals in" rather than a parallel
execution machine.

### Numbers

- 235 tests across the workspace (230 → 235: +5 ANALYZE
  integration tests). About 4 s added to the suite (mostly
  the snapshot-isolation test's setup).
- Touched: `engine/explain.rs` (~80 lines: `AnalyzeStats`,
  `format_plan_analyzed`, `annotate_root_with_actual`, +
  the v0.39 `Plan::Explain` pattern match updated to struct
  form), `engine/executor.rs` (`explain` helper grew the
  ANALYZE branch and an `analyze` parameter; the
  `execute_streaming` match arm became one struct-pattern
  line), `engine/planner.rs` (`Plan::Explain` became a struct
  variant), `sql/{ast,parser,token.rs}` (`Analyze` keyword,
  `Statement::Explain` as struct variant, parser branch),
  `lib.rs` (`is_read_only` and `write_scope` pattern updates),
  `tests/integration.rs` (5 new tests). README + DEEP_DIVE
  updates.
- On-disk format unchanged (`PREHNDB6`). Wire protocol
  unchanged. v0.39 databases open cleanly under v0.40.
- Deferred to v0.41: per-operator actuals via a `Counting<O>`
  adapter threaded through `select()` construction.

## Session 41 — Per-operator EXPLAIN ANALYZE actuals (v0.41)

v0.40 shipped `EXPLAIN ANALYZE` with the actual row count on the
root operator only and a final `Execution time:` footer. The
deferred work was clear: wrap *every* operator, surface a per-line
`actual`, turn the whole tree into a calibration signal. v0.41
does that.

```
> EXPLAIN ANALYZE SELECT n FROM t WHERE n < 30;
Project  (n)  (rows: 33, actual: 30)
  Filter  ((n < 30))  (rows: 33, actual: 30)
    SeqScan t  (rows: 100, actual: 100)
Execution time: 0.612 ms
```

Three numbers per line now tell a story: the planner estimated
~33 rows past the filter (the default 1/3 range selectivity), the
filter actually kept 30, and underneath it the scan read all 100
of the table's rows. The `(rows: ...)` → `(rows: ..., actual: ...)`
gap is where a future cost-model improvement would deliver its
biggest gains.

### The wrapper

`Counting` is a 30-line operator that wraps another and ticks an
`Rc<Cell<u64>>` per yielded row:

```rust
struct Counting {
    inner: Box<dyn Operator>,
    count: Rc<Cell<u64>>,
}
impl Operator for Counting {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        let row = self.inner.next(pager)?;
        if row.is_some() { self.count.set(self.count.get() + 1); }
        Ok(row)
    }
}
```

`Rc<Cell<u64>>` is the right shape because execution is
single-threaded per statement. The wrapper is the same `Box<dyn
Operator>` shape it wraps, so it slots into the volcano tree
transparently — every existing operator sees a `Box<dyn Operator>`
child whether or not it's actually a `Counting` underneath.

A plain SELECT (no ANALYZE) never constructs `Counting`, so it
pays zero — not even a branch on the per-row hot path.

### Threading counters through `select()` and `build_from()`

The hard part. `select()` and `build_from()` already had a dozen
operator construction sites (base scan, three flavours of join,
each join's right scan, Filter, Sort, Project, Limit). Each one
needs to know whether to wrap. I added an
`Option<&mut OperatorCounters>` parameter, and a tiny helper:

```rust
fn wrap_into(
    op: Box<dyn Operator>,
    slot: &mut Option<Rc<Cell<u64>>>,
) -> Box<dyn Operator> {
    let count = Rc::new(Cell::new(0u64));
    *slot = Some(count.clone());
    Box::new(Counting { inner: op, count })
}
```

So every wrap site is one line:

```rust
op = scan_operator(pager, &base_schema, base_access, snapshot.clone())?;
if let Some(counters) = instrument.as_deref_mut() {
    op = wrap_into(op, &mut counters.base_scan);
}
```

The `as_deref_mut()` matters: `Option<&mut OperatorCounters>` needs
to be reborrowed cheaply in a loop without dropping the outer
borrow. `as_deref_mut()` gives an `Option<&mut OperatorCounters>`
that borrows from the original for one iteration.

`OperatorCounters` itself is a struct with one optional `Rc` per
operator role:

```rust
pub(crate) struct OperatorCounters {
    pub base_scan: Option<Rc<Cell<u64>>>,
    pub join_outputs: Vec<Rc<Cell<u64>>>,
    pub join_right_scans: Vec<Option<Rc<Cell<u64>>>>,
    pub filter: Option<Rc<Cell<u64>>>,
    pub sort: Option<Rc<Cell<u64>>>,
    pub project: Option<Rc<Cell<u64>>>,
    pub limit: Option<Rc<Cell<u64>>>,
    pub grouped_output: Option<Rc<Cell<u64>>>,
}
```

`join_right_scans` is `Vec<Option<...>>` because an
`IndexNestedLoopJoin` has no streaming right scan — it does
per-left-row index probes inside its own `next` — so its slot
stays `None`.

After execution, `OperatorCounters::snapshot()` reads every cell
into a plain `OperatorActuals` (just `u64`s, no `Rc`s, `Clone`
for the renderer to consume freely).

### Matching counters to lines in the renderer

The trick is matching counters to the lines `format_plan` emits.
The orderings differ: `select()` builds bottom-up (scan first,
then joins, then Filter/Sort/Project/Limit), while `format_plan`
emits top-down (Limit first, then Project, Sort, Filter, joins,
scans). For joins specifically:

- Build order: `J0`, `J1`, `J2` (inner-to-outer left-deep)
- Emit order: `J2`, `J1`, `J0` (outermost-first)

And for scans:

- Build order: base, `R0` (J0's right), `R1`, `R2`
- Emit order: base, `R0`, `R1`, `R2` (luckily, same order!)

`annotate_lines` walks the rendered text line by line, detects
each operator's role from its leading token (`Limit` / `Project`
/ `Sort` / `Filter` / `InnerJoin` / `LeftJoin` / ... / `SeqScan`
/ `IndexScan`), and pulls from the matching counter slot. For
joins it tracks a `joins_seen` counter and indexes
`join_outputs[total_joins - 1 - joins_seen]` — the reverse-index
that converts emit order back to build order. For scans it
tracks `scans_seen`: the first scan is the base, the rest are
right scans by build order.

The splice itself is straightforward:

```rust
if let Some(stripped) = line.strip_suffix(")\n") {
    out.push_str(stripped);
    write!(&mut out, ", actual: {n})\n").unwrap();
}
```

`split_inclusive('\n')` keeps the trailing newline on each chunk;
`strip_suffix(")\n")` cleanly handles the format because every
operator line ends with `(rows: N)`.

### The grouped path

`GROUP BY` queries hit a pipeline-breaker: `select()` drains the
scan-filter-join volcano tree into a `Vec`, then calls
`grouped_select` which materialises everything (HashAggregate,
Having, ORDER BY, Project, Limit) in one shot. There's no
operator tree to instrument past `Filter`.

For v0.41, the grouped path:
- counts base scan, joins, right scans, and filter per-operator
  (those still go through the volcano tree)
- records a single `grouped_output` counter for the
  fully-materialised result

`annotate_lines` then assigns that one observation to all the
post-aggregation operators (HashAggregate / Having / Sort /
Project / Limit). The user sees five lines with the same actual,
which is honest — they're all reporting the same observation,
because we made one observation. A future session that splits
`grouped_select` into separate operators (each its own
`Counting`-wrappable thing) could narrow this.

### Vectorised path: forced fallback

`select()` has a vectorised dispatch at the top: a SELECT without
joins / GROUP BY / correlation / etc. goes through
`select_vectorised`, which builds a batched (BatchOperator) tree.
v0.41's `Counting` doesn't wrap `BatchOperator`. Simplest
solution: when instrumentation is requested, skip the vectorised
fast path entirely. One condition added to the dispatch gate:

```rust
if instrument.is_none() && joins_qualify && !has_correlated && aggregation_vectorisable {
    return select_vectorised(...);
}
```

A plain SELECT still gets the fast path; only ANALYZE forces the
row pipeline. ANALYZE is, by definition, a debugging tool —
asking it to bypass the vectoriser for visibility is the right
trade.

### Three subtle correctness properties

1. **A `LIMIT 7` actually short-circuits.** The streaming
   volcano stops pulling from below the moment Limit has
   produced 7 rows. The base SeqScan's actual stays at 7, not
   1000 — visible in the test
   `explain_analyze_limit_short_circuits_the_scan`. That's the
   *streaming pipeline working* showing up in the EXPLAIN output.
2. **Counters survive snapshot isolation.** ANALYZE inside a
   transaction observes the snapshot pinned at BEGIN. The
   per-operator counters reflect that snapshot's row counts, not
   the live ones. A peer writer committing between two
   transactions' ANALYZEs doesn't leak into the first
   transaction's actuals — the test
   `explain_analyze_inside_transaction_uses_snapshot` (carried
   over from v0.40) still passes.
3. **An IndexNestedLoopJoin's right side has no scan counter.**
   It doesn't do a streaming scan — it does an index probe per
   left row. `join_right_scans[i] = None` for that join; the
   renderer emits no `actual` for that line. The user sees the
   structural difference between a hash-join's two scans and an
   index-nested-loop's one scan-plus-probes.

### Why the rendered output uses three numbers, not two

A line like:

```
Filter  ((n < 30))  (rows: 33, actual: 30)
```

tells you three things:
- the predicate (`n < 30`)
- the planner's estimate (`33`, the 1/3 range default)
- the observation (`30`)

The estimate-actual gap is the v0.41 payoff. With per-operator
visibility, a user can read down the tree:

```
Project  (rows: 33, actual: 30)
  Filter  (rows: 33, actual: 30)
    SeqScan t  (rows: 100, actual: 100)
```

and immediately see: "Scan was right (100 = 100), filter was off
by a bit (33 vs 30)." The diagnostic story isn't "X is wrong"
but "here's where the estimator's belief diverges from reality"
— which is the precise question a real cost-model upgrade
(NDV/histograms, a future session) would set out to solve.

### What surprised me

How much pre-existing infrastructure paid off. The
`Box<dyn Operator>` interface lets `Counting` slot in
transparently with no special-casing. The streaming volcano
makes `LIMIT 7`'s short-circuit *visible* in the actuals — that
property would be invisible in a materialised query engine. The
existing snapshot/SSI plumbing means ANALYZE inside a
transaction Just Works. The diff that adds this feature is
~250 lines of executor changes plus 130 lines of formatter
changes, and most of those lines are wrapping decisions, not
new logic.

### Numbers

- **239 tests** across the workspace (was 235; +4
  per-operator ANALYZE integration tests). Test suite ~9 s
  longer (the join + filter ANALYZE tests do a lot of inserts).
- Touched: `engine/executor.rs` (added `Counting`,
  `OperatorCounters`, `wrap_into`; threaded
  `Option<&mut OperatorCounters>` through `select()` and
  `build_from()`; reworked `run_analyze` to use the
  instrumented path), `engine/explain.rs` (`OperatorActuals`
  struct; `format_plan_analyzed` gained an
  `Option<OperatorActuals>` parameter; new `annotate_lines`
  helper for per-line splicing), `tests/integration.rs` (4 new
  tests).
- On-disk format **unchanged** (`PREHNDB6`). Wire protocol
  unchanged. v0.40 databases open cleanly under v0.41.

## Session 42 — WAL group commit (v0.42)

PrehniteDB's commit log (clog) has been single-fsync-per-commit
since v0.26. That's fine at idle, ruinous under contention: N
concurrent writers each take the clog mutex, write 9 bytes,
fsync, release. The fsync is the slow part (100µs to 10ms
depending on storage and how the kernel feels about your
workload that microsecond) and there's no overlap — each
writer waits for the previous one's fsync to finish before
even taking the mutex. With 32 writers, that's 32 sequential
fsync calls per round.

v0.42 introduces a leader/follower group-commit protocol so N
concurrent commits cost **one** fsync. Throughput goes from
"how fast can your disk fsync sequentially" to "how fast can
your disk fsync in batches". On consumer SSDs that's roughly a
10-20× win at 32-way concurrency.

### The protocol

Two stages, two mutexes, one condvar:

```rust
pub struct Clog {
    state: Arc<Mutex<ClogState>>,
    file: Arc<Mutex<File>>,
    flush_done: Arc<Condvar>,
}

struct ClogState {
    map: HashMap<u64, Status>,    // visible-to-readers status, updated only after fsync
    pending: Vec<(u64, Status)>,  // enqueued but not yet fsynced
    next_lsn: u64,                 // monotonic ticket
    durable_lsn: u64,              // highest LSN that has been fsynced
    flushing: bool,                // true while a leader holds the slot
}
```

**Stage 1 — Enqueue.** Every writer takes `state`, pushes its
record onto `pending`, claims `next_lsn += 1`, and releases.
This is microseconds of work; no I/O.

**Stage 2 — Flush.** Every writer then calls `flush_until(my_lsn)`:

```rust
fn flush_until(&self, target_lsn: u64) -> Result<()> {
    let mut state = self.state.lock().expect("poisoned clog");
    loop {
        if state.durable_lsn >= target_lsn { return Ok(()); }
        if state.flushing {
            state = self.flush_done.wait(state).expect("...");
            continue;
        }
        // I'm the leader.
        state.flushing = true;
        let batch = std::mem::take(&mut state.pending);
        let snapshot_lsn = state.next_lsn;
        drop(state);

        let result = self.write_and_fsync(&batch);

        let mut state = self.state.lock().expect("...");
        if result.is_ok() {
            for (id, status) in &batch { state.map.insert(*id, *status); }
            state.durable_lsn = snapshot_lsn;
        }
        state.flushing = false;
        self.flush_done.notify_all();
        return result;
    }
}
```

The leader's life:
1. Take `state` mutex, snapshot the batch, mark `flushing = true`, release.
2. Take `file` mutex (separate!), write all records as one buffer, fsync, release.
3. Re-take `state`, update map for the whole batch, set `durable_lsn = snapshot_lsn`, clear `flushing`, notify all.

A follower's life:
1. Take `state` mutex, see `flushing = true`, park on `flush_done`.
2. Wake, re-check: is `durable_lsn >= my_lsn`? If so, done. If not, claim the leader slot (the previous leader cleared it).

### Why two mutexes are non-negotiable

I had a one-mutex draft. It was wrong, and the bug is instructive.

If the leader holds *one* combined mutex through the fsync, no
other writer can enqueue during the I/O window. The pending
buffer stays at size 1 — the leader's own record. There's no
batching. You've added the protocol's overhead for nothing.

Two mutexes fix this. The `state` mutex covers the in-memory
queue (microsecond contention). The `file` mutex covers the
actual write+fsync (millisecond contention). They never overlap
on the slow path. While the leader holds `file` and is mid-fsync,
peers can freely take `state`, push onto `pending`, and release.
The next leader's drain picks up everything those peers added.

### Pipelining at steady-state

Under sustained concurrency, the steady-state batch size is
~equal to the number of in-flight writers. Trace:

- t=0: A enqueues, becomes leader, starts fsync (covers {A}).
- t=0.1ms: B enqueues, sees `flushing`, parks.
- t=0.2ms: C enqueues, sees `flushing`, parks.
- t=0.3ms: D enqueues, sees `flushing`, parks.
- t=10ms: A's fsync returns, A sets `durable_lsn = 1`, notifies.
- B wakes, sees `durable_lsn = 1 < 2`, sees `flushing = false`,
  becomes leader. Drains pending = {B, C, D}, starts fsync (covers {B,C,D}).
- t=10.1ms: E enqueues, parks.
- t=10.2ms: F enqueues, parks.
- t=20ms: B's fsync returns, sets `durable_lsn = 4`, notifies.
- C, D, E, F all wake. C and D see `durable_lsn >= my_lsn`, return.
  E sees `2 < 5`, becomes leader for {E, F}.

So: 4 writers (A, B, C, D) cost 2 fsyncs. 8 writers cost 3. With
N concurrent writers the steady-state amortized cost is ~1
fsync per ~N/2 commits.

### Durability before visibility

The subtle correctness rule: a record's entry in the in-memory
`map` is inserted *only after* fsync returns. Until then, a
reader looking up the TX gets `None` (treated as "in flight").

Why this matters: if we updated the map at enqueue time, a
reader could see "TX 5 = committed" and return rows to a user.
If the engine then crashed before the fsync landed, the next
open's clog would have no record of TX 5 — recovery would
classify it as rolled back. The rows the user already saw would
silently vanish. Visibility must follow durability, never the
other way round.

That's why the map insert is in the *post*-fsync branch of the
leader's code:

```rust
if result.is_ok() {
    for (id, status) in &batch { state.map.insert(*id, *status); }
    state.durable_lsn = snapshot_lsn;
}
```

On fsync error, the records stay out of the map. Followers
waiting on us see `durable_lsn` unchanged, wake up, and discover
their LSN was never durable — they'll either retry (next leader's
batch may include them via a re-push from a higher layer, since
their commit error propagated) or fail their own attempt. The
crash-recovery rule (TX ID ≤ next_tx_id with no clog entry =
rolled back) catches anything that fell through the cracks.

### What surprised me

The cleanest design fell out of a wrong design. My first cut had
one mutex; the moment I drew the pipeline diagram I saw the
batching would never happen. Splitting state from file is
*structurally obvious* once you realize the leader is doing two
fundamentally different things: brief queue manipulation
(microseconds) and slow durable I/O (milliseconds). They should
never be under the same lock.

The other thing: Condvar in Rust is straightforward. Postgres's
group-commit machinery has a small custom scheduler around
PGSemaphore. In std Rust, `Condvar::wait` + `notify_all` does
the right thing in 4 lines — including the spurious-wakeup
handling via the loop.

### Numbers

- **243 tests** across the workspace (was 239; +3 clog unit
  tests + 1 integration test). Test suite essentially unchanged
  (~22 s integration, same as v0.41).
- Touched: `engine/clog.rs` — substantial rewrite of `Clog` and
  `ClogState` (now separate from `file` behind its own mutex);
  new `flush_until` + `write_and_fsync` helpers implementing
  leader/follower; new module-level docs explaining group commit.
  `tests/integration.rs` — one new test
  (`group_commit_handles_concurrent_writers_durably`) that
  proves 16 concurrent writers × 25 inserts each all land
  durably. No changes to `transaction.rs`, `database.rs`, or any
  other engine code — the `Clog::record_commit` and
  `Clog::record_rollback` public API stayed identical, and the
  group-commit work lives entirely inside `append`.
- On-disk format **unchanged** (`PREHNDB6`). The clog file
  format is byte-identical (9-byte records). Wire protocol
  unchanged. v0.41 databases open cleanly under v0.42.
- Committed as `<HASH>`, pushed to `origin/main`.

## Session 43 — PRIMARY KEY / NOT NULL / UNIQUE (v0.43)

PrehniteDB has had typed columns since v0.1 but no real constraints
on values. v0.43 adds the three foundational column-level
constraints — `PRIMARY KEY`, `NOT NULL`, `UNIQUE` — checked at
INSERT and UPDATE time. Real relational schemas, finally.

### Scope choice: column-level only

I deliberately punted on the composite forms (`PRIMARY KEY (a, b)`,
table-level `UNIQUE (col)`), foreign keys, and CHECK constraints.
Each is its own big lift; for v0.43 we ship the column-level shapes
that cover the 80% of real use:

```sql
CREATE TABLE users (
    id    INT PRIMARY KEY,
    email TEXT UNIQUE,
    name  TEXT NOT NULL
);
```

### The pieces (six of them)

**1. Parser/AST.** A new `ColumnConstraint` enum (`PrimaryKey`,
`NotNull`, `Unique`) attached to `ColumnDef` as a `Vec`. The parser
gains a `column_constraints` method that loops over the trailing
keywords after a type:

```rust
fn column_constraints(&mut self) -> Result<Vec<ColumnConstraint>> {
    let mut out = Vec::new();
    loop {
        match self.peek() {
            Some(Token::Keyword(Keyword::Primary)) => {
                self.pos += 1;
                self.expect_keyword(Keyword::Key)?;
                out.push(ColumnConstraint::PrimaryKey);
            }
            Some(Token::Keyword(Keyword::Not)) => {
                self.pos += 1;
                self.expect_keyword(Keyword::Null)?;
                out.push(ColumnConstraint::NotNull);
            }
            Some(Token::Keyword(Keyword::Unique)) => {
                self.pos += 1;
                out.push(ColumnConstraint::Unique);
            }
            _ => break,
        }
    }
    Ok(out)
}
```

Three new keywords: `PRIMARY`, `KEY`, `UNIQUE`. (`NULL` and `NOT`
were already keywords.) The reservation of `KEY` broke exactly one
integration test (`hash_aggregation_handles_many_distinct_groups`,
which had `(key INT, value INT)`); renamed to `k`. Major SQL
dialects all reserve `KEY`, so this is the expected ergonomic cost.

**2. Catalog format bump PREHNDB6 → PREHNDB7.** `Schema::Column`
gains `not_null: bool`. `Schema::Index` gains `unique: bool`.
`Schema` gains `primary_key_column: Option<usize>` (which column
position holds the PK, if any). The on-disk encoding adds these
fields to the per-column and per-index sections, plus a trailing
`u16` for the PK column index (`u16::MAX` as the "no PK" sentinel).

PREHNDB7 is a hard break: opening an older database fails with a
"file format unrecognised" error. The project's philosophy has
been "break format when needed" rather than maintain migration
paths for every version — easier to recreate small databases than
debug a half-migrated catalog.

**3. Planner: validate + lower.** The planner walks the
constraints once per column: detecting `PRIMARY KEY` (rejecting a
second PK on the same table), normalising PK to imply NOT NULL,
collecting UNIQUE columns. It hands the executor a
`Plan::CreateTable { columns, primary_key_column, unique_columns
}` with `not_null` already set on the columns and the PK / UNIQUE
positions pre-collected — no constraint AST in the executor.

**4. Auto-created unique indexes.** `CREATE TABLE` execution
walks `primary_key_column` and `unique_columns`, allocating a B+tree
for each and recording it as a `Schema::Index` with `unique = true`.
Names follow a convention: `_pk_<table>` for the PK,
`_uq_<table>_<col>` for each UNIQUE. These look like ordinary
secondary indexes to the rest of the engine — the planner can use
them for access-path selection, `DROP TABLE` reclaims them — but
their `unique` flag changes the INSERT path.

**5. The UNIQUE check inside `index_insert_row`.** This is where
the actual enforcement happens:

```rust
for index in &schema.indexes {
    if index.unique {
        let any_null = index.columns.iter()
            .any(|&c| matches!(values[c], Value::Null));
        if !any_null {
            let value_prefix = encode_index_value_prefix(values, &index.columns);
            if index_has_value(pager, index.root, &value_prefix)? {
                return Err(Error::exec(format!(
                    "duplicate key value violates UNIQUE constraint '{}' on '{}'",
                    index.name, schema.name
                )));
            }
        }
    }
    let key = codec::encode_index_key(values, &index.columns, rowid_key);
    BTree::open(index.root).insert(pager, &key, &[])?;
}
```

The check is a bounded B+tree cursor scan over the value-prefix
range. The trick: index entries use keys of shape `(value_bytes,
rowid_bytes)` — for non-unique indexes the rowid makes them
unique-at-the-tree-level even with repeated values. For uniqueness
*checking* we want any entry sharing the value-prefix portion,
regardless of rowid. `prefix_upper_bound(value_prefix)` gives us
the exclusive upper of the range; `cursor.next()` returns `Some`
iff at least one matching entry exists.

The NULL exemption is the SQL standard: `NULL ≠ NULL` for
uniqueness, so any column-value that's NULL skips the check.
Multiple rows with NULLs in the same UNIQUE column are fine.

**6. NOT NULL check at INSERT/UPDATE.** Straightforward: after
evaluating every value, walk the column list, and reject if any
`column.not_null` column got assigned `Value::Null`. Applies to
both INSERT (where omitted columns default to NULL) and UPDATE
(where a SET expression could evaluate to NULL).

### What surprised me

How much was already in place. The B+tree's per-table mutex and
per-page latches already handle concurrent insert-with-index. The
existing `index_insert_row` was the natural place to hook unique
checking. The catalog format bump is one record encoding change;
the rest of the engine just sees richer `Column` and `Index` structs.

The whole feature is ~250 lines of engine changes plus ~150 lines
of test code. A modest diff for a foundational feature.

### Known limitation: rolled-back rows leave index entries

Index entries are written at INSERT time and don't carry MVCC
visibility (the table row is the authority). If a transaction
INSERTs then ROLLBACKs, the row becomes invisible to readers but
its index entry persists until VACUUM reclaims it. A subsequent
INSERT with the same UNIQUE value will spuriously reject — the
index says "taken" even though the actual row is invisible.

For single-statement workloads (no explicit BEGIN/COMMIT/ROLLBACK)
this never happens. For multi-statement transactions that
occasionally roll back, it's a transient window: VACUUM removes
the orphan entries and uniqueness recovers. A v0.44+ could check
row visibility before rejecting, closing the window entirely.

### Known limitation: search-then-insert race

`insert_if_absent` does a B+tree `search` then an `insert` — not
an atomic check-and-set under one leaf latch. Under high
concurrency (two writers hitting the same unique key in parallel),
both could search and find nothing, both insert, both succeed —
producing a duplicate. The window is the time between the two
operations, microseconds, and the catalog mutex held during
schema-mutating work usually serialises constraint-relevant
inserts.

A v0.44+ could add a real `insert_if_absent` inside the B+tree
that does the existence check inside the leaf's exclusive latch.

### Numbers

- **252 tests** across the workspace (was 243; +9 constraint
  integration tests). Test suite duration unchanged.
- Touched: `sql/{ast,parser,token}.rs` (ColumnConstraint enum,
  three new keywords, the constraint-parsing loop),
  `engine/schema.rs` (not_null, unique, primary_key_column
  fields), `engine/codec.rs` (PREHNDB7 schema encoding with the
  new fields), `engine/planner.rs` (CreateTable validation +
  lowering), `engine/executor.rs` (auto-create unique indexes
  in `create_table`, NOT NULL check in INSERT/UPDATE, UNIQUE
  check inside `index_insert_row`), `engine/explain.rs`
  (Plan::Explain pattern updated), `storage/btree.rs`
  (`insert_if_absent` helper for future use),
  `storage/pager.rs` (MAGIC bumped to PREHNDB7),
  `tests/integration.rs` (9 new constraint tests, one keyword
  collision rename), README + DEEP_DIVE updates.
- **On-disk format CHANGED**: `PREHNDB6` → `PREHNDB7`. Existing
  v0.42 databases fail to open with a clear "unrecognised file
  format" error. Recreate the database with the new constraints.
- Wire protocol unchanged.

## Session 44 — `NOT IN` → anti-join (using NOT NULL) (v0.44)

v0.37 shipped the `IN (simple subquery)` → semi-join rewrite — one
inner-table scan per outer query instead of one per outer row, the
classic decorrelation win. But it punted on `NOT IN`:

> `NOT IN` is intentionally skipped — SQL's three-valued `NOT IN`
> is `NULL` (not `TRUE`) when the set contains a `NULL`, so an
> anti-join rewrite would be wrong unless the inner projection is
> provably non-nullable.

In v0.37 the planner had no way to *prove* an inner column
non-nullable. v0.43 added `NOT NULL` constraints. v0.44 closes the
loop: when the inner projection is a `NOT NULL` column, the planner
rewrites to an `AntiJoin`; when it's nullable, it falls back to
v0.31's per-row path. Two sessions, one feature.

### The SQL semantics problem

Why `NOT IN` is delicate. Consider:

```sql
SELECT * FROM users WHERE id NOT IN (SELECT bid FROM banned);
```

With three-valued logic:
- `x NOT IN (1, 2, 3)` is `TRUE` iff `x != 1 AND x != 2 AND x != 3`.
- `x NOT IN (1, 2, NULL)` is `(x != 1) AND (x != 2) AND (x != NULL)`.
- `x != NULL` is `NULL`, never `TRUE`.
- `TRUE AND TRUE AND NULL` is `NULL`.
- `WHERE` only keeps rows whose predicate is exactly `TRUE`, so
  every outer row gets filtered out the moment one inner row is
  `NULL`.

An anti-join, on the other hand, decides "no inner row matches the
`ON` predicate" by walking the inner set. A `NULL` inner value
doesn't satisfy `outer.x = inner.bid` (NULL = anything yields
NULL), so the anti-join would still report "no match" and emit the
outer row. That's the opposite of `NOT IN` semantics.

The simple rule: **if the inner column can't carry `NULL`, the two
agree**. The anti-join's "no match in the non-NULL set" is exactly
`NOT IN`'s "every value differs from x" when there are no NULLs to
poison the picture. `NOT NULL` constraints give the planner that
guarantee directly.

### The change

One added check inside `try_extract_in_join`:

```rust
if *negated {
    let Some(idx) = inner_schema.column_index(&inner_colref.name) else {
        return Ok(None);
    };
    if !inner_schema.columns[idx].not_null {
        // Nullable inner column — fall back to v0.31 per-row eval.
        return Ok(None);
    }
}
```

And one tweak at the end:

```rust
Ok(Some(Join {
    kind: if *negated { JoinKind::Anti } else { JoinKind::Semi },
    table: inner_from.table.clone(),
    on: Some(combined_on),
}))
```

Total engine diff: about 30 lines, half of them comments and
docstrings explaining the NULL-safety story.

### Why this stays correct on the per-row path too

For nullable inner columns, the rewrite returns `None`, leaving
the `Expr::InSubquery { negated: true, .. }` node in place. The
v0.31 per-row evaluator handles it via the v0.20 `InList`
resolution path: pre-evaluate the subquery, get a `Vec<Value>` of
inner values, then for each outer row compute `x NOT IN (set)`
using three-valued logic that *does* honour NULLs. So nullable
inputs keep the slower but always-correct path; non-nullable
inputs get the join speedup.

A SQL semantic test pins this: `banned` with two rows `(2, NULL)`
and `users` with rows `(1, 2, 3)`. The query `SELECT name FROM
users WHERE id NOT IN (SELECT bid FROM banned WHERE ...)` returns
*zero* rows — because three-valued `NOT IN` poisons every outer
row. The planner correctly refuses to rewrite (since `banned.bid`
is nullable) and the per-row path produces the right answer.

### Tests

Four new integration tests, picking apart the matrix:

| Inner has NULLs? | NOT NULL constraint? | Path | Expected rows |
|---|---|---|---|
| no | yes | AntiJoin | matching `NOT IN` set diff |
| no | no | per-row | matching `NOT IN` set diff |
| yes | no | per-row | **zero** (NULL poison) |
| empty | yes | AntiJoin | all outer rows |

`EXPLAIN` is the witness for which path was taken: the first and
fourth cases must contain an `AntiJoin` line; the second and third
must not.

### The pitfall I tripped on

My first version of the tests used `id` as the column name in both
the outer and inner tables. The rewrite copies the inner WHERE
into the join's ON verbatim — `WHERE id > 0` becomes part of an
ON that sees both tables — so bare `id` becomes ambiguous in the
combined scope and the query errors. v0.37 had the same pitfall
but its tests happened to use distinct column names. v0.44
documents this as a pre-existing limitation: write `inner.id`
qualified, or use distinct names. A future v0.45+ could
re-qualify the inner WHERE during the rewrite.

### What surprised me

How thin v0.44's diff is — ~30 engine lines for a feature that
requires understanding three-valued SQL semantics, MVCC-friendly
nullability proofs, and the v0.31/v0.37 evaluation paths it spans.
That thinness is the dividend from v0.37 and v0.43 both doing
their work well: the planner had the right rewrite scaffold, and
v0.43 gave it the right metadata to make the decision safely.
v0.44 was the obvious next call.

### Numbers

- **256 tests** across the workspace (was 252; +4 NOT IN
  integration tests). Suite duration unchanged.
- Touched: `engine/planner.rs` — `try_extract_in_join` lost its
  early-return on `negated`, gained a NULL-safety check via the
  inner column's `not_null` flag, and the final `JoinKind`
  selection became `Anti` when negated. `tests/integration.rs` —
  four new tests covering the four-corner matrix above. README +
  DEEP_DIVE.
- On-disk format **unchanged** (`PREHNDB7`). Wire protocol
  unchanged. v0.43 databases open cleanly under v0.44.

## Session 45 — FOREIGN KEY constraints (v0.45)

v0.43 added the three intra-table constraints — `PRIMARY KEY`,
`NOT NULL`, `UNIQUE`. v0.45 adds the inter-table one: column-level
`REFERENCES tbl(col)`. INSERT-child and UPDATE-child check that
the parent row exists; DELETE-parent and UPDATE-parent's-referenced
column are RESTRICTed when any child still points at the affected
row; DROP TABLE on a parent with live children is refused.

```sql
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(id)
);
```

### Scope

For v0.45: column-level single-column FKs, RESTRICT semantics only.
Deferred:
- Composite FKs (`FOREIGN KEY (a, b) REFERENCES other(x, y)`)
- `ON DELETE CASCADE` / `SET NULL` / `SET DEFAULT`
- `ON UPDATE` actions
- Self-referential FKs (the parent's catalog entry doesn't exist
  at the child's CREATE time, so this fails the "parent table
  exists" check — a future version could special-case)

The hardest scope decision was RESTRICT vs CASCADE. RESTRICT is
the SQL default and the safer one — it surfaces every potential
data-integrity hazard as an error the user can decide what to do
about. CASCADE is convenient but easy to misuse. v0.45 ships
RESTRICT; CASCADE is naturally additive later.

### What the parent must be

A `REFERENCES tbl(col)` requires the parent column to be `PRIMARY
KEY` or `UNIQUE`. The planner enforces this at CREATE TABLE: it
checks `parent_schema.primary_key_column == Some(parent_idx)` or
walks the parent's indexes for a unique one over `parent_idx`. Two
reasons:

1. **Semantic clarity.** A FK should reference a *uniquely
   identifying* parent row. If the parent column could repeat,
   "does my FK value have a matching parent" is ambiguous: which
   one?
2. **Fast lookup.** The FK check at every child INSERT is a
   parent-side existence query. v0.43's auto-created unique
   index gives us the right data structure for free — a B+tree
   prefix-range scan, O(log N). Without it we'd need a full
   table scan per child insert. The constraint "parent must be
   unique" is also "we have the index we need".

### The four enforcement points

**1. INSERT child.** After the NOT NULL check,
`check_foreign_keys` walks every FK column with a non-NULL value
and looks up the parent's unique index. NULL values are exempt
(`NULL` means "no parent" per SQL) and the lookup uses
`index_has_value`, the same helper UNIQUE constraint enforcement
already uses.

```rust
let parent_index_root = parent_schema.indexes.iter()
    .find(|i| i.unique && i.columns == vec![parent_idx])
    .map(|i| i.root)
    .ok_or_else(|| Error::corruption(...))?;
let key = codec::encode_index_value(&values[idx]);
if !index_has_value(pager, parent_index_root, &key)? {
    return Err(Error::exec(format!(
        "FOREIGN KEY violation: ..."
    )));
}
```

**2. UPDATE child.** The same `check_foreign_keys` call on the
new row values — fires after the SET expressions evaluate. v0.45
doesn't try to skip the check when the FK column didn't change;
the lookup is cheap enough that the extra work isn't worth the
bookkeeping.

**3. DELETE/UPDATE parent.** The new helper
`check_no_child_references` walks the catalog: for each table with
an FK pointing at this parent, scan for any row whose FK column
matches the about-to-be-affected parent value. If the child table
has an index on its FK column we use it (one prefix-range scan);
otherwise we full-scan the child table. The catalog walk is
O(tables × FKs-per-table) — fine for the small-catalog case; a
future version could maintain a reverse-FK map keyed by parent
table.

The UPDATE path adds one optimization: the parent-side check only
runs when the SET actually touches a PK/UNIQUE column. That's
detected by comparing old vs new values column-by-column for the
indexed positions.

**4. DROP TABLE parent.** `child_referencing` scans the catalog
for any FK pointing at the parent and returns the first child it
finds (table + column). DROP refuses with that name in the error:

```
cannot drop table 'customers': it is referenced by FOREIGN KEY 'orders.customer_id'
```

### Catalog format bump PREHNDB7 → PREHNDB8

`Schema::Column` gains `foreign_key: Option<ForeignKeyTarget>`.
The encoding adds, per column, after the existing `not_null` byte:

- `0u8` → no FK
- `1u8` followed by two strings → FK present (parent table, parent column)

The format bump is a hard break, per the project's convention:
opening a PREHNDB7 database under v0.45 errors clearly. Recreate
the database.

### Where the per-FK action lives

I kept the FK check inline in `insert()`, `update()`, `delete()`,
and `drop_table()` rather than refactoring to a generic
"constraint check phase". Each check has different inputs and
timing:

- INSERT-FK runs on `values` (new row), once per inserted row.
- UPDATE-FK runs on `new` (post-SET row), once per updated row.
- UPDATE-parent-RESTRICT runs on `old` (pre-SET row), once per
  updated row, only when an indexed column changed.
- DELETE-parent-RESTRICT runs on `record.values`, once per
  deleted row.
- DROP-parent-RESTRICT runs once per DROP TABLE.

A generic phase would have to plumb all these contexts through
one interface; for v0.45's needs, three call sites and four
helpers are simpler to read.

### What surprised me

The reverse direction (parent → children) is what made me
hesitate on scope. INSERT-FK is one lookup per inserted row — fast,
local. DELETE-parent is "walk the catalog, scan every child table
that points here, check each row". With dozens of FK
relationships in a real schema, DELETE-parent could touch many
tables.

For v0.45 I accepted the cost: the catalog walk is bounded by
table count (which a small embedded DB stays small for), and the
per-child scan uses the child's index on the FK column when one
exists. A future v0.46+ could materialise a reverse-FK map on
schema change, turning the "which tables reference me" lookup
from O(tables) to O(1).

The other surprise: how much v0.43 paid off. The unique-index
auto-creation gave the FK check a ready-made lookup mechanism.
The NOT NULL flag isn't needed for FKs themselves (NULL is fine
in a child column), but the same constraint plumbing made the
catalog format bump feel routine.

### Known limitations (documented)

- **No self-references.** `CREATE TABLE t (id INT PRIMARY KEY,
  parent_id INT REFERENCES t(id))` fails: the CREATE TABLE
  planner looks up the parent in the catalog *as it stands now*;
  the in-progress table isn't there yet. Fixing this means
  deferring the FK target validation to after the table is
  committed, or special-casing the parent name. Future work.
- **CASCADE / SET NULL not supported.** RESTRICT only. The user
  must delete or reassign children before deleting the parent.
- **Reverse-FK discovery is O(tables).** Each DELETE-parent /
  UPDATE-parent / DROP-parent walks the catalog. Cheap for small
  schemas, expensive for very large ones.

### Numbers

- **262 tests** across the workspace (was 256; +6 FK integration
  tests). Suite duration unchanged.
- Touched: `sql/{ast,parser,token}.rs` (`REFERENCES` keyword,
  `ColumnConstraint::References` variant, parser branch),
  `engine/schema.rs` (`Column.foreign_key` field, new
  `ForeignKeyTarget` type), `engine/codec.rs` (PREHNDB8 encoding
  with FK metadata), `engine/planner.rs` (FK validation at CREATE
  TABLE: parent table + column + uniqueness + type), `engine/
  executor.rs` (three new helpers — `check_foreign_keys`,
  `check_no_child_references`, `child_referencing` — wired into
  `insert`, `update`, `delete`, `drop_table`),
  `storage/pager.rs` (MAGIC bumped to `PREHNDB8`),
  `tests/integration.rs` (6 new FK tests).
- **On-disk format CHANGED**: PREHNDB7 → PREHNDB8. Existing v0.44
  databases fail to open with a clear "unrecognised file format"
  error. Recreate.
- Wire protocol unchanged.

## Session 46 — Concurrent crash-recovery stress test (v0.46)

v0.38 shipped a single-writer crash test: spawn one worker process,
let it loop autocommit INSERTs while fsync-logging each ACKed id,
SIGKILL at a random point, restart, verify every logged id
survived. That validated the basic durability claim.

Eight sessions later, the engine looks very different:
- **v0.42 group commit** added a leader/follower fsync protocol
  with a `pending` buffer between the in-memory state and the
  one-shared-fsync.
- **v0.30 per-page B+tree latches** let multiple writers split
  the same table's leaves in parallel.
- **v0.28 per-table mutexes** in shared mode let concurrent
  INSERTs touch the same table.
- **v0.43 PRIMARY KEY** auto-creates a unique index whose B+tree
  every concurrent inserter hits.

v0.38's single-writer test exercises none of this concurrency. A
crash that lands while two writers are mid-group-commit, or one is
mid-leaf-split and the other is in the optimistic-insert race,
is a different recovery scenario entirely. v0.46 extends the test
to that case.

### The worker

`crash_worker_concurrent` differs from v0.38's `crash_worker` in
three places:

1. **Per-thread `Database` handles**, all sharing one
   `SharedPool` and one `TxState`. Exactly the way `prehnited`'s
   per-connection Databases work at runtime — so the test
   exercises the same plumbing the server uses.
2. **Disjoint id ranges** per thread: thread `tid` writes ids
   in `[tid * STRIDE, (tid+1) * STRIDE)`. That guarantees
   concurrent inserts never collide on the PRIMARY KEY's unique
   index — any failure is a recovery bug, not a duplicate.
3. **Per-thread log files** `<base>.log.<tid>`, each fsync'd
   after every ACKed insert.

```rust
let pool = SharedPool::new();
let tx_state = {
    let mut bootstrap = Database::open_with_pool(&*db_path, pool.clone())?;
    let _ = bootstrap.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, thread INT, n INT)"
    );
    bootstrap.tx_state()
};
for thread_id in 0..n_threads {
    thread::spawn(move || {
        let mut db = Database::open_shared(&*db_path, pool, tx_state)?;
        let mut log = OpenOptions::new().create(true).append(true).open(&log_path)?;
        loop {
            let id = next_id; next_id += 1;
            if db.execute(&format!("INSERT INTO t VALUES ({id}, ...)")).is_ok() {
                writeln!(log, "{id}")?;
                log.sync_all()?;
            }
        }
    });
}
```

### The harness

`crash_recovery_concurrent.rs` mirrors v0.38's pattern: spawn the
worker, sleep `200–600 ms`, SIGKILL it, restart, scan every
per-thread log, verify the union of ids is in the DB. Five
iterations per test run, randomised kill timings.

Two harness details worth flagging:

- **`SharedPool`-via-cloned-handles is single-process**. The
  threads are inside ONE worker process; SIGKILL takes the whole
  process down. v0.46 isn't testing "what if one thread's
  process dies while another's keeps running" — that's a
  multi-process distributed-systems story. It's testing "what
  if N concurrent writers in one process get killed mid-flight,
  does recovery handle the mid-protocol crash state correctly."
- **Log per thread, not per worker**. The single-log version
  would force every thread through a `Mutex<File>` for its
  fsync, which would serialize them at the test harness level
  and mask the very concurrency we're trying to stress. Per-
  thread logs let each fsync race the others naturally.

### What this stresses that v0.38 didn't

When SIGKILL lands during the worker:
- ...mid-`pending.push`: that record is in volatile memory,
  not durable. Harness expects it absent.
- ...mid-leader's fsync: the previous batch's records may or
  may not have landed. v0.42's design has the leader publish
  `durable_lsn` only after fsync returns; if the kill is before
  that, the records' in-memory map entries also haven't been
  set, so visibility-wise they're "in flight" — and crash
  recovery treats in-flight TXs as rolled back. Harness
  tolerates this gap (those IDs weren't logged).
- ...just after the leader's `sync_all` returned but before
  `durable_lsn` got published: the records ARE on disk. On
  restart, the clog file replays them and the in-memory map
  is rebuilt — so they show up as committed. Followers waiting
  on the condvar never got their notification, but their TXs
  are durable. Their log entries also never landed (the
  followers were still blocked, so the worker thread never
  reached the log fsync), so the harness simply doesn't expect
  them — that's the v0.38 "killed between ack and log fsync"
  gap, now generalized.
- ...mid-leaf-split in the B+tree: the optimistic-insert path
  detected an overflow and fell back to the pessimistic
  tree-wide exclusive descent. v0.30's WAL discipline means the
  split's page writes either all made it to the WAL (then to
  the DB file) or none did. Recovery restores the pre-split
  state.
- ...mid-clog-write: the clog file might have a partial 9-byte
  record. The clog reader (`Clog::open`) reads in 9-byte chunks
  and treats `UnexpectedEof` as "end of valid log"; partial
  records get truncated on the next append. Harness tolerates
  this (the ID wasn't logged).
- ...with one thread mid-insert and seven others queued behind
  the catalog mutex / per-page latch: the queued threads
  haven't ACKed yet → they haven't logged → harness doesn't
  expect their IDs.

### Results

Five iterations per run, run five times in a row: **25/25 passes**.
Every logged id from every thread survives every kill.

What the test rules out, concretely:
- **Lost group-commit batches.** A leader's fsync covers N
  records from N writers; the kill can hit before, during, or
  after. Whatever's "after fsync returned" must survive.
- **B+tree split partials under concurrent writers.** Two
  writers splitting different leaves at once, killed mid-second-
  split — recovery must restore both consistently.
- **Unique-index split under concurrent writers.** Every INSERT
  goes through the PK's unique index. The B+tree's optimistic
  path can race with the pessimistic fallback; recovery has to
  handle either state.
- **Per-table `next_rowid` atomic surviving kill.** v0.30's
  `SharedMeta::next_rowid` lives in the database header; if a
  writer reserved a rowid but didn't commit, the next opener
  must not reuse it. The disjoint-id-range design means no
  thread reuses any id, so a duplicate post-restart would
  immediately fire the PK constraint and the worker would log
  the error — the test would detect it.

### What this test doesn't rule out yet

- **WAL/clog file corruption** (truncation mid-record, garbage
  bytes). The test kills cleanly, never injects bytes. A
  separate fault-injection harness could do this.
- **Cross-process concurrent crash.** Multiple worker
  processes against the same DB file, one killed. This breaks
  ground that v0.27 (single-file multi-pager) handles
  conceptually, but the test wouldn't.
- **Concurrent VACUUM crash.** v0.36's background reclaimer
  runs in `prehnited`, not in the test's bootstrap-only
  setup. A future v0.47+ could spawn a reclaimer thread in
  the worker.

### Numbers

- **263 tests** across the workspace (was 262; +1 concurrent
  crash test that does its own work across 5 iterations × 8
  threads × 200–600 ms each). Suite runtime grew by ~3 s.
- Touched: `crates/prehnitedb/src/bin/crash_worker_concurrent.rs`
  (new binary, ~120 lines, mirrors v0.38's worker with per-
  thread `Database::open_shared` and disjoint id ranges),
  `crates/prehnitedb/tests/crash_recovery_concurrent.rs` (new
  integration test, ~170 lines, reuses v0.38's LCG + temp-path
  pattern, generalised log-cleanup to handle per-thread files).
  No engine changes — the property either holds or it doesn't.
- On-disk format **unchanged** (`PREHNDB8`). Wire protocol
  unchanged. v0.45 databases open cleanly under v0.46.

## Session 47 — Column statistics (`ANALYZE table`) (v0.47)

v0.39 shipped `EXPLAIN` with hardcoded Postgres-style default
selectivities: `=` → 10%, range → 33%, `IS NULL` → 10%. v0.40-41
extended this to `EXPLAIN ANALYZE` with observed actuals on every
operator. But the *estimates* stayed defaults. v0.47 closes the
calibration loop opened back in v0.39: `ANALYZE table` scans the
table, builds per-column statistics, persists them, and the
planner's selectivity estimator consults them on every subsequent
query.

```sql
> EXPLAIN SELECT * FROM t WHERE n = 5;
Project  (*)  (rows: 10)
  Filter  ((n = 5))  (rows: 10)        -- 10% default
    SeqScan t  (rows: 100)

> ANALYZE t;
> EXPLAIN SELECT * FROM t WHERE n = 5;
Project  (*)  (rows: 1)
  Filter  ((n = 5))  (rows: 1)         -- 1 / n_distinct (100)
    SeqScan t  (rows: 100)
```

### Stat shape

Per column:
- `n_distinct: u64` — distinct non-NULL values, for equality estimates.
- `null_count: u64` and `total_rows: u64` — `null_frac = null_count
  / total_rows`, for `IS NULL`.
- `histogram: Vec<HistogramBucket>` — 16 equi-depth buckets, each
  with `(lower, upper, count)`, for range queries.

Equi-depth means each bucket holds approximately the same row count;
bucket widths vary so the buckets stay even. A range estimate walks
the buckets — buckets fully on one side of the literal contribute
their entire count, the straddling bucket contributes a linear
interpolation. Standard textbook histogram.

NULLs are tracked separately via `null_count` and excluded from
the histogram. The histogram only describes non-NULL distribution,
matching SQL's three-valued `WHERE`: a comparison against NULL is
NULL, never TRUE, so NULL rows never satisfy `col > lit`.

### How the executor builds it

`analyze_table` is one full scan + per-column sort + bucket
construction:

```rust
let mut per_column: Vec<Vec<Value>> = vec![Vec::new(); column_count];
for (_, encoded) in tree.scan(pager)? {
    let record = codec::decode_row(&encoded, column_count)?;
    for (col_idx, value) in record.values.into_iter().enumerate() {
        per_column[col_idx].push(value);
    }
}

for (col_idx, values) in per_column.into_iter().enumerate() {
    let null_count = values.iter().filter(|v| matches!(v, Value::Null)).count() as u64;
    let mut non_null: Vec<Value> = values.into_iter()
        .filter(|v| !matches!(v, Value::Null)).collect();
    non_null.sort_by(|a, b| {
        codec::encode_index_value(a).cmp(&codec::encode_index_value(b))
    });
    let n_distinct = count_distinct(&non_null);
    let histogram = build_equi_depth(&non_null, 16);
    schema.columns[col_idx].stats = Some(ColumnStats { ... });
}
```

The sort uses the order-preserving byte encoding (`encode_index_value`)
that B+tree keys use. `Value` doesn't impl `PartialOrd` (NULLs and
cross-type comparisons make a sensible global order hard), but every
column has one declared type, so all non-NULL values in a column
share a type, and the byte encoding gives a total order within any
one type. Same comparison the B+tree uses, same one a future ORDER
BY on this column would respect.

Memory: O(table_rows × column_count) Values in flight during the
build. For v0.47 that's fine — small embedded DBs. A future version
could swap to streaming reservoir-sample histograms.

### How the planner consults stats

`selectivity()` gained an `Option<&Schema>` parameter. When `Some`
(single-table query), it looks up the column's stats:

```rust
fn sel_eq(left: &Expr, right: &Expr, stats: Option<&Schema>) -> Option<f64> {
    let (col, _literal) = orient_column_literal(left, right)?;
    let schema = stats?;
    let idx = schema.column_index(&col.name)?;
    let s = schema.columns[idx].stats.as_ref()?;
    let non_null = s.total_rows.saturating_sub(s.null_count);
    if non_null == 0 || s.n_distinct == 0 { return None; }
    let non_null_frac = non_null as f64 / s.total_rows as f64;
    Some((1.0 / s.n_distinct as f64) * non_null_frac)
}
```

The `(1 / n_distinct) * non_null_frac` formula scales by the
non-NULL fraction because NULL never satisfies `=` (three-valued
logic). A column where 30% of rows are NULL has 30% fewer rows that
could match any equality.

For range: walk the buckets, sum matching counts, linear-interpolate
the straddling bucket via byte-distance between `lower`, `upper`,
`literal` encoded keys. The interpolation walks the first byte
position where the three differ, takes the next 8 differing bytes,
treats them as a base-256 number, and computes the linear position.
Coarse for strings (byte-distance ≠ semantic distance, lexicographic
weirdness) but correct enough for histogram bucket-fraction.

Multi-table queries fall back to defaults: the column-ref → stats
lookup needs a per-scope schema map, which I'm deferring to v0.48+.
Joined query EXPLAIN looks the same as today.

### Catalog format bump PREHNDB8 → PREHNDB9

Each column entry now carries a tag byte (0 = no stats, 1 = stats)
and, if 1, the stats blob: three `u64`s (n_distinct, null_count,
total_rows), then a `u32` bucket count, then per bucket a
length-prefixed lower value, length-prefixed upper value, and a
`u64` count. The length-prefix lets the reader skip values without
knowing their per-type size.

Hard break, per project convention — opening a PREHNDB8 database
under v0.47 errors clearly.

### The interpolation gotcha

First version of the byte-distance interpolation just compared
length-equal bytes directly. That broke immediately for TEXT
columns where bucket lower/upper differ by more than one byte —
e.g. lower="aaa" upper="zzz" lit="mmm" — because comparing only
the first byte gives a useful answer, but comparing the next
position assumes lock-step which only holds when lengths agree.

Fix: take the first 8 differing bytes from each, pad with 0 if
short, treat as base-256 unsigned integers, divide. That handles
any string length difference up to ~8 bytes of variation; longer
suffixes shift the ratio by less than the typical bucket spread,
so we accept the imprecision. The point of a histogram is "what
fraction of rows match," not "the exact fraction down to the byte."

### What surprised me

How much existing infrastructure paid off:
- `encode_index_value` already gives order-preserving bytes for
  every type — perfect for the sort and the histogram bucket key.
- The catalog's `Catalog::put` already serialises schema mutations
  against concurrent writers via the catalog mutex — ANALYZE
  is just another `put` after the scan.
- `write_scope` for `ANALYZE` is naturally `Catalog`: it's a
  schema-changing operation that serialises with other DDL but
  not with per-table data writes.

The selectivity wiring was where most of the cycles went — adding
an `Option<&Schema>` parameter to `selectivity()` cascaded to
several call sites, and matching column references through the
SELECT path required care to avoid passing the wrong table's
stats for a join.

### Known limitations (documented)

- **Single-table queries only.** Multi-table joins fall back to
  defaults because the column-ref → stats lookup needs a per-scope
  schema map. v0.48+ could thread one.
- **Stats go stale on mutation.** Insert/Update/Delete don't
  invalidate or refresh stats. The user has to re-ANALYZE. Auto-
  analyze is a future feature.
- **No MCV (most-common-values) list.** A skewed column (one
  value covering 90%) gets a poor `=` estimate from
  `1 / n_distinct`. Postgres's MCV list captures the long tail
  separately; v0.47 doesn't.
- **No multi-column / functional-dependency stats.** Two-column
  predicates are estimated as `sel(p1) * sel(p2)` (independence
  assumption); correlated columns get wrong estimates.

### Numbers

- **269 tests** across the workspace (was 263; +6 ANALYZE integration
  tests). Suite runtime grew ~2 s (the new tests build histograms
  on 100-row tables).
- Touched: `sql/{ast,parser}.rs` (`Statement::Analyze`, top-level
  ANALYZE parser branch), `lib.rs` (`write_scope` recognises
  ANALYZE), `engine/schema.rs` (`ColumnStats`, `HistogramBucket`
  types, `Column.stats` field), `engine/codec.rs` (PREHNDB9 stats
  encoding/decoding with length-prefixed histogram values),
  `engine/planner.rs` (Plan::Analyze + validation), `engine/
  executor.rs` (analyze_table + helpers — full scan, sort, build
  histogram), `engine/explain.rs` (selectivity gained
  `Option<&Schema>`, three new sub-helpers for eq / range / IS NULL,
  byte-interpolation), `storage/pager.rs` (MAGIC bumped to PREHNDB9),
  `tests/integration.rs` (6 new ANALYZE tests).
- **On-disk format CHANGED**: PREHNDB8 → PREHNDB9. Existing v0.46
  databases fail to open with a clear "unrecognised file format"
  error. Recreate.
- Wire protocol unchanged.

## Session 48 — FK ON DELETE CASCADE / SET NULL (v0.48)

v0.45 shipped FOREIGN KEY with RESTRICT-only semantics: parent
DELETE refused if any child referenced it. v0.48 adds the two
other standard SQL referential actions: `ON DELETE CASCADE`
(delete the children too) and `ON DELETE SET NULL` (set the
child's FK column to NULL).

```sql
CREATE TABLE customers (id INT PRIMARY KEY);
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(id) ON DELETE CASCADE
);
-- DELETE FROM customers WHERE id = 1
--   → also deletes every order with customer_id = 1
```

### Scope

For v0.48: `ON DELETE {RESTRICT | NO ACTION | CASCADE | SET NULL}`.
Skipped: `ON UPDATE` actions (parent UPDATE of a referenced column
still always RESTRICTs), `SET DEFAULT` (no DEFAULT support yet).

RESTRICT remains the default; `NO ACTION` parses as RESTRICT (SQL's
two names for "refuse"). v0.45 databases that pre-date the format
bump won't open under v0.48 — every FK gets a fresh `on_delete`
action byte in the catalog, defaulting to RESTRICT when written
fresh. Hard format break.

### The dispatcher

The v0.45 `check_no_child_references` (single behaviour: error if
any child match) becomes `apply_parent_delete_actions` (dispatches
on each child FK's action):

```rust
match action {
    Restrict => {
        if scan_child_for_fk_value(...)? {
            return Err(...);
        }
    }
    Cascade => {
        // Build a synthetic DELETE on the child table, run it
        // through the engine's own execute path. The recursion is
        // free: each cascaded delete applies ITS own FK actions.
        let sql = format!("DELETE FROM {child} WHERE {col} = {value}");
        let plan = planner::plan(parse(&sql)?, pager, catalog)?;
        execute_streaming(pager, catalog, snapshot, plan)?;
    }
    SetNull => {
        if child_col.not_null {
            // SET NULL violates the child's NOT NULL → runtime error.
            if scan_child_for_fk_value(...)? {
                return Err(Error::exec("ON DELETE SET NULL ... violates NOT NULL"));
            }
        } else {
            let sql = format!("UPDATE {child} SET {col} = NULL WHERE {col} = {value}");
            let plan = planner::plan(parse(&sql)?, pager, catalog)?;
            execute_streaming(pager, catalog, snapshot, plan)?;
        }
    }
}
```

### Why dispatching via dynamic SQL is correct

Running CASCADE'd deletes through the engine's own DELETE path
(rather than directly manipulating the child B+tree) gives us
several properties for free:

1. **Recursion just happens.** The cascaded DELETE walks its own
   FK actions. A three-table chain A → B → C with `ON DELETE
   CASCADE` at both edges: deleting C recursively deletes its
   matching B rows, which recursively deletes *their* matching A
   rows. No special recursion code — each call is just another
   `delete()`.
2. **MVCC honored.** The cascade runs under the same snapshot, so
   visibility rules are consistent with the originating DELETE.
3. **SSI conflict tracking.** The cascaded writes record their
   own rw-edges with peer readers. If a concurrent transaction
   read the cascaded child rows, the cascade's writes turn that
   into a serialization conflict at commit time.
4. **WAL discipline.** Each cascade goes through the normal write
   path → WAL → fsync. Crash recovery handles partially-cascaded
   deletes the same way it handles any partial transaction.

The cost: building SQL strings and re-parsing them isn't the
fastest possible path. For v0.48 that's fine — CASCADE is rare
relative to plain INSERT/SELECT. A future v0.49+ could synthesise
a Plan directly without going through SQL text.

### The SET NULL + NOT NULL gotcha

SQL standard: `ON DELETE SET NULL` on a child column declared
`NOT NULL` is a conflict — the action would violate the column's
own constraint. Postgres raises this at CREATE TABLE; SQLite
raises at runtime. v0.48 raises at runtime, but only when an
actual match exists — if no child row points at the parent, the
SET NULL is a no-op and doesn't fire.

The runtime error message:
```
ON DELETE SET NULL on 'orders.customer_id' violates NOT NULL constraint
```

Surfaces both sides so the user can fix either the constraint or
the action.

### The pre-existing v0.43 bug I had to fix in passing

The first version of the SET NULL test failed with `duplicate key
value violates UNIQUE constraint '_pk_orders' on 'orders'`. Tracing:

- UPDATE child to set FK column NULL
- The UPDATE path: tombstone old row, INSERT new with fresh rowid
- `index_insert_row` on the new row, runs the v0.43 unique check
- Unique check scans the PK index for the row's `id` value
- Finds the OLD row's index entry (still physically present —
  MVCC tombstone doesn't touch index entries until VACUUM)
- Returns false-positive duplicate

The bug existed in v0.43 but no v0.43-or-later test happened to
UPDATE a non-PK column on a PK table. v0.48's SET NULL test does
exactly that — and tripped it.

Fix (small): `index_insert_row` got a sibling
`index_insert_row_with_old(values, old_values)` that skips the
unique check on any index whose column values match between old
and new. UPDATE calls the new helper; INSERT keeps the v0.43
behaviour. The fix lives next to the constraint check; about 20
lines.

### Catalog format bump PREHNDB9 → PREHNDB10

The previous bumps used ASCII digits in byte 7 of the magic:
`PREHNDB7`, `PREHNDB8`, `PREHNDB9`. v0.48 would have been
`PREHNDB10` — but the magic is fixed at 8 bytes, and "PREHNDB10"
is 9. The encoding had to change.

I switched to: first 7 bytes always `"PREHNDB"`, last byte the
version as a raw `u8`. So v0.10 is `b"PREHNDB\x0a"`, v0.11 would
be `b"PREHNDB\x0b"`, and so on. 256 versions of headroom before
we'd need to revisit. The hard break is the same as any other
format bump — every previous database fails to open with a clear
error.

### Numbers

- **274 tests** across the workspace (was 269; +5 new FK action
  integration tests).
- Touched: `sql/{ast,parser,token}.rs` (CASCADE/NO/ACTION/RESTRICT
  keywords, `ReferentialAction` enum, `ON DELETE` parser branch),
  `engine/schema.rs` (`ForeignKeyAction` + `on_delete` field on
  `ForeignKeyTarget`), `engine/codec.rs` (PREHNDB10 encoding +
  `fk_action_tag` helpers), `engine/planner.rs` (carries the
  action from AST to Schema), `engine/executor.rs` (renamed
  helper into `apply_parent_delete_actions` + cascade/SET NULL
  via synthesized SQL; pre-existing v0.43 unique-check bug fixed
  via `index_insert_row_with_old`), `storage/pager.rs` (MAGIC
  bumped to PREHNDB10 with the new byte-version scheme),
  `tests/integration.rs` (5 new FK action tests).
- **On-disk format CHANGED**: PREHNDB9 → PREHNDB10. Magic encoding
  changed too (was `PREHNDB<ascii-digit>`, now
  `PREHNDB<raw-u8-version>` — 256 versions of room before the next
  encoding shift). v0.47 databases fail to open with a clear
  "unrecognised file format" error. Recreate.
- Wire protocol unchanged.

## Session 49 — Auto-analyze on mutation (v0.49)

v0.47 added column statistics; v0.48 closed the FK story. The
biggest open hole from v0.47's deep dive: stats go stale on
mutation. Every INSERT/UPDATE/DELETE shifts the n_distinct,
the null_count, the histogram — but the catalog's stats keep
the old picture until the user remembers to re-`ANALYZE`. v0.49
closes that loop by triggering ANALYZE in the background when a
table has mutated enough to warrant it.

### The trigger

Per-table `mutations_since_analyze: u64` on Schema, bumped on
every INSERT (by inserted rows), UPDATE (by updated rows),
DELETE (by deleted rows), and reset to 0 on each ANALYZE
completion. When `mutations > 50 + 0.10 * row_count_at_last_analyze`
the table is "stale enough" to warrant re-analysis — exactly
Postgres's `autovacuum_analyze_threshold` formula. Tiny tables
need 50 mutations to fire (so a fresh table's first 50 rows
don't waste cycles); larger tables need proportionally more.

### Where it runs

The v0.36 reclaimer thread in `prehnited`. Already had a per-tick
loop calling `reclaim_dead_rows`; now also calls a new
`Database::auto_analyze_pass()` after the reclaim. The pass walks
the catalog, finds the first stale table, runs `ANALYZE` on it,
returns. At most one ANALYZE per tick — repeated ticks walk
through queued tables one at a time.

The reclaimer's `Database` handle is a normal `Database::open_shared`
on the same SharedPool + TxState every connection uses, so its
ANALYZE goes through the catalog mutex like any other write —
serialising with concurrent INSERTs (briefly, only when
`catalog.put` lands).

```rust
pub fn auto_analyze_pass(&mut self) -> Result<Option<String>> {
    let names = self.catalog.table_names(&mut self.pager)?;
    for name in names {
        let Some(schema) = self.catalog.get(&mut self.pager, &name)? else {
            continue;
        };
        let threshold = 50 + (schema.row_count as f64 * 0.10) as u64;
        if schema.mutations_since_analyze > threshold {
            let sql = format!("ANALYZE {name}");
            self.execute(&sql)?;
            return Ok(Some(name));
        }
    }
    Ok(None)
}
```

### What surprised me

The thinness of the diff. Total: ~30 engine lines (counter
field, encode/decode, three increment sites, one reset site),
~25 lines for the new `auto_analyze_pass`, 8 lines in the
reclaimer for the per-tick call, ~140 lines of integration
tests. Everything else was already in place: the v0.47 ANALYZE
machinery, the v0.36 reclaimer thread, the catalog's serialised
`put`. Auto-analyze is the smallest possible bridge between
existing pieces.

The other surprise: the counter doesn't need separate locking.
Every place that bumps it (INSERT/UPDATE/DELETE) is already
inside the table's RwLock and inside the catalog mutex via
`catalog.put`. The reclaimer's read+ANALYZE goes through the
same. No new synchronisation primitives, no race window — the
existing locks already covered it.

### Why the counter survives close+reopen

The counter is a `Schema` field, encoded in the catalog blob
alongside `row_count`, persisted by `catalog.put` like every
other schema change. On reopen, `decode_schema` reads it back.
A crash mid-INSERT might lose the counter increment for the
in-flight transaction (the catalog write rolls back with the
rest), but that's fine — the rows themselves also rolled back,
so the mutation count and row count both stay accurate.

### Why one-table-per-tick, not all stale tables

If two tables both went stale in the same tick (a batch INSERT
across them, say), the reclaimer ANALYZEs the first and waits
until next tick for the second. Two reasons:
1. **Latency cap.** ANALYZE is a full scan; doing N back-to-back
   could pin the reclaimer thread for seconds on a large schema.
   One per tick caps the per-iteration work.
2. **Spread the catalog mutex pressure.** ANALYZE serialises
   with foreground writers at the catalog `put` step. Spreading
   keeps foreground latency smooth.

For v0.49 the reclaimer tick is `RECLAIM_INTERVAL = 1s`, so a
schema with 10 stale tables analyzes them over 10 seconds. Fine
for v0.49; future could batch.

### Known limitations

- **No suppression for big single-table ANALYZEs.** A 100M-row
  table's ANALYZE could pin the reclaimer for many seconds.
  Future: sampling instead of full scan, or yield-during-scan.
- **No urgency tiers.** A table at 2× threshold gets the same
  priority as one at 1.01×. Postgres-style ordering by relative
  staleness is a future refinement.
- **No way to opt out.** Some workloads (truly static lookup
  tables) don't need re-analysis. v0.49 always re-analyses.
  Future: `ANALYZE` flag on the table, or per-table threshold.

### Numbers

- **279 tests** across the workspace (was 274; +5 auto-analyze
  integration tests). Test suite ~3s longer.
- Touched: `engine/schema.rs` (`mutations_since_analyze` field),
  `engine/codec.rs` (PREHNDB11 encoding adds one u64),
  `engine/executor.rs` (three increment sites + ANALYZE reset),
  `engine/database.rs` (new `auto_analyze_pass`),
  `storage/pager.rs` (MAGIC bumped to PREHNDB11),
  `prehnited/src/lib.rs` (reclaimer thread calls the pass),
  `tests/integration.rs` (5 new tests).
- **On-disk format CHANGED**: PREHNDB10 → PREHNDB11. Existing
  v0.48 databases fail to open with a clear "unrecognised file
  format" error. Recreate.
- Wire protocol unchanged.

## Session 50 — Parallel intra-query execution (v0.50)

v0.42 added group commit, which got concurrent *writers* sharing one
fsync. Every other operator has stayed single-threaded — every
SELECT, however big, has driven one CPU core. v0.50 changes that for
the simplest and highest-impact shape: a single full-table scan with
optional filter + projection + LIMIT. Worker threads scan partitions
of the table in parallel; the receiver drains them in order.

### Scope choice

For v0.50: only "scan-shape" SELECTs — single FullScan, no joins,
no GROUP BY / aggregates / ORDER BY / HAVING, no correlated
subqueries. Any of those gates kicks back to the existing serial
or vectorised path. The simplest version that still wins on the
heaviest queries.

What I considered and rejected:
- **CPU-only-parallel decode**, where a single coordinator does the
  page reads and ships raw bytes to workers. Cleaner architecturally
  (no peer Pager construction) but doesn't parallelise I/O. For a
  database the I/O latency is often the bottleneck — defeats the
  point.
- **Materialise + parallel**, where the whole result is collected
  into a Vec, then chunks are processed in parallel. Loses
  streaming. A `LIMIT 1` over a million-row scan would buffer
  everything before clipping.

The chosen approach: per-worker peer Pagers via `SharedPool`. Each
worker independently scans a key range using the standard
`BTree::cursor` API.

### The architecture

```
                                  ┌─ worker 0 [None, K1)   ──► chan 0 ─┐
                                  ├─ worker 1 [K1,   K2)   ──► chan 1 ─┤
main thread (coordinator) ────────┼─ worker 2 [K2,   K3)   ──► chan 2 ─┼─► drain
  ▪ leaf_pages(pager)             ├─ worker 3 [K3,   K4)   ──► chan 3 ─┤   in order
  ▪ sample boundary keys          └─ ...                              ─┘   for the
  ▪ spawn N workers                                                       caller
  ▪ build RowStream
```

One channel per worker so the receiver drains them in worker-index
order (which equals key order). A `sync_channel(64)` bounds memory
— a fast worker doesn't race ahead of the receiver and balloon the
queue.

### Why per-worker channels preserve order

The receiver pulls from `receivers[current]` until it returns
`Err(_)` (worker disconnect), then advances to `current + 1`. Since
worker `i`'s key range is strictly less than worker `i+1`'s (we
partitioned by leaf first-keys), the resulting stream is byte-for-
byte identical to a serial scan. Workers still run concurrently —
worker 3 can be churning at the same time as worker 0 — only the
*consumption* serialises by index. A slow worker `i` does back up
the readers of channels `i+1..N` (their channels fill up, sends
block), but the workers' CPU work proceeds in parallel.

That ordering property turned out to be critical. My first version
used a single shared channel with workers racing; existing tests
that called `SELECT n FROM t` without ORDER BY assumed key order
and failed when rows came back interleaved. Real SQL says SELECT
without ORDER BY has undefined order, but in practice every test
assumes the natural ordering. Preserving it kept the parallel path
a drop-in replacement.

### How partitioning works

```rust
let leaves = BTree::open(schema.root).leaf_pages(pager)?;
if leaves.len() < MIN_LEAVES_FOR_PARALLEL { return Ok(None); }

let n_workers = available_parallelism().min(8).min(leaves.len());
let chunk_size = (leaves.len() + n_workers - 1) / n_workers;
let mut boundaries: Vec<Option<Vec<u8>>> = vec![None];
for i in 1..n_workers {
    let leaf_idx = i * chunk_size;
    if leaf_idx >= leaves.len() { break; }
    if let Some(key) = BTree::open(schema.root).leaf_first_key(pager, leaves[leaf_idx])? {
        boundaries.push(Some(key));
    }
}
boundaries.push(None);
```

`leaf_pages` (new in v0.50) walks the leaf chain via `right_link`s
to collect every leaf's page number. `leaf_first_key` reads one leaf
to get its first key. We sample N-1 boundary keys; each worker gets
`[boundaries[i], boundaries[i+1])`. The first worker starts at
`None` (table start); the last ends at `None` (table end).

### The Pager refactor

Each worker thread needs its own `Pager` because `Pager` holds
`&mut self`-style state (WAL index, dirty-page tracking). I added
two accessors:

- `Pager::pool() -> SharedPool` — cloneable
- `Pager::path() -> &Path` — newly-stored field

`Pager::shared_meta()` already existed (for `Database::open_shared`).
Each worker calls `Pager::open_shared_with_meta(path, pool, meta)`
to get a peer Pager onto the same file via the shared pool +
header. Peer Pagers each get their own per-pager WAL file
(automatically cleaned up on drop) and share the buffer cache.

The path field is small (one `PathBuf`) but it was a missing piece
— the Pager opened a file then forgot where it came from. v0.50
gives it a memory.

### Worker thread body

```rust
thread::spawn(move || {
    let mut worker_pager = Pager::open_shared_with_meta(&path, pool, meta)?;
    let tree = BTree::open(table_root);
    let mut cursor = tree.cursor(&mut worker_pager, start.as_deref(), end)?;
    loop {
        if stop.load(Ordering::Acquire) { return; }
        let (_, encoded) = match cursor.next(&mut worker_pager)? {
            Some(e) => e,
            None => return,
        };
        let record = codec::decode_row(&encoded, column_count)?;
        if !snapshot.visible(record.tx_min, record.tx_max) { continue; }
        if let Some(f) = &filter {
            if eval(f, Some(&RowContext { scope: &scope, values: &record.values }))?
                != Value::Bool(true) { continue; }
        }
        let projected = plain.iter().map(|&i| record.values[i].clone()).collect();
        if tx.send(projected).is_err() { return; }
    }
});
```

Each worker:
1. Opens its peer Pager.
2. Walks the standard B+tree cursor over its key range.
3. Decodes each row, checks MVCC visibility against the shared
   `Snapshot`, applies the filter, projects the columns.
4. Sends matched rows to its per-worker channel.

Visibility uses the same `Snapshot.visible(tx_min, tx_max)` the
serial `TableScan` uses. `Snapshot` is `Clone` and `Send` — every
worker gets a clone.

### LIMIT short-circuit

The `RowStream` drops the `ParallelSource` when the caller stops
pulling. The Drop impl sets `stop.store(true)`. Workers check
`stop` between rows; on `true`, they return — channels close,
peer Pagers drop, WAL files clean up.

The Drop trigger means even an early-error `?` in the caller
walks the cleanup path. Critical for not leaving worker threads
spinning on closed file descriptors.

### What the workers don't do

- **SSI relation lock**: the main thread takes this once eagerly
  before spawning workers. Letting workers take it independently
  would record N read-set entries instead of one — wrong for the
  SSI conflict-cycle math.
- **Correlated subquery evaluation**: a correlated filter needs
  to re-execute a query per outer row. The per-worker `Snapshot`
  is `Send`, but plumbing a `Catalog` reference into worker
  threads (the Filter operator needs it for subquery resolution)
  is more refactor than v0.50 wanted. Gate kicks parallel back
  for these.
- **Aggregation**: each worker would compute a partial aggregate,
  the main thread would combine them. v0.50 just punts —
  aggregated queries take the existing vectorised hash-aggregate
  path, which is already fast.

### What this stresses about existing infrastructure

- **SharedPool isolation**: workers share the buffer cache but
  each has its own `Pager`-level state (WAL index, dirty pages).
  No worker dirties pages — they're read-only — so the per-pager
  WAL files stay empty.
- **Per-page latches**: v0.30's `latch(no)` calls from N threads
  scanning their own leaves never collide (different leaves) and
  serialise cleanly when they do (same internal node during
  cursor descent).
- **MVCC across pagers**: each worker reads from the same shared
  `Snapshot`. The `Clog` reads atomic `tx_id` status; no
  cross-pager contention.

### What surprised me

How little of the engine needed changing. Two new pager accessors
(`path()`, `pool()`), one new BTree method (`leaf_first_key`), and
~180 lines of `try_parallel_scan` are the entire feature. The
per-worker channels were the architectural insight that made
order-preservation free — once channels-per-worker drained in
order, the rest fell out.

The other surprise: how much v0.13 (SharedPool), v0.27
(per-connection Pagers), and v0.30 (per-page latches) paid off.
Every primitive for safe parallel reads was already in place;
this session just put them together.

### Known limitations

- **Single-table only**: joins fall back to serial. Parallel
  joins would partition by hash, then merge per-partition — a
  much bigger lift.
- **Aggregation falls back**: covered above.
- **The threshold is fixed**: `MIN_LEAVES_FOR_PARALLEL = 16`. A
  future version could pick adaptively based on the table's
  `row_count` stat or per-row cost estimate.
- **No vectorised + parallel**: vectorised batch operators don't
  yet thread through the channel. Parallel scan uses the
  row-at-a-time path.

### Numbers

- **284 tests** across the workspace (was 279; +5 parallel-scan
  integration tests). Suite ~90s longer because the new tests
  insert 2000 rows in setup loops (per-row INSERT cost dominates).
- Touched: `storage/btree.rs` (`leaf_pages`, `leaf_first_key`),
  `storage/pager.rs` (`path` field + `path()` + `pool()`
  accessors), `engine/executor.rs` (new `RowSource::Parallel`
  variant, `ParallelSource` struct, `try_parallel_scan` and
  `build_parallel_scan`, dispatch gate in `select()`),
  `tests/integration.rs` (5 new tests). README + DEEP_DIVE.
- On-disk format **unchanged** (`PREHNDB11`). Wire protocol
  unchanged. v0.49 databases open cleanly under v0.50.

## Session 51 — Parallel hash joins (v0.51)

v0.50 parallelised single-table scans. v0.51 takes the same
infrastructure to two tables. A single `INNER JOIN` with an
equi-predicate over a large outer + small-or-medium inner — the
classic star-schema fact-to-dimension shape — now runs in parallel.

```sql
SELECT orders.id, users.name FROM orders
INNER JOIN users ON orders.uid = users.id;
```

If `orders` has ≥ 16 leaf pages, this dispatches to the new path:
build the `users` hash table once on the main thread, broadcast it
via `Arc`, partition `orders` across N workers, each worker probes
the shared hash and emits combined rows to its per-worker channel.
The receiver drains workers in index order, so output preserves
outer-table key order — same property v0.50 gives.

### Architecture: broadcast inner, partitioned outer

```
main thread                      ┌──► chan 0 ─┐
  ▪ build inner hash table       ├──► chan 1 ─┤
  ▪ partition outer leaves    ───┼──► chan 2 ─┼─► drain in order
  ▪ spawn N workers             ┤                  for the caller
  ▪ each gets Arc<HashTable>     └──► chan 3 ─┘

worker i:
  ▪ open peer Pager via SharedPool
  ▪ cursor [boundary_i, boundary_{i+1}) over outer
  ▪ for each outer row:
      ▪ MVCC visibility check
      ▪ apply outer-only filter (cheap, before probe)
      ▪ hash-probe shared inner table
      ▪ for each match: combine, eval ON, project, send
```

"Broadcast" means the inner side isn't partitioned — every worker
sees the whole hash table. This wins when the inner is small
relative to the outer (the star-schema case: dimension joined to
fact). For big-on-big joins, the next step (a partitioned hash
join — v0.52+) would hash-partition both sides so each worker only
holds 1/N of the inner.

### What gates the parallel join

```rust
if from.joins.len() == 1
   && join.kind == Inner
   && find_equi_join(on, ...).is_some()
   && !predicate_has_correlated(on)
   && filter_uses_only_outer_columns
   && outer.leaf_count >= 16
   && no GROUP BY / aggregate / ORDER BY / HAVING { ... }
```

Each gate maps to a complication the simplest version doesn't
handle. WHERE filters that reference inner columns would need to
go after the probe (and after the row is combined) — possible but
not in v0.51. Multi-table joins (`a JOIN b JOIN c`) chain
hash-probes — a future version could parallelise the outermost
scan and keep the deeper joins serial. ORDER BY needs a merge.

### Why the outer-only filter is pushed before probe

Workers apply the WHERE clause to the outer row *before* hashing
the join key. Saves the hash + lookup cost for filtered-out rows.
The gate `expr_uses_only_left_columns` proves the filter doesn't
depend on the inner side, so the per-row scope reduces to outer.

### Order preservation

Each worker scans its outer key range in ascending key order
(B+tree cursor invariant). For each outer row, matches come back
in the order they appear in the inner hash bucket (insertion
order from the build). So worker `i`'s output is: row_outer_i_1's
matches, row_outer_i_2's matches, ... in outer key order. The
receiver concatenates worker 0's output, then worker 1's, then
... — preserving global outer order.

Same property the serial `HashJoin` operator has (it iterates
outer left-to-right, probes per row). The parallel version
matches byte-for-byte.

### The hash table is shared, read-only

`Arc<HashMap<Vec<u8>, Vec<Vec<Value>>>>` — keyed by encoded join
value, valued by the list of inner rows with that key. Workers
clone the Arc (cheap), call `.get(key)` (read-only HashMap
access). No locking; `HashMap` is `Sync` for reads. The build
phase finishes before any worker spawns, so there's no read/write
contention.

### Scratch buffer reuse

Each worker keeps one `Vec<Value>` "combined" buffer, capacity
sized to `outer_width + inner_width` once. Each match clears and
re-fills it instead of allocating fresh — measurable improvement
for high-fanout joins where one outer row matches many inner rows.

```rust
let mut combined: Vec<Value> = Vec::with_capacity(outer_width + inner_width);
loop {
    combined.clear();
    combined.extend_from_slice(&outer_row);
    combined.extend_from_slice(&inner_row);
    // eval ON, project, send
}
```

### What surprised me

How much v0.50 set us up. The Pager peer-open, per-page latches,
per-worker channels, `stop` AtomicBool with Drop cleanup, even
the `ParallelSource` struct — all already in place. The new code
is one ~200-line function (`try_parallel_hash_join`) plus a
30-line `build_inner_hash_table` helper plus a 25-line
`expr_uses_only_left_columns` walker.

The dispatch gate is a 16-line match-shape check. The whole
feature is ~300 lines of executor changes.

### Known limitations

- **Single join only**: `a JOIN b JOIN c` falls back.
- **INNER only**: LEFT JOIN / CROSS JOIN / Semi/Anti use serial.
- **Outer-only WHERE**: a filter referencing inner columns gates back.
- **Inner builds single-threaded**: big inner tables still spend
  build time serially. v0.52+ partitioned join would parallelise
  the build.
- **No spilling**: the inner hash lives entirely in memory. Huge
  inner tables would benefit from a parallel grace-hash variant.

### Numbers

- **288 tests** across the workspace (was 284; +4 parallel-join
  integration tests). The `group_commit_handles_concurrent_writers_durably`
  test flakes intermittently under heavy parallel test load —
  passes reliably in isolation and in serial test runs. Test-load
  artifact, not a v0.51 regression.
- Touched: `engine/executor.rs` (added `expr_uses_only_left_columns`,
  `try_parallel_hash_join`, `build_inner_hash_table`, dispatch
  gate in `select`). `tests/integration.rs` (4 new tests covering
  the matches-serial, outer-filter, LIMIT, and empty-inner cases).
  README + DEEP_DIVE.
- On-disk format **unchanged** (`PREHNDB11`). Wire protocol
  unchanged. v0.50 databases open cleanly under v0.51.

## Session 52 — Fix the v0.42 lost-write race (v0.52)

v0.51 surfaced an intermittent durability bug that v0.42's group
commit had been carrying since it shipped. v0.52 hunts and fixes
it. No new feature; a real correctness repair.

### The symptom

`group_commit_handles_concurrent_writers_durably`: 16 writer
threads × 25 INSERTs each = 400 expected rows. Failed ~⅓ of runs
with anywhere from 1 to 250+ rows missing. Every failed run
showed the same pattern: each writer was losing its TAIL — the
first ~10–17 of every writer's 25 inserts survived, the rest
vanished.

Crucially, every insert's `db.execute(...)` returned `Ok`. So 400
inserts ack'd as durable, but the final SELECT saw far fewer.

### The diagnosis

The tail pattern was the smoking gun. As each writer pushed
INSERTs, the B+tree allocated new pages: `shared_meta.page_count`
grew. The earlier inserts went to pages within range of the
file's current `page_count`; the later ones to pages beyond.

The race had two distinct prongs, both involving the per-pager
WAL applies racing on the file:

**1. Header staleness.** Each commit did `write_page(0,
encode_header(self.shared_meta.snapshot()))` at start. By the
time `wal.apply` actually wrote that page 0 to disk, a peer's
allocation might have bumped `shared_meta.page_count`, but our
WAL still carried the older snapshot. If our apply ran after
the peer's, the file ended up with the older `page_count`,
making peer-allocated pages unreachable.

**2. Data-page interleave.** Two writers modifying the same
leaf L: A latches L, modifies, drops; B latches L, modifies on
top of A's changes, drops. A's `flush_own_dirty` reads the pool's
L (after B's modifications) but if A's flush ran *before* B's
modification, A's WAL carries the pre-B L. If A's apply ran
after B's, A wrote the pre-B L over the file — losing B's row.

Both prongs share the same root: **per-pager WALs are written
independently and applied in whichever order races dictate**.
The later-applied WAL wins on the file's bytes regardless of
which commit completed last logically.

### The fix

Serialise the entire commit's WAL flush + apply + header write
under the `shared_meta` mutex. One pager at a time runs the
"durable" portion of commit; peer pagers' allocations and peer
commits wait their turn.

```rust
// Old: separate flush, apply, header writes — all racing.
self.write_page(0, encode_header(self.shared_meta.snapshot()))?;
self.flush_own_dirty()?;
self.wal.seal()?;
self.wal.apply(&mut self.file)?;

// New (v0.52): one atomic transaction under the shared meta lock.
self.dirty_pages.remove(&0); // page 0 written direct, not via WAL
self.shared_meta.commit_apply(&mut self.file, |file| {
    // Under the lock:
    for &no in dirty {
        if wal_index.contains_key(&no) { continue; }
        let frame = pool.get(no).ok_or(...)?;
        wal.append_page(no, &frame.page)?;
    }
    wal.seal()?;
    wal.apply(file)?;
    Ok(())
})?;
// commit_apply also writes the fresh header to the file after apply.
```

`commit_apply` (a new method on `SharedMeta`) takes the meta
mutex, runs the apply closure, then writes the latest `Meta` to
page 0 directly + fsyncs. The header bytes are encoded AFTER apply,
so they reflect every allocation that landed in this commit (and
any peer's that landed before).

### Trade-offs

The fix serialises commit applies + the entire flush phase. Peer
pagers' allocations block waiting on the meta mutex during a
commit's apply. For the test workload (16 writers × 25 inserts),
this slowed the test from ~700 ms to ~6 s — 8× — but it's now
100% reliable.

For real workloads the cost is fsync wait time × N concurrent
writers, instead of fsync wait time. v0.42's group commit (the
clog write) still batches, so the per-statement commit cost is
unchanged for the clog. The new serialisation only affects the
WAL+header apply phase, which is brief on modern storage.

A future v0.53+ could split the meta mutex from the
apply-serialisation lock so allocations don't block during apply.
For v0.52 the simple fix is correct and shippable.

### Why this wasn't caught earlier

v0.46 added the concurrent crash-recovery test, but that test
runs in a separate process (under SIGKILL) and reads "logged
ids" from a file the worker fsyncs after every insert. The
*log* tracks what the worker thinks succeeded; the *DB* tracks
what actually landed. The crash test passes if logged ⊆ DB —
it doesn't care if some inserts succeeded without being logged.

The bug here is the opposite: inserts succeeded (ack'd to the
test) but weren't actually in the DB. The crash test couldn't
catch that asymmetry.

v0.51's stress (concurrent test load from the new parallel
tests) amplified the race window enough to surface it
reliably in the v0.42 test that had quietly been passing in
isolation.

### What surprised me

How obvious the pattern was once the test reported missing
values. Each writer losing its tail screamed "allocation
race". The fix took less code than the investigation took:
~40 lines of pager.rs + ~30 lines of SharedMeta.

The other thing: how long this had been latent. v0.42 shipped
in session 42, and the bug rode along through v0.43–v0.50 (10
sessions) without triggering a failure that anyone noticed.
That's the cost of fast-iterating without aggressive concurrent
stress; the race window was just narrow enough to skate by.

### Numbers

- **288 tests across the workspace** (unchanged from v0.51, no
  new tests — the existing flaky one passes reliably now).
  Test suite slowed by ~9 seconds on the integration suite from
  the new serialisation, but no flakes across 10 consecutive runs.
- Touched: `storage/pager.rs` — `SharedMeta::commit_apply`
  (new, ~30 lines), `Pager::commit` (rewritten to use it,
  ~40 lines). Test: tightened
  `group_commit_handles_concurrent_writers_durably` to print
  missing values on failure for future diagnosis.
- On-disk format **unchanged** (`PREHNDB11`). Wire protocol
  unchanged. v0.51 databases open cleanly under v0.52.

## Session 53 — Split apply lock from meta lock (v0.53)

v0.52 fixed the lost-write race with one big hammer: hold the
single `shared_meta.inner` mutex across the entire commit's
flush + WAL apply + header write. Correct, but it also blocks
allocations (which use the same mutex) for the apply duration.
On a write-heavy workload, allocators serialise behind commits.

v0.53 splits the lock. The apply phase gets its own dedicated
mutex; allocations keep using the fast meta lock and no longer
wait for commits.

### The split

```rust
pub struct SharedMeta {
    inner: Arc<Mutex<SharedMetaInner>>,  // existing: page_count,
                                          //  freelist_head,
                                          //  catalog_root,
                                          //  next_tx_id
    apply_lock: Arc<Mutex<()>>,           // v0.53: serialises commits only
}
```

`apply_lock` is taken at the start of `commit_apply` and held
until the function returns. While held, peer commits queue but
peer allocations don't (they take `inner`, not `apply_lock`).

The header write inside `commit_apply` briefly takes `inner` to
snapshot the latest `Meta` and encode it. That brief acquisition
ensures the snapshot is consistent with the meta state at the
moment of the encode — no allocator can race in between
snapshot and write.

### Race-correctness argument

After v0.53, a peer allocator that bumps `meta.page_count`
during our commit's apply phase falls into one of two cases:

1. **Allocator finishes before our header snapshot.** Our
   header writes the latest meta, including the peer's bump.
   File has the peer's bump on disk.
2. **Allocator finishes after our header snapshot.** Our header
   writes the older meta (without the peer's bump). The peer's
   own next commit captures the bump via the same `commit_apply`
   path. Eventually the bump lands.

Case 2 leaves a window where the in-memory `shared_meta` has
the peer's bump but the on-disk header doesn't. That window
closes the next time the peer commits. Until then, the
peer's allocated pages are reachable in-process (the peer
wrote to them via its own pool) but not durable. That's
exactly the v0.42+ invariant: a page is durable only after the
allocating writer's commit, not at allocation time.

The lost-write race fixed in v0.52 is *not* re-opened. The
v0.52 race was: two pagers' applies racing on the file, with
the later-applied carrying an older snapshot. The `apply_lock`
prevents that interleave just as well as v0.52's broader
mutex did.

### What this restores

Throughput under write-heavy concurrent workloads. v0.42's
group commit batches the clog fsync; v0.52 forced the WAL
apply to serialise; v0.53 lets allocators continue while
commits apply. The full picture:

| Stage | v0.42 | v0.52 | v0.53 |
|---|---|---|---|
| Clog append + fsync | batched (group commit) | batched | batched |
| WAL apply | concurrent (RACY) | serial via meta lock | serial via apply lock |
| Allocations | concurrent via meta lock | blocked during apply | concurrent via meta lock |

v0.53 reaches the right point: the inherently-serial parts
(WAL apply, header write) serialise; the cheap-and-frequent
parts (allocations, page lookups) stay parallel.

### Trade-offs

The split-lock pattern requires care with lock ordering.
`commit_apply` takes `apply_lock` *outside* `inner`. Allocators
take `inner` alone. No call site takes `inner` then tries to
take `apply_lock` — that ordering would deadlock with a
commit holding apply_lock and trying to take inner inside.

In practice the codebase only takes `inner` via the existing
`SharedMeta::lock()` helper, and `commit_apply` is the sole
caller of `apply_lock`. The two are layered cleanly: apply
outside, meta inside.

### What surprised me

How small the fix is — 10 lines (one new `apply_lock` field,
two extra lock calls in commit_apply, one in the constructor).
The investigation work was in v0.52 (finding the race);
restoring throughput is mechanical once the race is properly
understood.

### Numbers

- **288 tests across the workspace** (unchanged from v0.52).
  All 10 consecutive runs of the v0.42 group-commit test pass.
  Integration suite duration ~144s, same as v0.52 — the
  allocator-block removal helps tests that allocate heavily
  during commit (none in the current suite). Production
  workloads with high page-allocation rates under concurrent
  writers benefit most.
- Touched: `storage/pager.rs` — `SharedMeta` gets an
  `apply_lock` field, `commit_apply` rewritten to take
  apply_lock outside + meta lock briefly inside for the
  header snapshot.
- On-disk format **unchanged** (`PREHNDB11`). Wire protocol
  unchanged. v0.52 databases open cleanly under v0.53.

## Session 54 — Bind parameters (v0.54)

After 53 sessions of formatting values into SQL strings, v0.54 adds
the standard SQL escape hatch: `?` placeholders bound at execute
time. Library API only this session; wire-protocol Prepare/Execute
frames are v0.55+ work.

```rust
db.execute_with_params(
    "SELECT name FROM users WHERE id = ? AND active = ?",
    &[Value::Int(42), Value::Bool(true)],
)?;
```

A user-supplied string bound as a `Value::Text` parameter is routed
to evaluation, never to the parser — no SQL injection vector.

### Three small parts

**Token + AST.** New `Token::Question` (the `?` character). The
parser tracks a `placeholder_count` on its `Parser` struct; each
`?` it sees in expression position becomes `Expr::Placeholder(idx)`
where `idx` is the count at the time of consumption. Auto-numbered
0-based, left-to-right within one statement.

**Bind walker.** A new `engine::bind` module with `bind_plan(plan,
params)` that recursively rewrites every `Placeholder(i)` in the
Plan tree into the literal `Expr::Integer` / `Real` / `Str` /
`Bool` / `Null` for `params[i]`. Walks all Expr positions:
WHERE/HAVING filters, INSERT VALUES rows, UPDATE assignments,
join ON predicates, projection expressions, and any nested
subquery's filter/projection too. Arity mismatch (`params.len()`
< placeholder count) is a plan-time error.

**Database API.** `Database::execute_with_params(sql, &[Value])`
parses, plans, binds, then routes through the existing
`run_plan`. The original `execute(sql)` becomes a thin wrapper
calling `execute_with_params(sql, &[])`.

The executor never sees `Expr::Placeholder` — the bind step
substitutes them away before plan execution. The existing
`eval`/`eval_batch`/`prepare_subqueries` paths get one extra
match arm (`Placeholder => unreachable!`) just to satisfy
exhaustiveness; in practice the unreachable never fires.

### Why bind after plan rather than at parse

The planner's work — validation, join reordering, access-path
selection — doesn't depend on parameter values. Doing one
parse+plan and many binds is the foundation for true prepared
statements: a v0.55+ `Prepare` step would parse and plan once,
cache the Plan, and each `Execute` call would clone + bind +
run. v0.54 does the bind step alone, and the same machinery
will serve when wire-protocol Prepare lands.

### Error: arity mismatch

```rust
db.execute_with_params("SELECT n FROM t WHERE n = ?", &[]).unwrap_err();
//  Err(Exec("bind: placeholder $1 has no matching parameter (got 0 params)"))
```

Caught before execution. The placeholder count is known after parse;
we check it against `params.len()` at the start of the bind walk.
Extra params are silently ignored — a future version could
require strict arity if that turns out to be a frequent footgun.

### EXPLAIN renders placeholders

`Expr::Placeholder(idx)` renders as `?N` (1-indexed for human
readability) in EXPLAIN output, so `EXPLAIN SELECT n FROM t
WHERE n = ?` shows `Filter ((n = ?1))`. After bind, the literal
shows up directly. Both code paths exercised in the formatter.

### Three subtler properties

1. **NULL as a parameter follows SQL three-valued logic.** A
   bound `Value::Null` substitutes as `Expr::Null`. The WHERE
   predicate `n = ?` with `?` bound to NULL evaluates to NULL
   (never TRUE), so zero rows match. Test
   `bind_null_param_follows_three_valued_logic` pins this.
2. **Subqueries inside expressions are walked.** A `WHERE x IN
   (SELECT ... WHERE y = ?)` binds the `?` inside the subquery's
   filter. The bind walker recurses through `Expr::InSubquery`,
   `Expr::Exists`, `Expr::ScalarSubquery` and their correlated
   forms.
3. **A statement with zero placeholders binds cheaply.** Empty
   `params` slice + zero-occurrence walk = O(plan size) but no
   allocations. The plain `execute(sql)` is now `execute_with_params(sql,
   &[])` with no measurable overhead.

### What surprised me

How small the diff was once the AST grew the new variant. The
bind walker is ~150 lines including comments, the parser change
is two lines, the Database method is one method body. Most of
the work was hitting every existing `match expr { ... }` site
across executor/planner/explain to add the `Placeholder`
unreachable arm — Rust's exhaustiveness checking did the
discovery for me; the compiler errored out at every site that
needed an update.

### What's deferred to v0.55+

Wire-protocol Prepare/Execute frames: the prehnited server would
gain `Prepare` (parse+plan, return a handle) and `Execute` (with
the handle + binds). The plan-cache lives in the server's
per-connection state. The library side is ready for this:
`bind_plan` mutates a Plan in place, so a v0.55 server can clone
a cached Plan, bind, execute, discard — no parser/planner work
per Execute.

Named placeholders (`$name`) and Postgres-style `$1`/`$2`
syntax are also future work; `?` is the SQL standard form and
covers the common case.

### Numbers

- **300 tests across the workspace** (was 288; +8 bind
  integration tests + 3 unit tests in `engine::bind` + 1 doctest
  in the new `execute_with_params`).
- Touched: `sql/token.rs` (`Token::Question`), `sql/lexer.rs`
  (one new `'?'` case), `sql/parser.rs` (placeholder_count
  field + Question arm in `primary`), `sql/ast.rs`
  (`Expr::Placeholder` variant), `engine/bind.rs` (new module,
  ~250 lines including tests), `engine/mod.rs` (module
  registration), `engine/database.rs` (`execute_with_params`
  method, `execute` becomes a thin wrapper), `engine/executor.rs`
  (3 unreachable arms for exhaustiveness),
  `engine/planner.rs` (1 no-op arm), `engine/explain.rs`
  (renders unbound placeholders as `?N`), `tests/integration.rs`
  (8 new tests).
- Cleanup: removed dead `flush_own_dirty` method left over
  from v0.52's commit-path rewrite.
- On-disk format **unchanged** (`PREHNDB11`). Wire protocol
  unchanged. v0.53 databases open cleanly under v0.54.

## Session 55 — Prepare/Execute wire frames (v0.55)

v0.54 added the bind step over a fully planned tree. v0.55 closes
the loop: the network protocol now has Prepare/Execute/Deallocate
frames, and the library gains a matching `prepare` /
`execute_prepared` / `deallocate_prepared` triple. One parse + one
plan, many parameterised executes — over the wire.

```rust
// Library API
let h = db.prepare("SELECT name FROM users WHERE id = ?")?;
for id in 1..=1000 {
    let r = db.execute_prepared(h, &[Value::Int(id)])?;
    // ...
}

// Wire frames
Request::Prepare("SELECT ...".into())    -> Response::Prepared { handle }
Request::Execute { handle, params }      -> RowsBegin / Row* / RowsEnd
Request::Deallocate { handle }           -> Ack
```

The wire shape is deliberately Postgres-like (extended-query
protocol): a parse step returns an opaque handle, an execute step
binds parameters and streams results, and a deallocate step frees
the cache slot. PrehniteDB skips Postgres's separate `Bind` step
— the parameter list rides with `Execute`, the same trip — but the
underlying lifecycle is the same.

### The cache lives in the Database

The simplest possible cache: a `HashMap<u64, Plan>` inside the
`Database` struct, plus a monotonic `u64 next_handle` counter
seeded at 1.

```rust
pub struct Database {
    // ... existing fields ...
    prepared_statements: HashMap<u64, Plan>,
    next_handle: u64,
}
```

`prepare(sql)` parses, plans, inserts into the map, returns the
counter value, increments. `execute_prepared(handle, params)`
looks up the Plan, **clones it**, calls v0.54's `bind_plan` on
the clone, and routes through the existing `run_plan`. The
original cached Plan is never mutated — bind is the only step
that ever needed to mutate, and it works on a fresh clone every
call.

The cache lives on the `Database`, not in the
`prehnitedb` crate root or in a global. The server opens one
`Database` per TCP connection (this has been the model since
v0.27), so each connection's prepared-statement cache is
naturally isolated. Two clients can independently allocate
handle 1, and they refer to different plans. This matches
Postgres's session-level scoping; the test
`prepared_handles_are_per_connection` pins it down — connection
A's handle gets a "no prepared statement with handle N" error
when sent on connection B.

### Plans are `Clone`

The `Plan` enum and every type reachable from it (`AccessPath`,
`Projection`, `Expr`, `FromClause`, `Join`, `OrderKey`, …) was
already `#[derive(Clone)]`-able — most of these types are small
ASTs with `Vec`/`Box` indirections, and the planner already
clones Plan subtrees in a few places (the subquery
pre-evaluation pass, EXPLAIN ANALYZE's inner). Per-execute
clones are O(plan-size); for the common shape (one filter, one
projection, a few `?` placeholders) we're talking a few hundred
bytes of allocation. The work that used to dominate
parse+plan+validate is now done exactly once at prepare time;
the per-execute cost is `Clone` + bind walk + run.

If `Plan::Clone` were *not* cheap enough, the alternative would
be to bind in-place with a save-and-restore: walk the cached
plan, swap each placeholder for the literal, run, then walk
again to restore the placeholders. That doubles the walk cost,
adds a correctness hazard (a panic between swap and restore
corrupts the cache), and serialises executes on the same
handle. `Clone` is the right trade.

### Why handles never recycle

`next_handle` is strictly increasing — even after `deallocate`
frees a slot, the freed value is never reused. A stale handle
from a deallocated prepared statement reliably errors with "no
prepared statement with handle N" instead of silently running a
different cached plan. With `u64` we get 2^64 handles before
overflow; a busy connection allocating one handle per
nanosecond would still take 584 years to wrap. The overflow
case still returns an error from `prepare` rather than
panicking — defence in depth.

The counter starts at 1, not 0. Zero is a useful sentinel value
that the test `one_prepare_serves_many_executes_over_the_wire`
asserts the server never hands out. (Postgres uses string names
for prepared statements; integers are simpler and match SQLite's
prepared-statement handle model.)

### The server's dispatch problem

`prehnited`'s lock model picks the right granularity per
statement:
- `SELECT` / `EXPLAIN` → lockless (MVCC snapshot at statement
  start)
- `INSERT` / `UPDATE` / `DELETE` → per-table shared RwLock
- `CREATE INDEX` → per-table exclusive RwLock
- `CREATE TABLE` / `DROP TABLE` / `VACUUM` / `ANALYZE` →
  catalog mutex (taken inside the engine on catalog write)
- `BEGIN` / `COMMIT` / `ROLLBACK` → no lock

For a `Query(sql)` request the server inspects the SQL text
(via `prehnitedb::is_read_only` and `prehnitedb::write_scope`)
to decide. For `Execute { handle }` there is no SQL text — only
a handle pointing into the cache. **The server has to ask the
Database what shape the cached plan has.**

Two new methods on `Database`:

```rust
pub fn prepared_write_scope(&self, handle: u64) -> Result<WriteScope>;
pub fn execute_prepared_streaming(
    &mut self,
    handle: u64,
    params: &[Value],
) -> Result<Execution>;
```

`prepared_write_scope` looks up the Plan and delegates to a new
free function `prehnitedb::plan_write_scope(&Plan) -> WriteScope`
that mirrors the existing `write_scope(&str)` exactly, just
matching on Plan variants instead of Statement variants.
`execute_prepared_streaming` is the streaming twin of
`execute_prepared`, returning an `Execution` whose rows are
pulled by the existing `stream_next` machinery.

The server's `serve_client` loop grows three new match arms:
`Request::Prepare`, `Request::Execute`, `Request::Deallocate`.
Execute calls `prepared_write_scope`, branches on the result,
takes the right lock (or no lock for `WriteScope::None`), then
calls `execute_prepared_streaming`. The row streaming after
that is identical to a plain Query — both paths converge on a
shared `stream_execution` helper.

### What Prepare can refuse

Two statement shapes don't fit the prepared model and are
rejected with a clear error message:

```rust
db.prepare("BEGIN")?;  // Err: transaction-control statements cannot be prepared
db.prepare("VACUUM")?; // Err: VACUUM cannot be prepared
```

`BEGIN` / `COMMIT` / `ROLLBACK` have engine-side side effects
(transaction state machine transitions) the prepare path
doesn't model — they're cheap to parse, so just call `execute`.
`VACUUM` is special because it replaces the pager's contents
wholesale; the existing logic that handles it short-circuits
before `run_plan`, and the prepare path would need a parallel
short-circuit. Not worth the surface area for a statement
nobody parameterises.

`Database::execute_prepared` and `execute_prepared_streaming`
both also short-circuit `Plan::Vacuum` defensively, in case
some path slipped past `prepare`'s gate.

### The borrow-checker's lesson

The server's helper for streaming a result:

```rust
fn respond_prepared(stream: &mut TcpStream, db: &mut Database,
                    handle: u64, params: &[Value]) -> Result<()> {
    stream_execution(stream, db, db.execute_prepared_streaming(handle, params))
    //                       ^^                                                ^^
    //                       first &mut db                                     second &mut db
}
```

Rust said no: the third argument's `db.execute_prepared_streaming`
takes `&mut self`, and the second argument is already `&mut db`,
so the two `&mut` borrows overlap inside the call's argument
list. The fix is to evaluate the call first into a local:

```rust
let execution = db.execute_prepared_streaming(handle, params);
stream_execution(stream, db, execution)
```

This is a frequent pattern when refactoring code that grew from
"compute and pass" into "share the call site" — the compiler
forces you to make the borrow order explicit. The same fix
applies to the SQL-text `respond` helper, which had the same
shape (`stream_execution(stream, db, db.execute_streaming(sql))`).
Both got the same one-line edit.

### Frame layout

Per the existing protocol, every frame is `[tag: u8][length:
u32 BE][payload]`. The new tags fit the existing namespace:

| Tag    | Direction | Frame                                           |
|--------|-----------|-------------------------------------------------|
| `0x02` | C → S     | `Prepare`        – payload: SQL text            |
| `0x03` | C → S     | `Execute`        – payload: `u64 handle, u16 N, N×value` |
| `0x04` | C → S     | `Deallocate`     – payload: `u64 handle`        |
| `0x15` | S → C     | `Prepared`       – payload: `u64 handle`        |

`Execute`'s params reuse the existing tagged-value encoding for
row values (`VAL_NULL`/`VAL_INT`/`VAL_REAL`/`VAL_TEXT`/`VAL_BOOL`)
— so a row value pulled from one query and bound back into
another is a one-line round-trip. `Deallocate` is a fixed
8-byte payload. `Prepared` is the symmetric server-side fixed
8-byte payload.

`Deallocate` ALWAYS acks, even for an unknown handle. SQL's
`DEALLOCATE` works the same way; freeing what isn't there is
benign. This also avoids a tricky shutdown race where a client
deallocates handles in flight and the network response order
doesn't matter.

### Eight new tests, all wire-level

Five integration tests in `crates/prehnited/tests/prepared_statements.rs`
that boot the server in-process and drive real TCP traffic:

- `one_prepare_serves_many_executes_over_the_wire` — one
  Prepare, three Executes with different params, three correct
  rowsets. The "this is the point" test.
- `prepared_handles_are_per_connection` — A's handle is invisible
  to B; B's Execute returns an `Error` frame naming the unknown
  handle. The per-connection cache contract.
- `prepared_dml_writes_and_is_visible_to_plain_query` — Prepare
  an INSERT, Execute it three times, then a plain Query
  observes all three rows, and a fresh connection sees them
  too (writes were committed).
- `deallocate_frees_the_handle_and_the_server_acks_unknowns`
  — first deallocate kills the slot, subsequent Execute errors;
  redundant deallocates still ack.
- `execute_arity_mismatch_returns_an_error_frame` — too-few
  params surfaces as an `Error` frame at the wire (not a
  connection drop), and the connection remains usable for a
  second Execute with proper params.

Plus seven library tests in `engine::database::tests::prepared_*`
and four protocol tests covering frame round-trip. Total: **316
tests pass** (was 300 at v0.54; +16 this session: 7 library +
4 protocol + 5 wire).

### A subtle CAP-of-prepared-statements decision

What does the server do when a prepared SELECT's schema changes
under it? Concretely: client A prepares `SELECT id FROM t`, then
client B drops the table, then client A executes. The cached
Plan still references the old table's root page; the executor
fails the row scan with a corruption-flavoured error.

Today this surfaces as an `Error` frame at A's next Execute,
which is correct — A's snapshot is stale, A needs to re-prepare.
A future version could be helpful and invalidate prepared
statements whose schema dependencies changed (a `schema_version`
counter on each cached plan, checked at Execute time). Postgres
does this. PrehniteDB doesn't yet; the user-visible difference
is "no rows" vs. "schema changed, re-prepare". Documented as
limitation, not a correctness bug.

### What's deferred

- **Named placeholders** (`:name`) and Postgres-style `$1`/`$2`
  — `?` is the SQL standard, and the bind/execute machinery is
  number-indexed anyway. Named placeholders are a parser
  surface change, not an engine change.
- **Cross-session prepared statements**, a la Postgres's
  `PREPARE name AS ...` + `EXECUTE name(...)` SQL syntax. This
  would persist the cache outside one connection, which means
  the cache would need a string key, a longer lifecycle, and a
  story for what to do on schema change. Not worth the surface
  area; the handle API is the common case.
- **Plan invalidation on schema change** (above). The CAP
  discussion above lays out the trade.
- **Statement metadata frames** — Postgres has `Describe` for
  introspecting a prepared statement's parameter and result
  types. Useful for client-library bindings; not on the path
  for a single SQL-text-in, rows-out client like
  `prehnite-cli`.

### What surprised me

How small the diff was once the v0.54 bind step was in place.
The whole feature is ~250 LOC across protocol.rs (frame plumbing),
database.rs (3 new methods + 2 new fields), lib.rs (one new
classification helper), and prehnited/lib.rs (3 new match arms
+ 2 prepared-flavoured helpers). The bind walker, which is the
real "interesting" work of parameter substitution, was already
written in v0.54.

The other thing that struck me: how much the Plan-cloning model
gives us for free. Each Execute gets a fresh Plan, so an Execute
panicking, an error mid-statement, an in-flight transaction
rolling back — none of it can corrupt the cached Plan, because
the cached Plan was never mutated. Bind is the only step that
mutates, and bind walks the clone. The cache lifecycle is
straight-line: insert at prepare, lookup-and-clone at execute,
remove at deallocate. No locks, no shared mutable state, no
poisoning.

### Numbers

- **316 tests across the workspace** (was 300; +16 this
  session: 7 library tests in `engine::database::tests`, 4
  protocol round-trip tests in `protocol::tests`, 5 wire-level
  integration tests in `prehnited/tests/prepared_statements.rs`).
- Touched: `protocol.rs` (3 new request tags + 1 response tag,
  Request/Response variants, encode/decode helpers, 4 round-trip
  tests), `engine/database.rs` (`prepared_statements` +
  `next_handle` fields, `prepare` / `execute_prepared` /
  `execute_prepared_streaming` / `deallocate_prepared` /
  `prepared_write_scope` methods, 7 unit tests), `lib.rs`
  (new `plan_write_scope` helper), `prehnited/src/lib.rs`
  (3 new request branches in `serve_client`, new
  `respond_prepared` / `run_write_prepared` /
  `stream_execution` helpers — the SQL-text `respond` now
  delegates to the same shared streaming helper),
  `prehnited/tests/prepared_statements.rs` (new integration
  test file, 5 tests).
- On-disk format **unchanged** (`PREHNDB11`). Catalog format
  unchanged. The wire protocol gains four new tag bytes
  (`0x02`, `0x03`, `0x04`, `0x15`); old clients that only send
  `Query` and read the existing response tags talk to a v0.55
  server unchanged. A v0.55 client talking to a v0.54 server
  would fail on the unknown-tag error path — wire forward-compat,
  not backward-compat.
