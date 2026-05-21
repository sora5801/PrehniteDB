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
