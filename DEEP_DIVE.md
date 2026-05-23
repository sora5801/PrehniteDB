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
