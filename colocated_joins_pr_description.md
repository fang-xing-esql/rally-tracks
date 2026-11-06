## Add the `colocated_joins` track: colocated `JOIN` vs `LOOKUP JOIN` on a multi-node cluster

### Summary

This adds a Rally track, `colocated_joins`, that compares the ES|QL **colocated `JOIN`** (an inner equi-join against a regular index that is
routed by the join key, see the `colocated-join-route-rhs` branch of Elasticsearch) with **`LOOKUP JOIN`**, on the same data, the same
cluster and the same Elasticsearch build. It needs a build that has the dev-only `JOIN` command (a snapshot build), and a cluster with **3 data
nodes**: on one node every shard is local and the variants cannot differ. The existing `joins` track (and its nightly benchmark) runs on one node
and is untouched. `colocated_joins` is added to `skip_tracks` of `it_tracks`, because that test runs every track on release builds.

The data is the corpora of the `joins` track. Nothing new is generated or uploaded.

### What is compared

All variants join the same documents and differ in how the indices are laid out and in the join command.

| Variant | Left index | Right index | Command |
|---|---|---|---|
| `lookup-unrouted` (baseline) | `colo_lhs_unrouted`, default routing | `lookup_idx_<key>_f10`, lookup mode, 1 shard, a copy on every node | `LOOKUP JOIN` |
| `rhs-sharded-on-join-key` | `colo_lhs_unrouted` | `colo_rhs_<key>`, routed by the join key, 3 shards | `JOIN` |
| `both-colocated` | `colo_lhs_aligned_<key>`, routed by the join key, shard *i* on the node of shard *i* of the right index | `colo_rhs_<key>` | `JOIN` |
| `lookup-aligned` (control) | `colo_lhs_aligned_<key>` | `lookup_idx_<key>_f10` | `LOOKUP JOIN` |

The control separates the effect of the left index layout from the effect of the join implementation. The hypotheses the track is built to test:
**H1** `rhs-sharded-on-join-key` is faster than `lookup-unrouted` (a sharded right side against a single shard), and **H2** `both-colocated` is faster
than `rhs-sharded-on-join-key` (every lookup is local).

### Setup (done by the track)

- **Indices.** Left: `colo_lhs_unrouted` (10M documents at 100%, `ingest_percentage` applies) and `colo_lhs_aligned_<key>` (routed by
  `index.routing_field: key_<key>`, `_routing` required). Right: `colo_rhs_<key>` and `lookup_idx_<key>_f10` (the same documents), plus **key-only**
  variants `colo_rhs_<key>_key_only` and `lookup_idx_<key>_key_only` that have only the join key, so there are no right columns to load. Join keys are
  `100000` and `1000000` (`include_fanout` adds a 10-documents-per-key right side).
- **One corpus, several indices.** Rally prepares document sets in parallel, so one source file can feed only one index. Each corpus is bulk loaded
  once and the other indices are copied with `_reindex` by a small custom runner (`track.py`), which refreshes the source first and waits for the task.
- **Aligned placement.** Empty seed indices (3 primaries, one shard per node, `routing_field`) are created and the aligned left index and the
  routed right indices are cloned from them, so `ResizeAllocationDecider` puts shard *i* of every clone on the node of shard *i* of its seed. The sorted
  left index has its own seed, and the setup moves the right seed's shards to the nodes of the left seed's before cloning. `layout=misplaced` skips this.
- **Same document order for both left indices.** The speed of a lookup depends on the order of the left documents in a shard. Both left indices get
  `index.sort.field: id` (`left_sort_field`). Without it the reindexed aligned index had a worse order and was 15-20% slower for both engines, which
  looked like an effect of sharding by the key but was not (`colocated_joins_plan.md`, 8.6b).
- **Force merge of every index.** All indices the track builds (left, colocated right and lookup) are force merged to one segment before the queries
  (`force_merge`), because a lookup probes every segment and different segment counts would favour one variant.
- **Verification, untimed, before the queries.** Shard *i* of the aligned left index and of the right indices are started on the same node, and all
  eight join variants (with all and with only the key column) return the expected row count (left documents times the fan-out). A wrong layout fails the
  race instead of producing misleading numbers. `layout=aligned` also checks that `number_of_shards` equals the data node count and that there are no replicas.

### Query shapes

Joins only: no filters, one aggregation. Every query exists for every variant with only the join clause changed:
`FROM <left> [| <pre>] | <JOIN <right> | LOOKUP JOIN <right>> ON key_<key> | <tail>`.

| Shape | pre | tail | What it measures |
|---|---|---|---|
| `limit1` | | `LIMIT 1` | latency of the first rows |
| `limit10000` | | `LIMIT 10000` | latency of the first rows, all columns |
| `keep-before-join-limit10000` | `KEEP @timestamp, key_<key>` | `LIMIT 10000` | narrow left rows |
| `keep-after-join-limit10000` | | `KEEP @timestamp, lookup_keyword_0 \| LIMIT 10000` | `KEEP` after the join |
| `stats-count` | | `STATS c = COUNT(*)` | full join, no `LIMIT` in the query |
| `stats-count-right-columns` | | `STATS c = COUNT(*), c0 = COUNT(lookup_keyword_0), ..., c9 = COUNT(lookup_keyword_9)` | full join, the same ten right columns in every variant |
| `stats-count-key-only` | | `STATS c = COUNT(*)` against the key-only right indices | full join, no right columns at all |

Two things about the shapes are worth knowing. A `LIMIT` above a `LOOKUP JOIN` (explicit, or the implicit `LIMIT 1000`) is copied below the join and the
lookup join then runs on the coordinator only, while the colocated join always runs on the data nodes; only the three `stats-*` shapes (no limit)
compare both distributed. And a bigger `LIMIT` cannot make a query join every row: results are truncated at 10,000 rows. The `stats-*` shapes set the
`page_size` pragma (`stats_page_size`, default 4096) so that every variant reads about the same number of left pages.

### Measurements (local, 3 nodes)

Local 3-node cluster (one laptop, 2 GB heap per node) running a snapshot build of the colocated join branch with the streaming colocated join.
Fresh load with `ingest_percentage: 1` (100,000 left documents), `key_cardinalities: ["100000"]`, every index force merged to one segment, 50 iterations
after 10 warmup, 28 query tasks, error rate 0, verification passed. Rally p50 service time in ms (fastest per row in bold):

| Shape | `lookup-unrouted` | `rhs-sharded-on-join-key` | `both-colocated` | `lookup-aligned` |
|---|---|---|---|---|
| `limit1` | 5.8 | 6.6 | 5.1 | 2.9 |
| `limit10000` | 731.0 | 891.2 | **494.4** | 735.8 |
| `keep-before-join-limit10000` | 43.0 | **23.8** | 23.9 | 42.9 |
| `keep-after-join-limit10000` | 12.0 | 10.8 | **10.0** | 11.6 |
| `stats-count` | 18.6 | 18.0 | **16.3** | 17.8 |
| `stats-count-right-columns` | 69.2 | **49.4** | 61.9 | 69.0 |
| `stats-count-key-only` | 17.9 | 18.1 | **15.8** | 17.3 |

- **`both-colocated` is 11-44% faster than the baseline on all seven shapes** (`limit1` differs by 0.7 ms, which is noise) and the fastest or within noise of
  the fastest on five (not on `limit1` and `stats-count-right-columns`).
- **H1 holds on three shapes** (`rhs-sharded-on-join-key` is 45% faster on `keep-before-join-limit10000`, 29% on `stats-count-right-columns`, 10% on
  `keep-after-join-limit10000`), is a tie on `stats-count` and `stats-count-key-only`, and fails on `limit10000` (891 against 731 ms).
- **H2 is mostly confirmed.** `both-colocated` is faster than `rhs-sharded-on-join-key` on `limit10000`, `stats-count`, `stats-count-key-only` and
  `keep-after-join-limit10000`, equal on `keep-before-join-limit10000`, and slower only on `stats-count-right-columns` (61.9 against 49.4 ms), where splitting
  each page by right shard and looking the pieces up in parallel pays off.
- **The aligned layout is not slower in itself.** `lookup-aligned` is within 5% of `lookup-unrouted` on all four aggregate and `keep` shapes once the left
  indices have the same document order.
- **Merging the right side matters.** An earlier run merged only the left indices; the lookup indices then had 2 segments and the lookup joins were 20-30%
  slower (for example `stats-count`: 24.3 against 18.6 ms), which made the colocated advantage look larger. That is why every index is merged.
- **Caveats.** The three nodes share one machine, so there is no network cost between them; colocation saves network traffic, so this understates its benefit.
  The left side is small, and at this ingest percentage it contains duplicate keys. The limit shapes vary by several percent between runs. Earlier builds of the
  branch gave the opposite picture (the colocated join was 3-6 times slower on the aggregate shapes because it kept unused right columns and could not use the
  streaming lookup); the numbers above are for the build with the streaming colocated join and column pruning, which cannot be told from the version or the
  build hash (it plans `StreamingLookupOperator`).

### Running it

```
esrally race --track-path=colocated_joins --pipeline=benchmark-only --target-hosts=<3 node cluster> \
  --track-params='{"ingest_percentage": 10}'
```

Main parameters (all documented in `colocated_joins/README.md`): `number_of_shards` (3, must equal the data node count for `layout=aligned`), `layout`
(`aligned` or `misplaced`), `key_cardinalities` (a JSON list), `ingest_percentage` (left indices only, the right indices are always loaded completely),
`include_fanout`, `left_sort_field`, `force_merge`, `stats_page_size`, `query_clients`, `iterations`, `warmup_iterations`. Lists must be passed as JSON.
A local `./gradlew run --nodes=3` needs a heap of 2 GB or more (`-Dtests.heap.size=2g`); the default test cluster heap ran out of memory.

### Testing

- `esrally info --track-path=colocated_joins` renders the track for the parameter combinations used.
- The setup, reindex and verification runners were run against a fake cluster client (both layouts, the fan-out card, the failure paths) and the shard
  alignment against the live cluster with a deliberately mismatched seed pair.
- The whole challenge was run end to end on a local 3-node cluster, several times, including fresh index creation and loading.

### Not done / follow-ups

- A run on a real 3-node cloud cluster at normal size (`ingest_percentage: 10`, both key sizes) through `esbench`. The benchmark entry for
  `elasticsearch-benchmarks` is in `colocated_joins_plan.md` (section 8); it is not part of this change.
- `JOIN` is dev-only, so the track runs only against snapshot builds of the branch; it is not meant for the nightly `joins` benchmark.
- Force merging every index will take time at full size (`force_merge` can be turned off).
- `colocated_joins_plan.md` has the design, the investigations behind the setup (page sizes, document order, the streaming lookup operator) and the
  hand-over notes for the `esbench` run; it can be dropped from this PR if it is only wanted as context.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
