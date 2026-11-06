# Plan: `colocated_joins` Rally workload

Status: implemented in `colocated_joins/` (next to `joins/`, so the dev-only `JOIN` syntax stays out of the nightly `joins` track).
Validated locally on a 3-node cluster with a small load. Not yet run at normal size on a real cluster.

Branch under test: https://github.com/fang-xing-esql/elasticsearch/tree/colocated-join-route-rhs
(design docs on the branch: `colocated_join_design.md`, `colocated_join_improvements.md`, `colocated_join_setup.sh`).

## 1. Goal and hypotheses

Measure pure join performance on a **3-node cluster**, with no filters (and a single `COUNT(*)` aggregation to force the full join), for three configurations of the same
logical join:

| Id | Name | Left index | Right index | Join command |
|----|------|------------|-------------|--------------|
| A | Lookup join (baseline) | unrouted, N shards | lookup-mode, 1 shard, copy on every node | `LOOKUP JOIN` |
| B | RHS sharded on join key | unrouted, N shards | standard index, routed by join key, N shards | `JOIN` |
| C | Both sides colocated | routed by join key, N shards, aligned with right | same as B | `JOIN` |

N = the data node count = 3. Control `A2`: `LOOKUP JOIN` on the routed left index, which separates the effect of the left layout from
the effect of the join implementation.

Hypotheses (expectations, not facts):

- **H1: B is faster than A.** The right side is sharded across the 3 nodes instead of being one shard. B hashes each left row and
  looks it up on the right shard that owns its key, which is sometimes a remote lookup.
- **H2: C is faster than B.** Every lookup is local and no hashing is needed (the "aligned" path on the branch).

What could falsify them: the lookup index has a full copy on every node, so A never makes a remote call; per-row hashing and page
splitting in B cost CPU; on small right sides the sharding gain may not show.

First local result (3 nodes on one laptop, 2 GB heap, 100K left documents, 2 iterations, so only a smoke test): the colocated variants
were *slower* than `LOOKUP JOIN` on the full-join query (about 5-6 s against 2-3 s). That was on the first build tested; section 8.6
("Build with streaming colocated joins", and 8.6c for the final measurement with every index force merged) has the results with a later build, where the colocated joins are as fast or faster. One machine has
no network cost, which is what colocation saves, so this does not settle H1 or H2. A real multi-node cluster at normal size is needed.

## 2. Queries

Joins only. **No filters**, and one aggregation, `COUNT(*)`, as the only shape without a `LIMIT`. Every query exists for every variant, with only the join clause changed:

`FROM <left> [| <pre>] | <JOIN <rhs> | LOOKUP JOIN <lookup>> ON key_<key> | <tail>`

| Shape | pre (before the join) | tail | Measures |
|-------|-----|------|----------|
| `limit1` | | `LIMIT 1` | latency of the first rows |
| `limit10000` | | `LIMIT 10000` | latency of the first rows |
| `keep-before-join-limit10000` | `KEEP @timestamp, key_<key>` | `LIMIT 10000` | latency of the first rows with narrow left rows |
| `keep-after-join-limit10000` | | `KEEP @timestamp, lookup_keyword_0 \| LIMIT 10000` | latency of the first rows; baseline for the colocated join not pruning left columns |
| `stats-count` | | `STATS c = COUNT(*)` | **throughput**: the full join of all left rows, no `LIMIT` in the query |
| `stats-count-right-columns` | | `STATS c = COUNT(*), c0 = COUNT(lookup_keyword_0), ..., c9 = COUNT(lookup_keyword_9)` | **throughput**: as `stats-count`, but every variant needs the same ten right columns |
| `stats-count-key-only` | | `STATS c = COUNT(*)` | **throughput**: as `stats-count`, against right indices that have only the join key |

The limit shapes can stop as soon as the limit is reached, so they measure latency. The three stats shapes are the throughput measures. A
bigger `LIMIT` is no substitute: results are truncated at 10000 rows (checked: `LIMIT 50000` returned 10000 rows). A `SORT` shape
(`SORT lookup_keyword_0 ASC | LIMIT 10000`) was in the track for a while and was removed; its profile findings are kept in section 8.6.

There are two `KEEP` shapes on purpose, so that the effect of the colocated join not pruning left columns stays captured as a baseline for
future improvements: with `KEEP` before the join the colocated join reads only the kept left columns, with `KEEP` after the join it still
reads all of them (section 8.6). When the branch prunes columns across the join, the two shapes should converge.

Operation names are `esql_<variant>_<key>_<shape>`, variants are `lookup-unrouted` (A), `rhs-sharded-on-join-key` (B), `both-colocated` (C)
and `lookup-aligned` (A2). Variants of one query run next to each other. All queries were run against the branch build: every shape
parses and runs, with 0 errors.

## 3. Cluster

Both engines must run on the same 3-node cluster and the same Elasticsearch build.

- 3 data nodes (`elasticsearch.node.count: 3`), same hardware as the `esql-joins` nightly (`aws::c6id_2xlarge-8-16-1x474GB_nvme_local`,
  8g heap). The load driver is a separate node group by default.
- Build the fork for both the colocated and the lookup runs, so surrounding code does not differ:
  `esbench benchmark ... --es-repo-url https://github.com/fang-xing-esql/elasticsearch.git --revision colocated-join-route-rhs`
  (config keys `elasticsearch.remote.repo.url` and `.revision`). The `JOIN` command is dev-only (`COLOCATED_JOIN_V1`), so it needs a
  snapshot build.
- The existing `esql-joins` nightly runs on **one node** (`elasticsearch.node.count` defaults to 1), so its lookup join numbers are
  not the baseline. The baseline is part of this workload.
- Custom track: `track.repository.url` and `track.revision`.
- Local runs: `./gradlew run --nodes=3 -Dtests.heap.size=2g`. The default test cluster heap is 512m, which ran out of memory with this
  data (100 text fields per left document).

Benchmark entry for `elasticsearch-benchmarks/.../stateful/main.yml` (separate repo, not added):

```yaml
- name: colocated-joins-3n
  description: Colocated JOIN vs LOOKUP JOIN on 3 nodes
  use-case: simple
  use-case-params:
    cloud.region: "eu-west-1"
    elasticsearch.node.count: 3
    elasticsearch.heap.size: 8g
    elasticsearch.instance.name: aws::c6id_2xlarge-8-16-1x474GB_nvme_local
    track.name: colocated_joins
    track.challenge: colocated-vs-lookup
    track.params:
      number_of_shards: 3
      ingest_percentage: 10
```

## 4. Schema

No new data. The corpora of the `joins` track feed new indices, with one restriction: Rally prepares document sets in parallel, so a
source file can feed only one index. The other indices are copied with `_reindex`.

Left, from `documents.json.bz2` (10M documents; `id`, `@timestamp`, `key_1000` ... `key_100000000` keyword, `field_0..99` dynamic):

| Index | Shards | Routing | Loaded by |
|-------|--------|---------|-----------|
| `colo_lhs_unrouted` | N | default; `index.sort.field: id` | bulk |
| `colo_lhs_aligned_<key>` | N | `index.routing_field: key_<key>`, `_routing` required; `index.sort.field: id` | `_reindex` from `colo_lhs_unrouted` |

Every index of the track (both left indices, the colocated right indices and the lookup indices) is force merged to one segment after loading
(`force_merge`), so that segment counts do not favour a variant and the two left indices have the same document order (section 8.6b).

Right, from `lookup_idx_<key>_f10` (`key_<key>`, `lookup_keyword_0..9`, all explicit keyword), always loaded completely so that every
left key finds its match:

| Index | Shards | Routing | Loaded by |
|-------|--------|---------|-----------|
| `colo_rhs_<key>` | N, 0 replicas | `index.routing_field: key_<key>`, `_routing` required | bulk |
| `lookup_idx_<key>_f10` | 1 (lookup mode) | `auto_expand_replicas: 0-all` | `_reindex` from `colo_rhs_<key>` |
| `colo_rhs_<key>_key_only` | N, 0 replicas | `index.routing_field: key_<key>`, `_routing` required; only the field `key_<key>` | `_reindex` from `colo_rhs_<key>`, `_source: [key_<key>]` |
| `lookup_idx_<key>_key_only` | 1 (lookup mode) | `auto_expand_replicas: 0-all`; only the field `key_<key>` | `_reindex` from `colo_rhs_<key>`, `_source: [key_<key>]` |

`<key>` is `100000` or `1000000`. Optional `include_fanout` adds `colo_rhs_100000_x10` and `lookup_idx_100000_f10_x10` (10 right
documents per key), with the key-only pair `colo_rhs_100000_x10_key_only` and `lookup_idx_100000_x10_key_only`. In the aligned layout the key-only
colocated index is cloned from the same seed as the aligned left index, so shard i of both is on the same node. The `_reindex` runner refreshes the source first, because the bulk load uses a long refresh interval; without
that it copied 0 documents.

### Aligned layout

For each key empty seed indices are created with N primaries, no replicas,
`index.routing.allocation.total_shards_per_node: 1`, no rebalancing and `index.routing_field`: `colo_seed_<key>_lhs` (sorted by `id`, for the
aligned left index) and `colo_seed_<key>` (for the right indices, which have no `id`). The clones inherit the placement of their seed, so the
setup first moves the shards of `colo_seed_<key>` to the nodes of the same shard numbers of `colo_seed_<key>_lhs` (the one-shard-per-node limit
of the right seed is lifted for the moves and restored; `align_seed_placement` in `track.py`), and `ResizeAllocationDecider` then puts shard i
of every clone on the node holding shard i of its seed. This needs
`number_of_shards` equal to the data node count and `number_of_replicas: 0`; the track checks both and fails early. `layout=misplaced`
creates the indices independently, with no placement guarantee.

## 5. Track

| File | Content |
|------|---------|
| `track.json` | corpora, and the shared definitions (join keys, variants, query shapes) |
| `track.py` | runners `colocated-setup-indices`, `colocated-reindex` and `colocated-verify` |
| `operations/default.json` | bulk operations, setup and verify operations, one `esql` operation per join, variant and shape |
| `challenges/default.json` | challenge `colocated-vs-lookup` |
| `README.md` | parameters, variants, queries, how to run |

Untimed `verify` task before the queries: with `layout=aligned`, shard i of each aligned left index and its right index are started on the
same node; and all four variants return the same row count (left documents times the fan-out).

Parameters: `number_of_shards` (3), `number_of_replicas` (0), `layout` (`aligned`), `key_cardinalities` (JSON list, default
`["100000","1000000"]`), `include_fanout` (false), `include_lookup_aligned_control` (true), `ingest_percentage` (100; left indices only),
`bulk_size`, `bulk_indexing_clients`, `reindex_batch_size` (1000), `left_sort_field` (`id`), `force_merge` (true), `auto_expand_replicas`, `stats_page_size` (4096), `index_settings`, `cluster_health`,
`verify_results` (true), `query_clients` (1), `warmup_iterations` (10), `iterations` (50), `max_concurrent_shards_per_node` (10).

`colocated_joins` is also in `skip_tracks` of `it_tracks/test_all_tracks_and_challenges.py`, because that test runs every track on a
release build.

## 6. Status

Validated on a local 3-node cluster of the branch build (2 GB heap, `key_cardinalities: ["100000"]`, `ingest_percentage: 1`):

- index setup, left reindex, lookup copy, the shard alignment check and the row-count check pass (100,000 rows for each variant);
- all 16 queries of the first four-shape set (4 variants x 4 shapes) ran with 0 errors; the two `KEEP` shapes were run separately afterwards.

Not validated: the 1M join, the fan-out variant, `layout=misplaced`, and any run at normal size or on a real cluster.

Deviations from the first version of this plan: index setup and verification are a custom runner instead of `raw-request` steps; the
second corpus use is a `_reindex` instead of a second bulk load (Rally cannot download one file for two indices in parallel); lookup
mappings are explicit keyword; right indices are always loaded completely; no separate placement or small challenge (use `layout`,
`ingest_percentage` and `key_cardinalities`); `source_mode` is not a parameter, because clones cannot change `_source`; the earlier
filter queries (`WHERE`/`LIKE`, left filter) were dropped.

## 7. Next steps

1. Run at normal size on a 3-node cloud cluster, with `ingest_percentage: 10` and both join sizes, then check H1 and H2.
2. Add the benchmark entry to `elasticsearch-benchmarks`.
3. Local checks still open: the 1M join, `include_fanout`, `layout=misplaced`.
4. PROFILE on the branch does not report local and remote lookup counts, so differences between B and C can only be explained by latency.

## 8. Running it with esbench on a real 3-node cluster

This section is the hand-over for a run from the `elasticsearch-benchmarks` repo. Parts marked *unverified* were read from the esbench
source and were not run.

### 8.1 Before the run

1. This branch of `rally-tracks` has to be pushed to GitHub, because esbench checks the track out of Git. Note the branch name.
2. The Elasticsearch fork branch `colocated-join-route-rhs` of `fang-xing-esql/elasticsearch` has to be pushed too. Both engines run
   on that one build; `LOOKUP JOIN` exists on it. The `JOIN` command is dev-only (`COLOCATED_JOIN_V1`), so the build has to be a snapshot.

### 8.2 Benchmark entry

Add this entry to `benchmarks/automated/nightlies/production/stateful/main.yml` (or a config file of your own) in
`elasticsearch-benchmarks`. The `track.repository.url` and `track.revision` parameters are defined in
`resources/use_cases/params/common.py` (defaults: `https://github.com/elastic/rally-tracks` and none); the `defaults:` block at the top
of `main.yml` sets `track.revision: "master"`, so the entry has to override it. *unverified*

```yaml
- name: colocated-joins-3n
  description: Colocated JOIN vs LOOKUP JOIN on 3 nodes
  use-case: simple
  use-case-params:
    cloud.region: "eu-west-1"
    elasticsearch.node.count: 3
    elasticsearch.heap.size: 8g
    elasticsearch.instance.name: aws::c6id_2xlarge-8-16-1x474GB_nvme_local   # same hardware as the esql-joins nightly
    # the fork build; both join engines run on it
    elasticsearch.remote.repo.url: "https://github.com/fang-xing-esql/elasticsearch.git"
    elasticsearch.remote.repo.revision: "colocated-join-route-rhs"
    # the track from this branch of rally-tracks
    track.repository.url: "https://github.com/<owner>/rally-tracks"
    track.revision: "<branch of rally-tracks>"
    track.name: colocated_joins
    track.challenge: colocated-vs-lookup
    track.params:
      number_of_shards: 3          # must equal elasticsearch.node.count for layout=aligned
      ingest_percentage: 10
```

Notes:

- `esbench benchmark` also takes `--es-repo-url` and `--revision` on the command line, which override the Elasticsearch repo and
  revision of the entry (`esbench/cli/benchmark.py`). *unverified*
- The load driver is a separate node group by default (`loaddriver.node.count` defaults to 1), so it is not on a data node. To choose its
  size, set `loaddriver.instance.name`. Rally decompresses the whole 43.8 GB left corpus whatever `ingest_percentage` is, so the load driver
  needs enough disk (about 50 GB free plus the 1 GB download).
- `elasticsearch.node.count` defaults to 1. Without setting it to 3, `layout=aligned` fails in `setup-indices` with
  "number_of_shards (3) must equal the number of data nodes".
- Keep `number_of_replicas` at its default 0 for `layout=aligned`.
- List parameters must be real YAML/JSON lists, for example `key_cardinalities: ["100000"]`. A string such as `['100000']` is read
  as characters and produces a track with nonsense index names.
- Other parameters are documented in `colocated_joins/README.md`.

### 8.3 Command

From the `elasticsearch-benchmarks` repo. *unverified*

```
esbench benchmark --dry-run test colocated-joins-3n        # check what would run first
esbench benchmark test colocated-joins-3n
```

The exact subcommands (`test`, `all`, benchmark group and directory options) are in `esbench/cli/benchmark.py`; the scale test template
documents the form `esbench benchmark --config-file <file> test all`. Use `--es-repo-url` and `--revision` to try other Elasticsearch
revisions without editing the entry.

### 8.4 What a good run looks like

In the Rally output:

1. `setup-indices` succeeds. It deletes and creates the indices, with seed and clones for the aligned layout.
2. The bulk tasks load `colo_lhs_unrouted` (10% of 10M documents, so about 1M), then `colo_rhs_<key>` for each key.
3. `reindex-lhs-aligned-<key>` copies the same number of documents into the aligned left index; `reindex-lookup-<key>` copies the right side
   into the lookup index. Each logs `reindexed N documents`; N must not be 0.
4. `verify` passes. It logs `row counts for key [...]` with the same number for `lookup-unrouted`, `rhs-sharded-on-join-key`, `both-colocated` and
   `lookup-aligned`.
5. The 28 queries per key size run: `esql_<variant>_<key>_<shape>` for variants `lookup-unrouted`, `rhs-sharded-on-join-key`, `both-colocated`,
   `lookup-aligned` and shapes `limit1`, `limit10000`, `keep-before-join-limit10000`, `keep-after-join-limit10000`, `stats-count`, `stats-count-right-columns`, `stats-count-key-only`. Error rate must be 0 for every one.

What to compare (service time per task):

- H1: `rhs-sharded-on-join-key` against `lookup-unrouted`.
- H2: `both-colocated` against `rhs-sharded-on-join-key`.
- Control: `lookup-aligned` against `lookup-unrouted`. If it differs, the left layout matters on its own, and A, B and C are not
  comparable on the left side alone.
- Run the three stats shapes with the default settings, and optionally again with `esql.query.lookup_join_streaming: false` (section 8.6), which
  takes the streaming operator, that only `LOOKUP JOIN` can use, out of the comparison.
- Weigh `stats-count-key-only` and `stats-count-right-columns` most. The three stats shapes are the only ones that join every left row. The limit shapes can stop
  after the first rows, so they show latency only.

### 8.5 What went wrong locally, and what it looked like

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Cannot connect to host` partway through a race | test cluster node ran out of memory at the default 512 MB heap | heap of 2 GB or more (locally `-Dtests.heap.size=2g`); the real cluster uses 8 GB |
| `circuit_breaking_exception ... [<reused_arrays>] ... larger than the limit` | a join with sort on a 2 GB heap, tripped once on a filter-before-join shape that has since been dropped | more heap; the final shapes passed at 2 GB with 100K left documents, but the real run holds about 10x more |
| `FileNotFoundError ... .json.bz2.tmp` during track preparation | two document sets sharing one source file are downloaded in parallel | already fixed: one document set per file, the rest is `_reindex` |
| `reindexed 0 documents` | the source index had a 30 s refresh interval and was not searchable yet | already fixed: the reindex refreshes the source first |
| `layout=aligned needs number_of_shards (3) to equal the number of data nodes` | `elasticsearch.node.count` is not 3 | set it to 3 |
| `shards of [...] are not on the same node` in `verify` | the clone did not put shard i of both indices on one node | check `total_shards_per_node` and rebalancing; this stops the race on purpose, because the aligned results would be wrong |
| `[_routing] must be required in the mapping of index [...] because [index.routing_field] is set` | an index with `index.routing_field` and no `_routing.required` mapping. The track sets both on every routed index. This message was reported from a restarted local cluster, which had just crashed, and creating the seed index by hand with the track's request did not reproduce it | if it appears on a fresh cluster, send the request that created the index |

### 8.6a Build with streaming colocated joins (local, 2026-11-06)

The local cluster was rebuilt with a branch build in which the colocated join uses the streaming lookup operator. The version and build hash did
not change (`9.7.0-SNAPSHOT`, `023d6fa67c`), so a build is recognised by its plan: the colocated join now plans `StreamingLookupOperator`
(before: `LookupOperator`). Everything in section 8.6 below that mentions the colocated join not streaming, loading unused right columns or
not pruning left columns describes the **previous build**. In this build the column pruning is also fixed: for `COUNT(*)` the colocated join
has no right columns in its output and an estimated row size of 54, the same as the lookup join, and `keep-after-join-limit10000` keeps one
right column (values loaded 24,900 against 60,000 for the lookup join).

The whole challenge was re-run on a restarted cluster with freshly created and loaded indices: `ingest_percentage` 1 (100,000 left documents),
`key_cardinalities` `["100000"]`, 3 nodes, 2 GB heap, `bulk_size` 1000, 2 bulk clients, 50 iterations after 10 warmup, 28 query tasks, error rate 0
everywhere, `verify` passed (eight join variants returned 100,000 rows each, shard alignment checked). Rally p50 in ms:

| Shape | `lookup-unrouted` | `rhs-sharded-on-join-key` | `both-colocated` | `lookup-aligned` |
|---|---|---|---|---|
| `limit1` | 7.5 | 9.3 | 7.4 | 6.8 |
| `limit10000` | 964.9 | 948.5 | **519.7** | 1018.0 |
| `keep-before-join-limit10000` | 33.6 | **21.5** | 23.7 | 42.7 |
| `keep-after-join-limit10000` | 14.6 | 12.3 | **12.2** | 16.6 |
| `stats-count` | 26.9 | **22.0** | 27.4 | 29.2 |
| `stats-count-right-columns` | 92.4 | **57.5** | 113.4 | 127.9 |
| `stats-count-key-only` | 25.1 | **19.3** | 20.4 | 28.6 |

- **H1 holds on the aggregate shapes**: `rhs-sharded-on-join-key` is faster than `lookup-unrouted` by 18% (`stats-count`), 38%
  (`stats-count-right-columns`) and 23% (`stats-count-key-only`).
- **H2 does not hold**: `both-colocated` is slower than or equal to `rhs-sharded-on-join-key` (27.4 against 22.0, 113.4 against 57.5,
  20.4 against 19.3). Against the control on the same left index it is faster: `both-colocated` against `lookup-aligned` is 27.4 against 29.2,
  113.4 against 127.9 and 20.4 against 28.6.
- **The aligned left index is slower for both engines** (`lookup-aligned` against `lookup-unrouted`, `both-colocated` against
  `rhs-sharded-on-join-key`), as before; section 8.6b shows this comes from the document order of the aligned index in this setup.
- **Profile of `stats-count-key-only`** (per data node, about 33K left rows, 8-9 left pages, left pages equal in all variants): the lookup per
  row is 0.71 us for `lookup-unrouted`, 0.8 us for `lookup-aligned`, 0.5 us for `both-colocated`. `rhs-sharded-on-join-key` splits each page into
  three sub-pages by right shard (27 pages emitted for 9 input pages): 21 ms for the query at about twice the CPU (35 ms against 18 ms).
- **Limit shapes**: the lookup joins run on the coordinator and the colocated joins on the data nodes (section 8.6). `limit10000` (all 219
  columns, early termination) shows `both-colocated` about twice as fast as the other three; `rhs-sharded-on-join-key` is no faster than
  `lookup-unrouted` there. `limit1` is the same everywhere.
- Caveats: one machine (no network cost between the nodes), 100,000 left documents, a streaming colocated join build whose commit is not
  identifiable from the build hash, 50 iterations per task.

### 8.6b Why the aligned left index is slower (local investigation, 2026-11-06)

In every comparison above the aligned left index (`colo_lhs_aligned_<key>`) was slower than the unrouted one, for both engines
(`stats-count-key-only`: `lookup-aligned` 28.6 ms against `lookup-unrouted` 25.1 ms, `both-colocated` 20.4 against `rhs-sharded-on-join-key`
19.3; larger with right columns). Profile (`stats-count-key-only`, per data node, about 33K left rows): scan and key read are equal
(0.6-1.9 ms and 2.5-3.5 ms), the whole difference is the lookup stage (`lookup-aligned` 0.82-0.84 us per row, `lookup-unrouted` 0.72-0.75).

Controlled copies of the left index (3 shards, only `id`, `@timestamp`, `key_100000`, same 100,000 documents; server `took` p50, 40
iterations after 10 warmup, `stats-count-key-only` against the key-only right indices):

| Left index | Lookup join | Colocated join |
|---|---|---|
| unrouted, as the track loads it | 24.0 | 16.0 |
| aligned, as the track loads it (reindexed) | 27.5 | 19.0 |
| unrouted, only ids below 50,000 (no duplicate keys) | 13.0 | 9.0 |
| aligned, only ids below 50,000 | 15.0 | 10.0 |
| unrouted, sorted by key | 21.0 | |
| aligned, sorted by key | 19.0 | |
| unrouted, sorted by a random number | 42.0 | |
| aligned, sorted by a random number | 40.0 | |
| unrouted, loaded from one ordered stream | 22.0 | 15.0 |
| aligned (cloned, so aligned with the right index), loaded from one ordered stream | 22.0 | 14.0 |

- **Not the cause: duplicate keys.** With `ingest_percentage: 1` and 2 bulk clients the load takes ids 0-49,999 and 5,000,000-5,049,999, and
  `key_100000` is the id modulo 100,000, so every key appears about twice (100,000 rows, 49,905 distinct keys). The routed index keeps a
  key's duplicates in one shard (16.6K distinct keys per shard against 27.7K), but the copy without duplicates is slower in the aligned
  layout by the same margin.
- **Not the cause: routing or sharding by the join key.** Once both layouts are filled from one ordered stream, aligned and unrouted are
  equal for the lookup join (22 and 22 ms) and the aligned colocated join is the fastest (14 against 15 ms).
- **The cause is the physical order of the left documents in a shard.** Sorting the same documents by key makes both layouts faster (19-21 ms),
  sorting them by a random number makes both about twice as slow (40-42 ms), and the difference between the layouts disappears or
  reverses. As a proxy for the lookup's access pattern, the share of consecutive left keys that sit in the same 48-term block of the lookup
  index's (lexicographic) term order is 70% for the unrouted index, 52-57% for the aligned index, 94% sorted by key, 0.1% random, 82%
  for both layouts loaded from one ordered stream, and the lookup time follows it in every case. That the cost is term-dictionary seek
  locality is an interpretation; the lookup code was not read.
- **Why the aligned index has the worse order here:** the setup fills `colo_lhs_aligned_<key>` with `_reindex` from the three-shard
  `colo_lhs_unrouted`, and a routed shard then receives interleaved pieces of three source-shard streams, while a unrouted shard keeps the
  order of the bulk load. Loading the aligned copy from a single ordered stream removes the difference. The mechanism is inferred from
  that experiment, not traced.

So the aligned-is-slower result is mostly an artifact of how the track loads the aligned left index, not a property of sharding the left
side by the join key.

**Applied to the track (2026-11-06).** Both left indices now get `index.sort.field: id` (`left_sort_field`) and are force merged to one segment
(`force_merge`, which since 2026-11-07 covers every index of the track); the aligned layout uses a separate sorted seed for the left index and moves the right seed's shards to match it (section 5).
Measured on the indices the track built (100,000 left documents, shard 0, same-block share and server `took` p50 over 40 iterations,
`stats-count-key-only`):

| Left indices | Same-block share (unrouted / aligned) | Lookup join (unrouted / aligned) | Colocated join (unrouted / aligned) |
|---|---|---|---|
| before: bulk loaded / reindexed | 70.5% / 51.6% | 24-25 / 27.5-29 ms | 16-17 / 19-20 ms |
| sorted by `id` | 79.7% / 71.3% | 25 / 26 ms | 16 / 17 ms |
| sorted by `id` and force merged | 82.2% / 82.1% | 24 / 23 ms | 16 / 14 ms |

Sorting alone shrinks the gap, force merging to one segment removes it. Sorting only the unrouted source and reindexing from it was tried
first and did not remove the gap (66.6% against 82.2%, 27 against 24 ms), because a single reindex scroll still hands each routed shard
three restarting runs from the three source shards; the sort on the destination index makes the order independent of the arrival order.

**Results with equal left order (current setup).** The challenge's setup and the three stats shapes were run again with the sorted and merged
left indices (100,000 left documents, 1 segment per shard, same-block share 82.3% unrouted and 82.1% aligned, 3 nodes, 2 GB heap, streaming
colocated join build, 50 iterations after 10 warmup, error rate 0). Rally p50 in ms:

| Shape | `lookup-unrouted` | `rhs-sharded-on-join-key` | `both-colocated` | `lookup-aligned` |
|---|---|---|---|---|
| `stats-count` | 24.7 | **20.5** | 21.7 | 23.9 |
| `stats-count-right-columns` | 93.8 | **60.1** | 79.5 | 92.4 |
| `stats-count-key-only` | 24.7 | 17.4 | **15.1** | 23.9 |

- The aligned left index is no longer slower: `lookup-aligned` is on par with `lookup-unrouted` (23.9 against 24.7 ms on `stats-count-key-only`), and
  `both-colocated` is faster than `rhs-sharded-on-join-key` there (15.1 against 17.4 ms), so H2 holds on `stats-count-key-only` and `stats-count`
  within noise.
- On `stats-count-right-columns` `rhs-sharded-on-join-key` (60.1 ms) is still faster than `both-colocated` (79.5 ms); the unrouted path splits each page
  by right shard and looks the pieces up in parallel, which pays off when ten right columns have to be loaded.
- Against the lookup join on the same left index, `both-colocated` is faster on all three shapes (21.7 against 23.9, 79.5 against 92.4,
  15.1 against 23.9 ms).
- The tables in section 8.6a were measured before this change, with the aligned left index in its worse document order.

### 8.6c Final local measurement, all shapes, every index force merged (2026-11-07)

Whole challenge on a fresh load: every index of the track force merged to one segment (`force_merge`; checked: all 16 shard copies, including the
lookup replicas, have 1 segment), left indices sorted by `id` (same-block share 82.3% unrouted, 82.1% aligned), `ingest_percentage` 1 (100,000 left
documents, duplicate keys, see 8.6b), `key_cardinalities` `["100000"]`, 3 nodes on one machine, 2 GB heap, streaming colocated join build, streaming
lookup operator on, 50 iterations after 10 warmup, 28 query tasks, error rate 0, `verify` passed (eight variants returned 100,000 rows each).
Rally p50 in ms:

| Shape | `lookup-unrouted` | `rhs-sharded-on-join-key` | `both-colocated` | `lookup-aligned` |
|---|---|---|---|---|
| `limit1` | 5.8 | 6.6 | 5.1 | 2.9 |
| `limit10000` | 731.0 | 891.2 | **494.4** | 735.8 |
| `keep-before-join-limit10000` | 43.0 | **23.8** | 23.9 | 42.9 |
| `keep-after-join-limit10000` | 12.0 | 10.8 | **10.0** | 11.6 |
| `stats-count` | 18.6 | 18.0 | **16.3** | 17.8 |
| `stats-count-right-columns` | 69.2 | **49.4** | 61.9 | 69.0 |
| `stats-count-key-only` | 17.9 | 18.1 | **15.8** | 17.3 |

Relative to `lookup-unrouted` (negative is faster): `both-colocated` is 11% to 44% faster on all seven shapes (`limit1`: 0.7 ms); `rhs-sharded-on-join-key`
is 45% faster on `keep-before-join-limit10000`, 29% on `stats-count-right-columns`, 10% on `keep-after-join-limit10000`, equal on `stats-count` (-4%) and
`stats-count-key-only` (+1%), and slower on `limit10000` (+22%, 891 against 731 ms). `lookup-aligned` is within 5% of `lookup-unrouted` on all four
aggregate and `keep` shapes, so the left document order is equal.

**What merging the right side changed.** The previous run (section 8.6c before 2026-11-07) merged only the left indices; the lookup indices had 2
segments and `colo_rhs_<key>` 1-2 per shard. With everything merged the lookup joins got faster (p50 in ms, before to after, `lookup-unrouted`:
`stats-count` 24.3 to 18.6, `stats-count-right-columns` 95.4 to 69.2, `stats-count-key-only` 24.4 to 17.9, `limit10000` 905 to 731, `keep-before-join-limit10000`
55.3 to 43.0), and the colocated variants that read the merged `colo_rhs_<key>` gained too (`both-colocated`: `stats-count` 21.7 to 16.3,
`stats-count-right-columns` 80.0 to 61.9), while `colo_rhs_<key>_key_only` already had one segment and did not move (`both-colocated` 15.4 to 15.8). So the
colocated advantage over the baseline was overstated while the right side was unmerged:

- **H1** (`rhs-sharded-on-join-key` faster than `lookup-unrouted`) now holds on three shapes (`keep-before-join-limit10000`, `stats-count-right-columns`,
  `keep-after-join-limit10000`), is a tie on `stats-count` and `stats-count-key-only`, and fails on `limit10000`. It was 17-38% faster on the aggregate shapes before.
- **H2** (`both-colocated` faster than `rhs-sharded-on-join-key`) holds on `limit10000` (494 against 891 ms), `stats-count` (16.3 against 18.0),
  `stats-count-key-only` (15.8 against 18.1) and `keep-after-join-limit10000`, is equal on `keep-before-join-limit10000` and loses on
  `stats-count-right-columns` (61.9 against 49.4 ms), where splitting each page by right shard and looking the pieces up in parallel pays off.
- `both-colocated` is the fastest or within noise of the fastest on five shapes (not on `limit1`, which is about 3 ms for every variant, and `stats-count-right-columns`) and 11-44% faster than the baseline on all seven.

The limit shapes run `LOOKUP JOIN` on the coordinator and the colocated joins on the data nodes (section 8.6), so part of their difference is that
placement. Caveats: one machine (no network between the nodes), a small left side, one build, 50 iterations (the `limit` shapes vary by several percent).

### 8.6 What was and was not validated locally

Validated (3 local nodes, one machine, 2 GB heap, 100K left documents, 2 iterations): the track loads, setup and placement checks pass,
all four variants return 100,000 rows, and the 16 queries of the first four-shape set ran with 0 errors; the two `KEEP` shapes were run separately afterwards.

First local timings (median service time, not a benchmark): on `sort-limit10000`, `lookup-unrouted` 2.2 s, `lookup-aligned` 2.7 s,
`both-colocated` 5.3 s, `rhs-sharded-on-join-key` 6.3 s; `limit10000`: 804, 830, 515 and 846 ms in the same order. The colocated variants were not
faster on the full-join query locally, which does not test H1 or H2, because one machine has no network cost between the nodes. If the real
cluster shows the same, then the hypotheses fail at that data size, and the next thing to look at is where the colocated join spends its
time, which PROFILE on the branch does not currently break down into local and remote lookups.

#### Profile findings (local, `PROFILE` via `"profile": true` in the `_query` body, 100K left documents, 2 GB heap)

Only the join shapes below were profiled. The numbers come from one local cluster, so they show where the time goes, not final results.

- **A `SORT` shape (`sort-limit10000`, removed from the track) was slower with colocated joins because of eager field loading.** For `both-colocated` the data-node driver loads
  all 208 fields of every joined row before the top-N (6.9M values per node, 1.4 s, with the lookup taking another 0.4 s and the sort
  0.36 s), in left pages of 32 rows (about 1,000 lookups per shard). `lookup-aligned` sorts on a bare document id and loads fields only
  for the final 10,000 rows (73 left pages of 473 rows, 0.18 s per data driver). Total values loaded: 27.1M against 6.3M; the query took
  5.1 s against 2.5 s. The final field load (1.8 s) and the merge (0.35 s) are the same in both.
- **A `KEEP` after the join does not help the colocated path.** With `KEEP key_100000, lookup_keyword_0` before the sort, `LOOKUP JOIN`
  fell from 2.5 s to 87 ms (100K values loaded), while the colocated join stayed at 2.7 s and still loaded 20.9M values. The cause in the
  branch's code was not found: the optimizer and planner changes in the branch diff do not show it. The data-node plan around
  `ColocatedJoinExec` (projection pruning, and the late materialization that `LOOKUP JOIN` gets for top-N) is where to look. This also made
  the `keep-after-join-limit10000` shape slower for the colocated variants than for the lookup joins; it is kept as the baseline for that.
- **`limit10000` (all 219 columns) is the cleanest join comparison until that is fixed**, because both paths must load every field for the
  same 10K rows. Warm medians over 5 runs: `both-colocated` ~200 ms, `rhs-sharded-on-join-key` ~415 ms, `lookup-unrouted` ~520 ms,
  `lookup-aligned` ~540 ms. It reads only the first ~10-12K of 100K left documents, so it is a latency measure, not throughput.
- **`keep-before-join-limit10000`**: with `KEEP @timestamp, key_100000` before the join, the colocated variants no longer load every column
  (26K values against 60K for the lookup joins) and read half as many left documents before the limit is met (14.4K against 30K). Rally,
  50 iterations after 10 warmup, p50: `rhs-sharded-on-join-key` 24.3 ms, `both-colocated` 28.3 ms, `lookup-unrouted` 34.2 ms,
  `lookup-aligned` 42.8 ms, error rate 0. That supports H1 (about 29% faster than the baseline) and does not support H2: the routed left
  side was a few ms slower than the unrouted one, which one local run cannot separate from noise.
- **Baseline for both `KEEP` shapes** (Rally, 50 iterations after 10 warmup, p50 in ms, error rate 0, same local cluster):

  | Variant | `keep-after-join-limit10000` | `keep-before-join-limit10000` |
  |---|---|---|
  | `lookup-unrouted` | 14.1 | 34.7 |
  | `lookup-aligned` | 16.8 | 42.3 |
  | `rhs-sharded-on-join-key` | 18.8 | 23.9 |
  | `both-colocated` | 23.9 | 28.6 |

  The order flips with the position of the `KEEP`. With the `KEEP` after the join the lookup joins win (14-17 ms against 19-24 ms),
  because the colocated join still reads every left column. With the `KEEP` before the join the colocated joins win (24-29 ms against
  35-42 ms), but the lookup joins get slower than their `KEEP`-after numbers (14 to 35 ms), which was not investigated. Once the colocated
  path prunes columns across the join, `keep-after-join-limit10000` for the colocated variants should drop towards the lookup joins'
  numbers, which is the improvement this baseline is meant to show.
- **`LOOKUP JOIN` runs on the coordinator whenever the query has a limit.** The optimizer copies the `LIMIT` below a left join
  (`PushDownAndCombineLimits`, with a TODO in the code: "this means that the non-remote join will be always forced on the coordinator"), the
  limit is a pipeline breaker, and the mapper places the join above the exchange. This also happens with the implicit `LIMIT 1000`,
  so leaving the limit out does not help (checked: a lookup join without a limit returned 1,000 rows and ran on the coordinator). Without any
  limit in the plan, for example with `STATS`, the lookup join runs in the data-node drivers. The colocated join always runs on the data
  nodes. So the limit shapes compare a distributed colocated join with a lookup join on one node, and only `stats-count` compares both
  distributed.
- **`stats-count` (full join, no limit): the lookup joins are 3-6 times faster than the colocated joins.** Rally, 50 iterations after 10
  warmup, p50, error rate 0: `lookup-unrouted` 29.7 ms, `lookup-aligned` 30.9 ms, `rhs-sharded-on-join-key` 103.9 ms,
  `both-colocated` 168.2 ms. Profile (warm, per data node, about 33K left rows each):

  | Variant | Left rows per page (max page size) | Lookup operator | Per page | Per row | Whole query | Total CPU |
  |---|---|---|---|---|---|---|
  | `lookup-unrouted` | 3,672 (4,854) | 20.7 ms | 2.3 ms | 0.63 us | 25 ms | 14.4 ms |
  | `lookup-aligned` | 3,686 (4,854) | 24.9 ms | 2.8 ms | 0.75 us | 30 ms | 15.3 ms |
  | `rhs-sharded-on-join-key` | 459 (473) | 91.0 ms | 1.3 ms | 2.75 us | 105 ms | 43.4 ms |
  | `both-colocated` | 454 (473) | 146.9 ms | 2.0 ms | 4.43 us | 168 ms | 22.9 ms |

  *(Correction: this first reading blamed the left page count. The page-size test below shows the page count is not the main cause.)*

  The page counts differ because the left page size is `TARGET_PAGE_SIZE / estimatedRowSize` (`LocalExecutionPlanner`, unless the
  `page_size` pragma is set). For `COUNT(*)` the lookup join drops the right columns (estimated row size 54, so 4,854-row pages), while the
  colocated join keeps all ten right columns `lookup_keyword_0..9` in its output (estimated row size 554, so 473-row pages) and loads them for
  every match, although nothing uses them. When all four variants need the same right columns, the lookup join's pages also shrink to
  about 460 rows (72 per node).

  **Same left page count and the same right columns.** The aggregate shapes set `page_size: 4096` through the pragma
  (`stats_page_size`, 0 to leave it to the planner), so every variant reads 10-12 left pages per node (the page counts cannot be made
  exactly equal, because the unrouted and the aligned shards hold slightly different numbers of documents). `stats-count-right-columns` is
  `STATS c = COUNT(*), c0 = COUNT(lookup_keyword_0), ... c9 = COUNT(lookup_keyword_9)`, so all variants need the same right columns. Rally, 50
  iterations after 10 warmup, p50 in ms, error rate 0:

  | Variant | `stats-count` | `stats-count-right-columns` | Left pages per node |
  |---|---|---|---|
  | `lookup-unrouted` | **25.9** | 95.6 | 11-12 |
  | `lookup-aligned` | 30.3 | 137.5 | 10 |
  | `rhs-sharded-on-join-key` | 79.3 | **81.1** | 11-12 |
  | `both-colocated` | 158.5 | 162.9 | 10 |

  - **Page count is not the main cause.** With 4096-row pages in all variants, `COUNT(*)` still takes 79 and 158 ms for the colocated joins against
    26 and 30 ms for the lookup joins (the lookup operator takes 66 and 137 ms against 21 and 26 ms). The remaining gap is the unused right
    columns that only the colocated join loads. Pruning them is the improvement `stats-count` is kept as a baseline for.
  - **With equal pages and equal columns** (`stats-count-right-columns`) `rhs-sharded-on-join-key` is the fastest, about 15% faster than
    `lookup-unrouted`, with a lookup cost per row that is equal (2.8 us at the natural page size, 2.1-2.5 us at 4096 rows). In
    `rhs-sharded-on-join-key` each page is split into three sub-pages by right shard that are looked up in parallel, at about twice the CPU.
  - **The aligned left index is slower for both engines**: `lookup-aligned` is 20-40% slower than `lookup-unrouted` for the same plan, and
    `both-colocated` is about twice as slow as `rhs-sharded-on-join-key`. This is consistent with the more scattered key order of the aligned
    index (median gap between consecutive keys 10, against 3), but that was not tested.
- **Key-only right indices (`stats-count-key-only`) and the streaming lookup operator.** With the join key as the only right field, all four
  variants get the same estimated row size (54), the same natural left page size (4,854) and 8-9 left pages per node, with no pragma. The
  lookup operator also emits about the same number of real output pages in all four (134-140 per node, measured with a downstream
  operator); the `pages_emitted` counter of the streaming lookup operator counts completed input batches, not output pages, so it must not
  be compared with the counter of the colocated join's operator. Measured by hand (a script, 10 warmup and 50 interleaved iterations, wall p50
  in ms, every query returned 100,000), without Rally:

  | Variant | `lookup_join_streaming` on (default) | off |
  |---|---|---|
  | `lookup-unrouted` | **32.5** | 71.9 |
  | `lookup-aligned` | 38.5 | 74.1 |
  | `rhs-sharded-on-join-key` | 41.3 | **38.2** |
  | `both-colocated` | 66.7 | 60.4 |

  `LOOKUP JOIN` uses `StreamingLookupFromIndexOperator` by default (pages are streamed to the lookup service through a long-lived exchange); the
  colocated join cannot (the code says "co-located lookup does not support the streaming operator" and "the streaming lookup operator does
  not support inner joins") and uses the per-page-request `LookupFromIndexOperator`. The dynamic cluster setting
  `esql.query.lookup_join_streaming: false` makes `LOOKUP JOIN` use that operator as well. With it off, the lookup joins are about 2.3 times
  slower and the two colocated variants do not move, so most of the lookup join's lead over the colocated join in the default comparison is the
  streaming operator. With streaming off, `rhs-sharded-on-join-key` is the fastest variant on key-only indices, about 47% faster than the lookup
  joins. Rally, same setting off, original shapes, p50: `stats-count` 60.3 (`lookup-unrouted`), 64.5 (`lookup-aligned`), 83.9
  (`rhs-sharded-on-join-key`), 161.0 (`both-colocated`); `stats-count-right-columns` 136.6, 173.4, 84.0, 166.1. The setting was restored to its
  default afterwards. To compare without the streaming operator on a real cluster, set it before the run and reset it after
  (`PUT _cluster/settings`, `transient`, `esql.query.lookup_join_streaming`, `false` and then `null`).
- **Full results are not measurable on this cluster.** `LIMIT` above 10000 returns 10000 rows, because
  `esql.query.result_truncation_max_size` caps results (it can be raised; `result_truncation_default_size` cannot exceed 10000). One
  10,000-row, 219-column result is 59 MB of JSON, so the 100K-row join would be about 590 MB per response, which is too much for 2 GB
  nodes. The real cluster has 8 GB heaps.

The whole challenge, including the key-only indices and their verification (eight join variants returning 100,000 rows each), was also run
end to end on the local cluster with `ingest_percentage: 1`, 3 iterations, 28 query tasks per key size, 0 errors. Those timings are not
meaningful (3 iterations on freshly loaded indices); they only validate the setup.

Not validated: the 1M join, `include_fanout`, `layout=misplaced`, `index_settings` overrides, and any run on a real cluster or through
esbench.
