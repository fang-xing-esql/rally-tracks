## COLOCATED_JOINS track

Compares the ES|QL **colocated `JOIN`** with **`LOOKUP JOIN`** on a multi-node cluster. Only joins are measured: there are no filters,
and the only aggregations are `COUNT(*)` shapes that force the full join (`stats-count`, `stats-count-right-columns`).

This track needs an Elasticsearch build that has the `JOIN` command (a snapshot build of
<https://github.com/fang-xing-esql/elasticsearch/tree/colocated-join-route-rhs>). Run both variants on the same build and the
same cluster. It is meant for a cluster with 3 data nodes: with one node every shard is local and the variants cannot differ.
It is not run by the nightly `joins` benchmark, and it is skipped by `it_tracks`.

### Join variants

All variants join the same data. They differ in how the indices are laid out and in the join command.

| Variant | Left index | Right index | Command |
|---------|------------|-------------|---------|
| `lookup-unrouted` (baseline) | `colo_lhs_unrouted`: default routing | `lookup_idx_<key>_f10`: lookup mode, 1 shard, a copy on every node | `LOOKUP JOIN` |
| `rhs-sharded-on-join-key` | `colo_lhs_unrouted` | `colo_rhs_<key>`: routed by the join key, `number_of_shards` shards | `JOIN` |
| `both-colocated` | `colo_lhs_aligned_<key>`: routed by the join key, shard i on the node of shard i of the right index | `colo_rhs_<key>` | `JOIN` |
| `lookup-aligned` (control) | `colo_lhs_aligned_<key>` | `lookup_idx_<key>_f10` | `LOOKUP JOIN` |

The control separates the effect of the left index layout from the effect of the join implementation. Skip it with
`include_lookup_aligned_control:false`.

The expectation to test is `both-colocated` < `rhs-sharded-on-join-key` < `lookup-unrouted`. Remember that the lookup index has a full copy
on every node, so `LOOKUP JOIN` never makes a remote request, while `rhs-sharded-on-join-key` has to send the rows whose key lives on
another node's shard.

### Indices

The data is the corpora of the `joins` track, nothing new is generated.

* Left, from `documents.json.bz2` (10M documents): `id`, `@timestamp`, `key_1000` ... `key_100000000` (keyword) and
  `field_0` ... `field_99` (dynamic). `colo_lhs_aligned_<key>` is routed by `key_<key>` (`index.routing_field`, `_routing` required).
* Right, from `lookup_idx_<key>_f10` (`key_<key>` and `lookup_keyword_0` ... `lookup_keyword_9`, all keyword): each corpus is loaded
  into the routed `colo_rhs_<key>` and into the lookup index with the same documents. Right indices are always loaded
  completely, so every left key finds its match. The key values are `0` to `<key> - 1` on both sides.
* Key-only right indices, with the join key as their only field: `colo_rhs_<key>_key_only` (routed by the join key like `colo_rhs_<key>`, shard
  i on the node of shard i of the aligned left index) and `lookup_idx_<key>_key_only` (lookup mode, 1 shard). They are copied from `colo_rhs_<key>`
  with `_reindex`, keeping only `key_<key>`, and used by `stats-count-key-only`.
* `colo_seed_<key>` is only used to place shards, see below.

`<key>` is `100000` or `1000000`. With `include_fanout:true` there is also `colo_rhs_100000_x10` and `lookup_idx_100000_f10_x10`
(10 right documents per key), which multiplies the number of output rows by 10; its key-only indices are `colo_rhs_100000_x10_key_only` and
`lookup_idx_100000_x10_key_only`.

#### Placement

`layout:aligned` (default): for every join key empty seed indices are created with `number_of_shards` primaries,
`index.routing.allocation.total_shards_per_node: 1`, no rebalancing and `index.routing_field`. The aligned left index and the
colocated right indices are cloned from them, so that `ResizeAllocationDecider` puts shard i of both on the node holding shard i of
the seed. A clone keeps the index sort of its seed, and only the left indices are sorted (see below), so the left index has a seed of its
own (`colo_seed_<key>_lhs`) and the setup moves the shards of the right seed (`colo_seed_<key>`) to the nodes of the left seed's shards
before cloning. This needs `number_of_shards` equal to the number of data nodes and `number_of_replicas:0`. The track checks both and
verifies the final placement before any query runs.

#### Document order of the left indices

How fast a lookup into the right index is depends on the order of the left documents in a shard (consecutive left keys that are close in
the right index's term order are faster). Both left indices are therefore created with `index.sort.field: id` (`left_sort_field`; empty to
turn it off), so that the unrouted and the aligned left index have the same order whatever order the documents arrived in.

#### Force merge

After loading, **every** index the track builds is force merged to one segment (`force_merge`): the left indices, the colocated right indices
(`colo_rhs_<key>` and the key-only index) and the lookup indices. A lookup probes every segment of the right index, so different segment counts
would favour one variant; with one segment each, the variants differ only in layout and in the join. Without it the aligned index, which is filled by `_reindex`, had a worse order
and was slower for both engines (`colocated_joins_plan.md`, section 8.6b).

`layout:misplaced`: the indices are created independently with the same settings and no allocation constraints. Shards land
wherever the allocator puts them, which may or may not be aligned, so this is no guarantee of misalignment.

### Queries

Joins only: no filter, and one aggregation, `COUNT(*)`. Every query exists for every variant, with only the join clause changed:

`FROM <left> [| <pre>] | <JOIN | LOOKUP JOIN> <right> ON key_<key> | <tail>`

| Shape | pre | tail | Measures |
|-------|-----|------|----------|
| `limit1` | | `LIMIT 1` | latency of the first rows |
| `limit10000` | | `LIMIT 10000` | latency of the first rows |
| `keep-before-join-limit10000` | `KEEP @timestamp, key_<key>` | `LIMIT 10000` | latency of the first rows with narrow left rows |
| `keep-after-join-limit10000` | | `KEEP @timestamp, lookup_keyword_0 \| LIMIT 10000` | latency of the first rows; keeps the colocated join's wide left rows (see below) |
| `stats-count` | | `STATS c = COUNT(*)` | **throughput**: the full join of all left rows, with **no `LIMIT` in the query** |
| `stats-count-right-columns` | | `STATS c = COUNT(*), c0 = COUNT(lookup_keyword_0), ..., c9 = COUNT(lookup_keyword_9)` | **throughput**: as `stats-count`, but every variant needs the same ten right columns |
| `stats-count-key-only` | | `STATS c = COUNT(*)` | **throughput**: as `stats-count`, but the right indices are the key-only ones (`colo_rhs_<key>_key_only`, `lookup_idx_<key>_key_only`), so there are no right columns to load |

The two `KEEP` shapes ask for the same kind of narrow output and differ only in where the `KEEP` is. With `KEEP` before the join the colocated
join reads only the kept left columns. On the first build tested, with `KEEP` after the join it still read every left column (see `colocated_joins_plan.md`,
section 8.6); a later build prunes them. `keep-after-join-limit10000` is kept so that regression stays visible. The limit shapes can stop once their limit is reached, so they measure latency. Raising the limit does not give a throughput measure,
because results are truncated at 10000 rows; the three stats shapes are the throughput measures.

`stats-count` is the one shape without a limit. That matters for `LOOKUP JOIN`: with a limit in the query (explicit or the implicit
`LIMIT 1000`), the optimizer copies the limit below the join and the lookup join then runs on the coordinator only. Without a limit it runs
on the data nodes like the colocated join (`colocated_joins_plan.md`, section 8.6). So the limit shapes compare a colocated join
distributed over the data nodes with a lookup join on one node, and the three stats shapes compare both distributed.

The left page size is set from the plan's estimated row size. On the first build tested the colocated join estimated a wider row than the
lookup join for `COUNT(*)`, because it kept the unused right columns (a later build does not). All three stats shapes therefore set the `page_size` pragma (`stats_page_size`, default
4096) so that every variant reads about the same number of left pages. `stats-count` shows the cost of unused right columns where a build keeps them, and
`stats-count-right-columns` needs the same columns in every variant, and `stats-count-key-only` has no right columns at all, so those two
are the fair join comparisons on any build. Operation names
are `esql_<variant>_<key>_<shape>`; variants of one query run one after the other.

### Verification

Before the queries run, the `verify` task (untimed) checks that

* with `layout:aligned`, shard i of each aligned left index and of its right index are started on the same node, and
* every variant returns the same row count, which must be the number of left documents times the fan-out. This holds because
  the lookup join keeps unmatched rows and the colocated join drops them, and no left key is unmatched.

Disable it with `verify_results:false`.

### Parameters

This track allows to overwrite the following parameters using `--track-params`:

* `number_of_shards` (default: 3): primary shards of the left indices and of the colocated right indices. Must equal the number of
  data nodes for `layout:aligned`.
* `number_of_replicas` (default: 0): replicas of the left indices and of the colocated right indices. Must be 0 for `layout:aligned`.
* `layout` (default: `aligned`): `aligned` or `misplaced`.
* `key_cardinalities` (default: `["100000", "1000000"]`): which joins to run. Only these two values have a corpus in this track. A list has to be passed as JSON, for example `--track-params='{"key_cardinalities":["1000000"]}'`; the `key:value` shorthand turns it into a string.
* `include_fanout` (default: false): also run the join to the right index with 10 documents per key (needs `100000` in
  `key_cardinalities`).
* `include_lookup_aligned_control` (default: true): also run `lookup-aligned`.
* `ingest_percentage` (default: 100): how much of the left corpus to ingest, in percent. Right indices are always ingested completely.
* `bulk_size` (default: 10000) and `bulk_indexing_clients` (default: 8). Left documents have 100 text fields, so a small local heap needs lower values, for example `bulk_size:1000` and `bulk_indexing_clients:2`.
* `reindex_batch_size` (default: 1000): documents per scroll batch when copying indices with `_reindex`.
* `left_sort_field` (default: `id`): `index.sort.field` of both left indices; an empty string turns index sorting off.
* `force_merge` (default: true): force merge every index of the track (left, colocated right and lookup indices) to one segment after loading and
  before the queries.
* `auto_expand_replicas` (default: `0-all`): for the lookup indices.
* `index_settings`: a dictionary of settings added to every index. Placement and routing settings of the track win over it.
* `cluster_health` (default: `green`).
* `verify_results` (default: true).
* `stats_page_size` (default: 4096): left page size of the three stats shapes, set with the `page_size` pragma; 0 leaves it to the planner.
* `query_clients` (default: 1): clients that issue queries in parallel.
* `warmup_iterations` (default: 10) and `iterations` (default: 50).
* `max_concurrent_shards_per_node` (default: 10).

### Running

Locally against a 3-node build of the branch (the same setup as the branch's `colocated_join_setup.sh`):

```
./gradlew run --nodes=3
esrally race --track-path=colocated_joins --pipeline=benchmark-only --target-hosts=localhost:9200 \
  --client-options="basic_auth_user:'elastic',basic_auth_password:'password'" \
  --track-params="ingest_percentage:1"
```

Compare placements with a second run with `layout:misplaced`. Local numbers are not comparable with cloud numbers.

Nightly-style entry for `elasticsearch-benchmarks` (3 data nodes, the same hardware as the `esql-joins` benchmark, which runs on one
node). Build the fork with `esbench benchmark ... --es-repo-url <fork> --revision colocated-join-route-rhs`:

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

### License

The data is the data of the `joins` track, which is available as public domain under the
[Open Data Law](https://opendata.cityofnewyork.us/open-data-law/).
