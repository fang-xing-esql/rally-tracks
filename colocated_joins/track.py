"""
Custom runners for the colocated_joins track.

* ``colocated-setup-indices`` deletes and creates every index the track uses, either with the aligned
  layout (shard i of the left and the right index on the same node) or without placement control. Both left indices are sorted by
  ``id`` (``left_sort_field``), so that the document order in a shard, which decides how fast lookups into the right index are, is the same
  for the unrouted and the aligned left index whatever order the documents arrive in.
* ``colocated-reindex`` copies one index into another with ``_reindex`` (optionally only some source fields) and waits for it. Rally downloads the document sets of a
  track in parallel, so one source file can feed only one index; the other indices are copies.
* ``colocated-verify`` checks the layout and that every join variant returns the same number of rows.

Index definitions are built in Python (see ``build_plan``) so that the aligned and the misplaced layout share
one description of settings and mappings.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

LEFT_UNROUTED = "colo_lhs_unrouted"
KEY_FIELDS = ["key_1000", "key_100000", "key_200000", "key_500000", "key_1000000", "key_5000000", "key_100000000"]
LOOKUP_FIELDS = [f"lookup_keyword_{i}" for i in range(10)]

# settings every index gets; users can override them with the ``index_settings`` track parameter
BASE_SETTINGS = {"index.requests.cache.enable": False, "index.refresh_interval": "30s"}


def _fail(message):
    try:
        from esrally import exceptions

        raise exceptions.RallyAssertionError(message)
    except ImportError:  # pragma: no cover
        raise AssertionError(message)


def left_mapping(routed):
    mapping = {
        "properties": {
            "id": {"type": "long"},
            "@timestamp": {"type": "date"},
            **{key: {"type": "keyword"} for key in KEY_FIELDS},
        }
    }
    if routed:
        mapping["_routing"] = {"required": True}
    return mapping


def right_mapping(key, routed):
    mapping = {"properties": {key: {"type": "keyword"}, **{field: {"type": "keyword"} for field in LOOKUP_FIELDS}}}
    if routed:
        mapping["_routing"] = {"required": True}
    return mapping


def lookup_mapping(key):
    return right_mapping(key, routed=False)


def key_only_mapping(key, routed):
    """The join key and nothing else, so that a join has no right-hand columns to load."""
    mapping = {"properties": {key: {"type": "keyword"}}}
    if routed:
        mapping["_routing"] = {"required": True}
    return mapping


class Plan:
    """Everything the setup runner does, as data, so it can be checked without a cluster."""

    def __init__(self):
        self.indices = []  # all index names this track manages, for deletion
        self.creates = []  # (name, body): plain index creations
        self.seeds = []  # (seed_name, seed_body, [(clone_name, clone_body, mapping)])
        self.align = []  # (follower_seed, leader_seed): the follower's shards are moved to the nodes of the leader's shards

    def all_names(self):
        names = list(self.indices)
        names += [seed for seed, _, _ in self.seeds]
        return names


def build_plan(params):
    shards = int(params.get("number_of_shards", 3))
    replicas = int(params.get("number_of_replicas", 0))
    layout = params.get("layout", "aligned")
    if layout not in ("aligned", "misplaced"):
        raise ValueError(f"layout must be 'aligned' or 'misplaced' but was [{layout}]")
    extra = dict(params.get("index_settings") or {})
    base = {**BASE_SETTINGS, **extra}
    lookup_auto_expand = params.get("auto_expand_replicas", "0-all")
    # Index sorting fixes the document order inside every shard, whichever order the documents arrive in. Without it the aligned left
    # index, which is filled by _reindex from three source shards, has a worse order than the bulk loaded unrouted one, and lookups into the
    # right index are slower for it (see the plan, section 8.6b).
    left_sort_field = params.get("left_sort_field", "id")
    sort_settings = {"index.sort.field": left_sort_field, "index.sort.order": "asc"} if left_sort_field else {}

    plan = Plan()
    plan.creates.append(
        (
            LEFT_UNROUTED,
            {
                "settings": {**base, **sort_settings, "index.number_of_shards": shards, "index.number_of_replicas": replicas},
                "mappings": left_mapping(routed=False),
            },
        )
    )
    plan.indices.append(LEFT_UNROUTED)

    for card in params["cards"]:
        key = card["key"]
        field = f"key_{key}"  # the join key and routing field
        aligned = card["aligned"]
        rhs = card["rhs"]
        lookup = card["lookup"]

        plan.creates.append(
            (
                lookup,
                {
                    "settings": {**base, "index.mode": "lookup", "index.auto_expand_replicas": lookup_auto_expand},
                    "mappings": lookup_mapping(field),
                },
            )
        )
        plan.indices.append(lookup)
        lookup_key_only = card.get("lookup_key_only")
        if lookup_key_only:
            plan.creates.append(
                (
                    lookup_key_only,
                    {
                        "settings": {**base, "index.mode": "lookup", "index.auto_expand_replicas": lookup_auto_expand},
                        "mappings": key_only_mapping(field, routed=False),
                    },
                )
            )
            plan.indices.append(lookup_key_only)
        rhs_key_only = card.get("rhs_key_only")
        if rhs_key_only:
            plan.indices.append(rhs_key_only)

        routed_settings = {**base, "index.routing_field": field}
        # the fan-out join reuses the left index of the join with the same key
        new_left = aligned not in plan.indices
        for name in (aligned, rhs):
            if name not in plan.indices:
                plan.indices.append(name)

        if layout == "misplaced":
            targets = [(rhs, right_mapping(field, routed=True))]
            if new_left:
                targets.insert(0, (aligned, left_mapping(routed=True)))
            if rhs_key_only:
                targets.append((rhs_key_only, key_only_mapping(field, routed=True)))
            for name, mapping in targets:
                plan.creates.append(
                    (
                        name,
                        {
                            "settings": {
                                **routed_settings,
                                **(sort_settings if name == aligned else {}),
                                "index.number_of_shards": shards,
                                "index.number_of_replicas": replicas,
                            },
                            "mappings": mapping,
                        },
                    )
                )
        else:
            # Clones inherit routing_field, the index sort and the allocation settings of the seed. ResizeAllocationDecider puts shard i
            # of a clone on the node that holds shard i of the seed, which aligns the left and the right index. A clone must keep the
            # index sort of its seed, and the right indices have no `id` to sort by, so the sorted left index has a seed of its own; the
            # setup then moves the shards of the right seed to the nodes of the left seed's shards before cloning.
            seed = f"colo_seed_{key}"
            left_seed = f"colo_seed_{key}_lhs" if sort_settings else seed
            clone_settings = {**base, "index.number_of_replicas": replicas}

            def seed_body(sort=None):
                properties = {field: {"type": "keyword"}}
                if sort:
                    properties = {"id": {"type": "long"}, **properties}
                return {
                    "settings": {
                        "index.number_of_shards": shards,
                        "index.number_of_replicas": 0,
                        "index.routing.allocation.total_shards_per_node": 1,
                        "index.routing.rebalance.enable": "none",
                        "index.routing_field": field,
                        "index.blocks.write": True,
                        **(sort or {}),
                    },
                    "mappings": {"_routing": {"required": True}, "properties": properties},
                }

            def add_clones(seed_name, body, clones):
                existing = next((entry for entry in plan.seeds if entry[0] == seed_name), None)
                if existing:
                    existing[2].extend(clones)
                else:
                    plan.seeds.append((seed_name, body, clones))

            if new_left:
                add_clones(
                    left_seed, seed_body(sort_settings), [(aligned, {"settings": clone_settings}, left_mapping(routed=True)["properties"])]
                )
            right_clones = [(rhs, {"settings": clone_settings}, right_mapping(field, routed=True)["properties"])]
            if rhs_key_only:
                right_clones.append((rhs_key_only, {"settings": clone_settings}, key_only_mapping(field, routed=True)["properties"]))
            add_clones(seed, seed_body(), right_clones)
            if left_seed != seed and (seed, left_seed) not in plan.align:
                plan.align.append((seed, left_seed))
    return plan


async def _request(es, method, path, body=None, params=None):
    kwargs = {"method": method, "path": path}
    if body is not None:
        kwargs["body"] = body
    if params:
        kwargs["params"] = params
    return await es.perform_request(**kwargs)


def _body(response):
    return response.body if hasattr(response, "body") else response


async def _wait_green(es, index, timeout="300s"):
    await _request(
        es,
        "GET",
        f"/_cluster/health/{index}",
        params={"wait_for_status": "green", "wait_for_no_relocating_shards": "true", "timeout": timeout},
    )


async def _primary_nodes(es, index):
    rows = _body(await _request(es, "GET", f"/_cat/shards/{index}", params={"format": "json", "h": "index,shard,prirep,node,state"}))
    return {int(row["shard"]): row["node"] for row in rows if row["prirep"] == "p"}


async def align_seed_placement(es, follower, leader):
    """Moves the shards of the (empty) follower seed to the nodes that hold the same shard numbers of the leader seed.

    Clones follow the placement of their seed, so two seeds on different nodes would give left and right indices that are not aligned.
    The follower allows one shard per node, so that limit is lifted while the shards are moved and restored afterwards."""
    want = await _primary_nodes(es, leader)
    have = await _primary_nodes(es, follower)
    if want == have:
        return
    limit = "index.routing.allocation.total_shards_per_node"
    await _request(es, "PUT", f"/{follower}/_settings", body={limit: None})
    commands = [
        {"move": {"index": follower, "shard": shard, "from_node": have[shard], "to_node": node}}
        for shard, node in want.items()
        if have[shard] != node
    ]
    await _request(es, "POST", "/_cluster/reroute", body={"commands": commands})
    await _wait_green(es, follower)
    await _request(es, "PUT", f"/{follower}/_settings", body={limit: 1})
    have = await _primary_nodes(es, follower)
    if have != want:
        _fail(f"could not move the shards of [{follower}] to the nodes of [{leader}]: wanted {want}, got {have}")


async def setup_indices(es, params):
    plan = build_plan(params)
    layout = params.get("layout", "aligned")
    shards = int(params.get("number_of_shards", 3))
    replicas = int(params.get("number_of_replicas", 0))

    if layout == "aligned":
        if replicas != 0:
            _fail("layout=aligned needs number_of_replicas=0, the seed allows one shard copy per node")
        nodes = _body(await _request(es, "GET", "/_cat/nodes", params={"format": "json", "h": "name,node.role"}))
        data_nodes = [n for n in nodes if "d" in n.get("node.role", "")]
        if len(data_nodes) != shards:
            _fail(
                f"layout=aligned needs number_of_shards ({shards}) to equal the number of data nodes ({len(data_nodes)}), "
                "because every index allows one shard per node"
            )

    for name in plan.all_names():
        await _request(es, "DELETE", f"/{name}", params={"ignore_unavailable": "true"})

    for name, body in plan.creates:
        await _request(es, "PUT", f"/{name}", body=body)

    for seed, seed_body, _ in plan.seeds:
        await _request(es, "PUT", f"/{seed}", body=seed_body)
    for seed, _, _ in plan.seeds:
        await _wait_green(es, seed)
    for follower, leader in plan.align:
        await align_seed_placement(es, follower, leader)

    for seed, _, clones in plan.seeds:
        for name, body, _ in clones:
            await _request(es, "POST", f"/{seed}/_clone/{name}", body=body, params={"wait_for_active_shards": "1"})
        for name, _, properties in clones:
            await _wait_green(es, name)
            await _request(es, "PUT", f"/{name}/_mapping", body={"properties": properties})
            await _request(es, "PUT", f"/{name}/_settings", body={"index.blocks.write": None})

    for name in plan.all_names():
        await _wait_green(es, name)
    return {"weight": 1, "unit": "ops", "success": True}


async def reindex(es, params):
    source = params["source"]
    dest = params["dest"]
    poll_interval = float(params.get("poll_interval", 2))
    # _reindex reads through search, and the source was bulk loaded with a long refresh interval
    await _request(es, "POST", f"/{source}/_refresh")
    reindex_source = {"index": source, "size": int(params.get("batch_size", 1000))}
    if params.get("source_fields"):
        reindex_source["_source"] = list(params["source_fields"])
    response = _body(
        await _request(
            es,
            "POST",
            "/_reindex",
            body={"source": reindex_source, "dest": {"index": dest}},
            params={"wait_for_completion": "false", "slices": "auto", "refresh": "false"},
        )
    )
    task = response["task"]
    while True:
        status = _body(await _request(es, "GET", f"/_tasks/{task}"))
        if status.get("completed"):
            break
        await asyncio.sleep(poll_interval)
    result = status.get("response", {})
    if status.get("error") or result.get("failures"):
        _fail(f"reindex of [{source}] into [{dest}] failed: {status.get('error') or result.get('failures')}")
    count = int(result.get("created", 0))
    logger.info("reindexed %s documents from [%s] into [%s]", count, source, dest)
    return {"weight": count, "unit": "docs", "success": True}


async def _shard_nodes(es, index):
    rows = _body(await _request(es, "GET", f"/_cat/shards/{index}", params={"format": "json", "h": "index,shard,prirep,node,state"}))
    nodes = {}
    for row in rows:
        if row["prirep"] != "p":
            continue
        if row["state"] != "STARTED":
            _fail(f"primary of [{index}] shard [{row['shard']}] is in state [{row['state']}]")
        nodes[int(row["shard"])] = row["node"]
    return nodes


async def check_placement(es, left, right):
    """Raises unless shard i of both indices is on the same node, for every i."""
    left_nodes = await _shard_nodes(es, left)
    right_nodes = await _shard_nodes(es, right)
    if set(left_nodes) != set(right_nodes):
        _fail(f"[{left}] has shards {sorted(left_nodes)} but [{right}] has shards {sorted(right_nodes)}")
    misplaced = {i: (left_nodes[i], right_nodes[i]) for i in left_nodes if left_nodes[i] != right_nodes[i]}
    if misplaced:
        _fail(f"shards of [{left}] and [{right}] are not on the same node: {misplaced}")


async def _row_count(es, query):
    response = _body(await _request(es, "POST", "/_query", body={"query": query}))
    return int(response["values"][0][0])


async def verify(es, params):
    layout = params.get("layout", "aligned")
    for card in params["cards"]:
        key = card["key"]
        fanout = int(card.get("fanout", 1))
        aligned = card["aligned"]
        rhs = card["rhs"]
        lookup = card["lookup"]

        rhs_key_only = card.get("rhs_key_only")
        lookup_key_only = card.get("lookup_key_only")

        if layout == "aligned":
            await check_placement(es, aligned, rhs)
            if rhs_key_only:
                await check_placement(es, aligned, rhs_key_only)

        left_docs = _body(await _request(es, "GET", f"/{LEFT_UNROUTED}/_count"))["count"]
        aligned_docs = _body(await _request(es, "GET", f"/{aligned}/_count"))["count"]
        if left_docs != aligned_docs:
            _fail(f"[{LEFT_UNROUTED}] has {left_docs} documents but [{aligned}] has {aligned_docs}")
        expected = left_docs * fanout

        counts = {
            "lookup-unrouted": f"FROM {LEFT_UNROUTED} | LOOKUP JOIN {lookup} ON key_{key} | STATS c = COUNT(*)",
            "rhs-sharded-on-join-key": f"FROM {LEFT_UNROUTED} | JOIN {rhs} ON key_{key} | STATS c = COUNT(*)",
            "both-colocated": f"FROM {aligned} | JOIN {rhs} ON key_{key} | STATS c = COUNT(*)",
            "lookup-aligned": f"FROM {aligned} | LOOKUP JOIN {lookup} ON key_{key} | STATS c = COUNT(*)",
        }
        if rhs_key_only and lookup_key_only:
            counts.update(
                {
                    "lookup-unrouted-key-only": f"FROM {LEFT_UNROUTED} | LOOKUP JOIN {lookup_key_only} ON key_{key} | STATS c = COUNT(*)",
                    "rhs-sharded-on-join-key-key-only": f"FROM {LEFT_UNROUTED} | JOIN {rhs_key_only} ON key_{key} | STATS c = COUNT(*)",
                    "both-colocated-key-only": f"FROM {aligned} | JOIN {rhs_key_only} ON key_{key} | STATS c = COUNT(*)",
                    "lookup-aligned-key-only": f"FROM {aligned} | LOOKUP JOIN {lookup_key_only} ON key_{key} | STATS c = COUNT(*)",
                }
            )
        results = {}
        for variant, query in counts.items():
            results[variant] = await _row_count(es, query)
        logger.info("row counts for key [%s]: %s (expected %s)", key, results, expected)
        wrong = {variant: count for variant, count in results.items() if count != expected}
        if wrong:
            _fail(
                f"join variants for [key_{key}] disagree with the expected row count {expected} "
                f"(left documents {left_docs} x fanout {fanout}): {wrong}"
            )
    return {"weight": 1, "unit": "ops", "success": True}


def register(registry):
    registry.register_runner("colocated-setup-indices", setup_indices, async_runner=True)
    registry.register_runner("colocated-reindex", reindex, async_runner=True)
    registry.register_runner("colocated-verify", verify, async_runner=True)
