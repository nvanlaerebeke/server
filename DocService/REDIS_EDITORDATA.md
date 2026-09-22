# Redis-backed editorData (locks, presence, document data and statistics)

## Problem

`editorDataMemory.js` is per-process and entirely in-memory: every save
lock, auth lock, and connected-user's presence lives only in the docservice
process that handled the request. On a single-replica deployment that's
fine — there's only one process to be the source of truth. On a
multi-replica deployment (several docservice instances behind a load
balancer) it isn't: each replica has its own private, disconnected view.

Concretely, this means:

- Two co-authors who land on different replicas can both be granted the
  same save lock at the same time. Each replica sees itself as the only
  editor and grants the lock unconditionally.
- A user joining on one replica has no way to see that the same document is
  already open on another replica — presence is derived purely from that
  replica's own live connections.
- A WOPI host asking "is anyone still editing this document" gets an
  answer scoped to whichever replica happens to serve the request, not the
  real cross-replica state.

None of this shows up in local development or in a single-instance
deployment, which is why it's easy to miss.

## Scope

This module replaces the complete `editorDataMemory` interface with
Redis-backed implementations that are shared across replicas. Save/auth
locks fail closed, while presence, document data and statistics fail open to
the replica-local memory backend when Redis is unavailable. That distinction
is deliberate: granting a lock without a shared Redis result is unsafe, while
temporarily losing shared presence, messages or telemetry is preferable to
making the document service unavailable.

## Enabling it

Set, per replica (all replicas in a deployment must point at the same
Redis):

```json
"services": {
  "CoAuthoring": {
    "server": {
      "editorDataStorage": "editorDataRedis"
    },
    "redis": {
      "host": "...",
      "port": 6379
    }
  }
}
```

The two images differ, and it matters:

- **Orchestrated image** (`docker-entrypoint.sh`): set
  `EDITOR_DATA_STORAGE`. That entrypoint exports `NODE_CONFIG`, which
  outranks every config _file_, so editing `local.json` there has no
  effect. This is also the image whose fabricated sentinel entry is
  described below.
- **Standalone image** (`entrypoint.sh`): it writes `local.json` directly
  and exports no `NODE_CONFIG`, so `local.json` is the only way in and
  `EDITOR_DATA_STORAGE` is inert.

```sh
docker run -e EDITOR_DATA_STORAGE=editorDataRedis \
           -e REDIS_SERVER_HOST=redis ...
```

`EDITOR_STAT_STORAGE` pins the `EditorStat` half separately; left unset it
follows `editorDataStorage`. That matters while `EditorStat` is still
delegated to the memory backend — see Scope.

**Ungated, but not on by default.** Upstream treats a Redis-backed
editorData as a paid-edition addon, selected by the packaging Makefile for
`-ee`/`-ie`/`-de` builds only. Euro-Office does not gate it: the module
ships in the standard `documentserver` build with no licence check, and
any deployment can enable it.

It is still off unless asked for, and deliberately so. The locks fail
closed, so a deployment that selected Redis without a reachable Redis
would have every save refused while the healthcheck stayed green. Opting
in is one environment variable; the failure mode of getting it wrong by
default is a silent save outage.

Note the module name is upstream's addon name. It is chosen so that a
config written against upstream documentation boots here rather than
crash-looping, **not** to claim compatibility: the key schema is ours and
the two implementations share nothing.

`services.CoAuthoring.redis` already exists as a config block
(`canvasservice.js` already reads its `prefix` for an unrelated purpose);
this module reuses the block rather than introducing a second Redis config
surface.

### Minimum Redis version

| Deployment                   | Minimum    | What sets the floor                                            |
| ---------------------------- | ---------- | -------------------------------------------------------------- |
| Standalone / sentinel        | **2.6.12** | `EVAL`, `PEXPIRE` (2.6.0), `SET … PX` (2.6.12)                 |
| Cluster                      | **3.0**    | Cluster mode and hash-tag slot routing did not exist before it |
| Any deployment naming a user | **6.0**    | Two-argument `AUTH`, i.e. ACLs — see below                     |

The command set is `SET … NX EX/PX`, `GET`, `DEL`, `HSET`, `HGET`, `HDEL`,
`HMGET`, `HGETALL`, `HLEN`, `HEXISTS`, `HINCRBY`, `HVALS`, `RPUSH`,
`LRANGE`, `ZADD`, `ZSCORE`, `ZREM`, `ZRANGE`, `ZRANGEBYSCORE`,
`ZREMRANGEBYSCORE`, `SADD`, `SREM`, `SCARD`, `EXPIRE`, `PEXPIRE`, `PTTL`,
`PING`, plus `EVAL` for the Lua scripts. None of those set a floor above
2.6.12.

**Naming a Redis user raises the floor to 6.0.** node-redis sends the
two-argument `AUTH` form when `username` is configured, and two-argument
`AUTH` is Redis 6.0 and later. On Redis 5 or below the server rejects that
form; with fail-closed locks, a password-protected deployment then refuses
every save until its Redis version or configuration is corrected.

If a Redis username is configured, the orchestrated entrypoint places it in
`redis.options.user`, and node-redis maps that to `username`. The current
entrypoint defaults that value to `default`; deployments targeting Redis 5 or
older must adjust the generated configuration to omit `user` and use
password-only authentication.

**Raise this deliberately, not by accident.** A single convenient command
can move the floor a long way — `GETDEL`, the obvious implementation of
`getdelSaved`, would take the whole module from 2.6 to **6.2**. That may
well be an acceptable trade, but it is a deployment-compatibility decision
and belongs in this table with a reason next to it, not buried in whichever
method happened to need it. A `GET`-then-`DEL` inside a Lua script is
atomic on every version we already require, so the trade is avoidable.

### Why all three topologies, and not standalone only

Supporting only a standalone Redis would be cheaper to write. It is the
wrong trade, and the reasons are worth recording so the question does not
get reopened casually.

**It reintroduces the single point of failure this module exists to
remove.** Per-process editor state is the liability: lose one pod and you
lose that pod's sessions. Requiring one standalone Redis moves that
liability rather than removing it, and makes it worse — because the locks
fail closed, losing that one Redis means _every_ replica refuses _every_
save, behind a green healthcheck. Shipping a high-availability feature that
mandates a non-HA dependency is incoherent, and excluding sentinel is the
same argument in miniature: it leaves no HA story at all for the state that
the whole deployment now depends on.

**Clustered Redis is what real deployments of this run.** The only
production deployment to have independently measured this module runs
Valkey in cluster mode across six nodes; standalone-only would exclude it,
and with it the only external evidence the module works.

**It cannot be deferred, because it is a key-format decision.** Cluster
support is a hash tag in `buildKey` plus constructing `redis.createCluster`.
Adding it later changes the key format at that point, which means a rolling
upgrade where two cohorts do not share a lock — transiently reintroducing
the exact defect this module fixes. A brace pair now; a migration later.

**It would fail in the worst available way.** Nothing in a standalone-only
client detects that it is pointed at a cluster. The result is `CROSSSLOT`
at runtime on particular operations which, with fail-closed locks, means
refused saves rather than a startup error — a healthy-looking deployment
that silently will not save, which is precisely the failure this module was
written in response to.

### Picking the topology: `services.CoAuthoring.redis.mode`

| `mode`           | Client                                                                                                                  |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `auto` (default) | Cluster when `optionsCluster.rootNodes` is configured; sentinel when a credible sentinel list is; standalone otherwise. |
| `standalone`     | `redis.createClient({socket: {host, port}, ...options})`, with sentinel-only options stripped.                          |
| `sentinel`       | `redis.createSentinel(...)`, honouring `options.sentinels`. Errors if that array is empty.                              |
| `cluster`        | `redis.createCluster(...)` over `optionsCluster.rootNodes`. Errors if the list is empty.                                |

Anything else throws at startup and names the valid set. That is
deliberate: a mode that fell through to standalone turned a typo into
connection-refused against `localhost`, which sends an operator looking at
Redis rather than at their own configuration.

**Why `auto` does not simply trust `options.sentinels`.** Older image
entrypoints could emit a fabricated sentinel containing the standalone
server itself. Treating that as a real sentinel would select sentinel mode
on every non-sentinel deployment, where it never connects — and because the
locks fail closed, every save is refused while the deployment looks healthy.

So `auto` ignores exactly that shape: a single sentinel whose host and port
are the standalone server's own. A real sentinel list never looks like that,
since sentinels run on their own port, and such a list is still selected.
Set `mode` explicitly if you would rather not rely on the distinction —
an explicit mode always wins.

The orchestrated entrypoint states `mode: "auto"` and writes Sentinel
settings under `services.CoAuthoring.redis.options`. The client therefore
uses the same configuration shape for standalone, Sentinel and cluster
deployments. `iooptions` is no longer consumed; images or local configuration
still producing it must be updated.

`NODE_CONFIG` is exported by the same entrypoint and outranks every config
_file_, so on the official image `local.json` cannot override any of this;
only a `--NODE_CONFIG` command-line argument can, and it must be preceded
by a bare `--` because the entrypoint runs `getopts` over what follows.

## What you see when Redis is unavailable

Worth knowing before it happens, because most of it is deliberate and none
of it is loud.

| Symptom                                            | Meaning                                                                                                                                                                                                                                                      |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Saves refused                                      | The locks fail closed. Expected while Redis is unreachable; the alternative is granting a lock that isn't held.                                                                                                                                              |
| `/healthcheck` unhealthy on every replica          | `healthCheck()` is false unless the client is `ready`, and the route throws on that. **Check what your deployment probes**: a liveness probe here restarts every pod during a Redis outage, which does not help. A readiness probe is the defensible choice. |
| One `error` line naming the store, then quiet      | The throttle. A reminder line follows once a minute with a suppressed count, and an `info` on recovery.                                                                                                                                                      |
| `warn` lines about a replica-local presence read   | Presence fell back to this replica's own view. Documents stay openable; some decisions are deliberately skipped — see the reader table below.                                                                                                                |
| A force-save returning error 1 where it returned 0 | `startForceSave` refusing rather than silently saving nothing. See the reader table.                                                                                                                                                                         |
| Startup throws naming a config key                 | A mode, node list or `db` that cannot be honoured. The message names the key.                                                                                                                                                                                |

If you see _nothing at all_ and saves are failing, that is the failure this
module was built in response to — check that the deployment is actually
running a build containing it.

## Architecture

| File                             | Responsibility                                                                                                                                                                                      |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `editorDataRedis.js`             | Composition root: owns the Redis connection and memory delegate, wires the locks, presence, data and statistics stores against them, and exposes the existing `EditorData`/`EditorStat` interfaces. |
| `editorDataRedisClient.js`       | Builds the node-redis client for the deployment's topology — standalone, sentinel or cluster — per `services.CoAuthoring.redis.mode`.                                                               |
| `editorDataRedisData.js`         | Redis-backed object locks, messages, saved state, force-save state and timers.                                                                                                                      |
| `editorDataRedisStat.js`         | Redis-backed unique-user, connection, shard, shutdown, licence and notification statistics.                                                                                                         |
| `editorDataRedisSaveLock.js`     | `lockSave`/`unlockSave`/`lockAuth`/`unlockAuth`, as atomic Lua scripts.                                                                                                                             |
| `editorDataRedisReport.js`       | Throttled failure reporting shared by the stores, so a degraded backend is visible without flooding the log.                                                                                        |
| `editorDataRedisPresence.js`     | `addPresence`/`updatePresence`/`removePresence`/`getPresence`/`getDocumentPresenceExpired`/`removePresenceDocument`.                                                                                |
| `editorDataRedisShardedSweep.js` | A pre-sharded "which (tenant, docId) pairs are due for a sweep" structure, shared by presence's doc-expiry sweep.                                                                                   |
| `editorDataRedisKeys.js`         | Key/member encoding shared by all of the above.                                                                                                                                                     |

### Fail-closed locks, fail-open data and presence

The stores deliberately behave differently on a Redis error, because
the cost of being wrong differs:

- **Locks fail closed.** A Redis error or timeout comes back as a denial
  (`false` / `LOCKED`), never a throw and never a false grant. The caller
  already knows how to handle a denied lock (retry); it has no way to
  handle a lock that looked granted while it wasn't actually held.
  `commandTimeout: 300` on the Redis connection is what makes "Redis is
  unreachable" fail _promptly_ rather than hang every save/auth request on
  that document indefinitely — fail-closed only works if the failure
  itself arrives quickly.

  On a cluster the timeout is passed to node-redis for the routed command,
  and the client also has `disableOfflineQueue: true`. An unreachable node
  therefore rejects or aborts the command instead of retaining it for a
  later reconnect, so the caller receives a denial rather than a delayed
  lock result.

  **A denial has to stay denied.** node-redis' command timeout uses an abort
  signal for the queued command, while `disableOfflineQueue` prevents a
  command issued before readiness from being retained. Both protections are
  applied after the operator's options so a deployment cannot accidentally
  re-enable the replay path for lock operations.

- **Document data and presence fail open.** A Redis error falls back to the memory
  backend's local-connections-only view instead of throwing. The failure
  mode here is a replica-local view of the document state. That can make
  messages, object locks and force-save state temporarily inconsistent across
  replicas, but letting the error propagate would turn a Redis outage into
  document requests failing. The failure is throttled and reported.

- **Statistics fail open.** Statistics use the same memory fallback as the
  original backend. A Redis statistics failure must not prevent a connection
  from being added or removed; the affected counters remain local until Redis
  recovers.

### What each presence reader does with an unreliable result

A fail-open `getPresence` tags its result with `presenceUnknown`. Note this
is a property set on the returned array, so deriving a new array from it
(`filter`/`map`/`slice`/spread) drops the marker silently — read it before
transforming. `isPresenceUnreliable()` in `DocsCoServer.js` is the single
reader, so a degraded read is visible in the operational log rather than
silent. It logs at `warn` from the sites that decide something and at
`debug` from those called on every document operation — a sustained outage
makes every read unreliable, and a per-operation `warn` would bury the
handful of entries worth seeing.

There is no uniform "safe" answer, so each caller decides for itself:

| Reader                                           | Behaviour on an unreliable read                                                                                                                                                                     |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getEditorsCount`                                | Reports 1 editor, so `hasEditors` never releases the WOPI lock or save-lock keys on a local guess.                                                                                                  |
| `publish`                                        | Forces `needPublish`, and skips the "all connections are local" pubsub shortcut — the local-only fallback makes that count match trivially, which would strand the message on one replica.          |
| `startForceSave`                                 | Refuses the save and returns `UnknownError`. See below.                                                                                                                                             |
| `closeDocument`                                  | Skips `removePresenceDocument`, and the pre-stop `editorStatProxy.deleteKey` on the viewer path — both gated on a `length <= 0` that may be a false zero. The native key TTL reaps anything leaked. |
| `isUserReconnect`                                | Unchanged, logs only. Claiming a reconnect would strand the departing user's block locks until TTL and suppress the participants update; claiming none is caught downstream by `hasEditors`.        |
| `getOriginalParticipantsId`, `getParticipantMap` | Unchanged, log only — participants cannot be invented, so a short list is the only honest answer.                                                                                                   |

**Behaviour change for integrators.** `startForceSave` previously returned
`NoError` with nothing started whenever it decided the document was
encrypted. On an unreliable presence read an encrypted editor on another
replica is invisible, so the old code would instead force-save a document
it could not decrypt and write a corrupt file. It now treats an unreliable
read as encrypted _and_ sets `res.code` to `UnknownError`. Four callers
surface that code, and one of them is the integrator-facing path:

- **`Button`** (`forceSaveStart`) and **`Form`** (`sendForm` RPC) return it
  to the browser, where `DocsCoApi._onForceSaveStart` in
  `sdkjs/common/docscoapi.js` routes anything that is neither `NoError` nor
  `NotModified` to `onWarning(c_oAscServerError.Unknown)`. The user gets a
  visible error instead of a save that silently did nothing — the generic
  unknown-error warning, not a specific "your save failed" message.
- **`Command`** — the HTTP `forcesave` command sets `output.error` from
  this code, so a WOPI host or connector now sees error 1 where it
  previously saw 0 with nothing done. **This is the one a downstream
  integration will notice**, and the one to mention in release notes.
- **`Internal`** (`saveRelativeFromChanges`) returns it over `sendDataRpc`.
  `Timeout` force-saves from `gc.js` discard the result, so nothing stops
  retrying. This only fires when the Redis-backed backend is enabled and
  Redis is unreachable; the default `editorDataMemory` backend never sets the
  marker and is unaffected.

The pre-stop `editorStatProxy.deleteKey` call in `closeDocument` is guarded
too. It is the `else` of the editors-only branch — the _viewer_ disconnect
path, which is easy to misread as unreachable: it sits in an `else` whose
sibling handles editors, inside the `if (!reconnected)` block where `hvals`
is assigned. It fires when the last viewer of a document disconnects during
a pre-stop with `REDIS_SERVER_DB_KEYS_NUM` set, and its `length <= 0` test
can be a false zero like any other.

### Redis Cluster: one hash slot per document

On a cluster every key a multi-key command touches must live in one hash
slot. That applies to the two-key presence scripts (`WRITE`, `REMOVE`,
`REFRESH` span a hash and a sorted set) and equally to the two-key `DEL`s
in `editorDataRedisSaveLock.cleanup()` and `removePresenceDocument()`.

`buildKey` therefore wraps `tenant:docId` in a Redis hash tag —
`ds:presence:{localhost:doc1}` — so only that part is hashed and every key
belonging to one document co-locates, while different documents still
spread across the keyspace. The prefix that distinguishes the four key
kinds stays outside the braces, where it cannot affect the slot.

The sweep's own keys (`editorDataRedisShardedSweep.js`) are built
separately, and `CLAIM`/`TRACK` are single-key scripts, so they need no
tag and did not get one.

**Upgrading is not seamless.** This changes the key format, so during a
rolling restart old pods write `ds:lockSave:t:d` while new pods write
`ds:lockSave:{t:d}` and the two cohorts do not share a lock — briefly
reintroducing the very defect this module fixes. It is bounded by the lock
TTL (`expire.saveLock`, 60s) and the presence TTL (`expire.presence`,
300s) and needs no intervention, but a co-authoring session spanning the
rollout can lose a save. Drain rather than rolling-restart if that matters.

### Reporting failures without drowning in them

The stores swallow Redis errors by design, and originally none of them logged.
A deployment that could not reach Redis at all therefore refused every save
with **nothing whatsoever** in the log.

The fix is not an error per catch: a broken deployment fails on every
operation, hundreds a minute, which hides the signal just as effectively.
`editorDataRedisReport.js` logs the _transition_ into failure at `error`
level — the line worth alerting on — then stays quiet apart from a
once-a-minute reminder carrying the suppressed count, and logs recovery at
`info`.

Connection failures are reported through the same throttle. node-redis emits
those as `'error'` and `'reconnecting'` events rather than as ordinary
command rejections, so the composition root listens and routes them here.
The client applies the same error handling to standalone, sentinel and
cluster connections.

**One reporter per concern, not per store.** A reporter that sees an
interleaved failure and success re-arms its own throttle every time. That
is not hypothetical here: a failing `docExpSweep.track()` sits _inside_ an
otherwise successful presence write, so a shared reporter logged a failure
and then a recovery on every single heartbeat — the worst behaviour in
exactly the partial-outage case the throttle exists for. Presence and its
doc-expiry sweep therefore report separately. Any future best-effort
sub-operation nested under another needs its own reporter for the same
reason.

### Owner-token locks, not fencing tokens

`editorDataRedisSaveLock.js`'s locks are reentrant acquire-or-refresh by
the same owner with stateless release — not true Kleppmann fencing tokens.
A lock holder can prove it once acquired the lock, not that it still holds
it at the moment a write actually lands. This is a known, currently
accepted gap; it has not been evaluated against the actual write path, so
treat it as open rather than "fine because X."

### Key encoding and the sharded sweep

`editorDataRedisKeys.js` exists because naively joining `tenant` and
`docId` with a separator can collide (`tenant="a:b", docId="c"` vs.
`tenant="a", docId="b:c"`, joined on `:`) — every key and sorted-set member
goes through `encodeURIComponent` first so this can't happen.

`editorDataRedisShardedSweep.js` backs presence's "which documents have no
live presence left at all" sweep with N sorted sets rather than one,
sharded on `(tenant, docId)` - not `tenant` alone, which would otherwise
hash every document in a single-tenant deployment (the common case) to the
same one shard and defeat the whole point. This keeps that one structure
shardable across Redis Cluster hash slots, and bounds a thundering herd of
due entries against being claimed and processed in a single unbounded
call. It's written generically enough that another sweep needing the same
claim-once semantics (e.g. a future force-save timer) could reuse it
instead of duplicating the Lua scripts.

The sweep's keys are single-key throughout, so they were Cluster-ready
before the hash tag existed and are unaffected by it. What the tag fixed
was everything else — see "Redis Cluster: one hash slot per document"
above.

## Testing

`tests/unit/editorDataRedis.tests.js`, `editorDataRedisPresence.tests.js`,
`editorDataRedisData.tests.js`, `editorDataRedisStat.tests.js`,
`editorDataRedisReport.tests.js` and `editorDataRedisKeys.tests.js` cover
the Redis-backed implementation against a real
Redis via `redis-memory-server` (an in-memory Redis, no container needed —
portable to CI). Covered: cross-replica lock/presence discrimination
against isolated instances, reentrancy and TTL expiry, key-collision
avoidance, unlock/lockAuth outcomes, HASH/ZSET write atomicity, the sharded
sweep, fail-closed and fail-open behavior on a simulated Redis error,
memory fallback for document data and statistics, object locks, messages,
saved state, force-save state, timers, unique-user statistics, connection
samples, shard counts, notifications, shutdown and licence state. The suite
also checks tenant isolation, cleanup ordering and
`cleanDocumentOnExit` correctly leaving a still-connected viewer's presence
entry untouched.

`editorDataRedisClient.tests.js` covers topology selection — that an
entrypoint-fabricated `sentinels` array is ignored rather than acted on,
that an unrecognised mode is refused rather than silently downgraded, that
cluster root nodes are normalized, that offline queuing cannot be re-enabled
from operator config, that configured command timeouts reach node-redis, and
that cluster credentials land in node-redis' shared defaults.

**Independently measured.** A production deployment (2 and 4 docservice
replicas on Kubernetes, Valkey standalone and in 6-node cluster mode)
built this branch and measured it against the same binary running
`editorDataMemory`: 13 saves silently discarded, 14 tokens absent from the
saved document and 14 `doc_changes_pkey` collisions under the memory
backend, versus **zero of each** under `editorDataRedis` across five
runs. With the cluster fixes applied they then measured 38-42 cross-replica
lock denials per run across four different pod pairs, with placement read
from the ingress access logs rather than inferred — denials that a
per-process lock cannot produce, which is what makes them evidence the
lock is genuinely shared. Their reports are the source of the sentinel,
CROSSSLOT and silent-failure fixes above.

**What this suite does not cover:** two real docservice processes actually
talking to the same Redis and behaving correctly together end-to-end.
That was verified manually — two real EO docservice replicas sharing one
Redis, behind a real WOPI host (Nextcloud + the `office` app), with two
independent browser sessions split across the two replicas for the same
document — but that setup is not a committed integration test. It
confirmed, empirically, the actual real-world claim this module exists to
support: a joiner on one replica sees an editor already active on the
other, and a WOPI-level lock survives one co-author disconnecting while
another remains active on a different replica. Turning that manual setup
into a committed, CI-runnable integration test is still open.

## Known gaps

- Owner-token locks are not fencing tokens (see above) — open, not
  evaluated against the write path.
- The Redis-backed data and statistics paths have unit coverage, including
  their memory fallback, but document-server end-to-end flows still need a
  real multi-process integration test.
- No committed multi-replica integration test exists; the cross-replica
  claim above has only been verified manually.
- One question from manual testing was never resolved either way: whether
  the WOPI-level lock reliably releases once the _last_ real editor
  disconnects. The one attempt to observe this found no evidence the
  disconnect-cleanup code path had fired within the observation window —
  more likely a limitation of that manual test's timing than a real
  regression, but this was not confirmed in either direction.
- The Redis-error paths now report, and presence and its sweep report
  separately, but reporting is still per concern rather than per operation.
  Presence shares one reporter between reads and writes, so a Redis that
  serves reads while failing writes — a demoted master answering `READONLY`,
  or `maxmemory` with `noeviction` — makes every heartbeat log a failure and
  a recovery instead of one line a minute.
  Where one operation fails while its siblings under the same reporter
  succeed, those successes still re-arm the throttle. Enough to alert on an
  outage; not enough to characterise a partial one precisely.
- `editorDataRedisShardedSweep.claimExpired()` removes each shard's due
  entries as it walks the shards, so a throw partway through discards the
  entries already claimed from earlier shards — gone from Redis and never
  returned to the caller. Those documents' expiry-driven save
  (`checkDocumentExpire` → `createSaveTimer`) then never fires. Predates
  this work, but a cluster with one node down makes it reachable on every
  gc tick rather than only on a mid-loop timeout. Deserves its own fix.
- A docId or tenant containing malformed unicode makes `encodeURIComponent`
  throw, which is caught as a Redis-style failure and reported as one. The
  document then falls back to the memory-only backend permanently, which is
  consistent with presence's fail-open contract, but the log line will
  blame Redis for something Redis did not do.
- `claimExpired()` accumulates every due entry from every shard before
  returning. Each Lua call is batched, but the total is not, so the first
  successful sweep after an extended outage materialises the whole backlog
  in one array. Bounded by real document count, not attacker-influenced,
  but a per-invocation cap would stop recovery producing a spike.
- Pruning removes only members already past their expiry, so it bounds the
  drain rate, not the accrual rate. A party with access to one document who
  opens and ungracefully abandons many connections in a burst can push
  arbitrarily many live members into that document's presence hash inside a
  single `expire.presence` window. That is the mechanism which makes the
  next gap reachable at scale.
- `getPresence` spreads an uncapped `zrangebyscore` result into `hmget`.
  A document would need tens of thousands of simultaneously live
  connections to approach the argument-count ceiling — each requiring a
  real heartbeating connection — so this is noted, not led with.
- `EditorData.close()` has no production caller — nothing invokes it on
  shutdown, so the process relies on the OS tearing the socket down. Its
  `quit()`-then-`disconnect()` fallback is exercised only by the test suite.
- **Cluster support still needs a real-cluster CI run.** The client-selection
  tests cover construction and slot routing setup, but a Redis Cluster job
  should exercise `MOVED` following and cross-node script routing before this
  feature is considered production-ready.
- `getEditorsCount` returning "editors present" on an unreliable read means
  that when the last editor of a document disconnects during a Redis outage,
  `closeDocument` takes the `sendStatusDocument` branch instead of
  `createSaveTimer`, so no save is started at that moment. The save is
  deferred rather than lost: `checkDocumentExpire` in `gc.js` is driven by
  `getDocumentPresenceExpired`, not by a presence read, and calls
  `createSaveTimer` for any expired document that still has changes. The
  exception is a document whose `docExpSweep.track()` never succeeded (Redis
  unreachable at add-presence time too) — that one is not in the sweep set.
  In that scenario the backend has degraded to the memory delegate
  throughout, which is the shipped default's behaviour, so it is not a
  regression introduced here.
