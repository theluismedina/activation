# Tiny Activation Engine

A small, working model of a data activation pipeline: it computes what changed in large customer audiences each day and delivers only those changes, reliably, to multiple ad platforms with different rate limits.

Data activation means taking audience data from a warehouse or CRM and sending it to the platforms where it's used, such as ad networks, Connected TV, and email tools. At scale, the hard part isn't sending data; it's sending the right data, reliably, without overwhelming anyone.

## Problem

An activation pipeline has to move large datasets to many destinations on a schedule, and do it reliably. Four problems make this hard:

- **Most data doesn't change.** Audiences are large but typically change only a few percent a day. Re-sending everything wastes compute and uses up destination rate limits, so the pipeline should compute and send only adds and removes.
- **Destinations are slow, limited, and unreliable.** Each platform has its own rate limits and fails in its own ways. Deliveries must retry safely, without ever sending the same data twice.
- **Many tenants share one pipeline.** A huge backfill for one customer must not starve a small, urgent job for another.
- **Failures must be visible.** When a delivery stalls, operators need to see where and why, and trace it to a root cause quickly.

## Goal

Show how each of these problems can be solved with production patterns (delta computation, durable workflow orchestration, rate-limit-aware scheduling, and observability) in a project small enough to run on a laptop. It is a demonstration and research project, not a product.

## How it works

_Architecture diagram and component overview: coming in week 2._

## Results

| Measurement | Result |
|---|---|
| Data sent: delta vs. full refresh | 313k vs. 10M rows (21 MB vs. 637 MB) for a 3% daily change |
| Delta job runtime at 10M / 50M rows | ~27 s at 10M rows, 1–5% changed (16-core laptop, Docker); 50M _TBD_ |
| Recovery after worker crash | _TBD_ |
| Urgent-job latency during a large backfill | _TBD_ |

## Running it

`data-gen` and `delta-job` are functional so far; the other services will be added as they are built.

### With prebuilt images (just Docker)

Every merge to `master` publishes images to GitHub Container Registry for `linux/amd64` and `linux/arm64`.

```sh
git clone https://github.com/theluismedina/activation.git
cd activation
docker compose up -d
```

data-gen's API is then on <http://localhost:8081/api/generation>:

```sh
curl -X POST localhost:8081/api/generation/start   # first run builds a 10M-row baseline; later runs mutate it 1-5%
curl localhost:8081/api/generation                 # progress and last run
curl localhost:8081/api/generation/snapshots       # snapshots kept in the audience-data volume
```

To change the host port, set `DATAGEN_PORT=9000`. To use a specific build instead of `latest`, set `IMAGE_TAG=sha-<commit>`. To use another registry, set `IMAGE_REGISTRY`.

Once a snapshot exists, compute the delta. The job runs once and exits:

```sh
docker compose run --rm delta-job            # first run: the whole audience (FULL); later runs: only the changes
docker compose run --rm delta-job --verify   # also check the result against data-gen's expected delta
docker compose run --rm delta-job --full     # start over with the whole latest audience
```

Running it again when nothing changed does nothing. Each delta's base is the previous delta's target, so if data-gen ran several times in between, the next delta covers all of those runs.

### From source

You need JDK 21 and Docker. The Gradle wrapper downloads everything else.

```sh
./gradlew :data-gen:jibDockerBuild :delta-job:jibDockerBuild   # builds the ghcr.io/theluismedina/activation/* images locally
docker compose up -d                                           # uses the local images instead of pulling
```

`docker compose down` keeps the generated data; `docker compose down -v` deletes it.

## Delta format

The delta-job's output is what the activation workflow consumes. Every delta gets its own directory in the `delta-data` volume, named by its id (e.g. `000004-84d2be35-from-a3ba8247`). The directory is moved into place only when it is complete, and it never changes afterwards.

- **`changes.parquet`** uses the same schema as data-gen's expected deltas: `customer_id`, `segment_id`, `hashed_user_id`, `op` (`ADD`/`REMOVE`). Rows are sorted by `(customer_id, segment_id, op, hashed_user_id)`, so each customer's changes form one contiguous range.
- **`manifest.json`** is the claim check handed to the workflow: the delta id, kind (`INCREMENTAL`, or `FULL` meaning "replace the audience"), base and target snapshots, counts, the file's SHA-256, and each customer's row range (`firstRow`, `rows`, `added`, `removed`). The delta id is deterministic, so it can be used as the Temporal workflow id, and a repeated start is rejected instead of delivering twice.

The diff is a Spark (local mode) full outer join on all three columns, using a shuffled hash join. Spark sorts the result in parallel, and the driver streams it into a single Parquet file. It writes that file itself rather than going through Hadoop's FileSystem, so it runs on Windows without winutils.

## Non-goals

