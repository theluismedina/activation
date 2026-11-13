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
| Data sent: delta vs. full refresh | _TBD_ |
| Delta job runtime at 10M / 50M rows | _TBD_ |
| Recovery after worker crash | _TBD_ |
| Urgent-job latency during a large backfill | _TBD_ |

## Running it

Only `data-gen` is functional so far; the other services will be added as they are built.

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

### From source

You need JDK 21 and Docker. The Gradle wrapper downloads everything else.

```sh
./gradlew :data-gen:jibDockerBuild   # builds ghcr.io/theluismedina/activation/data-gen:latest locally
docker compose up -d                 # uses the local image instead of pulling
```

`docker compose down` keeps the generated data; `docker compose down -v` deletes it.

## Non-goals

