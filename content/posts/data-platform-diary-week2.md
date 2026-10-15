+++
authors = ["Carlos Zansavio"]
title = "My data platform diary - Week 2"
date = "2026-07-18"
description = "Let's build a data platform together"
tags = [
    "data-engineering",
	"my-data-platform"
]
categories = [
	"data",
]
draft = false
+++

Here's where things stood at the end of last week:

- [x] MinIO
- [x] Kafka
- [x] Stream Nginx logs into a Kafka topic
- [x] Read the streaming data locally and process it
- [ ] Plan the deploy

Later:

- [ ] Catalog
- [ ] Trino
- [ ] Metabase

## Day 1 — Laying out the repo

Time to actually start coding. Before writing a single job, I needed to figure out how the repository should be structured — something that could scale in more than one direction without becoming a mess. I also wanted a proper dev environment to test code in before it ever touches the cluster, so I'm setting up a devcontainer with a Spark instance for that.

This is a one-person operation, so a monorepo just makes sense. Everything lives together, which means less friction for me day to day. Splitting things into separate repos is a problem for future-me, if it ever becomes one.

After a couple of hours going back and forth on layouts, here's where I landed:

```
├── .devcontainer
├── docs
├── infra
├── spark-image
├── spark-jobs-src
│   ├── nginx-stream
│   │   ├── nginx_stream
│   │   ├── nginx_stream.egg-info
│   │   └── tests
│   └── silver-nginx
│       ├── silver_nginx
│       └── silver_nginx.egg-info
├── spark-manifests
│   ├── batch
│   └── streaming
└── spark-sdk
    ├── spark_sdk
    ├── spark_sdk.egg-info
    └── tests
```

- **`infra/`** — platform infrastructure: `bootstrap/` (applied once, manually, before ArgoCD can take over anything else — namespaces, quotas, priority classes, AppProjects, the root app-of-apps Application, baseline NetworkPolicies), `apps/` (the ArgoCD Applications that make up the app-of-apps ArgoCD watches continuously), and `spark-operator/` (Spark Operator Helm values plus supplementary RBAC manifests). No Spark applications live here.
- **`spark-image/`** — the generic Spark runtime image: Spark, Delta, the Kafka connector jars, and the runtime launcher. No business logic, long-lived, updated infrequently.
- **`spark-sdk/`** — a shared Python library (Kafka/Delta utilities, logging, metrics, config, base job classes) that every application package pulls from.
- **`spark-jobs-src/`** — the actual business logic. Each application is its own Python package, built to a wheel by CI and published to MinIO. No Docker image rebuilds for application-only changes.
- **`spark-manifests/`** — the `SparkApplication` manifests. Bumping a version is just a Git edit; ArgoCD reconciles the cluster from there.
- **`docs/runbooks/`** — VM-side procedures (MinIO, mainly) documented with the same rigor as the manifests, since the cluster isn't fully rebuildable from Git alone.
- **`docs/decisions/`** — lightweight ADRs for the open decisions I make along the way.

All the development happens inside the devcontainer Spark instance from here on.

## Day 2 — DNS, offsets, and a working local stream

Before going further, I needed to register the Kafka domain on my internal network — it was still a raw IP, which isn't great practice. Added a DNS record and updated the Fluent Bit config to match.

While I was in there, I ran into something worth flagging: when Fluent Bit crashes or restarts, it loses track of its log offset. Without persisting that state, a restart means either replaying logs or silently dropping them. The fix is simple enough — point it at a SQLite DB to track state across restarts:

```
[INPUT]
	DB /var/log/flb/nginx-access.db
	DB.Sync Normal
```

With that sorted, I went back to the code. Had to untangle a few devcontainer config issues, but once that was fixed I got a notebook running that consumes the stream locally, pointing at the Kafka broker running outside the container:

```python
from pyspark.sql import DataFrame

from spark_sdk.base import BaseStreamingJob
from spark_sdk.kafka import read_stream


def parse(raw: DataFrame) -> DataFrame:
    return raw.selectExpr("CAST(value AS STRING) AS raw_log", "timestamp")


class NginxStreamJob(BaseStreamingJob):
    app_name = "nginx-stream"

    def start_stream(self):
        raw = read_stream(
            self.spark, "x.y.z.a:9092", "nginx-access-logs"
        )

        return (
          parse(raw).writeStream
          .outputMode('append')
          .format('console')
          .option('truncate', 'false')
          .option('numRows', 20)
          .trigger(processingTime="5 seconds")
          .start()
        )

NginxStreamJob().run()
```

The common library lives in `spark_sdk`, and I'll keep growing it alongside whatever job needs it next.

Ran it, and the topic started flowing straight into the console output:

```
26/07/19 16:10:15 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect. 26/07/19 16:10:15 WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: /tmp/temporary-5c0bed23-b9e3-44a2-9a64-a43edef5e35d. If it's required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort. 26/07/19 16:10:15 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled. 26/07/19 16:10:16 WARN AdminClientConfig: These configurations '[key.deserializer, value.deserializer, enable.auto.commit, max.poll.records, auto.offset.reset]' were supplied but are not used yet.

------------------------------------------- Batch: 0 ------------------------------------------- +-------+---------+ |raw_log|timestamp| +-------+---------+ +-------+---------+

------------------------------------------- Batch: 1 ------------------------------------------- +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+ |raw_log |timestamp | +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+ |{"time":1784477440.0,"remote_addr":"104.23.254.53","request_method":"GET","request_uri":"/.well-known/security.txt","status":200,"body_bytes_sent":12167,"request_time":0.002,"upstream_response_time":"0.002","http_referer":"","http_user_agent":"Hello from Palo Alto Networks, find out more about our scans in [https://docs-cortex.paloaltonetworks.com/r/1/Cortex-Xpanse/Scanning-activity](https://docs-cortex.paloaltonetworks.com/r/1/Cortex-Xpanse/Scanning-activity)","host":"sjdr.cloud"}|2026-07-19 16:10:40.562| +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+
```

Everything's working end to end. Next up:

- [ ] Sink to MinIO instead of console
- [ ] Write unit tests for this code
- [ ] Deploy to K8s (this is the part I'm a little nervous about)

## Day 3 — Fixing the IP problem

Before pushing ahead with the MinIO sink, there was one thing worth fixing first: the IP address showing up in the logs was Cloudflare's, not the visitor's. Given I already had plans for later analysis, that wasn't going to cut it — I need the real client IP.

Cloudflare has a [document on restoring the original visitor IP](https://developers.cloudflare.com/support/troubleshooting/restoring-visitor-ips/restoring-original-visitor-ips) that pointed me in the right direction. Updated the Nginx log format to pull it out of the `CF-Connecting-IP` header:

```nginx
log_format json_combined escape=json
  '{'
    '"time":"$time_iso8601",'
    '"client_ip":"$http_cf_connecting_ip",'
    '"remote_addr":"$remote_addr",'
    '"request_method":"$request_method",'
    '"request_uri":"$request_uri",'
    '"status":$status,'
    '"body_bytes_sent":$body_bytes_sent,'
    '"request_time":$request_time,'
    '"upstream_response_time":"$upstream_response_time",'
    '"http_referer":"$http_referer",'
    '"http_user_agent":"$http_user_agent",'
    '"host":"$host"'
  '}';
```

`client_ip` now pulls straight from that header. Nothing needed to change on the Fluent Bit side — it just tails the log file into Kafka regardless of what's in it. From here on, any analysis will be working off real visitor IPs.

## Day 4 — Sinking to MinIO

Today's job was swapping the console sink for a real one: MinIO. Started from the code I already had and built on top of it:

```python
from pyspark.sql import DataFrame

from spark_sdk.base import BaseStreamingJob
from spark_sdk.kafka import read_stream
from spark_sdk.delta import BRONZE, table_path, write_stream
from spark_sdk.kafka import checkpoint_path
from spark_sdk.config import require_env


def parse(raw: DataFrame) -> DataFrame:
    return raw.selectExpr("CAST(value AS STRING) AS raw_log", "timestamp")


class NginxStreamJob(BaseStreamingJob):
    app_name = "nginx-stream"

    def start_stream(self):
        bucket = require_env("LAKEHOUSE_BUCKET")

        raw = read_stream(
            self.spark, "kafka.home.sjdr.cloud:9092", "nginx-access-logs"
        )

        return write_stream(
            parse(raw),
            table_path(bucket, BRONZE, "nginx_logs"),
            checkpoint_path(bucket, self.app_name),
        )
```

First run threw this at me:

```
IllegalArgumentException: bucket is null/empty
```

Turned out Spark simply couldn't see S3 yet. MinIO Community Edition doesn't expose a way to create per-user credentials through code, so I had to go into the console and manually create keys for the Spark user. Once that was done, it worked — I could watch the data landing in the bucket in real time.

Filed away for later: this is already producing a lot of small files, and that's going to need fixing before it becomes a real problem. Deployment is next, and I suspect it'll be the hardest part of this whole project so far.

## Day 5 — Getting ready to deploy

Where things stood going in:

- A notebook to test the SDK and the streaming code
- A notebook to query and analyze the data that's landed so far

Time to move this out of my laptop and into production — the first deployment of its kind on this platform, so there was no existing playbook to lean on.

The plan is to keep using the same container image (`spark-image`) without rebuilding it per change, and instead have it pull the Python package it needs from the bucket. That means the bucket now carries three jobs:

- Delta Lake storage
- Checkpoints and commits for structured streaming
- Python wheel packages

Building the wheel is straightforward with `uv`:

```
uv build
```

That drops the `.whl` into `/dist`. For now I'm deploying it manually — copying the wheel straight into the MinIO bucket — and I'll wire up CI/CD for this later.

First snag: the `nginx-stream` container came up without `spark-sdk` installed:

```
No module named 'spark_sdk'
```

Made sense — there was nowhere for it to pull that package from yet. Built the `spark_sdk` wheel and uploaded it to an `artifacts` path inside the `lakehouse` bucket alongside everything else.

With that in place, it's finally working end to end from my machine: streaming logs in, landing straight into Delta Lake on the `lakehouse` bucket.