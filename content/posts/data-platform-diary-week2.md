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
draft = true
+++

This is the tasks from last week:

- [x] MinIO
- [x] Kafka
- [x] Stream Nginx logs into a Kafka topic
- [x] Read the streaming data locally and process it
- [ ] Plan the deploy

Later:
- [ ] Catalog
- [ ] Trino
- [ ] Metabase
# Day 1
It's time to start coding. I need to organize how the repository will look like. I want this to scale in every direction. Also, I need to create a dev environment where I can test the code, before deploying. Probably I will use devcontainer to set this spark instance.

This is one man army operation, so I need to follow a monorepo archictecutre. Things will be easier to me, if everything is together. Maybe in the future we can separate the repos, but I don't this need now.

After some couple of hours trying to find the best monorepo architecture for my problem, I decided to stay in this one for now:
```
├── .devcontainer
├── docs
├── infra
├── spark-image
├── spark-jobs-src
│   ├── nginx-stream
│   │   ├── nginx_stream
│   │   ├── nginx_stream.egg-info
│   │   └── tests
│   └── silver-nginx
│       ├── silver_nginx
│       └── silver_nginx.egg-info
├── spark-manifests
│   ├── batch
│   └── streaming
└── spark-sdk
    ├── spark_sdk
    ├── spark_sdk.egg-info
    └── tests
```

- `infra/` - platform infrastructure: `bootstrap/` (applied once,
  manually, before ArgoCD can self-manage anything else - namespaces,
  quotas, priority classes, AppProjects, the root app-of-apps Application,
  baseline NetworkPolicies), `apps/` (the ArgoCD Applications that make up
  the app-of-apps ArgoCD watches continuously), `spark-operator/` (Spark
  Operator Helm values + supplementary RBAC manifests). Contains no Spark
  applications.
- `spark-image/` - generic Spark runtime image (Spark + Delta + Kafka
  connector jars + the runtime launcher). No business logic; long-lived,
  **updated infrequently**.
- `spark-sdk/` - shared Python library (Kafka/Delta utilities, logging,
  metrics, config, base job classes) used by every application package.
- `spark-jobs-src/` - business logic. Each application is an
  independent Python package, built to a wheel by CI and published to
  MinIO. No Docker images built per application change.
- `spark-manifests/` - `SparkApplication` manifests. Updating an
  application version is a Git edit; ArgoCD reconciles the cluster.
- `docs/runbooks/` - VM-side procedures (MinIO) with the same rigor as the
  manifests, since the cluster is NOT fully rebuildable from Git alone.
- `docs/decisions/` - lightweight ADRs for the open decisions made along
  the way.

I'm going to do all the development code using a devcontainer Spark instance.

# Day 2
Before keep working on this, I need to register the Kafka domain inside my network. It's still a raw IP, so it's not a good practice.
So I registered a new DNS register, this will make my life easier. It's a good practice to also update on fluent bit conf as well.
When updating the brokers field in FluentBit, I saw an issue: when FluentBit crashes (or in this case, restarted), it loses the logs offset, so I need to have a way to save this state after crashes or restarts.
And this is easy, I just need to add this configuration, so it saves on a SQLite database:

```
[INPUT]
	DB /var/log/flb/nginx-access.db
	DB.Sync Normal
```
After doing that, I went back to code. I had to fix some issues on devcontainer config but after fixing it, I could create a python notebook to consume my stream locally (from Kafka that is running outside).

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
The _common library_ is inside spark_sdk and I'm going to update the library with the projects as it's happening.

With this code, we can consume from topic:
```
26/07/19 16:10:15 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect. 26/07/19 16:10:15 WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: /tmp/temporary-5c0bed23-b9e3-44a2-9a64-a43edef5e35d. If it's required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort. 26/07/19 16:10:15 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled. 26/07/19 16:10:16 WARN AdminClientConfig: These configurations '[key.deserializer, value.deserializer, enable.auto.commit, max.poll.records, auto.offset.reset]' were supplied but are not used yet.

------------------------------------------- Batch: 0 ------------------------------------------- +-------+---------+ |raw_log|timestamp| +-------+---------+ +-------+---------+

------------------------------------------- Batch: 1 ------------------------------------------- +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+ |raw_log |timestamp | +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+ |{"time":1784477440.0,"remote_addr":"104.23.254.53","request_method":"GET","request_uri":"/.well-known/security.txt","status":200,"body_bytes_sent":12167,"request_time":0.002,"upstream_response_time":"0.002","http_referer":"","http_user_agent":"Hello from Palo Alto Networks, find out more about our scans in [https://docs-cortex.paloaltonetworks.com/r/1/Cortex-Xpanse/Scanning-activity](https://docs-cortex.paloaltonetworks.com/r/1/Cortex-Xpanse/Scanning-activity)","host":"sjdr.cloud"}|2026-07-19 16:10:40.562| +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+
```
Which means that everything is working as it should. The next days I will have to work on:
- [ ] Sink to MinIO, instead of console
- [ ] Create unittests for this code
- [ ] Deploy to K8s (and this is where I have to deploy all my manifests, and I'm kind worried about that).
# Day 3