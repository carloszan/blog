+++
authors = ["Carlos Zansavio"]
title = "Apache Spark exercises to test and improve your knowledge"
date = "2026-07-11"
description = "Math exercises to test and improve your theoretical knowledge about Apache Spark"
tags = [
    "exercise",
	"data-engineering",
]
categories = [
    "data",
]
+++

You've read the docs. You can write queries using SparkSQL or PySpark. You're pretty sure you understand how Spark works under the hood. Then an interviewer asks you to actually reason through a shuffle, and that confidence gets shaky fast.

Been there.

What actually got these concepts to stick for me was working through math problems tied to Spark's internals. You have to understand the mechanics or the numbers don't add up.
So I put together a set of exercises built around exactly that.

Full disclosure: I generated the first draft with AI, then went through the whole list myself to make sure every question actually holds up and reflects what gets asked in real interviews.

Stuck on a question? Check the solution. Still not clicking? Feed it to an AI and ask it to explain it differently. That’s what I did. It forced me to think about the gaps I didn't even know I had.

Give it a try.

# Exercise List 1

---

## Section 1 — Partitioning

### Exercise 1.1 — Basic partition count

You have a 500 GB dataset in S3 stored as uncompressed Parquet. Spark's default `spark.sql.files.maxPartitionBytes` is 128 MB. How many partitions will Spark create when reading this data?

### Exercise 1.2 — Compression skews the math

Same 500 GB dataset, but now it's Snappy-compressed Parquet with a compression ratio of ~4:1 (i.e., 500 GB on disk decompresses to ~2 TB in memory). Spark decides partition count based on the _file size on disk_, not decompressed size. Explain why this can cause OOM errors on executors, and calculate the actual in-memory size per partition if the file-based partitioning still targets 128 MB per partition.

### Exercise 1.3 — Shuffle partitions sizing

You have a job whose shuffle stage will move 800 GB of data. You want each shuffle partition to land around 200 MB (a healthy target to avoid small-file problems downstream). What value should you set `spark.sql.shuffle.partitions` to?

### Exercise 1.4 — Too many small partitions

A DataFrame has 40,000 partitions, but the total data size is only 10 GB. What's the average partition size? What problem does this cause, and what's the fix (name the config or technique)?

### Exercise 1.5 — Repartition vs coalesce cost

You have a DataFrame with 2,000 partitions and want to reduce it to 200 for writing output. a) If you use `coalesce(200)`, does this trigger a full shuffle? b) If you use `repartition(200)`, does this trigger a full shuffle? c) Given 2,000 partitions merging into 200, roughly how many source partitions feed each target partition under `coalesce`, and why does this matter for skew?

---

## Section 2 — Memory Management

### Exercise 2.1 — Unified memory pool

An executor is launched with `--executor-memory 20g`. Default settings: `spark.memory.fraction = 0.6`, `spark.memory.storageFraction = 0.5`. a) How much memory is reserved for "reserved memory" (assume the standard 300 MB reserved)? b) How much is allocated to the unified memory pool (execution + storage)? c) How much is the minimum guaranteed storage memory within that pool?

### Exercise 2.2 — Executor memory overhead

You request `--executor-memory 16g`. Spark's default overhead formula is `max(384 MB, 0.10 * executorMemory)`. a) What is the overhead in this case? b) What is the _total_ memory YARN/Kubernetes will actually reserve per executor? c) If your cluster node has 64 GB total RAM and you want to fit 4 executors per node, does 16 GB executor memory fit? Show the math including overhead.

### Exercise 2.3 — Broadcast join threshold

`spark.sql.autoBroadcastJoinThreshold` is set to the default 10 MB. You're joining a fact table (2 TB) with a dimension table. The dimension table's Parquet file is 8 MB on disk but has a 3:1 in-memory expansion ratio when deserialized. a) Will Spark broadcast it automatically? What number does Spark actually compare against the threshold — file size or in-memory size? b) If broadcasting causes an executor OOM despite passing the threshold check, what's the likely explanation?

### Exercise 2.4 — Spill estimation

A single executor has 4 cores and 8 GB usable execution memory (after overhead/reserved memory is subtracted). It's running a `groupByKey` aggregation where each of the 4 concurrent tasks needs to hold roughly 3 GB of shuffle data in memory to avoid spilling. a) Will this configuration spill to disk? Do the math. b) Name two ways to fix it without adding hardware.

---

## Section 3 — Cluster Sizing

### Exercise 3.1 — Executors per node

You have worker nodes with 128 GB RAM and 32 cores each. Best practice caps executor cores at 5 (to avoid HDFS I/O throughput issues) and leaves 1 core + ~1 GB per node for the OS/daemons. a) How many executors can you fit per node (core-wise)? b) Given that core constraint, how much memory should each executor get (ignore overhead for this step)? c) Recompute part (b) accounting for the default executor memory overhead (10%, min 384 MB).

### Exercise 3.2 — Total parallelism

Using your answer from 3.1, if the cluster has 20 worker nodes, what is the total number of concurrent tasks the cluster can run at once?

### Exercise 3.3 — Right-sizing for a job

A batch job reads 3 TB of input data. You want each task to process ~256 MB of input (a reasonable target for CPU-bound transforms). a) How many tasks/partitions do you need? b) Using the cluster from 3.1/3.2 (total concurrent task slots), how many "waves" of tasks will this job run in? c) If each wave takes ~40 seconds, what's your rough total runtime estimate for just the map stage (ignore shuffle/scheduling overhead)?

### Exercise 3.4 — Skewed partition, back-of-envelope

A shuffle stage has 500 output partitions. 499 of them contain 400 MB each; one partition (due to a hot key) contains 60 GB. a) What's the total shuffle output size? b) If every other stage bottleneck is solved, roughly how much longer will the skewed task take vs. a normal task (assume throughput is constant per core)? c) Name two concrete techniques to fix this (one config-based via AQE, one manual).

---

## Section 4 — Mixed / Rapid-fire (good for whiteboard drills)

1. A DataFrame has 1 billion rows, average row size 200 bytes. Estimate total in-memory size (uncompressed) and a reasonable partition count if targeting 128 MB/partition.
2. `spark.default.parallelism` is 200, but you're reading from a source with 1,000 input splits. Which value actually controls the number of partitions for the initial read stage?
3. Your job has `spark.executor.instances=50`, `spark.executor.cores=4`. What's the max number of tasks that can run simultaneously cluster-wide (ignoring dynamic allocation)?
4. You increase `spark.sql.shuffle.partitions` from 200 to 2000 on a job that shuffles only 5 GB. What's the average partition size now, and why might this hurt performance despite "more parallelism"?
5. A 100 GB table is broadcast-joined against a 50 TB table. Each executor has 8 GB memory. If there are 200 executors, what's the memory overhead as a _percentage of total cluster memory_ just from holding a copy of the broadcast table on every executor?

---

## Solutions

### 1.1

500 GB / 128 MB ≈ 500,000 MB / 128 MB ≈ **3,907 partitions** (Spark rounds up, so ~3,908).

### 1.2

Spark's `maxPartitionBytes` operates on the **on-disk file size**, not the decompressed in-memory size, because partition planning happens before decompression. So targeting 128 MB per partition on-disk with a 4:1 expansion ratio means each partition actually holds **~512 MB in memory** once decompressed and deserialized. Multiply across many concurrent tasks per executor and you can blow past execution memory — this is a classic cause of Parquet/Snappy-related OOMs. Fix: lower `maxPartitionBytes` (e.g., to 32 MB) so the decompressed size per partition stays near the real target.

### 1.3

800 GB / 200 MB ≈ 800,000 MB / 200 MB = **4,000 shuffle partitions**. Set `spark.sql.shuffle.partitions = 4000` (or let AQE's `coalescePartitions` handle it dynamically instead of hardcoding).

### 1.4

10 GB / 40,000 = 0.25 MB per partition on average. This is the **"small files / small partitions" problem**: task scheduling overhead (each task has fixed overhead of a few ms to tens of ms for scheduling, serialization, etc.) dominates actual compute time, so the job spends more time on overhead than work. Fix: `coalesce()` to reduce partition count, or enable **AQE with `spark.sql.adaptive.coalescePartitions.enabled=true`** to merge small shuffle partitions automatically.

### 1.5

a) No — `coalesce()` avoids a full shuffle by combining existing partitions locally where possible (no shuffle stage boundary). b) Yes — `repartition()` always triggers a full shuffle since it needs to redistribute data evenly (uses round-robin or hash partitioning). c) Roughly 2,000 / 200 = **10 source partitions per target partition**. Because `coalesce` just groups existing partitions without rebalancing data volume, if the source partitions are unevenly sized, the merged output partitions inherit that imbalance — you can end up with skewed output partitions since there's no shuffle to even things out.

### 2.1

a) Reserved memory: **300 MB** (fixed). b) Usable memory = 20 GB − 300 MB ≈ 19.7 GB. Unified pool = 0.6 × 19.7 GB ≈ **11.82 GB**. c) Minimum guaranteed storage memory = storageFraction × unified pool = 0.5 × 11.82 GB ≈ **5.91 GB**.

### 2.2

a) Overhead = max(384 MB, 0.10 × 16 GB) = max(384 MB, 1.6 GB) = **1.6 GB**. b) Total reserved = 16 GB + 1.6 GB = **17.6 GB**. c) 4 executors × 17.6 GB = 70.4 GB > 64 GB node capacity. **It does not fit** — you'd need to drop to 3 executors/node (52.8 GB, leaving headroom for OS) or reduce executor memory.

### 2.3

a) Spark compares the **in-memory (deserialized) estimated size**, not the on-disk file size, when deciding whether to broadcast (it uses stats from the query plan / table statistics). At 3:1 expansion, 8 MB on disk ≈ **24 MB in memory**, which exceeds the 10 MB threshold — so Spark would **not** auto-broadcast it in this case (this is the reverse of the classic mistake of assuming file size = broadcast size). b) If it does get broadcast (e.g., via explicit `broadcast()` hint) and OOMs anyway, likely causes: the estimate/statistics were stale or wrong (e.g., no recent `ANALYZE TABLE`), or the broadcast is being held on every executor _simultaneously_ while other memory-heavy operations are also running, exceeding the driver's or executor's available memory — broadcast variables also first collect to the **driver**, which can OOM there before it even reaches executors.

### 2.4

a) 4 concurrent tasks × 3 GB each = 12 GB needed, but only 8 GB execution memory available. **12 GB > 8 GB → yes, it will spill to disk.** b) Options: (1) reduce `spark.executor.cores` (fewer concurrent tasks per executor, e.g., 2 instead of 4, so only 6 GB is needed concurrently), or (2) increase `spark.memory.fraction` / reduce storage usage to give execution memory more room, or (3) use `reduceByKey`/`aggregateByKey` instead of `groupByKey` to reduce the amount of data held in memory per key via map-side combining.

### 3.1

a) 32 cores total, minus 1 for OS = 31 available. 31 / 5 cores per executor = **6 executors per node** (with 1 core left idle/spare). b) 128 GB − ~1 GB OS reserve = 127 GB / 6 executors ≈ **21.16 GB per executor** (before overhead). c) Solve for `executorMemory` where `executorMemory + max(384MB, 0.10 × executorMemory) = 21.16 GB`. Approximate: `1.10 × executorMemory ≈ 21.16 GB` → executorMemory ≈ **19.2 GB**, overhead ≈ 1.92 GB, total ≈ 21.1 GB. (In practice you'd round down to something clean like 19 GB.)

### 3.2

6 executors/node × 5 cores/executor × 20 nodes = **600 concurrent task slots**.

### 3.3

a) 3 TB / 256 MB = 3,000,000 MB / 256 MB ≈ **11,719 tasks/partitions**. b) 11,719 / 600 slots ≈ **19.5 → 20 waves**. c) 20 waves × 40 sec ≈ **800 seconds (~13.3 minutes)** for the map stage alone.

### 3.4

a) 499 × 400 MB + 60 GB = 199.6 GB + 60 GB = **~259.6 GB total shuffle output**. b) Normal task processes 400 MB; skewed task processes 60 GB = 60,000 MB. Ratio = 60,000 / 400 = **150x longer** — the skewed task alone could dominate the entire stage's wall-clock time since all other tasks finish and the job waits on this one straggler. c) (1) **AQE skew join optimization** — `spark.sql.adaptive.skewJoin.enabled=true`, which automatically splits oversized partitions into smaller sub-partitions during a join. (2) **Manual salting** — add a random "salt" key to the skewed join key, splitting the hot key into N sub-keys distributed across partitions, then aggregate/de-salt afterward.

### Rapid-fire answers

1. 1e9 rows × 200 bytes = 200 GB. 200,000 MB / 128 MB ≈ **1,563 partitions**.
2. For file-based sources, the **input split count / `maxPartitionBytes`** governs initial partitions — not `spark.default.parallelism` (that config mainly affects RDD operations without explicit partitioning, like parallelize, and non-file-based shuffles when AQE/shuffle partition count isn't otherwise set).
3. 50 × 4 = **200 concurrent tasks**.
4. 5 GB / 2000 ≈ 2.5 MB/partition — far too small. This causes **task scheduling overhead to dominate**: thousands of tiny tasks each pay fixed per-task overhead (serialization, scheduler latency, executor communication), so total overhead time can exceed actual compute time despite nominal "parallelism."
5. Broadcast table held on every executor: 100 GB × 200 = 20 TB, but that's clearly unrealistic for 8 GB executors (100 GB wouldn't even broadcast — it's way above threshold and wouldn't fit in one executor's memory anyway). The real lesson: total cluster memory = 200 × 8 GB = 1.6 TB. Broadcasting _any_ table means every executor pays that memory cost simultaneously, so broadcast is only viable for genuinely small dimension tables (typically under a few hundred MB) — this exercise is a trick question to test whether you catch that 100 GB is never a realistic broadcast candidate regardless of cluster size.

---

# Exercise List 2

---
## Part 1 — Partition Calculations

### Exercise 1 — Initial Partitions

You have a CSV file of **1.2 TB** stored in HDFS. HDFS block size = **256 MB**. You load it with `spark.read.csv(...)`.

1. How many input partitions will Spark create?
2. If you have 80 executors with 5 cores each, how many tasks can run simultaneously?
3. How many waves of tasks are needed?
4. If every task takes 45 seconds, estimate the total stage duration (ignore scheduling overhead).

### Exercise 2 — repartition()

A DataFrame currently has **180 partitions**. You execute `df2 = df.repartition(600)`.

1. Will this trigger a shuffle?
2. How many output partitions exist?
3. If the dataset size is 900 GB, what is the average partition size?
4. Why might this be a bad idea?

### Exercise 3 — coalesce()

A DataFrame has **1200 partitions**. You execute `df2 = df.coalesce(300)`.

1. Does Spark shuffle?
2. Why is coalesce faster than repartition here?
3. Under what situation would coalesce produce skewed partitions?

---

## Part 2 — Shuffle Calculations

### Exercise 4 — GroupBy Shuffle

Dataset: **2 TB**, 500 partitions. You execute `df.groupBy("country").count()` with `spark.sql.shuffle.partitions = 400`.

1. Is a shuffle required?
2. Approximately how much data moves across the network?
3. What determines the number of reduce tasks?
4. How many output partitions exist?

### Exercise 5 — Join Shuffle

Table A: 400 GB, 200 partitions. Table B: 600 GB, 300 partitions. You perform `A.join(B, "customer_id")`.

1. Does Spark shuffle both tables?
2. Approximately how much data is shuffled?
3. How many shuffle stages exist?

---

## Part 3 — Broadcast Join

### Exercise 6

Table A = 800 GB, Table B = 8 MB, broadcast threshold = 10 MB.

1. Which join strategy should Spark use?
2. Is there a shuffle?
3. How many copies of Table B exist if there are 100 executors?

### Exercise 7

Table A = 300 GB, Table B = 25 MB, broadcast threshold = 10 MB.

1. Will Spark broadcast automatically?
2. Could you force broadcasting?
3. What are the risks?

---

## Part 4 — Memory

### Exercise 8

Executor config: 8 GB executor memory, 4 cores. Spark storage fraction = 50%.

1. How much memory is available for storage?
2. How much for execution?
3. If cached data occupies 5 GB, what happens?

### Exercise 9

Executor: 16 GB, 8 cores. Task memory usage: 900 MB per task.

1. Can all 8 tasks run simultaneously?
2. Estimate total task memory required.
3. What happens if memory exceeds available execution memory?

---

## Part 5 — Cluster Utilization

### Exercise 10

Cluster: 20 workers, each with 16 cores / 64 GB RAM. Executors: 4 cores, 16 GB.

1. How many executors per worker?
2. Total executors?
3. Total parallel tasks?
4. Total executor memory?

---

## Part 6 — Stage Counting

### Exercise 11

```python
df = spark.read.parquet(...)
df = df.filter(...)
df = df.select(...)
df = df.groupBy(...).sum()
df.write.parquet(...)
```

1. How many stages?
2. Where does the shuffle happen?
3. Which transformations are narrow?
4. Which are wide?

### Exercise 12

```python
df1.join(df2, "id") \
   .groupBy("country") \
   .count() \
   .sort("count")
```

1. How many shuffles?
2. How many stages?
3. Which operation is the most expensive?

---

## Part 7 — Data Skew

### Exercise 13

Dataset: 100 million rows. Distribution: USA 90M, Canada 5M, Mexico 5M.

1. Why is this skew?
2. Which partition becomes the bottleneck?
3. Suggest three ways to fix it.

### Exercise 14

After a join, you observe these partition sizes: 300 MB, 290 MB, 310 MB, 305 MB, **7.8 GB**, 295 MB.

1. What is happening?
2. Why will one task run much longer?
3. Suggest two Spark techniques to mitigate this.

---

## Part 8 — File Size Problems

### Exercise 15

A job writes **18,000 parquet files**, each **3 MB**.

1. Why is this inefficient?
2. How many partitions likely existed before writing?
3. How could you produce ~256 MB output files?

### Exercise 16

Dataset size: 640 GB. Desired output file size: 256 MB.

1. How many output partitions should you use?
2. Which transformation would you use before writing?

---

## Part 9 — DAG Reasoning

### Exercise 17

```python
df = spark.read.parquet(...)
df = df.filter(df.age > 20)
df = df.filter(df.country == "US")
df = df.withColumn(...)
df = df.groupBy("city").count()
df = df.orderBy("count")
df.show()
```

1. Draw the DAG.
2. Where do stage boundaries occur?
3. Which operations are pipelined?
4. Which operations force a shuffle?

---

## Part 10 — Advanced Interview Math

### Exercise 18

Cluster: 25 executors, 6 cores each. Dataset: 1.5 TB. Partition size: 128 MB. Each task: 55 seconds.

1. Number of partitions?
2. Maximum parallelism?
3. Number of task waves?
4. Approximate execution time?

### Exercise 19

You have 600 partitions, average size 1 GB.

1. Is this a good partition size?
2. Recommend a better partition count.
3. Explain why.

### Exercise 20

Fact table: 8 TB. Dimension table: 150 MB.

1. Would Spark broadcast automatically?
2. Should you force a broadcast?
3. Estimate network traffic with and without broadcasting.
4. Which solution scales better?

---

## Bonus: Real Interview Challenge

A company has:

- 12-node cluster
- Each node: 32 cores, 128 GB RAM
- Executor configuration: 8 cores, 24 GB RAM
- Dataset: 9.6 TB
- HDFS block size: 256 MB
- `spark.sql.shuffle.partitions = 200`
- A `groupBy(user_id)` causes a shuffle
- The output is written to Parquet with one file per partition

Without running the job, answer:

1. How many executors run in the cluster?
2. What is the maximum task parallelism?
3. How many input partitions are created from the dataset?
4. How many waves of map tasks are required?
5. How many reduce tasks will execute after the shuffle?
6. Approximately how large is each output file?
7. What performance issues do you anticipate?
8. How would you tune the job to improve throughput?
9. If one `user_id` accounts for 40% of all rows, what issue could arise?
10. How would you detect and mitigate that issue?

---
## Solutions

### Part 1 — Partition Calculations

#### Exercise 1

1. 1.2 TB ≈ 1,200,000 MB. 1,200,000 / 256 ≈ **4,688 partitions**.
2. 80 executors × 5 cores = **400 concurrent tasks**.
3. Waves = ceil(4,688 / 400) = **12 waves**.
4. 12 waves × 45 sec = **540 seconds (~9 minutes)**.

#### Exercise 2

1. **Yes** — `repartition()` always triggers a full shuffle, regardless of whether you're going up or down in partition count.
2. **600 output partitions.**
3. 900 GB / 600 ≈ **1.5 GB per partition** (still larger than the ~128–200 MB sweet spot).
4. It's potentially wasteful: you pay the cost of a full network shuffle of 900 GB just to change partition count, and the result (1.5 GB/partition) is still too coarse for good parallelism. A cheaper option — like tuning `maxPartitionBytes` at read time, or `coalesce()` if reducing partitions — might get a similar or better result without the shuffle cost. If the goal really is to _increase_ parallelism, going to 600 was also probably not enough; a partition count based on `datasetSize / targetPartitionSize` (e.g., 900 GB / 200 MB ≈ 4,500) would be more principled.

#### Exercise 3

1. **No** — `coalesce()` avoids a full shuffle; it merges existing partitions locally (data doesn't need to move across the network in the general case).
2. Because it doesn't require the network shuffle-write/shuffle-read machinery — it just combines adjacent partitions in place, so it's far cheaper in I/O and network terms.
3. If the 1,200 source partitions are unevenly sized to begin with, `coalesce` merges roughly 1,200/300 = 4 source partitions per output partition **without rebalancing data volume**. Since there's no shuffle to even things out, any pre-existing imbalance carries straight through into the output partitions — producing skew.

---

### Part 2 — Shuffle Calculations

#### Exercise 4

1. **Yes** — `groupBy` is a wide transformation and always requires a shuffle.
2. Spark performs **map-side partial aggregation** before shuffling (similar to a combiner), so the actual network traffic is much less than the full 2 TB — it's roughly proportional to `(number of distinct countries) × (number of source partitions)`, i.e. small, since only partial counts per key per partition get shuffled rather than raw rows.
3. The number of reduce tasks is determined by **`spark.sql.shuffle.partitions`**.
4. **400 output partitions** (set directly by `spark.sql.shuffle.partitions = 400`).

#### Exercise 5

1. **Yes**, in a standard sort-merge join (no broadcast), both sides are shuffled and repartitioned by the join key.
2. Roughly **400 GB + 600 GB = 1 TB** moved across the network (both tables get shuffled).
3. Conceptually **2 shuffle-write stages** (one per source table) feeding into **1 shuffle-read/join stage** — so 3 stages total are involved in the join, often described loosely as "2 shuffles."

---

### Part 3 — Broadcast Join

#### Exercise 6

1. **Broadcast hash join** — Table B (8 MB) is under the 10 MB threshold.
2. **No shuffle** — Table A is read/processed locally per partition; only Table B is sent to every executor.
3. **100 copies** — one full copy of Table B is held in memory on each of the 100 executors.

#### Exercise 7

1. **No** — 25 MB exceeds the 10 MB `autoBroadcastJoinThreshold`.
2. **Yes**, via an explicit `broadcast(B)` hint.
3. Risks: the broadcast table must first be collected to the **driver**, which can OOM if the driver has limited memory; each executor then also holds a full copy in memory simultaneously, adding memory pressure that competes with execution/storage memory; if the actual in-memory (deserialized) size is much larger than the on-disk size, the "small" table might not be as small as it looks.

---

### Part 4 — Memory

#### Exercise 8

Using Spark defaults: reserved memory = 300 MB, `spark.memory.fraction = 0.6`.

- Usable memory = 8 GB − 300 MB ≈ 7.71 GB
- Unified memory pool = 0.6 × 7.71 GB ≈ **4.63 GB**
- With storage fraction = 50%:

1. Storage memory: guaranteed minimum ≈ **2.31 GB**, but can borrow up to the full unified pool (~4.63 GB) if execution isn't using it.
2. Execution memory: same split — guaranteed minimum ≈ **2.31 GB**, borrowable up to ~4.63 GB.
3. 5 GB of cached data **exceeds the entire unified pool (~4.63 GB)**. Spark will evict older cached blocks (LRU eviction) to make room, or — depending on the storage level (e.g., `MEMORY_AND_DISK`) — spill the excess to disk. If the storage level is memory-only, evicted partitions are simply dropped and recomputed on demand when needed again.

#### Exercise 9

- Usable memory = 16 GB − 300 MB ≈ 15.71 GB
- Unified pool = 0.6 × 15.71 GB ≈ **9.42 GB**

1. 8 tasks × 900 MB = **7.2 GB** needed concurrently. Since execution memory can borrow the full unified pool (~9.42 GB) when storage isn't competing for it, **yes, all 8 tasks can run simultaneously** — 7.2 GB fits comfortably under 9.42 GB. (If significant caching is also happening at the same time, it's tighter, since the guaranteed execution-only minimum is roughly half of that, ~4.7 GB.)
2. **7.2 GB total.**
3. If required memory exceeds what's available, Spark **spills** the relevant data structures (e.g., hash maps for aggregation, sort buffers) to disk rather than failing outright — this is slower but avoids an OOM in most cases. If spilling isn't possible for some reason (or disk is also exhausted), the task will fail with an OOM error.

---

### Part 5 — Cluster Utilization

#### Exercise 10

1. 16 cores / 4 cores per executor = **4 executors per worker** (memory check: 4 × 16 GB = 64 GB, which uses 100% of node RAM — tight, with no headroom for the OS in practice, but arithmetically it fits).
2. Total executors = 20 workers × 4 = **80 executors**.
3. Total parallel tasks = 80 × 4 cores = **320 tasks**.
4. Total executor memory = 80 × 16 GB = **1,280 GB (1.28 TB)**.

---

### Part 6 — Stage Counting

#### Exercise 11

1. **2 stages.**
2. The shuffle happens at `groupBy(...).sum()` — the only wide transformation in the chain.
3. Narrow: `filter(...)`, `select(...)` (and the final map-side portion of the write).
4. Wide: `groupBy(...).sum()`.

#### Exercise 12

1. **3 shuffle boundaries**: the join (`join`), the `groupBy`, and the `sort` (a global `orderBy` requires range partitioning, which is itself a shuffle).
2. Roughly **5 stages**: a map stage each for reading `df1` and `df2` (2 stages, can run in parallel), a stage for the join result, a stage for the groupBy/count result, and a final stage for the sort.
3. The **sort (`orderBy`)** is typically the most expensive in practice — it requires a full shuffle _plus_ range-partitioning/sampling to establish global ordering across partitions, on top of whatever cost the join and groupBy already incurred. In some cases the join is the single costliest step if the joined tables are very large — worth stating both and explaining your reasoning in an interview.

---

### Part 7 — Data Skew

#### Exercise 13

1. This is skew because **90% of the rows share a single key** ("USA"), producing an extremely uneven key distribution.
2. The partition/task handling the "USA" key becomes the **bottleneck** — it has to process ~18x more data than the Canada or Mexico partitions.
3. Three fixes:
    - **Salting**: append a random suffix to the skewed key, splitting "USA" into several sub-keys spread across partitions, aggregate, then combine the sub-results.
    - **AQE skew join handling** (`spark.sql.adaptive.skewJoin.enabled=true`), which automatically detects and splits oversized partitions.
    - **Two-phase / partial aggregation**: pre-aggregate locally before the shuffle to shrink the amount of skewed data that needs to move, or isolate the hot key and process it separately (e.g., filter it out, handle with different parallelism, union results back).

#### Exercise 14

1. This is **data skew** — one partition (7.8 GB) is dramatically larger than the rest (~300 MB), almost certainly due to a hot key concentrated there after the join.
2. Task duration is roughly proportional to data volume per core. 7.8 GB vs ~300 MB is about a **26x** difference (7,800 MB / 300 MB ≈ 26), so that task will take roughly 26x longer than a normal task and become the straggler the whole stage waits on.
3. Two techniques: **AQE skew join optimization** (auto-splits the skewed partition into smaller pieces during the join) and **manual salting** of the join key.

---

### Part 8 — File Size Problems

#### Exercise 15

1. This is the classic **small-file problem**: 18,000 tiny files create heavy metadata overhead (NameNode/S3 listing costs), and reading/writing many small files incurs per-file open/close overhead that dominates actual I/O time — very inefficient compared to fewer, larger files.
2. Roughly **18,000 partitions** existed at write time (Spark typically writes one file per partition per task, absent additional file-splitting settings).
3. Total data = 18,000 × 3 MB = 54,000 MB (54 GB). Target partitions = 54,000 / 256 ≈ **211 partitions**. So `coalesce(211)` (or `repartition(211)`) before writing would produce ~256 MB files. Alternatively, use a table format with compaction support (Delta/Iceberg `OPTIMIZE`) to fix it after the fact.

#### Exercise 16

1. 640 GB / 256 MB = 640,000 MB / 256 MB ≈ **2,500 partitions**.
2. Use **`repartition(2500)`** before the write (or `coalesce(2500)` if you're reducing from a higher existing partition count and don't need the data rebalanced evenly — `repartition` is generally the safer choice here since it evens out the size distribution across the new partition count).

---

### Part 9 — DAG Reasoning

#### Exercise 17

1. **DAG shape**: `read → filter → filter → withColumn` are all narrow and pipeline together into a single stage. `groupBy("city").count()` is a wide transformation, creating a shuffle boundary. `orderBy("count")` is also wide (global sort requires range partitioning), creating a second shuffle boundary. `show()` is the action that triggers the whole DAG to execute.
2. **Stage boundaries**: at `groupBy` (shuffle #1) and at `orderBy` (shuffle #2) — so **3 stages total**.
3. **Pipelined (narrow) operations**: `read`, `filter(age > 20)`, `filter(country == "US")`, `withColumn(...)` — all execute together within one stage without moving data across the network.
4. **Force a shuffle**: `groupBy("city").count()` and `orderBy("count")`.

---

### Part 10 — Advanced Interview Math

#### Exercise 18

1. 1.5 TB / 128 MB = 1,500,000 MB / 128 MB ≈ **11,719 partitions**.
2. Max parallelism = 25 executors × 6 cores = **150 concurrent tasks**.
3. Waves = ceil(11,719 / 150) ≈ **79 waves**.
4. 79 waves × 55 sec ≈ **4,345 seconds (~72 minutes, roughly 1.2 hours)**.

#### Exercise 19

1. **No** — 1 GB average partition size is well above the recommended ~128–200 MB sweet spot; large partitions increase per-task memory pressure, spill risk, and reduce effective parallelism relative to available cores.
2. Total data = 600 partitions × 1 GB = 600 GB. Target 128–200 MB per partition → 600,000 MB / 150 MB ≈ **~4,000 partitions** (any value in the ~3,000–4,700 range depending on your exact target is reasonable to state).
3. Smaller, well-sized partitions give better parallelism (more tasks to spread across available cores), lower per-task memory footprint (less spill risk), and cheaper task retries on failure — but not so small that scheduling overhead dominates actual compute time (the small-file/small-partition problem from Exercise 15). The 100–200 MB range is the conventional sweet spot.

#### Exercise 20

1. **No** — 150 MB exceeds the default 10 MB `autoBroadcastJoinThreshold`.
2. **Yes, generally** — 150 MB is still small relative to typical executor memory (fits easily on modern executors), and broadcasting avoids shuffling the 8 TB fact table entirely, which is a huge win. Worth forcing with a `broadcast()` hint (after confirming executor memory headroom).
3. **Without broadcasting**: a sort-merge join would shuffle both sides — roughly 8 TB + 150 MB ≈ **8.15 TB** moved across the network. **With broadcasting**: the fact table is read locally with no shuffle at all; only the dimension table moves, sent once to each executor — e.g., with 100 executors, that's roughly 150 MB × 100 = **~15 GB** total network traffic. This is a difference of roughly three orders of magnitude.
4. **Broadcast join scales far better** here — its cost scales with cluster size (number of executors receiving a copy), not with fact-table size, whereas a sort-merge join's cost scales directly with total data volume (8 TB+) regardless of cluster size.

---

### Bonus: Real Interview Challenge — Solution

1. **Executors per node** = 32 cores / 8 cores = 4 (memory check: 4 × 24 GB = 96 GB ≤ 128 GB, leaving 32 GB headroom for the OS — fits comfortably). **Total executors = 12 × 4 = 48.**
2. **Max task parallelism** = 48 executors × 8 cores = **384 concurrent tasks**.
3. **Input partitions** = 9.6 TB / 256 MB = 9,600,000 MB / 256 MB ≈ **37,500 partitions**.
4. **Waves of map tasks** = ceil(37,500 / 384) ≈ **98 waves**.
5. **Reduce tasks after shuffle** = `spark.sql.shuffle.partitions` = **200**.
6. **Output file size**: if the shuffle output volume is roughly comparable to the input (a conservative worst case, since `groupBy(user_id)` may or may not shrink the data much depending on aggregation type), 9.6 TB / 200 partitions ≈ **48 GB per output file** — extremely large, and a major red flag.
7. **Anticipated issues**: (a) `spark.sql.shuffle.partitions = 200` is drastically under-provisioned for 9.6 TB of data — target should be closer to `9.6 TB / 200 MB ≈ 48,000` partitions, not 200; (b) ~48 GB output files will be painfully slow to write, hard for a single task/executor to hold in memory, and will hurt downstream read parallelism; (c) with only 200 reduce tasks against 384 available slots, most of the cluster's parallelism goes unused in the shuffle stage; (d) latent risk of key skew (see Q9).
8. **Tuning**: dramatically increase `spark.sql.shuffle.partitions` (e.g., ~40,000–50,000 to target ~200 MB/partition), or better, enable **AQE** (`spark.sql.adaptive.enabled=true` with `spark.sql.adaptive.coalescePartitions.enabled=true`) to let Spark size shuffle partitions dynamically instead of hardcoding a number; enable `spark.sql.adaptive.skewJoin.enabled=true` proactively; reconsider the output write step to target a healthy per-file size (e.g., via `repartition` before write, or `maxRecordsPerFile`).
9. **If one `user_id` = 40% of rows**: severe data skew. That single key's reduce task would need to process roughly 40% of 9.6 TB ≈ **3.84 TB** in one task — an enormous straggler that will dominate the job's wall-clock time (the stage can't complete until this one task finishes), and is very likely to spill heavily to disk or OOM outright.
10. **Detection & mitigation**: Detect via the Spark UI — one task in the shuffle stage will show a shuffle-read size and duration far exceeding the others (or check `df.groupBy("user_id").count()` distribution ahead of time to spot the hot key). Mitigate with **AQE skew join handling**, **manual salting** of the hot key (split into N salted sub-keys, aggregate, then combine), or isolating and processing that specific key separately from the rest of the dataset before unioning results back together.

---

I suggest you print out these two exercise lists and do them with a pencil.