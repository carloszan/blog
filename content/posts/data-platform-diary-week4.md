+++
authors = ["Carlos Zansavio"]
title = "My data platform diary - Week 3"
date = "2026-09-26"
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
claude --resume 8f02df42-e5ea-46bb-9dc8-a433e90dcd45


  - [ ] **P1** `delta-maintenance` rollout: wheel published and manual test run OK (Day 3: 45 files -> 1, stream unaffected). Remaining: confirm the first scheduled run (2026-09-28 03:30 UTC). Regenerate `uv.lock` (new workspace member, not committed Day 3).
  
  To check that run in the morning:
  kubectl -n spark-jobs get scheduledsparkapplication delta-maintenance
  It should show a LAST RUN and a COMPLETED app.

Minio:
```
wget https://dl.min.io/aistor/mc/release/linux-amd64/mc chmod +x mc sudo mv mc /usr/local/bin/
```

mc alias set local http://192.168.3.242:9000 admin Fxt5Tuf4WBkXY7Vw/QTt7Wjxj6SrCWQn