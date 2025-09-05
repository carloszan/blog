+++
authors = ["Carlos Zansavio"]
title = "Difference between data warehouses and data lakes"
date = "2025-09-04"
description = "What's the difference?"
tags = [
    "data-engineering",
]
categories = [
    "tutorial",
]
+++

 This is a fundamental concept in data architecture. Here’s a clear breakdown of the differences between data warehouses and data lakes.

Basically **data warehouses** stores well structured data. So you can build a data warehouse with one or multiple SQL databases. On cloud, it could be, for example, Google BigQuery.
In this approach, you deal primarily with structured data that has been processed and transformed for a specific purpose. You just go there, get what you want and that's it.
Companies can create data warehouses using SQL databases like Postgres or it can be a cloud solution.

On the other hand, **data lakes** stores data files. It can any data file format.
So a data lake could be implemented using AWS S3, MinIO or any blob storage tool.
The thing here is what data can be saved on this blob storage and how you're going to process this data.
One of the most used format are [Delta](https://delta.io/), which is Parquet files but versioned.
Basically you can store these "parquet files" and then reads it and writes it.
To process those files, you can use, for example, Apache Spark, but there are other tools as well.

One is not better than the other, it always depends on the use cases.

Thanks
