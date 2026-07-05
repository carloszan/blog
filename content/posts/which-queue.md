+++
authors = ["Carlos Zansavio"]
title = "Which Queue should I use? Kafka vs RabbitMQ vs AWS SQS"
date = "2026-05-06"
description = "description"
tags = [
    "tag1",
]
categories = [
    "category1",
]
series = ["series1"]
aliases = ["aliases1"]
draft = true
+++

# The problem

Imagine we have a checkout service that depends on Inventory service using HTTP protocol. Inventory starts to slow down, but doesn't crash. So, checkout threads pile up waiting, until the entire service crashes because of too many threads. Retries pilling up, threads exhausted and queues filling. This is the problem we are trying to solve here. Imagine that we could have another service that depends on the checkout service, and now we have a cascade problem.