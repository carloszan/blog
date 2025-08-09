+++
authors = ["Carlos Zansavio"]
title = "How to create a blog with Hugo"
date = "2025-07-28"
description = "How to create a blog with Hugo"
tags = [
    "hugo",
    "tutorial"
]
categories = [
    "tutorials",
]
series = []
aliases = []
draft = false
+++

# Motivation
Like many developers, I'm constantly learning and creating—but where does all that knowledge go? We need a place to store our insights, and while platforms like Medium offer convenience (and even paywalls), they come with a catch: you're writing for their ecosystem, not truly for yourself. That's when I knew I needed my own platform.

The inspiration struck after watching NetworkChuck's excellent Hugo tutorial ([video here](https://youtu.be/dnE7c0ELEH8?si=NACUtjsy8aqHQFe_)). Hugo offered exactly what I wanted: a fast, Markdown-based static site generator that could work seamlessly with my existing workflow.

Speaking of workflows—I've been using Obsidian as my "second brain" for over a year. Its Git-backed note-taking system has been transformative for my knowledge management. The missing piece? A way to publish directly from Obsidian to my own blog without relying on third-party platforms.

NetworkChuck's approach used external hosting, but as a homelab enthusiast running my own services, I realized I could create something even more tailored. In this article, I'll walk through my solution—a self-hosted publishing pipeline that might just inspire you to build your own using the tools you already have.