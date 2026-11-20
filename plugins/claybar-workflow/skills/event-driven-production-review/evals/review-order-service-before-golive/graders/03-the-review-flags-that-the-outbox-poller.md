---
type: llm
focus: trace
weight: 1
---

The review flags that the outbox poller sends without waiting for the broker ack and marks rows published / keeps going after failures, risking loss or reordering
