---
tags: [claybar-workflow]
max_turns: 60
timeout_seconds: 1800
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

we're going live with the order service next week, can you review these for production issues? the status endpoint sometimes keeps showing PENDING even after the shipped event arrives and nobody knows why

Input files: OrderStatusProjector.java, OutboxPoller.java, order-approval.bpmn
