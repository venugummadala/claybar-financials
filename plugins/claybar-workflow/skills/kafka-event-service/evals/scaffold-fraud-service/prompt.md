---
tags: [claybar-workflow]
max_turns: 60
timeout_seconds: 1800
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

Create a fraud-service microservice in this repo. It consumes FraudCheckRequested commands (nationalId, loanAmount) and replies FraudCheckCompleted with a fraudScore 0-100 — use a deterministic simulator for now, we'll plug in the real vendor later. Add it to docker compose too.
