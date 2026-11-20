---
tags: [claybar-workflow]
max_turns: 60
timeout_seconds: 1800
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

add a fraud check to the loan-origination process. it should run in parallel with the KYC and credit checks, talk to a fraud service over kafka (it'll get FraudCheckRequested and answer FraudCheckCompleted with a fraudScore 0-100), give up after 10 minutes by default, and anything with a fraud score of 80+ gets rejected. don't build the fraud service itself yet, just the workflow side + tests
