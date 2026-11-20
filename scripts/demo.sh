#!/usr/bin/env bash
# Submits one loan application per decision path and polls until each reaches a stable status.
# Requires the stack from `make up`. Uses demo prefixes understood by the simulated credit bureau.
set -euo pipefail

API="${API:-http://localhost:8081/api/v1/loan-applications}"

submit() { # name nationalId amount income
  curl -sf -X POST "$API" -H 'Content-Type: application/json' -H "Idempotency-Key: demo-$(uuidgen 2>/dev/null || date +%s%N)" \
    -d "{\"applicantName\":\"$1\",\"nationalId\":\"$2\",\"email\":\"demo@example.com\",\"dateOfBirth\":\"1985-12-10\",
         \"loanAmount\":$3,\"termMonths\":36,\"annualIncome\":$4,\"purpose\":\"Demo\"}" | sed -E 's/.*"id":"([^"]+)".*/\1/'
}

status() { curl -sf "$API/$1" | sed -E 's/.*"status":"([^"]+)".*/\1/'; }

declare -a CASES=(
  "Ada Lovelace|PRIME-001|20000|90000|APPROVED      (auto-approve: prime score, affordable)"
  "Grace Hopper|NEAR-002|20000|90000|UNDER_REVIEW  (manual underwriting in Camunda Tasklist)"
  "Alan Turing|SUB-003|20000|90000|REJECTED      (auto-reject: sub-prime score)"
  "Ivan Sanctioned|PRIME-004|20000|90000|REJECTED      (KYC sanctions hit)"
)

declare -a IDS=()
for c in "${CASES[@]}"; do
  IFS='|' read -r name nid amount income expect <<< "$c"
  id=$(submit "$name" "$nid" "$amount" "$income")
  IDS+=("$id|$name|$expect")
  echo "submitted $id  $name"
done

echo; echo "Waiting for decisions..."
for i in $(seq 1 30); do
  pending=0
  for entry in "${IDS[@]}"; do
    id=${entry%%|*}; s=$(status "$id")
    [[ "$s" == "SUBMITTED" ]] && pending=1
  done
  [[ $pending == 0 ]] && break
  sleep 1
done

printf "\n%-26s %-16s %-13s %s\n" "APPLICATION" "APPLICANT" "STATUS" "EXPECTED"
for entry in "${IDS[@]}"; do
  IFS='|' read -r id name expect <<< "$entry"
  printf "%-26s %-16s %-13s %s\n" "$id" "$name" "$(status "$id")" "$expect"
done
echo
echo "Underwrite pending cases at http://localhost:8080/camunda/app/tasklist (admin/admin)."
