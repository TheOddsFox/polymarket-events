# Current catalogue v1 acceptance

**Status: pending.** No v1 acceptance run has been recorded. Earlier measurements were
taken under the backup and import design that v1 removes, so they are not carried forward.

## v1 bar

A root meets the v1 bar only when all of these hold on the same root:

- `reconcile --full-pass` reports `full_pass.remaining` of 0 and a set `completed_at`.
- Every active membership is under 24 hours old (`active_overdue_count` is 0 in `status`).
- `full_pass.unresolved` is 0.
- `verify` passes on the published release.
- `refresh` and `verify` each complete within 1200 seconds.

## Recording a run

Record, for each run: the invocation time, the effective `config show` values, the
elapsed seconds for `refresh` and `verify`, and the `status` and `verify` output. Add
measured values here only after a real run. Until a run meets every item above, v1 is
not accepted for deployment.

## Known limit

A known event whose source member list stays incomplete produces no membership receipt, so
its pass stays open until the source returns a complete list. A known entity that upstream
no longer serves is settled by a native 404 during a forced pass and then counts toward it.
