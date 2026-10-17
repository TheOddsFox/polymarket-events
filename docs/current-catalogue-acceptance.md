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

## Probe 2026-10-17

This probe is not a passed v1 bar. The full pass was not run to completion.

The seeded root verified: 1,138,596 events and 4,321,358 markets. After the probe, `full_pass.remaining` was 5,439,842, `active_overdue_count` was 21,874, and unresolved memberships were 0.

Reconcile held about 20 entities per second. A 1,200-second invocation that keeps about 255 seconds for publish clears about 18,900 entities, so the remainder is about 4 days. A 480-second invocation committed 3,938 entities and was cut off by the deadline during publish. `previous_publish_seconds` was 170.

Observed retained storage was 7.57 GiB (8,125,747,037 bytes) and peak RSS was 4.19 GiB. The probe floor is 1.5 times those peaks: 11.4 GiB retained and 6.3 GiB RSS. The sample `config/catalogue.toml` allows 160 GiB retained, which is above that floor. That 160 GiB value is not a measured full-run ceiling.

## Known limit

A known event whose source member list stays incomplete produces no membership receipt, so
its pass stays open until the source returns a complete list. A known entity that upstream
no longer serves is settled by a native 404 during a forced pass and then counts toward it.
