# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Groundwork for bounding a suspended caller. A caller that dispatches with
  `waitForReply` ends its execution and is revived by a `ResumeCommand`, so
  nothing inside it can time it out; `callAgent` and `askUser` now register the
  wait in a sharded index that a sweep will later read. Nothing reads it yet, so
  there is no behaviour change. `callAgent` gains an optional `replyTimeoutMs`.
- A suspended caller is persisted as `WAITING_AGENT` / `WAITING_USER` rather
  than whatever the handler returned while unwinding. Visible on the execution
  record and in the dashboard; `QUEUED` now means only "not yet picked up".

### Added
- A background sweep over the wait index, started with the worker. Its two
  halves are independent switches: **pruning is on by default**
  (`BY_FRAMEWORK_WAIT_PRUNE_ENABLED`) because nothing else ever removes a
  wait-index entry, so with compensation off every call whose reply never
  arrives would leave one behind forever in a structure with no TTL of its own.
  **Compensation is off by default** (`BY_FRAMEWORK_WAIT_SWEEPER_ENABLED`) and is
  the rollback switch for the whole liveness feature.
- Compensation: when a sub-task's reply can never arrive — its worker died, it
  was never picked up, or it ran past a generous absolute ceiling — the sweep
  synthesises the reply the callee would have sent, so its caller is resolved
  instead of hanging. A callee that finished but whose reply was lost has its
  real stored result recovered and forwarded rather than a failure fabricated.
  Task Group members are compensated through the group's existing join, never by
  writing its accounting a second time. Off by default.

### Changed
- A Task Group member whose target agent type has no online worker is no longer
  dispatched. It previously went out blind, so that member never replied and the
  group never completed, hanging the caller. A stand-in `FAILED` reply is queued
  for it instead and flushed after the handler returns, so the group's existing
  join accounting sees exactly one result per member. Matches the Python and
  TypeScript SDKs.
- A duplicate reply for an already-resolved wait is now dropped rather than
  waking the caller twice. Only reachable once something can synthesise a reply,
  so this is inert today; a reply whose wait was never registered is still let
  through, which is what keeps rolling upgrades safe.

### Fixed
- Task Group results were keyed by the caller's message id, so every sibling in
  a group wrote the same field and overwrote each other — the join then read one
  member's result repeated N times. They are now keyed by the sub-task's own
  message id, matching the Python and TypeScript SDKs.
- A single `callAgent` result is stored before the reply is sent, so a lost
  reply message no longer loses the answer with it.
- A resumed execution now replies to its real caller. Previously
  `hasSourceAgent` was gated on `!isResume`, so an `A -> B -> C` chain dropped
  B's result and a sub-agent that called `askUser` and then finished never
  answered either. The caller is read back from the execution record; a resume's
  own header names the sub-agent that just finished.
- A suspended execution no longer replies with the value its handler returned
  merely to unwind. That placeholder woke the caller early and consumed the one
  reply it was waiting on. A handler that reaches a terminal status after
  dispatching still replies immediately, since no resume is coming.
- The reply now carries the caller's own message id instead of a freshly minted
  one. The caller reattaches its suspended execution by that id, so a fresh id
  resolved to no execution and orphaned it.
- A `ResumeCommand` is no longer skipped by the terminal-replay guard, and one
  that resolves to no execution now logs a warning.
- `header.metadata` survives a suspend in both directions: a resumed handler
  reads its own dispatch metadata again (merged under the waking message's), and
  its caller receives the metadata it dispatched with (replacing the waking
  hop's). **Handlers that treated the resumed header's metadata as "only what
  this hop just sent" will now see additional keys.**

### Added
- GitHub PULL_REQUEST_TEMPLATE and ISSUE_TEMPLATEs.
- Standardized SECURITY.md and CODE_OF_CONDUCT.md.

## [0.2.7] - 2024-05-13
### Initial release
