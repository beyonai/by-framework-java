# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Agent returns now reattach to the caller's suspended execution.** `GatewayWorker`'s
  agent return minted a fresh `msg-<uuid>` for every reply, but `WorkerRunner` resolves the
  suspended execution via `getExecutionByMessageId(header.messageId())` — so the lookup never
  resolved and every reply started a new execution, orphaning the one it was meant to
  continue. Affects **every inter-agent call**, not only Task Groups. The reply now carries
  the caller's own message id.
- The terminal-state replay guard in `WorkerRunner` no longer skips `ResumeCommand`s. A
  resume is the continuation of an execution, not a replay of it; without this exception the
  fix above would turn an orphaned execution into a silently dropped reply.
- `WorkerRegistry.markExecutionFinished` stamps `finished_at` only for terminal statuses. It
  is called with whatever status a task returned, and a caller suspended on a Task Group
  returns `QUEUED: waiting_for_group` — a still-running execution was being recorded as
  finished, skewing latency and completed-count metrics.
- `WorkerRunner` warns when a `ResumeCommand` resolves to no execution instead of silently
  starting a disconnected one.

### Changed — breaking
- **Error codes no longer carry the `ERR_` prefix in their wire value**
  (`AGENT_TYPE_UNAVAILABLE`, `WORKER_NOT_ONLINE`, `AGENT_CIRCUIT_OPEN`,
  `TENANT_QUOTA_EXCEEDED`). The prefix belongs to the Java constant *name* only, which is
  what the Python and TypeScript SDKs already do; Java baking it into the value meant the
  same failure read differently depending on which runtime produced it. This affects every
  availability rejection, including client-facing `SendMessageResponse` error codes.
- Task Groups now follow the cross-runtime contract in
  `by-framework-python/docs/adr/0001-unify-call-agent-and-call-agents-behavior.md`: the
  caller is resumed with every sub-task's result aggregated in `replyData` (in dispatch
  order) and `content` cleared, rather than with whichever reply completed the group.
- `callAgents` is the primary batch API; `dispatchGroup` is a permanent alias. An empty task
  list throws `IllegalArgumentException` instead of returning `{"status": "EMPTY"}`, and a
  successful dispatch returns `QUEUED` instead of `GROUP_QUEUED`.
- Batch tasks now go through the same availability check a single `callAgent` does. An
  unavailable target fails that task only — recorded as a `FAILED` aggregate entry — instead
  of being blind-published to the control stream.

### Added
- Task Group hash fields `protocol_version`, `task_order` and `aborted`. `protocol_version`
  lets a new worker join a group written by an older dispatcher with the old semantics; the
  reverse is not possible, so **drain in-flight task groups (or restart the whole agent-type
  pool at once) before upgrading a mixed pool.**
- Per-task `message_id`, `route_policy`, `availability_timeout_ms`, `region` and `priority`
  in `callAgents`, matching `callAgent`'s own options.
- GitHub PULL_REQUEST_TEMPLATE and ISSUE_TEMPLATEs.
- Standardized SECURITY.md and CODE_OF_CONDUCT.md.

## [0.2.7] - 2024-05-13
### Initial release
