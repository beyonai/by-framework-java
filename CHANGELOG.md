# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
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
