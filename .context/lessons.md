# Lessons

- A dated changelog heading alone does not prove that a release shipped. Confirm a matching git tag, publication to
  Maven Central, or explicit maintainer state before treating the version as released.
- Questions to Johan go through the AskUserQuestion tool, always with a recommended option marked (2026-08-06
  correction). Never leave a decision as a prose question at the end of a report; the structured prompt is what he acts
  on.
- A fleet sweep must detect no-progress cycling, not just idleness (2026-08-06 correction). A session with a
  self-re-arming watch refreshes its activity timestamp every pass, so "recently active" is not evidence of progress.
  The reliable signal is the WORK ITEM: when a PR's observable state (rollup, unresolved-thread count) is unchanged
  across two consecutive sweep ticks while its session keeps cycling, read that session's tail; a human-gated hold
  says so in its last message and needs a decision routed, not more waiting.
- A sweep tick that finds work in a ready-for-Johan state (a PR ready to land, a held branch ready to push, a gate
  ready to run) must surface it as an AskUserQuestion in that same tick, not as a status line (2026-08-06 correction).
  "Ready" is a decision point, and prose status does not reach him the way a structured ask does.
- Release execution is Johan's manual act (2026-08-06 correction). Never plan or route changelog version stamping,
  `mvn_release.sh`, tagging, docs held-branch merges, the docs version bump, or post-release checks as agent-executed
  work. Plan up to a release-readiness gate and stop there; keep the release-day steps as a reference checklist only.
