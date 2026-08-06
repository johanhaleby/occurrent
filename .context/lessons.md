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
- Every sweep tick also OPENS with a one-line restatement of the previous tick's summary (2026-08-06
  correction, second occurrence of the collapsed-summary complaint). The push is the delivery channel, but
  the transcript record still matters to Johan: a tick's closing prose collapses under its re-arm call, so
  the visible restatement belongs at the top of the next tick's turn, before any tool call.
- Every sweep tick ends with a delivered summary (2026-08-06 correction). Prose written before the re-arming
  ScheduleWakeup call renders collapsed in Johan's view, so a tick that only writes text effectively reports nothing.
  Send the tick summary through a channel that displays: a PushNotification (what merged, what moved, what needs him)
  before re-arming, or the AskUserQuestion that the tick's decision already requires. Silence is only acceptable for a
  tick where literally nothing changed, and even then say so in the push.
- A sweep tick that finds work in a ready-for-Johan state (a PR ready to land, a held branch ready to push, a gate
  ready to run) must surface it as an AskUserQuestion in that same tick, not as a status line (2026-08-06 correction).
  "Ready" is a decision point, and prose status does not reach him the way a structured ask does.
- Orchestrator-spawned sessions follow the /orchestrator skill's conventions from the first chip (2026-08-06
  correction). Titles are `⌁[<theme>/<unit>#<issue>] <imperative summary>` so the fleet groups in the session list,
  and every session brief OPENS with a MODEL/EFFORT recommendation line, decided per unit at planning time in the
  meta-plan, not re-derived at dispatch. Retitle mis-titled spawned sessions via session tooling; hand Johan the
  orchestrator session's own title, since a session cannot rename itself.
- One failed lookup command is not evidence of absence (2026-08-06 correction). An `ls` of a skill's SKILL.md
  returned a false negative and the conclusion "no orchestrator skill" was acted on for a whole dispatch round.
  Before asserting a skill or file does not exist, list the parent directory. The user naming a thing is a strong
  prior that it exists.
- Coordination cadences are derived, not hardcoded (2026-08-06 correction). A tick interval written as a
  constant should instead be recomputed at every re-arm from the dominant wait class, the way pr-fix paces
  its polls: ci-wait paced on observed check durations with a ~3x stuck threshold, worker-wait and
  human-wait on a long heartbeat because end-of-session notifications arrive event-driven, action-ready
  acted on immediately. Only ceilings stay as numbers; the rule lives in the /orchestrator skill.
- Release execution is Johan's manual act (2026-08-06 correction). Never plan or route changelog version stamping,
  `mvn_release.sh`, tagging, docs held-branch merges, the docs version bump, or post-release checks as agent-executed
  work. Plan up to a release-readiness gate and stop there; keep the release-day steps as a reference checklist only.
