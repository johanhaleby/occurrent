# Lessons

- The unit-not-PR, verify-side-effects, and stalled-detection rules graduated into the /orchestrator
  skill's epic-state model on 2026-08-07 (schema v1, `.context/epics/<epic>.yml`, durable pending
  actions, computed completion). The entries below remain as history of how they were learned.

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
- Chat prose in a loop tick is NEVER a delivery channel (2026-08-07, third occurrence of the
  collapsed-summary complaint, superseding the restatement fix). Any text in a turn that continues into
  tool calls collapses, including a restatement at the top of the next tick. The surfaces that display
  are: a SendUserFile card carrying the full tick summary as a file (the primary channel), the one-line
  PushNotification, and an AskUserQuestion when the tick carries a decision. Write the summary file,
  send it, push the one-liner, every tick.
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
  Addendum (2026-08-07 correction, same family as the collapsed-summary rule): the MODEL/EFFORT
  recommendation goes IN THE CHIP TITLE (suffix like "· Opus/high"), because the model is chosen at the
  chip and the brief's opening line only displays after launch — a recommendation that renders after its
  decision informs nothing.
  Second addendum (2026-08-07): plan-first sessions end their planning phase with a downshift
  recommendation at the plan-approval gate ("implementation is Sonnet/medium, switch before approving"),
  derived from the approved plan's actual shape — the planning tier is often oversized for what follows,
  and the approval gate is the one moment the user is present with the model picker at hand.
- One failed lookup command is not evidence of absence (2026-08-06 correction). An `ls` of a skill's SKILL.md
  returned a false negative and the conclusion "no orchestrator skill" was acted on for a whole dispatch round.
  Before asserting a skill or file does not exist, list the parent directory. The user naming a thing is a strong
  prior that it exists.
- A fleet sweep tracks UNITS, not PRs (2026-08-07 correction). A unit whose current deliverable has no
  open PR (a second PR owed, an unconcluded planning round) drops out of PR-keyed monitoring exactly when
  it needs watching, so C1 sat idle 13 hours with an undelivered half and no tick flagged it. Every open
  unit gets a state line in every tick summary, and idle-with-obligation triggers the no-progress read.
  Also from the same correction: side effects issued in a tick (reruns, merges) are verified to have
  taken before the tick ends, or named as unverified, since a rerun can race the very merge that was
  meant to fix its base (72 seconds, #597 vs #595).
- Coordination cadences are derived, not hardcoded (2026-08-06 correction). A tick interval written as a
  constant should instead be recomputed at every re-arm from the dominant wait class, the way pr-fix paces
  its polls: ci-wait paced on observed check durations with a ~3x stuck threshold, worker-wait and
  human-wait on a long heartbeat because end-of-session notifications arrive event-driven, action-ready
  acted on immediately. Only ceilings stay as numbers; the rule lives in the /orchestrator skill.
- A merge under authority is compare-and-swap, never a plain merge (2026-08-07, found by the A2 session).
  gh pr merge acts on whatever head the API resolves at execution, so a worker push in the read-to-merge
  window is silently dropped from the squash — #597 lost a follow-up commit this way (it became #601).
  Always pin with --match-head-commit <verified-sha>; a refusal is a re-verify, and post-merge
  verification compares the merged head against the verified one.
- Worker-to-orchestrator messages are unreliable while the orchestrator is mid-turn (2026-08-07, A2's
  sends failed where A9's succeeded). Signals must ride the work item: the sweep computes merge-readiness
  from facts anyway, so pings are accelerators, but nuance a worker needs to convey goes in a PR comment,
  and a sweep that meets a surprising unit state reads the PR conversation before acting.
- Authorization is repo-scoped policy, never a skill default (2026-08-07 correction). Standing merge
  authority and the matrix-sibling green rule were briefly written into the global /orchestrator skill as
  defaults; both are grants Johan made for THIS repo and would violate process elsewhere (parkster-dev).
  Grants live in the checked-in `.context/orchestrator-policy.yml` with provenance; the skill carries the
  mechanism plus conservative defaults (no standing merge, strict green, threads block), and a
  conversational grant is offered persistence into the policy file rather than left as session lore.
- Effort tracks remaining uncertainty, not the phase name (2026-08-07 correction). "Routing low,
  execution high" was too broad: mechanical execution of a settled plan is low/medium, hard
  planning/debugging/integration is high or above. Same round: a named agent's fixed model is one
  complete choice (no conflicting override stacked on it), "shared cache wins" was an unmeasured cost
  claim (the fact is "avoids rebuilding context"), and effort the orchestrator cannot set (sessions,
  named agents) is stated as a recommendation, never as an applied setting.
- Release execution is Johan's manual act (2026-08-06 correction). Never plan or route changelog version stamping,
  `mvn_release.sh`, tagging, docs held-branch merges, the docs version bump, or post-release checks as agent-executed
  work. Plan up to a release-readiness gate and stop there; keep the release-day steps as a reference checklist only.
