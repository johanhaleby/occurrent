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
- The user noticing fleet progress before the orchestrator is a detection defect (2026-08-07
  correction). Worker sessions that deliver a PR and stay open emit no signal, and chip-start notices
  can arrive minutes late, so mid-tick deliveries silently waited up to a full heartbeat (A6 and B4 sat
  delivered for ~45 minutes until Johan asked). The fix is a persistent work-item Monitor armed at epic
  start, diffing the open-PR set so every delivery, head change, and mergeable flip wakes the loop
  immediately; the heartbeat then only catches stuck states. Never respond to a missed event by
  shortening the heartbeat.
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
- Fleet-monitor hardening from Johan's review (2026-08-07): pin the repo with -R and raise --limit
  past the gh default of 30 (silent truncation in a busy repo); count consecutive poll failures and
  emit one MONITOR-UNHEALTHY line carrying the real stderr at the threshold, MONITOR-RECOVERED on
  the next success (silence must never be ambiguous between quiet and dead); put reviewDecision in
  the delta key (it is an EMPTY STRING when absent, not null, the same trap as check conclusions);
  run a full sweep immediately after arming, because the first poll only sets a baseline and reports
  nothing already red, done, or reviewed; record the monitor task id in ORCHESTRATOR.md, never in
  the epic state file (the shared schema rejects unknown fields, additions need cross-tool
  agreement). Kept deliberately against the feedback: the poll stays repo-wide rather than scoped
  to the epic's PR numbers, because a pre-filtered poll misses exactly the spin-off PRs that matter
  (#614 was detected within one poll interval only because the watch was repo-wide); in a busy
  shared repo, post-filter the delta lines, not the poll.
  Addendum (2026-08-07, from Codex via Johan, all four adopted): MONITOR-READY confirms the baseline
  poll matched something before the arming sweep runs, MONITOR-INCOMPLETE flags a result that hit the
  configured limit so closure events are re-verified instead of trusted (a truncated set makes a
  live PR look closed), PR-OPENED separates discovery from state change so adopt-routing and
  merge-routing need no inference, and a multi-repo epic runs one monitor per repository with active
  PR work. The event vocabulary is now shared between the Claude and Codex orchestrators.
- Monitor v7 from the Codex cross-review (2026-08-07, four catches adopted, one pushback). Adopted:
  UNKNOWN suppression must key on PR number PLUS full head SHA, or a worker push during a recompute
  prints the old head's mergeability beside the new head (evidence binds to the exact commit, the
  same rule as the CAS pin); a truncated at-limit snapshot must pause delta and closure inference
  and keep the last complete baseline (v6 warned but still adopted the truncated set, so missing
  rows read as closures), with MONITOR-COMPLETE on recovery; the substituted mergeable value is a
  notification de-dup device and never enters the epic state as an observed fact; trap-cleanup for
  the stderr tempfile. Pushback kept with a capability distinction: the worker spawn-task fallback
  stays, because on this host a chip spawns nothing, it is a suggestion only Johan's click turns
  into a session, so the fallback passes through the human rather than becoming autonomous
  child-spawning, and the skill wording now says so explicitly.
- Worker sessions never block on Johan for delivery mechanics (2026-08-07, from Johan after a
  delivered archrev session idled waiting on a pr-create question). Brief defaults now: non-draft PR
  opened non-interactively (any question the PR skill would ask is decided from the brief or the
  worker's judgment), simplify at the worker's or brief's discretion, then pr-fix with autostop so
  the worker self-heals its CI and goes quiet exactly when the orchestrator's monitor sees green and
  CAS-merges. The head pin makes the worker-push-versus-merge race safe. Judgment gates stay
  interactive: plan approvals, shipped-contract structured asks, BLOCKED escalations.
- Out-of-scope findings route through the orchestrator, not through a worker's own spawn-task chip
  (2026-08-07, from Johan on how #613/#614 arrived). A worker-spawned chip has no fleet title, no
  model or effort recommendation, no claiming brief, and no registration, so the session enters the
  fleet invisible until its PR appears. Briefs now carry the orchestrator's session id and the rule:
  message the orchestrator AND leave a durable trace (PR comment or unclaimed issue), the
  orchestrator adopts, registers the unit at dispatch, and spawns the fleet-native chip. Direct
  spawn-task remains the fallback when the orchestrator is gone.
- A CAS merge pin is the FULL head SHA fetched from the API, never reconstructed from a truncated
  prefix (2026-08-07). The #609 merge was refused because the pin's tail was invented from a 9-char
  display prefix; the guard treated it as a moved head, which is exactly right, and the fix was to
  fetch headRefOid in full and re-issue.
- Release execution is Johan's manual act (2026-08-06 correction). Never plan or route changelog version stamping,
  `mvn_release.sh`, tagging, docs held-branch merges, the docs version bump, or post-release checks as agent-executed
  work. Plan up to a release-readiness gate and stop there; keep the release-day steps as a reference checklist only.
