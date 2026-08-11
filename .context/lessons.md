# Lessons

- A principle cited in a recommendation is checked against its source and its qualifiers,
  not recalled (2026-08-09, the #681 adoption reversal). The orchestrator recommended
  mechanizing an edge-case fix "per the unknown-callers principle" and an ADR quote, both
  from memory. Johan asked for the AGENTS.md investigation first, and the source's own
  qualifier ("an easier solution is fine when it yields roughly the same result. It is not
  fine when the gap is isolation or correctness") cut the other way: no correctness gap
  existed, and the fix would have bound every storage implementer through a permanent TCK
  case. Same family as the quote-conventions-verbatim lesson, applied to the orchestrator's
  own asks to Johan rather than to worker briefs. Before an adopt-or-defer recommendation
  leans on a principle, open the file and read the principle's boundary conditions.

- A convention relayed to a worker is quoted from its source, never paraphrased from
  memory (2026-08-08, the vgpr brief and a thread-triage message both said "unreleased
  capabilities get Highlights only", and the worker refuted it with AGENTS.md:71, the
  0.32.0 precedent, and PR 300's actual body). The real rule distinguishes dev-churn
  on an unreleased feature (folded, no Changes entry) from announcing a new capability
  (a Changes entry plus a Highlights teaser, always), and the compression collapsed
  the two. Same failure family as the johan-writing surface-compression lesson: a
  paraphrase of a nuanced rule becomes a wrong absolute exactly when it enters a brief.
  When a dispatch or triage message states a convention, quote the governing sentence
  from AGENTS.md or the memory file rather than restating it, and when a worker pushes
  back with sources, verify before defending. The worker's evidence-first refusal was
  correct behavior, not insubordination.

- A delivered store-backed component is checked for resilience parity with its closest
  sibling before merge (2026-08-08, Johan asked about DB-outage behavior on the
  applied-position store and the answer was a gap). The unit shipped Mongo
  implementations with no RetryStrategy while NativeMongoCheckpointStorage, the nearest
  structural sibling, carries one with an exponential default. Neither the brief nor
  the integration review asked the question. The rule is now a design intention in
  AGENTS.md (production-ready means surviving transient store outages, configurable
  retries where they make sense, starters auto-apply defaults), and the orchestrator
  side of it is a review check: when a unit adds a component that talks to a store,
  compare its failure handling against the nearest sibling component before the merge
  gate, not after a user question.

- A plan is not ready for approval until it has survived one adversarial self-review
  (2026-08-08, Johan had to ask "anything missing?" on the ccpause plan and the answer
  was four real gaps, all findable from evidence already in hand: a lifecycle door the
  fix direction left open, a missing overengineering scope guard, the central design
  question unnamed, unstated bookkeeping mechanics). Exploration, write, exit is not a
  planning process. Before ExitPlanMode, re-read the plan against the exploration
  evidence and hunt for what is missing the way a reviewer would: uncovered state
  transitions, scope guards, the question the unit actually hinges on, mechanics left
  to improvisation. The /johan-plan skill's self-review discipline is the model even
  when that skill is not formally invoked.
  Graduated into the /orchestrator skill the same day (with a routing correction from
  Johan: a cross-repo rule recorded only in a repo lessons.md is invisible everywhere
  else, since every repo's lessons file is a different file; the skill now carries the
  planning-review rule and the lesson-routing rule itself). This entry stays as history.

- An unclaimed issue with no unit and no PR is invisible to the whole loop (2026-08-08,
  Johan asked how #629 and #636 went unadopted for a day). The finding-routing protocol
  tells workers to file a durable trace, but sweeps iterate the epic state file's units
  and the monitor watches open PRs, so nothing ever read the trace: #629 (T1's side
  finding, filed 08-07) sat unrouted until a human asked what was queued. The rule now
  in the skill: every dispatch-time hygiene sweep and every epic closeout runs a tracker
  scan (gh issue list --search created:>last-scan) and routes each unclaimed,
  unregistered issue to adopt-or-ask. An epic does not close with an unrouted issue
  filed during its lifetime.
  Addendum (2026-08-08, from the cross-review via Johan, both adopted): the cursor
  advances to the previous scan's START time, never its completion, because the search
  index lags and a mid-scan issue would be skipped forever, and the overlap is
  idempotent for free since routing changes the issue's state. The candidate filter is
  mode-scoped: solo repositories sweep every unclaimed unregistered issue, shared
  repositories (marked by local-only tracking mode) sweep only fleet-produced traces
  and epic references, and the recorded scope decisions in ORCHESTRATOR.md serve as
  the decline-register so a declined adoption does not re-ask every sweep.

- Every user-facing documentation surface goes through /johan-writing, in worker briefs
  too, with the surfaces NAMED per unit (2026-08-08, Johan's correction after semicolons
  shipped in changelog entries). The orchestrator skill's prose bullet said "docs, PR
  titles, issue comments, ADRs" and briefs compressed it to "PR title, body, issue
  comments", so changelog and javadoc text went out unGated. The rule now in the skill:
  docs-site content, READMEs, javadoc and KDoc, ADRs, changelog entries, migration
  guides, PR and issue prose are all gated surfaces, and a brief lists the ones the unit
  touches explicitly. Commit messages and internal code comments stay technical style.
  Also fixed in the same edit: the skill wrongly listed commit messages as a gated
  surface, contradicting the global config.
  Addendum (2026-08-08, later the same day): Johan extended the rule to commit messages
  and internal code comments too, so nothing prose-shaped is exempt any more except
  internal planning docs (PLAN/ORCH). For code comments the skill's own code-comment
  surface rules apply (terse, subject-free, no semicolons, match file density), not the
  first-person prose voice. The commit-message trailer bans are unchanged.

- Sweep a worker's worktree only after its FINAL report, not after its PR merges
  (2026-08-08, B2's closeout). A clean tree is not proof the worker is finished: B2's
  worktree was removed in the merge sweep while the agent still owed itself a trailing
  `gh pr edit` (fixing a stale body line) and its DELIVERY_RESULT, so it woke to a dead
  cwd and the orchestrator had to make the body edit for it. The B5 rule (never remove a
  DIRTY worktree) already existed; this extends it: for in-session subagents, hold the
  worktree until the completion notification carrying DELIVERY_RESULT has arrived, or
  the agent is confirmed stopped. The cost of holding is nothing; the cost of the early
  sweep is a worker that cannot finish its own bookkeeping.

- Addendum (2026-08-10, ccfence): the brief line alone does not prevent the background trap.
  F4 and F9 both backgrounded long Maven runs and ended their turns waiting, with the
  sequential-FOREGROUND rule stated verbatim in their briefs. The working recovery is one
  SendMessage resume naming the death of the run ("that result does not exist and never
  will") plus the foreground chunking recipe, which both workers then followed exactly.
  Budget one recovery round trip per unit whose verification exceeds a few minutes, and
  treat a completion notification whose result text says "waiting for" as this trap on
  sight. Related new fact from F9: a worktree-pinned subagent must never call
  EnterWorktree/ExitWorktree (it wedges the Bash sandbox's tracked directory until an
  EnterWorktree back to the pinned path); branch inside the pinned worktree instead.

- A subagent's background processes die with its turn, and nothing wakes it for them
  (2026-08-08, B2 and B6 both lost detached JMH runs the same hour). A worker SESSION can
  run an hour-long benchmark in the background because the harness re-invokes sessions on
  task completion, but an in-session subagent that ends its turn on a detached process
  sleeps forever and the process is killed. Rule for briefs: any long-running command a
  subagent needs runs as sequential FOREGROUND chunks, each under the 10-minute Bash cap
  (JMH include regexes or -p subsets, appending to one results file). The orchestrator
  detects the trap by a completion notification whose result text says "waiting for" a
  background task, and recovers with a SendMessage nudge after confirming the process is
  gone.

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
- Whether .context/ is tracked in git is a per-repository decision asked once and recorded
  (2026-08-07, from Johan about parkster-dev shared repos where he is the only orchestrator user,
  corrected same day by the Codex cross-review). Conservative default with no recorded decision:
  local-only, nothing under .context/ committed, ignore rules in the file git rev-parse --git-path
  info/exclude resolves (never a literal .git/info/exclude, .git is a file in linked worktrees,
  and never .gitignore, which is itself a visible push in a shared repo). The local-only decision
  is recorded in ORCHESTRATOR.md, NOT the policy file, because the shared contract says an
  untracked policy grants nothing, which also means local-only mode runs on conservative authority
  defaults with broader authority granted per-session only. The durability trade (no git
  checkpoint trail, no cross-machine recovery) is named at the ask. A first-class forbidden value
  enters the policy schema only as an agreed cross-tool change.
- Worker exits end with a typed DELIVERY_RESULT block (2026-08-07, Johan's design, refined same
  day): PR number, full head SHA, pr-fix outcome as exactly done | attempts_exhausted | blocked |
  not_run, the blocker as its own separate field, and the reason whenever the outcome is not done,
  as the session's last message so a tail-read needs one glance. The success value is done, not
  green, because green CI with unresolved scope is not pr-fix done and the looser name invites
  claim inflation. Non-done exits mirror the block as ONE marker-tagged PR comment with an attempt
  counter, updated in place on retries so conflicting delivery results cannot accumulate. The block
  is a claim the sweep verifies, never evidence: the stated SHA is cross-checked before any CAS pin.
- The fleet monitor's lifetime is an every-sweep invariant, not a closeout step (2026-08-07,
  Johan spotted the leftover). "Stop at epic closeout" fails the moment an epic reopens: T7 was
  adopted after archrev's closeout, its re-armed monitor had no closeout left to catch it, and it
  polled an empty PR list for over an hour. The rule now in the skill: the monitor runs exactly as
  long as some unit has an unmet deliverable a PR event could advance, so the sweep completing the
  last such unit stops the monitor and the loop then and there. Zero open PRs alone never stops it,
  workers between deliveries are what it exists to catch.
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
  opened non-interactively, the worker deciding exactly three mechanical things (draft status, PR
  title and body wording, whether to run simplify) with everything else from the brief or BLOCKED,
  then pr-fix with autostop so the worker self-heals its CI and goes quiet when pr-fix is done,
  which the sweep reads as green, attempts exhausted, or blocked rather than assuming green. The
  head pin makes the worker-push-versus-merge race safe. Judgment gates stay interactive: plan
  approvals, shipped-contract structured asks, BLOCKED escalations. Refined same day per Codex
  feedback: the open-ended "any question the skill would ask" wording was an accidental license to
  self-decide authority questions, and "quiet when green" hid pr-fix's non-green exits.
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

- A worker's PR body can state bookkeeping in past tense before the bookkeeping happened
  (2026-08-08, ccrace U1). PR 658 said two lease weaknesses were "filed separately" at
  20:02Z, the orchestrator's searches at 20:05 and 20:13 found nothing, the orchestrator
  filed them itself at 20:14 per Johan's adoption ask, and the worker filed its own copies
  at 20:16 in its delivery tail, 110 seconds later, so two defects got four issues. Neither
  finding was ever at risk of being lost, the DELIVERY_RESULT message named both and the
  dispatch-time tracker scan would have caught the unclaimed pair, but discovery came from
  Johan's question rather than from the loop. Graduated into the /orchestrator skill the
  same night: briefs order the durable trace BEFORE any artifact references it plus a
  search-before-filing line, and the orchestrator verifies PR-body bookkeeping claims at
  merge-readiness, preferring to wait for the DELIVERY_RESULT over repairing a gap while
  the worker's tail may still be running. This entry stays as the history.

- A merged prose deliverable got no human read before the merge, and the read that came
  after found a modelling error (2026-08-09, ADR 116, PR 667 merged then amended by PR
  670). Every formal gate was green, no CI on markdown, no review requested, zero
  threads, DELIVERY_RESULT done, but a design document's only meaningful reviewer is a
  human, and mine merged while Johan was still reading. The concept-ownership question
  (does this belong on CheckpointStorage at all) had also survived three adversarial
  correctness reviews untouched, which graduated into the /orchestrator skill the same
  day as a required design-against-the-domain review pass. The orchestrator-side rule
  recorded here: an ADR or design-doc PR from a plan-first unit is not action-ready on
  gates alone, hold it one beat and ask, or confirm the user's read happened in the
  unit session, before the CAS merge. An amendment PR is cheap, but a merge mid-review
  reads as the decision being closed while it is still open.

  Addendum (2026-08-09, U6's handover, the expensive form of the same failure): three
  prose PRs (667, 670, 671) each merged while ONE live design review was still running,
  including one merged after a per-PR "is it settled?" ask answered yes, because the ask
  raced the discussion it asked about. Per-PR confirmation is the wrong unit: the design
  GATE is the unit. Graduated into the /orchestrator skill: a plan-first unit's prose
  deliverable auto-merges only after its design gate is explicitly closed (the unit's
  handover or the user saying the design is final), never on green gates or per-PR asks,
  because prose runs no CI and its only real reviewer is the human still reading it.

- A rename's ripples belong to the same unit as the rename, and every file the renamed
  type touches must be checked against the ownership map at planning time (2026-08-09,
  rv33 U2/U3). The briefs gave U3 "the rename plus ripples" and U2 the file
  RecordingReactiveUpdate.java, which implements the renamed marker, so U3 could not
  complete without editing U2's file. The worker did the right thing (mechanical
  type-name edits only, flagged it), and the recovery was a coordination message plus
  merge sequencing, but the contradiction was findable before dispatch by grepping the
  renamed type's implementors against the collision map. The adversarial plan review
  checked file sets for overlap, not type-reference closures.

- Historical files are out of scope for a rename, and a brief that says "the whole
  repository" actively causes the damage (2026-08-10, rv33 U11). The docs brief told the
  worker to grep the whole repository rather than only docs.md, so it renamed a type
  inside the January 2021 news post announcing 0.7.0, making that announcement claim
  0.7.0 introduced a name that did not exist for another five years. The library brief
  for the same rename got this right by scoping ADRs explicitly (update the unreleased
  one, amendment banner for released ones), so the rule existed and simply was not
  carried across to the sibling brief. Every rename brief now names the historical
  surfaces it must NOT touch, dated announcements, release notes and past changelog
  sections, alongside the reference surfaces it must.

- Measure timestamps, never estimate them, and re-arm the monitor when an epic reopens
  (2026-08-10, rv33 U10/U11, both caught by Johan's question rather than by the loop).
  Two failures with one root: writing plausible values instead of reading real ones. The
  epic state file accumulated hand-written ISO timestamps that were hours off, which made
  the derived health labels meaningless and produced a false STALLED that hid whether the
  workers were really progressing. Separately, the monitor was stopped correctly at the
  U9 closeout and never re-armed when U10 and U11 were adopted, so a delivered docs PR
  sat unseen. The skill already carries the lifetime invariant (the monitor runs as long
  as some unit has an unmet deliverable a PR event could advance); the gap was applying it
  at the reopening rather than only at the sweep. Run `date -u` before writing any
  timestamp, and re-arm the monitor in the same action that registers a post-closeout unit.

- An ADR number is audited across every remote branch at the moment of writing, never
  read once at planning time (2026-08-10, stepcond registration). The plan recorded
  "next free ADR number is 118" as a VERIFIED assumption with real evidence (ADR 0117
  was max on main), and it was already about to be false: a concurrent epic's unit
  merged 0118 minutes later and a second unit had 0119 claimed on an unmerged branch,
  invisible to both a working-tree check and a main-only check. ORCHESTRATOR.md
  already carried the cross-branch audit rule, but only as advice for after a
  collision recurs, so the registration used the cheap check and pinned the result
  into a brief. Two rules: a number claimed on an unmerged branch is real, so the
  audit is `git ls-tree` over every `origin/*` branch, and a brief that names an ADR
  number must carry the audit command rather than the number, because the dispatch
  and the write are hours apart in a repository with concurrent epics.
  The recovery also exposed a chip-replacement hazard worth its own line: the
  skill says spawn the replacement first and dismiss the old chip second, which is
  correct when the old chip is still pending, but when the user has ALREADY started
  it that ordering leaves a live worker plus a pending duplicate aimed at the same
  files. Check whether the chip started before replacing it, and if it did, correct
  the running session with a message and withdraw the replacement instead.

- The measure-timestamps rule recurred within one session of being written, and the
  tell was a false STALLED again (2026-08-10, stepcond). Registration read `date -u`
  correctly, then every timestamp written over the next hour and a half (dispatch,
  monitor arming, the ADR correction, the worker report) was estimated from the
  conversation's own sense of elapsed time and landed roughly an hour early. The
  derive step then compared a real clock against invented progress times and labelled
  a healthy unit STALLED. Writing the rule down clearly does not fire it, because
  each individual timestamp feels like bookkeeping rather than a measurement. Run
  `date -u` in the SAME command that writes a timestamp, and when a past time cannot
  be recovered, mark it approximate in the file rather than writing a confident wrong
  value.

- `FETCH_HEAD` is volatile and a later `git fetch` silently repoints it, so never verify
  a worker's branch through it (2026-08-10, stepcond U1). The sequence that nearly
  produced a wrong review: fetch the worker branch, read files from `FETCH_HEAD`
  correctly, then run a routine memory checkpoint whose `git fetch origin main`
  repointed `FETCH_HEAD` at main, then read another file from `FETCH_HEAD` and get
  main's OLD version while believing it was the worker's. The file still parsed and
  still looked plausible, so nothing announced the error. It was caught only because a
  Copilot comment quoted code that did not appear in what had just been read, and the
  mismatch was investigated rather than dismissed. Verify a worker's code at the PR's
  full head SHA (`git show <sha>:<path>`), which is the same pin the CAS merge uses,
  or through an explicit remote-tracking ref, never `FETCH_HEAD`. This is a git fact
  rather than an Occurrent one, so it belongs in the orchestrator skill itself at the
  next edit, not only here.

- GitHub's two API surfaces disagree on the CASE of a check conclusion, and mixing them
  manufactured a false all-red main (2026-08-10, stepcond U1). The REST endpoint
  `/commits/<sha>/check-runs` returns `"success"` in lowercase, while GraphQL's
  `statusCheckRollup` returns `SUCCESS` in uppercase. A watcher written with the
  uppercase comparison against the REST endpoint reported `total=27 success=0
  failed=27` on a commit whose 27 jobs had all passed. Nothing about the output looked
  malformed, it looked like a catastrophic regression, and the only reason it was not
  acted on is that 27 of 27 failing minutes after a green PR is implausible on its
  face. Compare conclusions case-insensitively (`ascii_downcase`), and treat an
  implausibly total failure as a probable query bug to disprove before it becomes a
  revert. Same family as the FETCH_HEAD lesson: the tooling answered a slightly
  different question than the one being asked, and answered it confidently.

- A worker's "writing gate ran clean" is a claim, and the cheapest place to catch a miss
  is the next unit that copies the text (2026-08-10, stepcond U3). The library unit
  reported the johan-writing greps run with zero hits, and a semicolon nonetheless
  shipped in a code comment in BOTH doc guard test files. It surfaced only because the
  documentation unit copied that comment into a snippet, noticed the semicolon was a
  borderline case, recast it, and SAID SO in its report rather than silently working
  around it. Two rules follow. Treat a gate claim like a DELIVERY_RESULT claim, spot-check
  it on the merged artifact when the text is short enough to grep, which for code comments
  it always is. And name comment text explicitly as a gate surface in any brief that
  copies code between repositories, because the copying unit inherits prose it did not
  write. The fix went out as its own small PR rather than a direct push, since the
  standing push grant covers memory checkpoints and never code.

- A subagent cannot Edit or Write outside its own session worktree, so a brief that tells
  one to `git worktree add` in ANOTHER repository and edit there is asking for something
  the harness blocks (2026-08-10, stepcond U6). The worker got the job done by applying
  edits through Python run from Bash, which the hook does not gate, and flagged the
  workaround rather than passing it off as normal. Two consequences. Briefs that send a
  subagent into a second repository should say up front that file edits go through a
  scripted Bash path, or the unit should run as a spawned session instead, which has its
  own worktree and no such restriction. And a worker reporting an environment constraint
  it worked around is doing exactly the right thing, that report is the only reason this
  is known.

- Merge authority is per REPOSITORY, and this fleet works in two (2026-08-10, stepcond).
  `.context/orchestrator-policy.yml` declares `repository: johanhaleby/occurrent`, so the
  standing merge grant covers the library and says nothing about
  `occurrent-org/occurrent-org.github.io`. Held docs PRs hid this for several epics
  because Johan merges them at release anyway, and the one earlier docs merge to main was
  explicitly at his request. The first ordinary, unheld docs PR is where the gap shows:
  it is green, mergeable, and still not mine to merge. Route it as a structured ask.

- Two branches rewriting the same paragraph DUPLICATE it silently, git reports no conflict
  (2026-08-10, stepcond, docs PRs 61 and 62). Both rewrote the flow saga `historyWindow`
  paragraph, one adding step-condition facts and the other recasting the prose for
  readability. Because the two versions had diverged enough to look like separate
  additions rather than competing edits, a cherry-pick produced BOTH paragraphs and exited
  zero. The result was the exact duplicated-paragraph defect PR 62 existed to fix, with the
  surviving stale copy missing every step-condition fact. A conflict marker would have been
  the safe outcome; silence was the dangerous one. Two rules. When two branches are known
  to touch the same prose region, TRIAL MERGE them and read the result, never infer safety
  from a clean exit code. And after any merge or rebase of prose branches, grep for the
  distinctive opening clause of each rewritten paragraph and assert it appears exactly
  once. Recorded on the merge-order issue too, because a later rebase can recreate it.


## The chip convention lives in the `title` parameter, not the prompt (2026-08-10, rv33 U18)

`spawn_task`'s `title` is what becomes the chip label AND the spawned session's name in
`list_sessions`. The prompt's first line is invisible there. U18 was spawned with the epic's
`⌁[rv33/U18] ... · Sonnet/high` convention on the prompt's first line and a plain imperative
phrase in `title`, so the running session appeared as "Add any/none predicates to
ReceivedEvents" while every sibling in the same epic carried its unit tag. Johan spotted it
before the orchestrator did, which is the tell that fleet visibility was actually lost rather
than merely being untidy: an orchestrator scanning `list_sessions` cannot group its own units
when one of them is missing the tag.

The tool's own description invites this, because its examples are all plain imperative phrases
("Fix stale README badge"), which is right for a one-off suggestion chip and wrong for a chip
that is one unit of a tracked epic. So restate the convention at every spawn rather than
trusting the tool's example.

**How to apply:** put `⌁[<epic>/U<n>] <short> · <Model>/<effort>` in `title` itself, and keep it
under the 60 character cap, which usually means shortening the description rather than dropping
the tag. Repeating the same line at the top of the prompt is fine and useful for the worker, but
it is not a substitute. **It is recoverable after the fact:** `set_session_title` renames another
session in place, so a mis-titled running unit is fixed with `list_sessions` to get the id and
one rename, with no need to dismiss the chip or restart the work.

## A `cd` to the repo root in a worktree session silently targets another branch (2026-08-10, rv33 U18)

The recorded worktree-path rule says never target the primary checkout from a worktree session,
but the way it actually happens is subtler than editing the wrong file by name. Every shell call
here starts in the session worktree, so a bare `python3 - <<EOF` writing `.context/epics/rv33.yml`
is correct. Prefixing the same call with `cd /Users/johan/devtools/java/projects/occurrent`,
which reads like "go to the repo", lands in the **primary checkout**, and another session had
left that on `stepcond/707-matcher-step-conditions`. The epic file there was 6 revisions and one
unit behind, so `validate` reported "revision 44, 17 units" for a file this session had already
taken to 49 and 18.

**The tell is a state file reading older than what you just wrote to it**, not an error, because
nothing fails. Here the string replacements simply found no match against the older content, the
file was rewritten byte-identical, and `git status` stayed clean, so the wrong-tree write left no
trace at all. A replacement that *had* matched would have committed epic state onto a sibling
epic's branch.

**How to apply:** in a worktree session, never `cd` to the repository root in a shell call. Let
the call inherit the session worktree, and when a path outside it is genuinely needed use
`git -C <path>` for that one command rather than moving the shell. Prefer the Edit tool for files
in the session worktree, since the `block-cross-worktree-edit.sh` hook catches this class of
mistake for Edit but cannot see a shell redirect. Cheap check before trusting any state-file read
in a shared repository: `git branch --show-current` with no `cd`.

## A hand-rolled monitor watches delivery, the tested one watches readiness (2026-08-10, rv33 U18)

U18's monitor was written fresh and keyed only on the open-PR set, so it fired on "a PR appeared"
and on "no PRs left" and said nothing in between. The unit then sat merge-ready for about twenty
minutes while this session waited on a matrix that had already finished, and Johan asked for status
twice before the orchestrator noticed. By the skill's own rule that is a detection defect, and the
fix is the missing signal rather than a shorter heartbeat.

The v7 pattern in `references/fleet-monitor.md` already selects `statusCheckRollup` and derives both
a failure count and a `DONE`/`running` flag, so it would have emitted `0fail DONE` against a `CLEAN`
mergeable state the moment the run completed. It also handles the case that actually confused this
session: a head carrying **no** checks at all yields an empty rollup, which the pattern reports as
`DONE` rather than leaving it looking like something still pending.

That matters because Occurrent's workflow has `paths-ignore: ['**/*.md']`, so pushing a changelog
correction onto a green PR produces a head with almost no checks. Two checks sat there unchanged
and were read as CI ramping up rather than as a terminal state. The governing fact was already
recorded in two places, the CI reference memory and `ORCHESTRATOR.md`, so this was not a missing
fact, it was a fact nothing forced anyone to apply.

**How to apply:** arm the v7 pattern from the reference, do not write a fresh monitor because the
epic looks simple. A monitor keyed on the PR set alone can only ever report arrival and departure,
which is the least useful pair, since arrival is already covered by the task notification and
departure arrives after the decision was needed. When a head shows fewer checks than the matrix
normally runs, treat it as terminal and settle it immediately with
`git diff --name-only <last-verified-sha>..HEAD`, judging the code on the last head that carried
code. See [[reference-ci-check-state-and-paths-ignore]].

## Merging on a green matrix is not merging on a green PR (2026-08-11, timerid U2)

PR 719 merged with an unresolved Copilot thread on it, and the thread was right: the javadoc on
`SagaInput.timeout(String, TimerName)` used `stepTimer("awaiting-players")` as its example, and
`stepTimer` did not exist yet, so the only occurrence of that symbol on `main` was the javadoc
naming it. Johan found it minutes after the merge.

Two failures, both the orchestrator's, and the second is the one that let the first through.

**The brief omitted the standing `/pr-fix` loop.** `ORCHESTRATOR.md:344` records it as Johan's
directive from 2026-08-06: every implementation brief includes a loop over the unit's pull request
until the matrix is green **and every review thread is addressed and resolved**. The U2 brief asked
for `pr-create` and a `DELIVERY_RESULT` block and never mentioned threads, so the worker had no
reason to look at them. The rule was in the file read at session start.

**The merge gate never looked at threads.** The check was the check-run rollup plus
`mergeStateStatus`, and `CLEAN` does not mean "no unresolved conversations" unless the repository
enforces it, which this one does not. A green matrix says the code compiles and passes, not that
review is finished.

**How to apply.** Put the `/pr-fix` loop in every implementation brief, and say why, since a worker
told only to open a pull request treats delivery as the finish line. Before any merge, query the
review threads as well as the checks, with a GraphQL call filtering `reviewThreads` on
`isResolved == false`, and treat empty output as part of the merge condition alongside the rollup.
A reasoned-decline thread left open on purpose is fine under the policy, but it has to be a
decision someone took rather than one nobody saw.

Related: this same epic already lost twenty minutes to a hand-rolled monitor when the tested
pattern was on the shelf. Both misses were recorded rules that existed and were not applied, which
is a different failure from not knowing them.

## A push you did not read back is not a push, 2026-08-11

Twenty memory checkpoints, every one of them reported to Johan as committed and pushed, were still
sitting on the session worktree branch when the `timerid` epic closed. `origin/main` carried none of
them, the last one that landed was `6f6d16ebf` from the previous epic. They surfaced only because a
routine push finally failed with a non-fast-forward, which prompted a rebase that replayed all twenty.

**Why it happened.** The checkpoint ran as one compound command, `git add && git commit && git push`.
The commit half printed its confirmation, the push half was rejected, and the combined output was
read as success because the first lines looked right. Nothing after that ever checked the remote.

**How to apply.** After a memory checkpoint, read the fact back from the remote rather than from the
command that was supposed to produce it. `git log --oneline origin/main -1` after a fetch, or a grep
for the text just written, costs one call and is the only evidence the push exists. This is the same
rule already recorded for fabricated timestamps and for merge gates, applied to the one place it had
not been, which is the orchestrator's own bookkeeping. A claim made to Johan about durable state is
worth exactly the verification behind it.

## A merged pull request is not a finished branch, 2026-08-11

The `timerid` closeout sweep found commit `8851a68e0` sitting on `timerid/u4-persistence-proof`,
pushed at 06:59:40Z, two minutes after the CAS merge pinned `0804b1dbd`. GitHub had deleted the
branch on merge and the worker's push recreated it, so nothing on the pull request showed the extra
commit. It was real work, the read direction of the Mongo persistence proof, and it would have been
lost with the worktree.

**Why the merge gate cannot catch it.** The gate checked the right things and the head was correct
when it merged. The commit did not exist yet. No merge-time check can see a commit pushed after it.

**How to apply.** In the closeout sweep, before removing any unit worktree or branch, ask whether the
branch tip is still the SHA the merge was pinned to. `git ls-remote origin 'refs/heads/<epic>/*'`
against the merged heads recorded in the epic state answers it for the whole epic in one call, and a
branch that reappeared after a delete-on-merge is the loudest form of the signal. Then adopt whatever
is there as a unit rather than deleting it, which is also why worktree removal belongs at the end of
the sweep and not at the merge.

## The v7 monitor cannot see a thread being resolved (2026-08-11, cdx33)

The v7 fleet-monitor pattern keys its delta on PR number, head SHA, mergeable, `reviewDecision`,
a failing-check count and a DONE/running flag. A Copilot review arrives as `COMMENTED`, which
leaves `reviewDecision` an EMPTY STRING, and resolving a thread changes no other field. So a
worker that answers a reviewer and resolves the thread without pushing a commit takes its PR
from blocked to merge-ready and the monitor emits nothing at all.

This was caught before it cost anything, by asking what signal would tell me that U2's and U8's
unresolved Copilot threads had been dealt with. The answer was none: U8's fix happened to need a
commit, so its head would move, but U2's might not have.

It is the same defect family as the already-recorded hand-rolled monitor that only reported
arrival and departure, and the same rule applies, wire the missing signal rather than shorten the
heartbeat. The v7 pattern is not wrong, it simply watches delivery and CI rather than review
completion, and the merge gate needs all three.

**How to apply:** when the merge gate includes review threads, and it always does here, arm a
companion watch that polls unresolved-thread counts per open PR and emits on change
(`THREADS-ALL-RESOLVED` is the merge-gate candidate, `THREADS-OPEN` means still blocked). Keep it
separate from v7 rather than widening v7's delta key, so a truncated or unhealthy poll in one
cannot silence the other, and retire both together under the monitor lifetime rule. Recorded in
`ORCHESTRATOR.md` with the task ids. Worth folding into `references/fleet-monitor.md` as a v8
addendum at the next edit of that file, since the gap is generic and not specific to this epic.

## Telling a worker not to background a wait does not stop it; removing the wait does (2026-08-11, cdx33)

Three stalls in one session on the same trap, and the second and third came AFTER the worker had
been corrected. U-ADR backgrounded a Copilot-review poll, was told the result would never arrive,
recovered, and finished. U7 then backgrounded a Maven run, got the same correction with the
foreground chunking recipe, ran the tests correctly, and immediately backgrounded a CI poll
instead, burning about 244k tokens across the unit.

The existing lesson says to budget one recovery round trip per unit whose verification runs long.
That is not enough, because the correction only names the mechanism the worker just used, and
waiting itself is what the worker believes it is required to do. Its brief said to run pr-fix
until CI is green, so with 17 checks pending it had an instruction it could not satisfy in a
single turn and no legal way to wait. Backgrounding is the only thing that looks like progress.

The fix that worked was structural rather than another prohibition: tell the worker it does not
need to wait for CI at all, because the orchestrator holds monitors and owns the merge, then give
it a short list of foreground edits and an explicit instruction to exit with pr-fix outcome
`not_run` and the blocker naming the orchestrator as the owner of the wait. A non-done exit that
is expected and accurate beats a worker looping on a wait it cannot perform.

**How to apply:** in any brief where CI is slow, say up front that the orchestrator owns the CI
wait and the merge, and that exiting with `not_run` plus a blocker naming the pending checks is a
CORRECT delivery rather than a failure. Keep the pr-fix loop requirement for review threads, which
a worker genuinely can finish in its own turn. And when a worker stalls twice, stop repeating the
prohibition and remove the obligation that is driving it.

## A stopped worker session with an open obligation is invisible to both monitors (2026-08-11, cdx33)

U8's chip session stopped at 09:35 with PR 727 green, one unresolved Copilot thread, and no
DELIVERY_RESULT ever reaching the orchestrator. Neither monitor could see it: the v7 work-item
watch reports head, mergeable, review decision and checks, all of which were static and healthy,
and the companion thread watch only emits when a COUNT CHANGES, so a thread that stays stubbornly
at one unresolved emits exactly once and then never again. A chip session is a separate top-level
session, so it sends the orchestrator no task notification when it ends. Three signals, and the
state fell through all of them.

It was caught by `list_sessions`, which carries `isRunning` per session, while checking something
else entirely (whether every chip had actually been started). `isRunning: false` beside an open PR
with an unmet deliverable is the signal, and nothing else in the loop reports it.

This is the unit-not-PR rule from 2026-08-07 in a new disguise. That rule said a unit whose
deliverable has no PR drops out of PR-keyed monitoring. The same hole exists one step later: a unit
whose PR exists but whose WORKER has stopped drops out of monitoring that only watches the PR.

**How to apply:** every sweep tick reads `list_sessions` and cross-checks `isRunning` against the
epic state's open units, not just the PR set. A unit with an unmet deliverable and a stopped
session is BLOCKED on the orchestrator and needs a `send_message` resume naming the exact
obligation, since a chip session cannot notify you that it gave up. Do not infer liveness from the
PR being healthy, which is precisely what a delivered-but-unfinished unit looks like.

## "No changelog entry for unreleased surface" is a wrong paraphrase, and it cost three review round trips (2026-08-11, cdx33)

Three unit briefs (U2, U7, U9) told the worker that a fix to unreleased 0.33.0 surface needs "no
Changes changelog entry, dev-churn on unreleased surface". Copilot then raised the same objection
on PR 729 and PR 728 independently, and it was right both times: the existing
`### Changelog next version` entry already DESCRIBES the unreleased capability, so a refinement
that changes what that entry claims has to be folded into it. Omitting the changelog entirely
leaves the entry describing behavior that no longer exists.

The governing rule in AGENTS.md distinguishes dev-churn on an unreleased capability, which gets no
NEW `#### Changes` bullet, from the separate obligation to keep an existing entry truthful. My
briefs collapsed those into "no changelog entry", which is the same compression failure already
recorded for the vgpr brief, committed again by the same hand that recorded it. Both workers
handled it correctly, but each paid a review round trip and a rebase to learn what the brief could
have told them.

**How to apply:** a brief for a change to unreleased surface says, in this shape rather than in
paraphrase: no new `#### Changes` bullet, AND check whether an existing `### Changelog next
version` entry describes the behavior you are changing, folding the refinement into that entry if
so. Quote the AGENTS.md sentence rather than summarizing it. And when the same reviewer objection
arrives on two different PRs from two different workers, treat the brief as the defect rather than
the workers.

## A green rollup is not proof the matrix ran, and on one PR it had not (2026-08-11, cdx33)

PR 731 (a gating unit, the migration recipes) reported `failing=0 pending=0 total=3` and every
recorded merge gate passed: checks not failing, nothing pending, zero unresolved review threads.
It was one command away from being merged. The matrix had never run on that head.

`.github/workflows/maven.yml` triggers on **push**, not `pull_request`, and carries
`concurrency: ci-${{ github.ref }}` with `cancel-in-progress: true`. Three other workflows
(ADR numbering, CI-runnable tests, MongoDB test containers) DO trigger on `pull_request`. So a head
can carry three genuinely successful `pull_request` checks and no matrix at all, and the rollup
cannot tell the difference between "27 jobs passed" and "the 27 jobs do not exist". Checking the
sibling branches showed every other PR had a matrix on its exact head, and one had a `cancelled`
run from a superseded push, so this was specific to that head rather than a repo-wide outage.

The delta between 731's last matrix-green head and its current head included
`MigrateSagaTimerName.java` and 99 new test lines, so this was unvalidated recipe code reading as
green on a unit whose entire purpose is migration correctness.

**How to apply:** the merge gate asks whether a maven.yml run EXISTS for the exact head SHA and
concluded successfully, via
`gh run list -R <repo> --branch <branch> --workflow maven.yml --json headSha,status,conclusion`,
never by reading the PR rollup alone. Three outcomes: present and green, merge; present and
running, wait; ABSENT, decide why before doing anything else. Absent is legitimate only when the
push since the last validated head touched nothing outside `**/*.md` (prove it with the compare
API, as the earlier `paths-ignore` lesson describes). Otherwise force one with
`gh workflow run maven.yml --ref <branch>`, which works because that workflow also declares
`workflow_dispatch`. Note this is the mirror image of the earlier lesson: that one warned against
mistaking a legitimately skipped matrix for a pending one, and this one warns against mistaking a
MISSING matrix for a passing one. Both come from the same root, that the rollup describes only the
checks that happen to exist.
