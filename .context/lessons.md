# Lessons

- A dated changelog heading alone does not prove that a release shipped. Confirm a matching git tag, publication to
  Maven Central, or explicit maintainer state before treating the version as released.
- Questions to Johan go through the AskUserQuestion tool, always with a recommended option marked (2026-08-06
  correction). Never leave a decision as a prose question at the end of a report; the structured prompt is what he acts
  on.
- Release execution is Johan's manual act (2026-08-06 correction). Never plan or route changelog version stamping,
  `mvn_release.sh`, tagging, docs held-branch merges, the docs version bump, or post-release checks as agent-executed
  work. Plan up to a release-readiness gate and stop there; keep the release-day steps as a reference checklist only.
