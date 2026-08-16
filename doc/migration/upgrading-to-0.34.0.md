# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

## 1. A flow saga's `stepWindow` now caps only its own declared events

No recipe, and most callers need to do nothing. This only matters if your flow sets a
`narrowingFilter`, a `replacementFilter` wider than the flow's own declared types, or uses a
`CloudEventTypeMapper` that collapses several domain types onto one CloudEvent type string.

The 0.33.0 upgrade guide's [section 9](upgrading-to-0.33.0.md#9-a-flow-saga-can-cap-the-events-of-the-step-it-is-parked-in)
and [section 10's replacement-filter caveat](upgrading-to-0.33.0.md#10-a-saga-or-subscription-declaring-a-supertype-event-is-refused)
describe `stepWindow` as it shipped in 0.33.0, where every correlated event counted toward the cap
regardless of whether any step declared its type. That let an event outside a flow's own declared
types evict one of the step's own events, and the absolute bound section 9 states,
`historyWindow + 2 * stepWindow + 1`, held because of that same defect.

`stepWindow` now counts and evicts only events of a type some step's `on(...)` branch or
window-condition leaf actually names. An event of any other type is still retained, never
discarded, but it no longer takes one of the cap's slots or evicts a declared event to make room
for itself. The bound in section 9 still holds for a flow's own declared-type events. It no longer
bounds a step fed only events of a type no step declares, which is not a new gap. It was always the
kind of growth `stepWindow` and `historyWindow` alone did not close, only masked. Watch the
0.33.0 store-boundary warning if your flow admits such events and you care about total document
size. See [ADR 129](../architecture/decisions/0129-a-flow-sagas-stepwindow-caps-only-its-own-declared-events.md)
for the full decision.
