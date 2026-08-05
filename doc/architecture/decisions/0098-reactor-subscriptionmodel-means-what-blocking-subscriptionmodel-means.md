# 98. Reactor SubscriptionModel means what blocking SubscriptionModel means

Date: 2026-08-05

## Status

Accepted. #535, and the interface TCK phase 7 consumes.

Supersedes the `ManagedSubscriptionModel` proposal in the phase 7 plan of record. Amends [ADR 94](0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md),
which recorded the shape of the problem but not this answer.

## Context

The two subscription stacks used the same type name for two different things.

Blocking `SubscriptionModel` is exactly `extends Subscribable, SubscriptionModelLifeCycle` and declares no members of
its own: a named, lifecycle-managed subscription model. Reactor `SubscriptionModel` extended nothing and declared one
member, `Flux<CloudEvent> subscribe(SubscriptionFilter, StartAt)`: a bare cold publisher the caller subscribes to and
disposes itself. Reactor had the blocking meaning too, but only as two separate interfaces, `Subscribable` and
`SubscriptionModelLifeCycle`, with nothing naming their conjunction.

That cost shows up in three places.

**A reader cannot carry one meaning across the stacks.** The reactor `Subscribable` javadoc already had to explain
itself as "a named, lifecycle-managed counterpart to the plain reactive `SubscriptionModel#subscribe` primitive",
which is a sentence you only write when a name has been taken by the wrong thing.

**Phase 7 of the TCK needs the conjunction as a type.** The suites are written against the blocking contract and
reached through a bridge (ADR 94), so the reactor side has to name what the bridge is over. Without the type, the
bridge and every out-of-tree implementor take the asymmetry: two interfaces on one stack, one on the other. ADR 94
already made exactly this argument when it gave `RegisteringSubscribable` the blocking `SubscriptionModel`, and the
plan of record's answer was to mint a third name, `ManagedSubscriptionModel`, on the reactor side. A third name for
the concept the blocking stack already names is the wrong shape: it makes a reader ask what the difference is, and
there is none.

**In a Spring codebase a type is also a bean selector.** #524 established this on the blocking side, where widening
`RegisteringSubscribable` to implement `SubscriptionModel` silently suppressed the whole asynchronous model through
`@ConditionalOnMissingBean(SubscriptionModel.class)`. #535 is the same defect standing on the reactive starter
already, independent of any widening.

## Decision

**Reactor gets `SubscriptionModel extends Subscribable, SubscriptionModelLifeCycle`, with no members, identical to
blocking.** The released Flux-returning interface is renamed `FluxSubscriptionModel`, with an
`org.occurrent.MigrateOccurrentRenames_0_32` `ChangeType` recipe under `UpgradeToOccurrent_0_32` and a section in
`doc/migration/upgrading-to-0.32.0.md`.

**The name is `FluxSubscriptionModel` because it names the one thing that distinguishes the two.** Three candidates
were weighed. `SubscriptionSource` does not discriminate, since every `Subscribable` is also a source of
subscriptions, and `@Projection(source = PUSH)` already owns "source" for a different axis. `Streamable` fits the
`-able` convention and matches the javadoc's own verb, but "stream" is this codebase's noun for the STREAM capability
(`StreamSubscriptionModel`, `Filter.capability(STREAM)`) and this type is capability-neutral, so it invites the
wrong reading. `FluxSubscriptionModel` is the first Occurrent type named after a Reactor type, which is the cost;
`Flux` is already in every signature this module publishes, so it leaks nothing that was not already public, and a
reader upgrading is told exactly which of their two candidate types moved.

**The two interfaces stay separate rather than merging into one, as `DcbSubscriptionModel` did.** A model fed by a
push source rather than by reading a change stream cannot return a cold `Flux` positioned at a `StartAt`, so folding
the primitive into `SubscriptionModel` would mint members that four shipped models cannot honour. `DcbSubscriptionModel`
carries both shapes on one interface only because its sole implementation is an adapter over a delegate that has them.

**The combining interface goes on the four models that carry a subscription id**: `ReactorMongoSubscriptionModel`,
`ReactorDurableSubscriptionModel`, `CatchupThenPushSubscriptionModel`, and `RegisteringSubscribable` (so the reactive
push and synchronous models get it). The three reactor catch-up models expose only a cold `Flux` and keep the
contract of their own that ADR 94 gave them, so they implement `FluxSubscriptionModel` through
`CheckpointAwareSubscriptionModel` and nothing else.

**Amended for #550 (2026-08-05): the three catch-up models now carry the combining interface too.** The placement
above was written while the catch-up models had no named subscriptions to offer, and #547 showed what that costs: the
durable model wrapping a catch-up model had nothing to delegate to, so the composition the reactive starter wires for
a position-writing store kept an unguarded delivery pipeline. The promotion gives the catch-up models a named
`subscribe(..)` that replays without retry (matching the blocking catch-up models) and then hands the live half to the
wrapped model's own named `subscribe(..)`, so retry and synchronous filter refusal are inherited rather than
reimplemented, and forwards the life cycle to the wrapped model, which is a real life cycle rather than an invented
one. The named machinery lives once, in `NamedCatchupSupport` beside the shared replay pipeline. The named path
requires the wrapped model to be named itself; over a cold-only wrapped model it refuses loudly with the remediation
in the message, because the alternative is a second copy of the named-over-cold driver that
`ReactorDurableSubscriptionModel` already owns. The cold `Flux` primitive is unchanged and stays the contract for
feeds. See ADR 101 for the staged decision this completes.

**`CatchupThenPushSubscriptionModel` gains it, and the `DcbSubscriptionModelAdapter` gate is unaffected.** ORCHESTRATOR
recorded that this model deliberately did not implement the reactor `SubscriptionModel`, on the grounds that the
Flux-returning primitive is one a register-and-wrap model cannot honour. That reason survives the rename intact and
now attaches to `FluxSubscriptionModel`, which the model still does not implement. `DcbSubscriptionModel.from(..)`
takes a `FluxSubscriptionModel`, so a model that only implements `SubscriptionModel` still cannot reach the adapter's
`instanceof Subscribable` and `instanceof SubscriptionModelLifeCycle` checks, and those checks stay reachable for a
`FluxSubscriptionModel` that is not also named and lifecycle-managed. The change closes an asymmetry instead: the
blocking twin has implemented blocking `SubscriptionModel` all along.

**Every existing selector over the renamed type keeps its current meaning and follows the Flux type. Nothing moves to
the new name.** The rule matters because the tempting reading is the other one. `ProjectionAnnotationRegistrar`'s
`applicationContext.getBean(SubscriptionModel.class)` resolves the composed durable model precisely because the
register-only models do not satisfy the Flux type; pointing it at the combining interface would make three beans
match.

**#535 is fixed by naming the exception, as #524 was.** The reactive starter's guard becomes
`@ConditionalOnMissingBean(value = {FluxSubscriptionModel.class, Subscribable.class}, ignored = RegisteringSubscribable.class)`.
A register-only model is not a substitute for the asynchronous one, so the guard must not step aside for it. One
exclusion covers both ways in, since the reactive `SynchronousSubscriptionModel` and `PushSubscriptionModel` both
extend `RegisteringSubscribable`. The second way in is the likelier one and the issue did not mention it: upgrading to
0.32.0 tells an application sharing a push sink to declare a `PushSubscriptionModel` bean per consumer (ADR 90), and
that bean tripped the same guard.

**A user-declared `CatchupThenPushSubscriptionModel` bean is deliberately left able to suppress the durable model.**
It is a genuine catch-up-capable, lifecycle-managed asynchronous model, so stepping aside for it is what the guard is
for. The blocking guard behaves identically, so the two stacks agree.

## Consequences

The reactor and blocking subscription APIs now say the same thing with the same words, so phase 7's bridge names the
type it bridges and an out-of-tree reactor implementor is handed the same contract shape a blocking one is. Phase 7
consumes `SubscriptionModel` and does not introduce a type of its own.

**A released type was renamed, which is what the recipe and the migration section are for.** The blocking
`SubscriptionModel` is untouched, and the two share a simple name, so the recipe matches on the fully qualified name
and a test asserts the blocking type is left alone. Without that assertion a recipe matching the simple name would
rewrite blocking code that has nothing to migrate, and the diff would look plausible.

**The audit that fixed #535 found a second defect, which is not fixed here.** Supplying your own asynchronous
subscription model, which the guard exists to invite, leaves two `Subscribable` beans with no `@Primary` between them,
so the subscription DSL's injection point is ambiguous and the context fails to start with a
`NoUniqueBeanDefinitionException`. Marking the supplied bean `@Primary` works around it. Both starters have it in the
same shape, so it is neither reactive-specific nor introduced here, and it is filed as #541 rather than riding along.
Its fix is a change to how the DSLs resolve their model, not to a conditional.

**The audit's own lesson is narrower than "check the annotations".** The site the annotation-and-`instanceof` sweep
missed was a `@Bean` method parameter, `occurrentDcbSubscriptions(SubscriptionModel, ..)`, and it was found by the
compiler rather than by the sweep. A by-type injection point is a bean selector too. What makes the class of defect
tractable is that the compiler finds every reference to a type that no longer exists, which is an argument for
renaming the released type rather than adding the new meaning under a third name.
