# 172. How the Spring Boot starters find and start annotated handlers

Date: 2026-11-13

## Status

Accepted. Decides [#983](https://github.com/johanhaleby/occurrent/issues/983) in decision point 1, and
[#979](https://github.com/johanhaleby/occurrent/issues/979) in decision points 2, 3 and 4. Amends Decision point 4 of
[ADR 127](0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md), "A descriptor
annotation is read after the singletons are instantiated". ADR 127 shipped in 0.34.0, so it is amended by reference
here and gets a pointer in its Status section. Decision point 3 supersedes the passage of
[ADR 85](0085-every-subscription-model-can-be-stopped.md) that lets a write to a stopped synchronous subscription
model succeed, and ADR 85 gets a pointer in its Status section too. Decision point 4 corrects the claim in
[ADR 59](0059-projection-annotation.md) that a synchronous projection "has no history to catch up on", and ADR 59
gets a pointer in its Status section.

## Context

Both starters, `OccurrentBlockingAnnotationBeanPostProcessor` and `OccurrentReactiveAnnotationBeanPostProcessor`,
read a bean's handlers from `getDeclaredMethods()` of the bean's class. That lists the methods the class declares
itself and none that it inherits. So an annotated method on a superclass that the class doesn't override never
registered, and neither did an annotated default method on an interface the class implements, except on a bean behind
a JDK proxy whose target can't be unwrapped, since the scan read the proxy's class, which declares every method of its
interfaces. Nothing was logged, and the application ran without that handler.

That covers `@Subscription`, `@StreamSubscription`, `@DcbSubscription`, `@SynchronousSubscription`, `@Projection` and
`@Snapshot` on both starters, and `@Saga` on the blocking one. An override was found, because the class declares it,
and `AnnotationUtils.findAnnotation` reads the overridden method's annotation when the override has none of its own.

Finding an inherited `@Projection`, `@Snapshot` or `@Saga` factory method would not have been enough.
`SubscriptionAnnotations.invokeDescriptorFactory` refused with an `IllegalStateException` unless the method's
declaring class was exactly the class of the bean. Decision point 4 of ADR 127 asks for that equality, because a
subclass could override the factory method with code the scan never read.

A scan of declared methods also lists the bridge method the compiler adds when a class overrides a generic handler,
such as `on(E event)` on `Base<E>`, with a concrete parameter type, such as `on(String event)` on a class that extends
`Base<String>`. `findAnnotation` gives the bridge `on(Object)` the handler's annotation, so the scan found that handler
twice under one id, and the application failed at startup with `DuplicateSubscriptionIdException` on both starters. A
class that extends `Base<String>` without overriding `on` has no bridge method, and its handler was missed like any
other inherited one. Finding it is not enough either. The `Method` declares the erasure of `E`, `on(Object event)`, and
the starters read the event type from the `Method`. A handler on `Base<E extends OrderEvent>` would subscribe to every
`OrderEvent` on a class that extends `Base<Placed>`. A factory method whose return type has a type variable for its
state type has the same problem.

The starters registered every annotated handler once all singleton beans existed, and two kinds of handler missed a
write made before that, from a `@PostConstruct` method for example. A `@Subscription`, `@StreamSubscription` or
`@DcbSubscription` that started live, at `NOW` or at `DEFAULT` with nothing stored, began where the event store was
when it registered. A `@SynchronousSubscription` runs on the thread that writes through the application service, and
nothing replays a write to it. Neither logged anything. Under `occurrent.subscription.mode=MANUAL` the starters kept
the synchronous subscription model stopped until the application started the subscriptions, and a write through the
application service in that time was stored without reaching a synchronous handler.

## Decision

### 1. A handler is an annotated method the bean's class runs, wherever it is declared

Both post-processors call `HandlerScan.handlerMethods`, which finds handlers by using Spring's
`MethodIntrospector.selectMethods`. It reads the bean's class, its superclasses and its interfaces, and gives each
method as the one the bean's class actually runs:

- A method inherited from a superclass and not overridden is a handler.
- An interface default method the class doesn't override is a handler.
- An override without an annotation of its own is one handler. It runs the override under the overridden method's
  annotation.
- An override that repeats the annotation is one handler under its own annotation.
- The bridge method the compiler adds for an override of a generic method is left out, so a generic handler is one
  handler. The bridge the compiler adds to a public class for a public method it inherits from a superclass that isn't
  public is the handler, since it has the method's annotation and calls the inherited method. 0.34.0 registered
  that bridge too.
- Overriding follows the JVM's rules. A package-private method is overridden by a method in its own package, and by a
  method in any package that overrides such a method.
- A method that only shares its name and parameters with an annotated method it doesn't override is refused at
  startup. That happens when the annotated method is package-private and a superclass in another package declares it,
  since Spring's annotation search ignores packages and gives the subclass's method that annotation. When the bean's
  class, or a superclass below that method, declares a method of the same signature, that method is checked instead.
- When the bean's class is in the package of a package-private annotated method, Spring takes the first method of the
  same signature up from the bean's class as the one it runs, even a method in another package that doesn't override
  it. The scan also reads each package-private method Spring replaces this way, so the annotated method is found, as
  the JVM runs it. When the method in between is public or protected, a CGLIB proxy of the bean's class runs that
  method instead, so such a handler is refused at startup on a bean behind one.
- A static method an interface declares is left out, since a class doesn't inherit it. A static method a superclass
  declares is a handler, and it's refused the way a static method the bean's class declares is. A static method of
  the same signature below it hides it and doesn't override it, so when Spring's search gives that method the
  superclass method's annotation, it's refused at startup like the package-private case above. When it repeats the
  annotation, both methods are handlers.
- When a method of a superclass or interface names a class missing at runtime, such as a type from an optional
  dependency, Spring can't read the hierarchy, and the scan reads only the methods the bean's class declares, as 0.34.0
  did. A handler any superclass or interface declares would then never run, so the scan looks for one. It reads the
  class file of each superclass and interface whose methods can't be read, without loading it, and the methods of each
  one that can be read. When one declares a handler, startup fails with an `IllegalStateException` naming the handler
  and the missing class.
  Startup doesn't fail when the bean's class declares a method that runs the handler, which is an override with a
  handler annotation, or the bridge javac adds to a public class for a public handler it inherits from a superclass
  that isn't public. The scan registers that bridge as the handler, as 0.34.0 did, and the code after the scan uses
  the bridge itself where it can't read the method the bridge calls. Which method a bridge calls comes from the
  bridge's code in the class file of the bean's class, since the bridge javac adds for an override with other erased
  types can have the same name and parameter types as that bridge, and only the code tells them apart. Whether a
  method overrides the handler follows the JVM's rules, so a method in another package overrides a package-private
  handler through a public or protected method that a class between them in the handler's package declares. When the
  scan can't read what it needs to tell, it counts the handler as never run and startup fails. Startup fails the same
  way when a class file can't be read either, since the scan can't rule out a handler there. When none declares a
  handler, the scan logs at debug level that it skips the inherited methods. A method the bean's class declares itself
  that names a missing class fails startup with `NoClassDefFoundError`, as in 0.34.0.

A handler's event type and a factory's state type are read against the bean's class with Spring's `ResolvableType`.
So `on(E event)` on `Base<E>` subscribes to `Placed` on a class that extends `Base<Placed>`. When the class doesn't give
`E` a type, as a raw `extends Base` doesn't, a handler gets the erasure of `E` as before, and a factory whose state type
is a type variable fails at startup with the error it gave before. The bridge the compiler adds to a public class for
a public method it inherits from a superclass that isn't public has only the erased types, so the types are read from
the method the bridge calls.

The scan reads the class behind a proxy it can unwrap, as in 0.34.0. A handler found on that class runs through the
proxy, so advice such as `@Transactional` still applies. When a JDK interface proxy doesn't have that method, because
the class overrides a generic interface method with a concrete parameter type, the handler runs through the interface
method it overrides.

A proxy whose target source isn't a fixed singleton can't be unwrapped. A CGLIB proxy like that is scanned as the class
it subclasses, and a JDK interface proxy like that as its own proxy class, both as in 0.34.0. So when such a JDK
proxy is the bean, as when a `@Bean` method or a `FactoryBean` returns it, only the annotations on the proxy's
interfaces count, and a handler annotated only on the class behind it doesn't run. Reading the class behind such a
JDK proxy instead would register a handler twice when the proxy's target is a bean of its own, such as the target of a
`LazyInitTargetSource` or a `HotSwappableTargetSource`, since the scan also finds that bean under its own name.

A handler declared with a type variable on a generic interface of such a JDK proxy takes its types from what the
proxy's interfaces bind. When `Handler<E extends OrderEvent>` declares it and the proxy implements
`PlacedHandler extends Handler<Placed>`, it subscribes to `Placed`. Only a proxy that implements `Handler` itself
subscribes to the bound, `OrderEvent`. 0.34.0 read the proxy class's own method, which has only the erased types, so it
subscribed to the bound in both cases. A factory method's state type is read through the same binding.

Two interfaces of such a JDK proxy can each declare an annotated method with the same name and parameter types. The
proxy class has one method for both, and 0.34.0 found the annotation of the first interface the proxy lists on it, so
only that handler registered. When the two annotations have the same type and differ in an attribute, such as the id,
the order of the interfaces decided which handler ran, and the other never did, with nothing logged. The scan now keeps
one method when the two annotations are equal in every attribute, and fails startup with an `IllegalArgumentException`
naming both methods when they differ in their type or in any attribute.

When a post-processor wraps the bean in such a JDK proxy instead, the startup scan reads the class the bean had before
the proxy wrapped it. A handler the proxy doesn't implement then fails startup with
`SubscriptionHandlerNotInvocableException`, as in 0.34.0, and so does a handler that overrides a generic interface
method with a concrete parameter type. A prototype that the post-processor only wraps after startup is scanned as the
proxy class, so a generic interface handler has another signature than the one the startup scan registered, and
building that instance fails with `DuplicateSubscriptionIdException`. In 0.34.0 that bean failed startup with the same
exception.

A bean with a scoped proxy has two bean definitions, the proxy's and its target's under the `scopedTarget.` name. The
scan skips the target's and reads the handlers under the proxy's name, from the class of the instance the scope holds at
startup. The handlers register once, under the proxy's name, and each delivery to a handler the proxy's class has goes
through the proxy, as with Spring's `@EventListener`, so it reaches the instance the scope holds at that moment. In a
scope that replaces its instance, such as a refresh scope, a handler runs on the new instance after a refresh.

A handler the proxy's class doesn't have runs on the bean the container returns for the target's `scopedTarget.` name,
asked for on every delivery, which is the instance the proxy would reach. A CGLIB proxy subclasses the type the target's
definition declares, so it lacks a handler declared only below that type, as for a `@Bean` method declared to return a
base class. A JDK proxy, from `ScopedProxyMode.INTERFACES` or a type declared as an interface, implements only
interfaces, so it lacks a handler only the class declares. Such a handler skips the interceptors on the proxy itself,
such as the lock Spring Cloud takes around each call through the proxy of a `@RefreshScope` bean. `@EventListener`
always calls through the proxy, so this is where the starters depart from it.

A delivery while the scope isn't active fails
with the exception the scope throws. I haven't tested what that does to a durable subscription's checkpoint. The scan
asks the scope for the instance only when the target's class shows an Occurrent annotation, the class of a target
already built when there is one and the predicted class otherwise. So a bean in request or session scope fails startup
with an `IllegalStateException` that names the bean when it has a handler, and starts when it has none. The scan also
asks when neither class is known, as for a lazy scoped proxy whose target definition has no class, so such a bean
registers its handlers or fails startup instead of being passed over. A scoped proxy that isn't lazy already fails
startup when its target's class is unknown, since Spring needs that class to create the proxy. Once every handler of
the target's class is registered the scan doesn't ask again, so a prototype bean with a scoped proxy builds one target
at startup and then one per delivery. When the predicted class shows no annotation, as for a `@Bean` method declared
to return an interface or a base class while the class it returns declares a handler, the scan doesn't ask at startup.
The handlers register when the target is first built, by a call through the proxy or by the scope, under the proxy's
name and from that instance. Until something builds the target its handlers don't run, as for a lazy bean whose
predicted class shows no annotation. In request or session scope building that target fails instead, in the request
that builds it, with the same `IllegalStateException`, since no subscription thread has a request or a session bound.
A bean in any other scope registers, and a delivery while its scope isn't active fails as described above. A bean that is only named `scopedTarget.` plus another bean's name, with no
scoped proxy decorating it, is scanned as any other bean. A `@Projection`, `@Saga` or `@Snapshot` factory method runs once, on the
instance the scan asked the scope for or on the first target built, so a refresh doesn't change its descriptor. That
descriptor keeps handling events after a refresh replaces the instance it came from ([#1492](https://github.com/johanhaleby/occurrent/issues/1492)).

A prototype bean without a scoped proxy runs every delivery on the one instance the scan asked for, as in 0.34.0.
Asking the container per delivery, as `@EventListener` does, would build the bean and its collaborators for each event
with nothing to destroy them, and a projection, saga or snapshot, which reads its descriptor once, couldn't follow.

The key that tells whether a handler is already registered is the bean name, the method's name and its parameter
types, as in 0.34.0, and each key records the methods registered under it, each with the class its scan read. A later
pass of the scan over the same bean skips a method that is the same handler as a recorded one. When the two scans read
one class, or one class extends the other, the two methods are one handler when an instance of the class further down
runs one method for both. So a private method on a superclass and a method of the same signature on the bean's class
are two handlers, in whichever order the instances come, and so are a private superclass method and an interface
default method of the same signature. When the two scans read unrelated classes, the methods are one handler, as in
0.34.0, so a prototype whose factory returns two unrelated classes, each with a private handler of one signature,
registers the handler of the first class only. That makes the outcome depend on the order of the instances when a
factory returns `A` with a private handler, `B` that extends `A` and declares a handler of the same signature, and an
unrelated `U`. When `A` or `B` comes first, the handlers of both register and the one of `U` doesn't. When `U` comes
first, only its handler registers. No rule keeps two unrelated classes as one handler and makes this independent of order,
since `U` is one handler with `A` and one with `B`, while `A` and `B` are two.

`invokeDescriptorFactory` runs a factory method that the bean's class declares or inherits unchanged. It refuses a bean
whose class overrides the method, since that runs code the scan didn't read. Two proxies can't be unwrapped to their
target, because their target source isn't a fixed singleton:

- A JDK interface proxy runs the factory through the proxy when one of the proxy's interfaces declares the method the
  scan read, and refuses any other method, since the proxy doesn't implement it. When the proxy is the bean, the scan
  reads the proxy's own class, so the factory it reads comes from one of the proxy's interfaces. When a post-processor
  wrapped the bean, the scan reads the class the bean had before, so a factory only that class declares is refused.
  It doesn't check that the target the proxy reaches runs the method the scan read.
- A CGLIB proxy refuses a package-private factory method declared in another package than the class it subclasses.
  The proxy can't override such a method, so invoking it would run the method's body on the proxy instance.

This replaces the exact class equality in Decision point 4 of ADR 127.

Interface default methods count because the bean runs them, the same way it runs a method inherited from a superclass.
Spring finds `@EventListener` and `@Scheduled` methods with `MethodIntrospector.selectMethods` too, so a default method
with one of those annotations already counts there.

I rejected three smaller changes:

- A loop over `getDeclaredMethods()` of each superclass. It still lists bridge methods, misses interface default
  methods, and needs code of our own to find which method overrides which. `selectMethods` already does all three.
- `getMethods()`. It lists inherited methods, but only public ones, and a handler that isn't public registers in
  0.34.0, so it would stop registering.
- A setting that turns the new scan on, with the old one as the default. Every application that doesn't find the
  setting keeps a handler that never runs and a log that says nothing about it. Occurrent is pre-1.0, and I'd rather
  correct a mistake with a migration path than keep it.

### 2. Where a subscription that starts live begins

The post-processors read where the event store is, its live start, as the event store bean is created, in
`postProcessAfterInitialization`. A first start at `NOW`, or at `DEFAULT` with nothing stored, of a `@Subscription`,
`@StreamSubscription`, `@DcbSubscription`, `@Projection` or `@Snapshot`, or of a `@Saga` on the blocking starter,
begins there, except in the cases the rest of this section lists. That answers the question #979 left open, whether `@Projection`, `@Snapshot` and `@Saga` should get
the startup writes too. They start live the same way, and a `@Projection` and a `@Saga` start at `DEFAULT` unless
they set `startAt`.

- With a `CheckpointStorage` that Spring can pick, a single one or a `@Primary` one among several, that is the MongoDB
  storage of the starter, `SpringMongoCheckpointStorage` on the blocking starter or `ReactorCheckpointStorage` on the
  reactive one, a subclass included, the starter stores the live start as the subscription's first checkpoint, with
  `CheckpointWriteCondition.ifAbsent()`, and starts it at the subscription model's default, which resumes from that
  checkpoint. A storage that answers `false` from
  `evaluatesWriteConditionsFor` fails the startup instead, unless
  `occurrent.subscription.start-when-first-position-can-be-overwritten` is set, and then gets an unconditional write
  with a warning, as the durable models do, per
  [ADR 184](0184-a-first-position-a-checkpoint-storage-can-only-write-unconditionally-is-refused.md).
- At `NOW` with `resumeBehavior = SAME_AS_START_AT`, nothing is stored and the live subscription model starts there.
- Over any other `CheckpointStorage`, such as `SpringRedisCheckpointStorage`, the starter stores nothing and the
  subscription starts as in 0.34.0. The live start is read long before the subscription asks the storage whether a
  checkpoint is stored, and such a storage keeps a checkpoint another node stored in between, so a subscription
  started at the live start would skip the events between the two. A write a `@PostConstruct` method makes directly
  to the event store before the subscription registers is then missed on a first start, as in 0.34.0, on a single
  node as much as on several. The starter asks `LiveStartCheckpoints.ordersCheckpointsIn`, which the MongoDB
  starters answer for their own storage only.
- A subscription that has a checkpoint stored already, or whose start the application's `SubscriptionResumePoints`
  gives, starts as in 0.34.0. So does any other one when Spring can't pick a `CheckpointStorage`. A checkpoint that
  another node stored after this node read its live start is different. On the MongoDB storage of the starter, when
  that node's first position is later than the live start, the first-position race of
  [ADR 130](0130-a-subscriptions-first-position-race-resolves-by-order-not-by-write-order.md) writes the live start
  over it, as [ADR 186](0186-a-save-from-a-later-position-is-refused-once-a-start-position-moved-it-earlier.md)
  records, and the subscription starts from the live start.

A subscription starts where the event store is when it subscribes, which decision point 3 places after the startup
scan, also in three cases where the starter has no live start to give it. On the reactor starter a bean built on a Reactor non-blocking thread reads none,
since the read blocks. An event store bean behind a Spring AOP proxy is read through its single target, and one
without a single target is skipped with a warning, since calling the proxy would run its advice. Only the MongoDB
starters turn a live start into a checkpoint, through the `LiveStartCheckpoints` bean they declare, and only over the
MongoDB storage of the starter.

The live start comes from the store the subscription model reads, the one Spring injects as its `EventStoreQueries`,
or as its `DcbEventStore` when the `STREAM` capability is off, so the only one or the `@Primary` one. Another bean over
the same collection can be created first and written through from a `@PostConstruct` method before that store
exists. So the first event store bean that is created makes the starter create the model's store too, and that store
reads its live start before such a write. When the first one is the model's store, Spring refuses to create it a
second time with a `BeanCurrentlyInCreationException`, and the starter ignores that.

The read happens at creation, not when a subscription first reads the event store, because a write between the two
is the write the subscription would miss. The Mongo event stores and the in-memory one always return a scope from
`openCatchupReadScope`. A Mockito mock returns `null`, so a context with one fails to start with an `IllegalStateException` that names that method.

A bean built after startup, such as a `@Lazy` bean the startup scan can't see a handler on, reads its own live start
when it is created, and its subscriptions begin there. A write between startup and that bean's creation is history
to it, so a subscription at `NOW` doesn't get it. The read comes before the bean's `@PostConstruct` method runs, since
Spring runs that method from `CommonAnnotationBeanPostProcessor`, which it moves to the end of the post-processor
chain, so a subscription at `NOW` gets what that method writes. A `FactoryBean` built after startup reads the live
start for its product the same way, before the factory's own `@PostConstruct` method runs, so the product's
subscription at `NOW` gets what the factory writes while it is initialized and while `getObject` runs. The product's
class is unknown until `getObject` runs, so every such factory makes that read, also one whose product declares no
handler. Spring calls no post-processor before `getObject` runs on a factory that already exists, so the live start
belongs to the factory, and the product can get events written before it was built:

- The product of a singleton or scoped factory starts where that factory read. When the factory was created in an
  earlier call, for example by `getBean("&name")` or by injecting the factory, or its `isSingleton()` returns false so
  each lookup builds a new product, the product also gets what was written between the factory's creation and its own.
- The product of a prototype factory starts where the oldest factory of its bean name read that its thread created, no
  product claimed, and the garbage collector hasn't collected. That's its own factory unless an older one is still
  there, such as the factory whose `getObject` asked for this product under its own name, one asked for with
  `getBean("&name")`, or one whose creation failed. The product then also gets what was written since that older
  factory was created. A new prototype factory reads its live start no earlier than the newest of those factories, so
  the oldest one's is the earliest even when two reads of the event store go back, as two MongoDB sessions can when the
  primary changes. That rests on MongoDB's documented `afterClusterTime` behaviour, which no test here runs against a
  primary change.
- The product of a factory built before the startup scan ended starts from the live start read when the event store
  bean was created, so it also gets what was written while the application started and while `getObject` ran. That
  includes a prototype factory whose creation began before the scan ended and whose `getObject` ran after it, and a
  prototype product whose oldest factory, as above, is such a factory.

The post-processors keep each bean's live start from before its initialization to after it by the bean instance, not
by its name, so two concurrent creations of one prototype bean each start from their own, and they keep a factory's
by the factory instance the same way.

A bean built after startup that declares a synchronous handler the scan didn't register fails its creation before its
`@PostConstruct` method runs, see decision point 3, so that method writes nothing the handler would miss. 0.34.0
registered such a handler once the bean's `@PostConstruct` method had run, and a write that method made through the
application service was stored without reaching it.

### 3. A write through the application service waits for the synchronous handlers

Each post-processor registers every handler in one scan, which runs again until a pass registers nothing, since
registering a handler can build beans. Then it registers the handlers of the beans other threads built during the
scan. A synchronous handler, `@SynchronousSubscription` and a synchronous `@Projection` or `@Snapshot`, registers
during the scan. Every asynchronous handler is held back until both are done, a `@Saga`, a push projection and a
domain feed's catch-up included. Then the post-processor admits writes and starts the held-back handlers on the thread
that ran the scan, in the order it registered them, and each `WAIT_UNTIL_STARTED` replay ends before the next handler
starts. A context with both starters' post-processors holds the handlers of both until the second has admitted writes,
since `StartupWriteGate` refuses a write while either post-processor says no, and a replay that writes through the
application service before then would fail the start. The post-processor whose scan ends last starts the held-back
handlers of both. So a write that a replay or a saga makes reaches every synchronous handler the scan registered. In
0.34.0 each handler started as the scan registered it, and a replay ran before the handlers registered after it. A
subscription that starts live begins at the boundary of decision point 2 where there is one, so starting it later
skips nothing.

Where decision point 2 gives no boundary, a subscription that starts live begins where the event store is when it
subscribes, which is now after the scan. Decision point 2 gives none for a bean built on a Reactor non-blocking
thread, for an event store bean behind a proxy without a single target, for an application with neither MongoDB
starter, and, except at `NOW` with `resumeBehavior = SAME_AS_START_AT`, for a context where Spring can't pick a
`CheckpointStorage`. Such a subscription misses an event written directly to the event store while the scan runs, for
example from the `@PostConstruct` method of a bean the scan creates, which 0.34.0 delivered when that bean was created
after the subscription had subscribed. A write through the application service can't be made then, since the starters
refuse it.

Until then the starters refuse every write through the application service, from any thread. They wrap the
synchronous dispatcher of the `ApplicationService` and `DcbApplicationService` they create in `StartupWriteGate`. Its
`beforeWrite` asks each post-processor through `StartupGap.admitsWrite()`, and throws `WriteRefusedException` when
one says no. A post-processor says no until its scan and the registration after it are done.

The application service calls `beforeWrite` in `execute`, on the caller's thread, before it reads anything, so nothing is stored. On
the reactor stack the `Mono` that `execute` returns calls it each time it is subscribed, and fails with the
refusal. That makes the refusal an error signal, so a `@PostConstruct` method that subscribes without an error
consumer stores nothing and the application starts, with only Reactor's log line about the dropped error. Asking at
assembly instead would have refused a `Mono` built during startup and subscribed later, and let one built before a
`stop()` and subscribed after it write past the stopped handlers. The message names `ApplicationRunner` and
`ApplicationReadyEvent`, since both run after the scan. A thread the application starts while it starts gets the same
answers. Whether a write from the application's own `SmartInitializingSingleton` is refused
depends on whether Spring calls it before or after the post-processor's, which follows the order the beans were
registered in.

A bean that would register a synchronous handler that isn't registered yet, created once writes are admitted, fails
its creation with an `IllegalStateException` before its init methods run. The scan builds every bean it sees a handler
on, `@Lazy` or not, so this is a bean whose declared type hides the annotation, `@Lazy` or under
`spring.main.lazy-initialization=true`, or one that a held-back replay builds for the first time. A bean whose
creation began before writes were admitted fails once it is initialized, if its handler is still unregistered then. So
does a bean that another post-processor replaced with an object that declares such a handler, since the check runs on
the object the starter's post-processor receives once the bean is initialized. Every bean with an unregistered
synchronous handler that is initialized before writes are admitted, on any thread, registers before they are. Whether
a handler is registered is read again once writes are admitted, so a prototype instance or the target of a scoped
proxy whose handler registered at startup is created as before. The message says to remove `@Lazy`, to declare the
`@Bean` method with the annotated class as its return type, or to make the handler asynchronous. 0.34.0 registered
such a handler when the bean was built, and it missed every write before that.

The starters refuse such a write also when the application declares no synchronous handler, since the scan that would
show there is none hasn't run yet either. A write made directly with `EventStore.write` or `DcbEventStore.append` is
not refused. It reaches a synchronous handler, whenever it is made, only if code passes its events to the
`SynchronousSubscriptionModel` while that handler is running, which the model's javadoc describes. Decision point 2
gives it to a subscription that starts live.

The synchronous subscription model also refuses a write while it is stopped and has a handler registered, and a write
whose events a paused synchronous subscription matches. In 0.34.0 the write was stored, and the stopped or paused
handler never got its events. The stream application service matches the paused filters against the events with the
stream id and stream version the write gives them. A filter on the position or on the append id can't match before
the event store writes the events, so it doesn't stop the write. `OccurrentSubscriptionsExtension` turns both refusals
off, since a test stops the subscriptions it doesn't name and keeps writing. The model refuses these writes without a
starter too, in any application service built with `synchronousSubscriptions(..)`. It supersedes the passage of
ADR 85 that left such a write to succeed.

The check runs before the event store writes and the dispatch after it, so a pause or stop in between would let a
checked write go past the handler. The model counts the writes it let through, and the application service tells it
when each has been dispatched or failed. A `pauseSubscription(id)` or `stop()` waits for the writes let through before
it, and a write that starts while it waits is refused when it matches the pausing subscription, or at all while the
model stops. A later write can't hold a waiting pause back. A handler that pauses or stops its own model doesn't
wait, since its own write is one it would wait for, and the pause or stop applies once the last of those writes ends.

On the reactor stack a handler runs on whatever thread the event store calls back on, a MongoDB driver thread for
example, and nothing tells that thread apart from one that pauses the model from outside. So while the model routes
the events of a write, a pause or stop from any thread returns at once, and so does one on a non-blocking thread. It
applies once the writes it would have waited for end, and `isPaused` answers `false` until then. The blocking model
runs its handlers on the writer's thread, so there a pause or stop waits unless it comes from that thread.

Under `MANUAL` the synchronous subscription model runs from the start. Stopped, it would refuse every write through
the application service until the application started it.

I rejected registering such a handler when the bean is built, as 0.34.0 did. The handler then gets only the writes
made after that, and nothing tells the application that the read model it fills is missing the earlier ones.

I rejected registering the synchronous handlers in a scan of their own, before the rest, and letting a write through
while the rest registered whenever no bean with an unregistered synchronous handler existed. A bean of that kind built
after such a write would have to fail, and whether a write from a replay's thread came before the bean was built
depended on timing, so the same application could start or fail. I also rejected admitting only the thread that runs
the scan. A `WAIT_UNTIL_STARTED` replay runs on a thread of its own, so its writes would fail, and a handler that
retries a refused write would hang startup while the scan waits for that replay.

I rejected registering the synchronous handlers on the first write instead, from the thread that writes. A write from
a `@PostConstruct` method asks for beans that are still being created, the writer itself among them, and the scan
then fails with a `BeanCurrentlyInCreationException` that says nothing about the write. Refusing the write gives one
error, with the place to write from in its message.

### 4. A synchronous handler gets the writes of its own process's application service, after it registered

A synchronous handler has no replay and no checkpoint. It gets the events written through the application service of
the process it runs in, from the moment it registers. Any other event reaches it only if code passes the event to the
`SynchronousSubscriptionModel` while the handler is running, with `dispatch` on either stack or `accept` on the
blocking one, which the model's javadoc describes. Without that, these don't reach it:

- The events stored before it registered. A synchronous handler added to an application whose event store has events
  starts from nothing. ADR 59 says a synchronous projection "has no history to catch up on", which is true only of an
  empty event store.
- A write made directly with `EventStore.write` or `DcbEventStore.append`.
- A write from another process, including one still running the previous version during a rolling deploy. Every
  process that writes through an application service must run the same synchronous handlers, so adding or removing one
  needs every process stopped.
- Without a transaction executor, a write whose handler failed after the write committed. Delivery is best-effort then.

A user who picks a synchronous handler opts into these, and the javadoc of `@SynchronousSubscription`, `@Projection`
and `@Snapshot` lists them. The two misses nobody opts into, a write while the application starts and a handler on a
bean built late, are decision point 3.

To read your own writes over the events already stored, an asynchronous `@Projection` with `startAt = BEGINNING`,
`recordAppliedAppends = true` and `waitUntilApplied` does it. To switch a projection from `ASYNC` to `SYNCHRONOUS`,
the asynchronous one has to have applied every stored event, with every process stopped so nothing is written
meanwhile. A rebuild shows that, its stored checkpoint deleted and `startAt = BEGINNING` with
`startupMode = WAIT_UNTIL_STARTED`, since startup then waits for the replay to reach the stored events' end. A resume
from a stored live checkpoint doesn't, because it subscribes live with no replay and startup can end before the events
written since that checkpoint are applied. The checkpoint stored under the projection's id then has to be deleted. The
synchronous path never moves it, so a later switch back to `ASYNC` would resume from the old position and apply the
events since then a second time.

## Consequences

- **A handler that never ran starts running after the upgrade.** It starts from its own start settings. When nothing
  is stored for its id, `startAt = BEGINNING`, or `startAt = BEGINNING_OF_TIME` on `@StreamSubscription`, replays the
  whole history on the first start, and so does a `@Snapshot`, whose `startAt` is `BEGINNING` by default. A
  `@Projection` or `@Saga` with `source = PUSH` replays the event store's history before it goes live, unless it sets
  `catchup = NONE`. When a handler with that id ran before, for example before the method moved into a superclass,
  its position is stored, and under the default `resumeBehavior` it resumes from there, so it gets every event written
  since that handler last ran. A handler that a public class inherits as a public method from a superclass that isn't public
  isn't one of them. 0.34.0 registered the bridge the compiler adds for it, so it keeps its id and its checkpoint.
- **Two beans that run one annotated handler without overriding it fail at startup with
  `DuplicateSubscriptionIdException`.** They are two subclasses of the class that declares it, or that class and a
  subclass, each a bean. Both handlers have the same id, and the id is the key a subscription's
  checkpoint is stored under, so they can't share it. Overriding the method in each subclass and repeating the
  annotation with an id of its own fixes it.
- **A class that repeats a handler annotation on a method it doesn't override registers both methods.** The
  superclass method is private, or package-private in another package, and the class's method has the same name and
  parameters. In 0.34.0 only the class's method ran. With different ids the superclass method starts running too, and
  with the same id the application fails at startup with `DuplicateSubscriptionIdException`. Both methods run on the
  bean, so the scan registers both, as it does for any two methods of one class tree that the bean runs.
- A subscription annotation on a superclass method that is static fails startup with
  `SubscriptionHandlerNotInvocableException`. So does one on a bean behind a CGLIB proxy when the method is private,
  final, or package-private in another package than the bean's class, since the proxy can't override it. It is refused
  there too when it is package-private and a class in another package, between it and the bean's class, declares a
  public or protected method with the same name and parameters, since the proxy overrides that method and runs it
  instead. Behind a JDK interface proxy, one that is private or package-private fails startup the same way, since
  the proxy runs only methods of its interfaces. When one of them declares a method with the same name and
  parameters, the bean's class implements that method with another body, which the proxy would otherwise run under
  the handler's id. A method the bean's class declares already gets the same refusal when it is static, private or final. An
  inherited factory method of those four kinds is refused only when the CGLIB proxy's target can't be unwrapped, since the factory otherwise
  runs on the unwrapped bean. A factory method the bean's class declares keeps running on such a proxy, as it did
  before, so no factory that worked in 0.34.0 starts failing.
- In Kotlin a superclass method is final unless it is marked `open`, and the `kotlin-spring` plugin opens a class only
  when the class itself has a Spring annotation. So a subscription handler on a plain Kotlin superclass fails startup
  when the bean that inherits it is behind a CGLIB proxy. A factory method there fails only when the proxy's target
  can't be unwrapped.
- A handler declared with a type variable on a superclass subscribes to the type the bean's class gives the variable.
  That includes a handler of the bridge kind, which 0.34.0 subscribed to the variable's bound, so it stops getting
  events of the bound's other types. When its `eventTypes` lists a type that isn't the one the bean's
  class gives the variable or a subtype of it, startup now fails with an `IllegalStateException`. A factory method of
  that kind found no state type in 0.34.0, so it failed at startup unless it named its store or the application had
  one bean of the store type. It keeps that store, and where it failed, it now gets the zero-config store.
- A handler declared with a type variable on a generic interface of a JDK proxy that can't be unwrapped subscribes to
  the type the proxy's interfaces give the variable, where 0.34.0 subscribed it to the bound. Only a proxy that
  implements the generic interface itself still gets the bound. When its `eventTypes` lists a type that isn't the one
  the interfaces give the variable or a subtype of it, startup now fails with an `IllegalStateException`. A factory
  method of that kind found no state type in 0.34.0 either, so it keeps the store it named or the one bean of the
  store type it got, and where it failed, it now gets the zero-config store for the type the interfaces bind.
- A JDK proxy that can't be unwrapped, with two interfaces that declare a method of the same name and parameter types
  under one handler annotation with different attributes, such as two ids, now fails startup with an
  `IllegalArgumentException`. 0.34.0 registered only the handler of the first interface the proxy lists.
- A static method that hides an annotated static superclass method ran under that method's annotation in 0.34.0. Now
  the application fails at startup when the hiding method has no annotation of its own, and registers both when it
  repeats the annotation. A hidden static subscription handler, refused before too, now fails with the scan's
  `IllegalStateException` instead of `SubscriptionHandlerNotInvocableException`.
- An application with a class that overrides a generic handler with a concrete parameter type starts, and the handler
  registers once.
- A method without a handler annotation that shares its name and parameters with a package-private annotated method
  of a superclass in another package ran under that method's annotation in 0.34.0, though it doesn't override it.
  Now the application fails at startup. Running the annotated method instead would swap one handler body for another,
  and a user who missed the upgrade guide would get no signal of it. Making both methods protected or public lets the
  subclass's method override the superclass method, which keeps the 0.34.0 handler. Renaming the subclass's method
  runs the superclass method.
- There is no OpenRewrite recipe, because nothing in the code says whether an inherited handler was meant to run. The
  code compiles unchanged. [The upgrade guide](../../migration/upgrading-to-0.35.0.md#a-handler-method-the-bean-inherits-is-registered)
  says who is affected and what to do.
- **A blocking write through the application service from a `@PostConstruct` method, an `InitializingBean` or a
  `@Bean` method fails startup with `WriteRefusedException`, and so does a reactive one whose error reaches that
  method.** Such a write worked in 0.34.0, and the synchronous handlers registered after it never got it. A reactive
  write subscribed without an error consumer stores nothing, and the application starts. Moving the write to an
  `ApplicationRunner` or an `ApplicationReadyEvent` listener fixes it.
- A test whose context has a Mockito mock of the event store fails to start with an `IllegalStateException` until the
  mock returns a scope from `openCatchupReadScope`.
- A subscription that starts live gets the events written directly to the event store while the application starts,
  which 0.34.0 skipped. That includes a default `@Projection` and a default `@Saga`, so such a saga now acts on, and
  sends commands for, those events.
- Under `MANUAL` the synchronous handlers run on every write through the application service, also before the
  application starts the subscriptions.
- A write while the synchronous subscription model is stopped, or one whose events a paused synchronous subscription
  matches, fails with `WriteRefusedException` instead of going past that handler, with or without a starter. A
  pause or stop waits for the writes the model already let through, so a long write holds it back. On the reactor
  stack it returns at once while the model routes the events of a write, and applies when that write ends.
- Every asynchronous handler starts after the startup scan, in the order the scan registered it, where 0.34.0 started
  each one as the scan registered it. A `WAIT_UNTIL_STARTED` replay still ends before any `ApplicationRunner` runs.
  A subscription that starts live without a boundary from decision point 2 misses an event written directly to the
  event store while the scan runs.
- A subscription at `NOW` on a bean built after startup gets what that bean writes from its `@PostConstruct` method. A
  write between startup and the bean's creation is history to it.
- **A bean with a synchronous handler its declared type hides fails its creation when it is created after the startup
  scan, also when a startup replay creates it.** In 0.34.0 the handler registered when the bean was
  built and never got the writes before that. Removing `@Lazy`, declaring the annotated class as the bean's type, or
  making the handler asynchronous fixes it.
- The code compiles unchanged. [The upgrade guide](../../migration/upgrading-to-0.35.0.md#a-write-through-the-application-service-is-refused-while-the-application-starts)
  says who is affected by decision points 2 and 3 and what to do.
