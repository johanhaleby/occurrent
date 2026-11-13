# 172. How the Spring Boot starters find and start annotated handlers

Date: 2026-11-13

## Status

Accepted. Decides [#983](https://github.com/johanhaleby/occurrent/issues/983). Amends Decision point 4 of
[ADR 127](0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md), "A descriptor
annotation is read after the singletons are instantiated". ADR 127 shipped in 0.34.0, so it is amended by reference
here and gets a pointer in its Status section.

## Context

Both starters, `OccurrentBlockingAnnotationBeanPostProcessor` and `OccurrentReactiveAnnotationBeanPostProcessor`,
read a bean's handlers from `getDeclaredMethods()` of the bean's class. That lists the methods the class declares
itself and none that it inherits. So an annotated method on a superclass that the class doesn't override never
registered, and neither did an annotated default method on an interface the class implements. Nothing was logged, and
the application ran without that handler.

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

## Decision

### 1. A handler is an annotated method the bean's class runs, wherever it is declared

Both post-processors call `SubscriptionAnnotations.handlerMethods`, which finds handlers by using Spring's
`MethodIntrospector.selectMethods`. It reads the bean's class, its superclasses and its interfaces, and gives each
method as the one the bean's class actually runs:

- A method inherited from a superclass and not overridden is a handler.
- An interface default method the class doesn't override is a handler.
- An override without an annotation of its own is one handler. It runs the override under the overridden method's
  annotation.
- An override that repeats the annotation is one handler under its own annotation.
- Bridge methods are left out, so a generic handler is one handler.
- A static method an interface declares is left out, since a class doesn't inherit it. A static method a superclass
  declares is a handler, and it's refused the way a static method the bean's class declares is.

A handler's event type and a factory's state type are read against the bean's class with Spring's `ResolvableType`.
So `on(E event)` on `Base<E>` subscribes to `Placed` on a class that extends `Base<Placed>`. When the class doesn't give
`E` a type, as a raw `extends Base` doesn't, a handler gets the erasure of `E` as before, and a factory whose state type
is a type variable fails at startup with the error it gave before.

The key that tells whether a handler is already registered is the bean name, the method name and the parameter types.
For a private method it also has the declaring class, since a private method on the bean's class and a private method
of the same signature on a superclass are two handlers, and neither overrides the other. A later pass of the scan finds
the same key and skips the handler, also when one pass reads a JDK proxy's class and finds the interface's method and
another reads the bean's class and finds the class's own.

`invokeDescriptorFactory` runs a factory method that the bean's class declares or inherits unchanged. It refuses a bean
whose class overrides the method, since that runs code the scan didn't read. Two proxies can't be unwrapped to their
target, because their target source isn't a fixed singleton:

- A JDK interface proxy runs the factory through the proxy when the method is declared on an interface the proxy
  implements, the way 0.34.0 ran it. It refuses any other method, since the proxy doesn't implement it.
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

## Consequences

- **A handler that never ran starts running after the upgrade.** It starts from its own start settings, and nothing is
  stored for its id. So `startAt = BEGINNING`, or `startAt = BEGINNING_OF_TIME` on `@StreamSubscription`, replays the
  whole history on the first start, and so does a `@Snapshot`, whose `startAt` is `BEGINNING` by default. A
  `@Projection` or `@Saga` with `source = PUSH` replays the event store's history before it goes live, unless it sets
  `catchup = NONE`.
- **Two beans whose classes extend one superclass with an annotated handler, and don't override it, fail at startup
  with `DuplicateSubscriptionIdException`.** Both handlers have the same id, and the id is the key a subscription's
  checkpoint is stored under, so they can't share it. Overriding the method in each class and repeating the annotation
  with an id of its own fixes it.
- A subscription annotation on a superclass method that is static fails startup with
  `SubscriptionHandlerNotInvocableException`. So does one on a bean behind a CGLIB proxy when the method is private,
  final, or package-private in another package than the bean's class, since the proxy can't override it. A method the
  bean's class declares already gets the same refusal when it is static, private or final.
- In Kotlin a superclass method is final unless it is marked `open`, and the `kotlin-spring` plugin opens a class only
  when the class itself has a Spring annotation. So an annotated method on a plain Kotlin superclass fails startup when
  the bean that inherits it is behind a CGLIB proxy.
- A handler declared with a type variable on a superclass subscribes to the type the bean's class gives the variable.
- An application with a class that overrides a generic handler with a concrete parameter type starts, and the handler
  registers once.
- There is no OpenRewrite recipe, because nothing in the code says whether an inherited handler was meant to run. The
  code compiles unchanged. [The upgrade guide](../../migration/upgrading-to-0.35.0.md#a-handler-method-the-bean-inherits-is-registered)
  says who is affected and what to do.
