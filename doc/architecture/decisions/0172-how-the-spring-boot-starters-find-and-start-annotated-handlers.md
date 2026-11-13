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
other inherited one.

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

The key that tells whether a handler is already registered includes the method's declaring class. A private method on
the bean's class and a private method of the same signature on a superclass are two handlers, since neither overrides
the other. Every pass of the scan reads the bean's class through the same call, so a later pass finds the same `Method`
and skips it.

`invokeDescriptorFactory` runs a factory method that the bean's class declares or inherits unchanged. It still refuses
a bean whose class overrides the method, since that runs code the scan didn't read, and a JDK interface proxy that
can't be unwrapped to its target, as it did before. This replaces the exact class equality in Decision point 4 of
ADR 127.

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
  whole history on the first start, and so does a `@Snapshot`, whose `startAt` is `BEGINNING` by default.
- **Two beans whose classes extend one superclass with an annotated handler, and don't override it, fail at startup
  with `DuplicateSubscriptionIdException`.** Both handlers have the same id, and the id is the key a subscription's
  checkpoint is stored under, so they can't share it. Overriding the method in each class and repeating the annotation
  with an id of its own fixes it.
- A subscription annotation on a superclass method that is static, or that is private on a bean behind a CGLIB proxy,
  fails startup with `SubscriptionHandlerNotInvocableException`, the same refusal a method the bean's class declares
  already gets.
- An application with a class that overrides a generic handler with a concrete parameter type starts, and the handler
  registers once.
- There is no OpenRewrite recipe, because nothing in the code says whether an inherited handler was meant to run. The
  code compiles unchanged. [The upgrade guide](../../migration/upgrading-to-0.35.0.md#a-handler-method-the-bean-inherits-is-registered)
  says who is affected and what to do.
