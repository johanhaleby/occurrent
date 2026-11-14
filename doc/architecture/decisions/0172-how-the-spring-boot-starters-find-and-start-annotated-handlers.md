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
interfaces count, a handler annotated only on the class behind it doesn't run, and a handler declared on a generic
interface subscribes to the type variable's bound. Reading the class behind such a JDK proxy instead would register a
handler twice when the proxy's target is a bean of its own, such as the target of a `LazyInitTargetSource` or a
`HotSwappableTargetSource`, since the scan also finds that bean under its own name.

When a post-processor wraps the bean in such a JDK proxy instead, the startup scan reads the class the bean had before
the proxy wrapped it. A handler the proxy doesn't implement then fails startup with
`SubscriptionHandlerNotInvocableException`, as in 0.34.0, and so does a handler that overrides a generic interface
method with a concrete parameter type. A prototype that the post-processor only wraps after startup is scanned as the
proxy class, so a generic interface handler has another signature than the one the startup scan registered, and
building that instance fails with `DuplicateSubscriptionIdException`. In 0.34.0 that bean failed startup with the same
exception.

A bean with a scoped proxy has two bean definitions, the proxy's and its target's under the `scopedTarget.` name. The
scan skips the proxy's, so the handlers register once, under the target's name. They run on the target instance the
scan built, not through the scoped proxy. In a scope that replaces its instance, such as a refresh scope, a handler
keeps running on the instance the scan built.
[#1492](https://github.com/johanhaleby/occurrent/issues/1492) changes that, so a handler runs through the scoped proxy
as Spring's `@EventListener` does.

The key that tells whether a handler is already registered is the bean name, the method's name and its parameter
types, as in 0.34.0, and each key records the methods registered under it. A later pass of the scan over the same bean
skips a method that is the same handler as a recorded one. That is the recorded method itself, a method that
overrides it or that it overrides, or a method of a class unrelated to its class, as in 0.34.0. So a prototype whose
factory returns two unrelated classes, each with a private handler of one signature, registers the handler of the
first class only. A private method on a superclass and a method of the same signature on the bean's class don't
override each other, so they register as two handlers, in whichever order the instances come. The relation between
the methods decides this, not what one scan found, since successive instances of a prototype can be of different
classes in one class tree.

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
  instead. A method the bean's class declares already gets the same refusal when it is static, private or final. An
  inherited factory method of those four kinds is refused only when the CGLIB proxy's target can't be unwrapped, since the factory otherwise
  runs on the unwrapped bean. A factory method the bean's class declares keeps running on such a proxy, as it did
  before, so no factory that worked in 0.34.0 starts failing.
- In Kotlin a superclass method is final unless it is marked `open`, and the `kotlin-spring` plugin opens a class only
  when the class itself has a Spring annotation. So a subscription handler on a plain Kotlin superclass fails startup
  when the bean that inherits it is behind a CGLIB proxy. A factory method there fails only when the proxy's target
  can't be unwrapped.
- A handler declared with a type variable on a superclass subscribes to the type the bean's class gives the variable.
  That includes a handler of the bridge kind, which 0.34.0 subscribed to the variable's bound, so it stops getting
  events of the bound's other types. A factory method of that kind found no state type in 0.34.0 and failed at
  startup unless it named its store, and now gets the zero-config store.
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
