## Changelog next version
                                                                                                                                                   
* Removed `org.occurrent:eventstore-inmemory` as dependency to `org.occurrent:application-service-blocking` (it should have been a test dependency) 
* Including a "details" message in `DuplicateCloudEventException` that adds more details on why this happens (which index etc). This is especially useful
  if you're creating custom, unique, indexes over the events and a write fail due to a duplicate cloud event.

## Changelog 0.7.0 (2021-01-31)
                                 
* Introduced many more life-cycle methods to blocking subscription models. It's now possible to pause/resume individual subscriptions
  as well as starting/stopping _all_ subscriptions. This is useful for testing purposes when you want to write events 
  to the event store without triggering all subscriptions. The subscription models that supports this 
  implements the new `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle` interface.
  Supported subscription models are: `InMemorySubscriptionModel`, `NativeMongoSubscriptionModel` and `SpringMongoSubscriptionModel`. 
* The `SpringMongoSubscriptionModel` now implements `org.springframework.context.SmartLifecycle`, which means that if you
  define it as a bean, it allows controlling it as a regular Spring life-cycle bean.
* Introduced the `org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel` interface. Subscription models
  that wraps other subscription models and delegates subscriptions to them implements this interface. 
  It contains methods for getting the wrapped subscription model. This is useful for testing
  purposes, if the underlying subscription model needs to stopped/started etc.
* Fixed a bug with command composition that accidentally included the "previous events" when invoking the generated composition function.
* Added more command composition extension functions for Kotlin. You can now compose lists of functions and not only sequences.
* The `SpringMongoSubscriptionModel` now evaluates the "start at" supplier passed to the `subscribe` method each time a subscription is resumed.
* Fixed a bug in `InMemorySubscription` where the `waitUntilStarted(Duration)` method always returned `false`.
* `InMemorySubscription` now really waits for the subscription to start when calling `waitUntilStarted(Duration)` and `waitUntilStarted`.
* Moved the `cancelSubscription` method from the `org.occurrent.subscription.api.blocking.SubscriptionModel` to the 
  `org.occurrent.subscription.api.blocking.SubscriptionModelCancelSubscription` interface. This interface is also extended by
  `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle`.
* Introduced a much improved `RetryStrategy`. You can now configure max attempts, a retry predicate, error listener as well as the backoff strategy.
  Retry is provided in its own module, `org.occurrent:retry`, but many modules already depend on this module transitively. Here's an example:
  
  ```java
  RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(200), 2.0)
                                     .retryIf(throwable -> throwable instanceof OptimisticLockingException)
                                     .maxAttempts(5)
                                     .onError((info, throwable) -> log.warn("Caught exception {}, will retry in {} millis")), throwable.class.getSimpleName(), info.getDuration().toMillis()));
  
  retryStrategy.execute(Something::somethingThing);  
  ```
  
  `RetryStrategy` is immutable, which means that you can safely do things like this:

  ```java
  RetryStrategy retryStrategy = RetryStrategy.retry().fixed(200).maxAttempts(5);
  // Uses default 200 ms fixed delay
  retryStrategy.execute(() -> Something.something());
  // Use 600 ms fixed delay
  retryStrategy.backoff(fixed(600)).execute(() -> SomethingElse.somethingElse());
  // 200 ms fixed delay again
  retryStrategy.execute(() -> Thing.thing());
  ```
  
## Changelog 0.6.0 (2021-01-23)

* Renamed method `shutdownSubscribers` in `DurableSubscriptonModel` to `shutdown`.
* Added default subscription name to subscription DSL. You can now do:

    ```kotlin
    subscriptions(subscriptionModel) {
        subscribe<NameDefined> { e ->
            log.info("Hello ${e.name}")
        }
    }
    ```
    
    The id of the subscription will be "NameDefine" (the unqualified name of the `NameDefined` class).
* Added `exists` method to `EventStoreQueries` API (both blocking and reactive). This means that you can easily check if events exists, for example:

    ```kotlin
    val doesSomeTypeExists = eventStoreQueries.exists(type("sometype"))
    ```
* Added retry strategy support to SpringMongoSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to NativeMongoSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to SpringRedisSubscriptionPositionStorage. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when reading/saving/deleting the subscription position.
* Added retry strategy support to SpringMongoSubscriptionModel. You can define your own by passing an instance of `RetryStrategy` to the constructor. By default
  it'll add a retry strategy with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between 
  each retry when exceptions are thrown from the `action` callback (the callback that you implement to handle a `CloudEvent` instance from a subscription).
* All blocking subscription models will throw an `IllegalArgumentException` if a subscription is registered more than once.

## Changelog 0.5.1 (2021-01-07)

* Renamed `org.occurrent.subscription.redis.spring.blocking.SpringSubscriptionPositionStorageForRedis` to `SpringRedisSubscriptionPositionStorage`.
* Renamed `org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscription` to `ReactorMongoSubscriptionModel`.

## Changelog 0.5.0 (2021-01-06)

* Renamed `org.occurrent.subscription.api.blocking.BlockingSubscription` to `org.occurrent.subscription.api.blocking.SubscriptionModel`. The reason for this is that it was previously
  very confusing to differentiate between a `org.occurrent.subscription.api.blocking.BlockingSubscription` (where you start/cancel subscriptions) and a `org.occurrent.subscription.api.blocking.Subscription` 
  (the actual subscription instance). The same thinking has been applied to the reactor counterparts as well (`org.occurrent.subscription.api.reactor.ReactorSubscription` has now been renamed to `org.occurrent.subscription.api.reactor.SubscriptionModel`).
* Derivatives of `org.occurrent.subscription.api.blocking.BlockingSubscription` such as `PositionAwareBlockingSubscription` has been renamed to `org.occurrent.subscription.api.blockking.PositionAwareSubscriptionModel`.
* Derivatives of the reactor counterpart, `org.occurrent.subscription.api.reactor.PositionAwareReactorSubscription` has been renamed `to`, such as has been renamed to `org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel`.
* `org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSubscriptionModelConfig` has been renamed to `org.occurrent.subscription.blocking.catchup.CatchupSubscriptionModelConfig`. 
* `org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSubscriptionModel` has been renamed to `org.occurrent.subscription.blocking.catchup.CatchupSubscriptionModel`.
* `org.occurrent.subscription.util.blocking.AutoPersistingSubscriptionModelConfig` has been renamed to `org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig`.
* `org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence` has been renamed to `org.occurrent.subscription.blocking.durable.DurableSubscriptionModel`.
* `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionForMongoDB` has been renamed to `NativeMongoSubscriptionModel`.
* `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionPositionStorageForMongoDB` has been renamed to `NativeMongoSubscriptionPositionStorage`.
* Removed `org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionWithPositionPersistenceInMongoDB`. Use an `org.occurrent.subscription.blocking.DurableSubscriptionModel` from module `org.occurrent:durable-subscription` instead.
* `org.occurrent.subscription.mongodb.spring.blocking.MongoDBSpringSubscription` has been renamed to `SpringMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB` has been renamed to `SpringMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoDBSubscriptionPositionStorage` has been renamed to `SpringMongoSubscriptionPositionStorage`.
* `org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionForMongoDB` has been renamed to `ReactorMongoSubscription`.
* `org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionPositionStorageForMongoDB` has been renamed to `ReactorSubscriptionPositionStorage`.
* `org.occurrent.subscription.util.reactor.ReactorSubscriptionWithAutomaticPositionPersistence` has been renamed to `org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel`.
* `org.occurrent.subscription.util.reactor.ReactorSubscriptionWithAutomaticPositionPersistenceConfig` has been renamed to `org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionConfig`.
* `org.occurrent.eventstore.mongodb.spring.reactor.SpringReactorMongoEventStore` has been renamed to `ReactorMongoEventStore` since "Spring" is implicit.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification` has been renamed to `MongoFilterSpecification`.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification` has been renamed to `MongoJsonFilterSpecification`.
* `org.occurrent.subscription.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification` has been renamed to `MongoBsonFilterSpecification`.
* `org.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer` has been renamed to `MongoCloudEventsToJsonDeserializer`.
* `org.occurrent.subscription.mongodb.internal.MongoDBCommons` has been renamed to `MongoCommons`.
* `org.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition` has been renamed to `MongoOperationTimeSubscriptionPosition`.
* `org.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition` has been renamed to `MongoResumeTokenSubscriptionPosition`.
* `org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper` has been renamed to `OccurrentCloudEventMongoDocumentMapper`.
* `org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore` has been renamed to `SpringMongoEventStore`.
* Renamed module `org.occurrent:subscription-util-blocking-catchup-subscription` to `org.occurrent:catchup-subscription`.
* Renamed module `org.occurrent:subscription-util-blocking-automatic-position-persistence` to `org.occurrent:durable-subscription`.
* Renamed module `org.occurrent:subscription-util-reactor-automatic-position-persistence` to `org.occurrent:reactor-durable-subscription`.
* Moved `org.occurrent.application.converter.implementation.GenericCloudEventConverter` to `org.occurrent.application.converter.generic.GenericCloudEventConverter`.
* Moved `org.occurrent.application.service.blocking.implementation.GenericApplicationService` to `org.occurrent.application.service.blocking.generic.GenericApplicationService`.
* Added a new "Subscription DSL" module that adds a domain event specific abstraction on-top of the existing subscription model api's. This DSL makes it easier to create subscriptions that are using
  domain events instead of cloud events. The module is called `org.occurrent:subscription-dsl`. For example:
  
  ```kotlin
  val subscriptionModel = SpringMongoSubscriptionModel(..)
  val cloudEventConverter = GenericCloudEventConverter<DomainEvent>(..)
  
  // Subscription DSL
  subscriptions(subscriptionModel, cloudEventConverter) {
    subscribe<GameStarted>("id1") { gameStarted ->
        log.info("Game was started $gameStarted")
    }
    subscribe<GameWon, GameLost>("id2") { domainEvent ->
        log.info("Game was either won or lost: $domainEvent")
    }
   subscribe("everything") { domainEvent ->
        log.info("I subscribe to every event: $domainEvent")
    }
  } 
  ```
* Implemented ability to delete cloud events by a filter in the in-memory event store.
* Added "listener" support to the in-memory event store. This means that you can supply a "listener" (a consumer) to the `InMemoryEventStore` constructor that
  will be invoked (synchronously) after new events have been written. This is mainly useful to allow in-memory subscription models.
* Added an in-memory subscription model that can be used to subscribe to events from the in-memory event store. Add module `org.occurrent:subscription-inmemory` and then instantiate it using:

  ```java
  InMemorySubscriptionModel inMemorySubscriptionModel = new InMemorySubscriptionModel();
  InMemoryEventStore inMemoryEventStore = new InMemoryEventStore(inMemorySubscriptionModel);
  
  inMemorySubscriptionModel.subscribe("subscription1", System.out::println);
  ```
* Renamed groupId `org.occurrent.inmemory` to `org.occurrent` for consistency. This means that you should depend on module `org.occurrent:eventstore-inmemory` instead of `org.occurrent.inmemory:eventstore-inmemory` when using the in-memory event store.
* Added support for querying the in-memory event store (all fields expect the "data" field works)
* Changed from `Executor` to `ExecutorService` in `NativeMongoSubscriptionModel` in the `org.occurrent:subscription-mongodb-native-blocking` module.
* Added a `@PreDestroy` annotation to the `shutdown` method in the `NativeMongoSubscriptionModel` implementation so that, if you're frameworks such as Spring Boot, you don't need to explicitly call the `shutdown` method when stopping.
* Added partial extension functions for `List<DomainEvent>`, import from the `partial` method from `org.occurrent.application.composition.command`. 

## Changelog 0.4.1 (2020-12-14)

* Upgraded to Kotlin 1.4.21
* Upgraded to cloud events 2.0.0.RC2

## Changelog 0.4.0 (2020-12-04)

* Upgraded to Kotlin 1.4.20
* Upgraded to cloud events 2.0.0.RC1
* Breaking change! The attributes added by the Occurrent cloud event extension has been renamed from "streamId" and "streamVersion" to "streamid" and "streamversion" to comply with the [specification](https://github.com/cloudevents/spec/blob/master/spec.md#attribute-naming-convention).
* Added optimized support for `io.cloudevents.core.data.PojoCloudEventData`. Occurrent can convert `PojoCloudEventData` that contains `Map<String, Object>` and `String` efficiently.
* Breaking change! Removed `org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventData` since it's no longer needed after the CloudEvent SDK has introduced `PojoCloudEventData`. Use `PojoCloudEventData` and pass the document or preferably, map, to it.
* Removed the `org.occurrent:application-service-blocking-kotlin` module, use `org.occurrent:application-service-blocking` instead. The Kotlin extension functions are provided with that module instead.
* Added partial function application support for Kotlin. Depend on module `org.occurrent:command-composition` and import extension functions from `org.occurrent.application.composition.command.partial`. This means that instead of doing:
    
  ```kotlin                                                
  val playerId = ...
  applicationService.execute(gameId) { events -> 
    Uno.play(events, Timestamp.now(), playerId, DigitCard(Three, Blue))
  }
  ```                                           
  
  you can do:

  ```kotlin                                                
  val playerId = ...
  applicationService.execute(gameId, Uno::play.partial(Timestamp.now(), playerId, DigitCard(Three, Blue))) 
  ```
* Added command composition support for Kotlin. Depend on module `org.occurrent:command-composition` and import extension functions from `org.occurrent.application.composition.command.*`. This means that you 
  can compose two functions like this using the `andThen` (infix) function:

    ```kotlin
    val numberOfPlayers = 4
    val timestamp = Timestamp.now()
    applicationService.execute(gameId, 
        Uno::start.partial(gameId, timestamp, numberOfPlayers) 
                andThen Uno::play.partial(timestamp, player1, DigitCard(Three, Blue)))
    ```  

    In the example above, `start` and `play` will be composed together into a single "command" that will be executed atomically.

    If you have more than two commands, it could be easier to use the `composeCommand` function instead of repeating `andThen`:
                                  
    ```kotlin
    val numberOfPlayers = 4
    val timestamp = Timestamp.now()
    applicationService.execute(gameId, 
        composeCommands(
            Uno::start.partial(gameId, timestamp, numberOfPlayers), 
            Uno::play.partial(timestamp, player1, DigitCard(Three, Blue)),
            Uno::play.partial(timestamp, player2, DigitCard(Four, Blue))
        )
    )
    ```
* Added Kotlin extension functions to the blocking event store. They make it easier to write, read and query the event store with Kotlin `Sequence`'s. Import extension functions from package `org.occurrent.eventstore.api.blocking`.
* Added support for deleting events from event store using a `org.occurrent.filter.Filter`. For example:

    ```java
    eventStoreOperations.delete(streamId("myStream").and(streamVersion(lte(19L)));
    ```
    
    This will delete all events in stream "myStream" that has a version less than or equal to 19. This is useful if you implement "closing the books" or certain types of snapshots, and don't need the old events anymore.
    This has been implemented for all MongoDB event stores (both blocking and reactive) but not for the InMemory event store.

## Changelog 0.3.0 (2020-11-21)
* Upgraded Java Mongo driver from 4.0.4 to 4.1.1
* Upgraded to cloud events 2.0.0-milestone4. This introduces a breaking change since the `CloudEvent` SDK no longer returns a `byte[]` as data but rather a `CloudEventData` interface.
  You need to change your code from:
  
  ```java
  byte[] data = cloudEvent.getData();
  ```           
  
  to 
  
  ```java
  byte[] data = cloudEvent.getData().toBytes();
  ```
* Fixed so that not only JSON data can be used as cloud event data. Now the content-type of the event is taken into consideration, and you can store any kind of data.
* Introduced `org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventData`, cloud event data will be represented in this format with loading events from an event store.
  This means that you could check if the `CloudEventData` returned by `cloudEvent.getData()` is instance of `DocumentCloudEventData` and if so extract the 
  underlying `org.bson.Document` that represent the data in the database.      
* Occurrent no longer needs to perform double encoding of the cloud event data if content type is json. Instead of serializing the content manually to a `byte[]` you can
  use either the built-in `JsonCloudEventData` class from the `cloudevents-json-jackson` module, or 
  use the `DocumentCloudEventData` provided by Occurrent to avoid this.
* Upgrading to spring-data-mongodb 3.1.1
* Upgrading to reactor 3.4.0
* The MongoDB event stores no longer needs to depend on the `cloudevents-json-jackson` module since Occurrent now ships with a custom event reader/writer. 
* The MongoDB event subscriptions no longer needs to depend on the `cloudevents-json-jackson` module since Occurrent now ships with a custom event reader/writer. 

## Changelog 0.2.1 (2020-11-03)
* Fixed typo in `CatchupSupportingBlockingSubscriptionConfig`, renamed method `dontSubscriptionPositionStorage` to `dontUseSubscriptionPositionStorage`.
* Added `getSubscriptionPosition()` to `PositionAwareCloudEvent` that returns `Optional<SubscriptionPosition>`.
* Removed duplicate `GenericCloudEventConverter` located in the `org.occurrent.application.service.blocking.implementation` package. Use `org.occurrent.application.converter.implementation.CloudEventConverter` instead.
* Handling if the domain model returns a null `Stream<DomainEvent>` in the `GenericApplicationService`. 

## Changelog 0.2.0 (2020-10-31)
* Renamed method `CloudEventWithSubscriptionPosition.getStreamPosition()` to `CloudEventWithSubscriptionPosition.getSubscriptionPosition()` since this was a typo.
* Added ability to provide a list of conditions when composing them with `and` and `or`.
* Added special convenience (overloaded) method for creating "or" with "equal to" conditions. For example you can now do: `filter(type(or("x", "y"))`. 
  Before you had to do: `filter(type(or(eq("x"), eq("y")))`.
* MongoDB event streams are now explicitly sorted by natural order by default. The reason for this is that just relying on default "sorting" on read lead to wrong order on certain occasions.
* Writing an empty stream to a mongodb-based event store will just ignore the stream and not try to persist the empty stream to the datastore.
* Upgraded to cloudevents sdk 2.0.0-milestone3
* Non-backward compatible change: `CatchupSupportingBlockingSubscription` no longer requires a subscription position storage during the catch-up phase. 
  Instead, you pass the storage implementation to `CatchupSupportingBlockingSubscriptionConfig` along with the position persistence predicate.
* `BlockingSubscriptionWithAutomaticPositionPersistence` now implements the `PositionAwareBlockingSubscription` interface
* Removed the generic type T from the `org.occurrent.subscription.api.blocking.SubscriptionModel` and `org.occurrent.subscription.api.reactor.SubscriptionModel`.
  The reason for this was the implementation returning different kinds of CloudEvent implementations where not compatible. For example if you created a Spring Bean
  with a `T` of `CloudEventWithSubscriptionPosition` then such a subscription couldn't be assigned to a field expecting a subscription with just `CloudEvent`.
  To avoid having users to know which cloud event implementation to expect, we change the API so that it always deals with pure `CloudEvent`'s. 
  Implementors now have to use `org.occurrent.subscription.PositionAwareCloudEvent.getSubscriptionPositionOrThrowIAE(cloudEvent)` to get the position.
  It's also possible to check if a `CloudEvent` contains a subscription position by calling `org.occurrent.subscription.PositionAwareCloudEvent.hasSubscriptionPosition(cloudEvent)`.
* Fixed several corner-cases for the `CatchupSupportingBlockingSubscription`, it should now be safer to use and produce fewer duplicates when switching from catch-up to continuous subscription mode.
* Added "exists" method to the `BlockingSubscriptionPositionStorage` interface (and implemented for all implementations of this interface).
* The global position of `PositionAwareBlockingSubscription` for MongoDB increases the "increment" of the current `BsonTimestamp` by 1 in order to avoid 
  duplicate potential duplication of events during replay.
* Added a generic application service implementation (and interfaces). You don't have to use it, it's ok to simply cut and paste and make custom changes. You 
  can also write your own class. The implementation, `org.occurrent.application.service.blocking.implementation.GenericApplicationService`, quite 
  simplistic but should cover most of the basic use cases. The application service uses a `org.occurrent.application.converter.CloudEventConverter` to
  convert to and from cloud events and your custom domain events. This is why both `CloudEventConverter` and `ApplicationService` takes a generic type parameter, `T`, 
  which is the type of your custom domain event. Note that the application service is not yet implemented for the reactive event store.
  The application service also contains a way to execute side-effects after the events are written to the event store. This is useful for executing 
  synchronous policies after the events are written to the event store. If policies write the the same database as your event store,  you start a transaction
  and write both policies and events in the same transaction!         
  There are also Kotlin extension functions for the application service and policies in the `org.occurrent:application-service-blocking` module.
* Added utilities, `org.occurrent:command-composition` for to easier do command composition when calling an application service.
  This module also contains utilities for doing partial application of functions which can be useful when composing functions.    

## Changelog 0.1.1 (2020-09-26):

* Catchup subscriptions (blocking)
* EveryN for stream persistence (both blocking and reactive)
* Added "count" to EventStoreQueries (both blocking and reactive)
* Added ability to query for "data" attribute in EventStoreQueries and subscriptions