### Changelog next version
* Major improvements to `CatchupSubscriptionModel`, it now handles and includes events that have been written while the catch-up subscription phase runs. Also, the "idempotency cache" is only used while switching from catch-up to continuous mode, and not during the entire catch-up phase.
* Major changes to the `spring-boot-starter-mongodb` module. It now includes a `CatchupSubscriptionModel` which allows you to start subscriptions from an historic date more easily.
* `StartAt.Dynamic(..)` now takes a `SubscriptionModelContext` as a parameter. This means that subscription models can add a "context" that can be useful for dynamic behavior. For example, you can prevent a certain subscription model to start (and instead delegate to its parent) if you return `null` as `StartAt` from a dynamic position.
* Added annotation support for subscriptions when using the `spring-boot-starter-mongodb` module. You can now do: 
  ```java
  @Subscription(id = "mySubscription")
  void mySubscription(MyDomainEvent event) {
      System.out.println("Received event: " + event);
  }  
  ```
  It also allows you to easily start the subscription from a moment in the past (such as beginning of time). See javadoc in `org.occurrent.annotation.Subscription` for more info.
* Added `org.occurrent.subscription.blocking.durable.catchup.StartAtTime` as a help to the `CatchupSubscriptionModel` to easier specify an `OffsetDateTime` or "beginning of time" when starting a subscription catchup subscription model. Before you had to do:
  ```java
  subscriptionModel.subscribe("myId", StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), System.out::println);
  ```
  but now you can do:
  ```java
  subscriptionModel.subscribe("myId", StartAtTime.beginningOfTime(), System.out::println);
  ```                                                                                     
  which is shorter. You're using Kotlin you can import `org.occurrent.subscription.blocking.durable.catchup.beginningOfTime` and do:
  ```kotlin
  subscriptionModel.subscribe("myId", StartAt.beginningOfTime(), ::println)
  ```  
* Changed the default behavior of `CatchupSubscriptionModel`. Before it replayed all historic events by default if no specific start at position was supplied, but now it delegates to the wrapped subscription and no historic events will be replayed. Instead, you need to explicitly specify `beggingOfTime` or an `OffsetDateTime` as the start position. For example:
  ```java
  subscriptionModel.subscribe("myId", StartAtTime.beginningOfTime(), System.out::println);
  ```
### 0.17.2 (2024-02-27)
* Fixed issue in CompetingConsumerSubscriptionModel in which it failed to reacquire consumption rights in some cases where MongoDB connection was lost.   

### 0.17.1 (2024-02-23)
* Fixed issue in [Subscription DSL](https://occurrent.org/documentation#subscription-dsl) when using "subscribe" functions with a single event type different from the "base event type", i.e. this didn't work in previous version in Java:
  ```java                
  // GameEvent is the "base event type"
  Subscriptions<GameEvent> subscriptions = new Subscriptions<>(..);
  
  // GameStarted has GameEvent as parent, the following didn't compile in version 0.17.0 
  subscriptions.subscribe("mysubscription", GameStarted.class, gameStarted -> System.out.println("gameStarted: " + gameStarted));
  ```
* Using slf4j-api and not logback-classic in several modules that accidentally brought logback in as a compile time dependency.                                                                                 
* Upgraded slf4j-api from 2.0.5 to 2.0.12
* In the `spring-boot-starter-mongodb` module, it's now possible to enable/disable the event store or subscriptions from the `application.yaml` file. For example, you can disable the event store like this:

  ```yaml
  occurrent:
    event-store:
      enabled: false # Disable the creation of an event store Spring bean
  ```
  
  and the subscriptions like this:

  ```yaml
  occurrent:
    subscription:
      enabled: false # Disable the creation of beans related to subscriptions
  ```                                                                        
  
  This is useful if you have an application where you only need the event store or only need the subscriptions.
* Added `queryForList` Kotlin extension function to `EventStoreQueries` and `DomainEventQueries`. It works in a similar way as `queryForSequence`, but returns a `List` instead of a Kotlin `Sequence`.
* Fixed an issue with `CatchupSubscriptionModel` in which it threw an IllegalArgumentException when storing the position of stored events when using Atlas free tier. 

### 0.17.0 (2024-01-19)
* spring-boot-starter-mongodb no longer autoconfigures itself by just importing the library in the classpath, instead you need to bootstrap by annotating your Spring Boot class with @EnableOccurrent.   
* Fixed bug in spring-boot-starter-mongodb module in which it didn't automatically configure MongoDB.
* Domain event subscriptions now accepts metadata as the first parameter, besides just the event. The metadata currently contains the stream version and stream id, which can be useful when building projections.
* Fixed a bug in SpringMongoSubscriptionModel in which it didn't restart correctly on non DataAccessException's 
* Introducing Decider support (experimental)
* Fixed a rare ConcurrentModificationException issue in SpringMongoSubscriptionModel if the subscription model is shutdown while it's restarting 
* Upgraded from Kotlin 1.9.20 to 1.9.22
* Upgraded amqp-client from 5.16.0 to 5.20.0
* Upgraded Spring Boot from 3.1.4 to 3.2.1
* Upgraded reactor from 3.5.10 to 3.6.0 
* Upgraded Spring data MongoDB from 4.1.4 to 4.2.0 
* Upgraded jobrunr from 6.3.2 to 6.3.3
* Upgraded mongodb drivers from 4.10.2 to 4.11.1
* Upgraded lettuce core from 6.2.6.RELEASE to 6.3.1.RELEASE
* Upgraded spring-aspects from 6.0.10 to 6.1.1
* Upgraded jackson from 2.15.2 to 2.15.3

### 0.16.11 (2023-12-01)
* Removed `isFinalError` method from `ErrorInfo` used by `RetryStrategy`, use `isLastAttempt()` instead.
* Added `RetryInfo` as argument to the `exec` extension function in `RetryStrategy`.
* Added `retryAttemptException` as an extension property to `org.occurrent.retry.AfterRetryInfo` so that you don't need to use the `getFailedRetryAttemptException` method that returns an `Optional` in the Java interface. Instead, the `retryAttemptException` function returns a `Throwable?`.  Import the extension property from the `org.occurrent.retry.AfterRetryInfoExtensions` file. 
* Added `nextBackoff` as an extension property to `org.occurrent.retry.ErrorInfo` so that you don't need to use the `getBackoffBeforeNextRetryAttempt` method that returns an `Optional` in the Java interface. Instead, the `nextBackoff` function returns a `Duration?`.  Import the extension property from the `org.occurrent.retry.ErrorInfoExtensions` file.
* In the previous version, in the retry strategy module, `onBeforeRetry`, `onAfterRetry`, `onError` etc, accepted a `BiConsumer<Throwable, RetryInfo>`. The arguments have now been reversed, so the types of the BiConsumer is now `BiConsumer<RetryInfo, Throwable>`.
* Added `onRetryableError` method to `RetryStrategy` which you can use to listen to errors that are retryable (i.e. matching the retry predicate). This is a convenience method for `onError` when `isRetryable` is true.
* Added Kotlin extensions to `JacksonCloudEventConverter`. You can import the function `org.occurrent.application.converter.jackson.jacksonCloudEventConverter` and use like this:
  
  ```kotlin
   val objectMapper = ObjectMapper()
   val cloudEventConverter: JacksonCloudEventConverter<MyEvent> =
      jacksonCloudEventConverter(
          objectMapper = objectMapper,
          cloudEventSource = URI.create("urn:myevents"),
          typeMapper = MyCloudEventTypeMapper()
      )
  ```
* Fixed problem with spring-boot autoconfiguration in which it previously failed to create a default cloud event converter if no type mapper was specified explicitly.
* Upgraded to Kotlin 1.9.20
* Added a "deleteAll" method to InMemoryEventStore which is useful for testing
* The `org.occurrent.eventstore.api.WriteConditon` has been converted to a java record.
* Removed the deprecated method "getStreamVersion" in `org.occurrent.eventstore.api.WriteConditon`, use `newStreamVersion()` instead. 

### 0.16.10 (2023-10-21)
* Several changes to `RetryStrategy` again:
  1. `onError` is will be called for each throwable again. The new `ErrorInfo` instance, that is supplied to the error listener, can be used to determine whether the error is "final" or if it's retryable.
  2. In the previous version, `onBeforeRetry` and `onAfterRetry`, accepted a `BiConsumer<RetryInfo, Throwable>`. The arguments have now been reversed, so the types of the BiConsumer is now `BiConsumer<Throwable, RetryInfo>`.  

### 0.16.9 (2023-10-20)
* Added `onAfterRetry` to `RetryStrategy`

### 0.16.8 (2023-10-20)
* Upgraded jakarta-api from 1.3.5 to 2.11 (which means that all javax annotations have been replaced by jakarta)
* Fixed a bug in CatchupSubscriptionModel that prevented it from working in MongoDB clusters that doesn't have access to the `hostInfo` command such as Atlas free-tier.
* Several changes to the `RetryStrategy`:
  1. Renamed `getNumberOfAttempts` to `getNumberOfPreviousAttempts`
  2. Added `getAttemptNumber` which is the number of the _current_ attempt
  3. `onError` is now _only_ called if the _end result_ is an error. I.e. it will only be called at most once, and not for intermediate errors. Because of this, the variant of `onError` that took a `BiConsumer<RetryInfo, Throwable>` has been removed (because there's no need for `RetryInfo` when the operation has failed). 
  4. Added the `onBeforeRetry` method, which is called before a _retry attempt_ is made. This function takes a `BiConsumer<RetryInfo, Throwable>` in which the `RetryInfo` instance contains details about the current retry attempt.    

### 0.16.7 (2023-09-29)
* Added equals/hashcode and toString to RetryInfo
* Small changes to how retries are performed in the competing consumer strategies for MongoDB
* Improved debug logging in competing consumer implementations
* Upgraded Spring Boot from 3.0.8 to 3.1.4
* Upgraded kotlin from 1.9.0 to 1.9.10
* Upgraded jobrunr from 6.3.0 to 6.3.2
* Upgraded spring data mongodb from 4.0.8 to 4.1.4
* Upgraded jackson from version 2.14.3 to 2.15.2
* Upgraded project reactor from 3.5.8 to 3.5.10
* Upgraded spring-retry from 2.0.0 to 2.0.3
* Upgraded lettuce-core from 6.2.2.RELEASE to 6.2.6.RELEASE

### 0.16.6 (2023-08-15)
* The SpringMongoSubscriptionModel is now restarted for all instances of `org.springframework.dao.DataAccessException` instead of just instances of `org.springframework.data.mongodb.UncategorizedMongoDbException`.
* Upgraded cloudevents from 2.4.2 to 2.5.0
* Upgraded Spring Boot from 3.0.7 to 3.0.8
* Upgraded project reactor from 3.5.6 to 3.5.8
* Upgraded spring data mongodb from 4.0.6 to 4.0.8
* Upgraded mongo driver from 4.8.1 to 4.10.2
* Upgraded jobrunr from 6.1.4 to 6.3.0

### 0.16.5 (2023-07-7)
* Improved debug logging in `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel`

### 0.16.4 (2023-07-7)
*  A kotlin extension function that makes it easier to execute a `RetryStrategy` with a "Supplier".
    
    The reasons for this is that when just doing this from kotlin:
    
    ```
    val string = retryStrategy.execute { "hello" }
    ```
    
    This will return `Unit` and not the "hello" string that you would expect.
    This is because execute in the example above delegates to org.occurrent.retry.RetryStrategy.execute(java.lang.Runnable)
    and not org.occurrent.retry.RetryStrategy.execute(java.util.function.Supplier<T>) which one would expect.
    Thus, you can use this function instead to avoid specifying the `Supplier` SAM explicitly.
    
    I.e. instead of doing:
    
    ```kotlin
      val string : String = retryStrategy.execute(Supplier { "hello" })
    ```
    
    you can do:
    
    ```kotlin
      val string : String = retryStrategy.exec { "hello" }
    ```
    
    after having imported `org.occurrent.retry.exec`.
* Kotlin jvm target is set to 17
* Added ability to map errors with `RetryStrategy`, either by doing:

  ```
  retryStrategy
              .mapError(IllegalArgumentException.class, IllegalStateException::new)
              .maxAttempts(2)
              .execute(() -> {
                  throw new IllegalArgumentException("expected");
              }));
  ```
  
  In the end, an `IllegalStateException` will be thrown. You can also do like this:

  ```
  retryStrategy
              .mapError(t -> {
                  if (t instanceof IllegalArgumentException iae) {
                      return new IllegalStateException(iae.getMessage());
                  } else {
                      return t;
                  }
              })
              .maxAttempts(2)
              .execute(() -> {
                  throw new IllegalArgumentException("expected");
              }));
  ```
* Added a new `execute` Kotlin extension function to the `ApplicationService` that allows one to use a `java.util.UUID` as a streamId when working with lists of events (as opposed to `Sequence`).
* Upgraded xstream from 1.4.19 to 1.4.20
* Added better logging to `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel`, including some debug logs that can be used for detailed information about what's going on. 
* Upgraded Kotlin from 1.8.21 to 1.9.0
* Upgraded Spring Boot from 3.0.6 to 3.0.7
* Upgraded Spring Aspects from 6.0.9 to 6.0.10

### 0.16.3 (2023-05-12)
* Added support to the retry module to execute retries with a function that takes an instance of `org.occurrent.retry.RetryInfo`. This is useful if you need to know the current state of your of the retry while retrying. For example:
  ```java  
  RetryStrategy retryStrategy = RetryStrategy
                                  .exponentialBackoff(initialDelay, maxDelay, 2.0)
                                  .maxAttempts(10)
  retryStrategy.execute(info -> {
        if(info.getNumberOfAttempts() > 2 &&  info.getNumberOfAttempts() < 6) {
            System.out.println("Number of attempts is between 3 and 5");
        }
        ...     
  });
  ```
* Fixed bug in the retry module, in which error listeners where not called for the last error.
* Upgraded jobrunr from 5.3.0 to 6.1.4
* Upgraded Kotlin from 1.8.0 to 1.8.21
* Upgraded Jackson from 2.14.1 to 2.14.3
* Upgraded project reactor from 3.5.0 to 3.5.6
* Upgraded to Spring Boot from 3.0.3 to 3.0.6
* Upgraded to Spring from 6.0.6 to 6.0.9
* Upgraded to Spring Data MongoDB from 4.0.0 to 4.0.6
* Upgraded cloudevents from 2.4.1 to 2.4.2

### 0.16.2 (2023-03-03)
* Upgraded Kotlin from 1.7.20 to 1.8.0
* Upgraded cloudevents to 2.4.1
* Improvements to `SpringMongoSubscriptionModel` in which it'll restart the subscription from the default subscription position instead of now on unknown or query-related MongoDB errors. This eliminates the risk of loosing messages if using a durable subscription model.
* Fixed a subtle bug in `SpringMongoLeaseCompetingConsumerStrategy` in which it _could_ crash in some cases where MongoDB was down for more than 30 seconds.
* Upgraded to Spring Boot 3.0.3
* Upgraded spring-aspects from 6.0.2 to 6.0.6

### 0.16.1 (2023-02-11)
* Fix error in the sequence command composition that leaves old events in the sequence (issue #131) (thanks to chrisdginn for pull request)

### 0.16.0 (2022-12-09)
* Occurrent now require Java 17 instead of Java 8. This is major change to support the latest Spring client libraries for various databases such MongoDB and Redis. 
  This was also done to better support Spring Boot 3 and jakartaee.
* Lots of changes under the hood, refactorings to make use of records, sealed classes and built-in functional constructs available in Java 17.
* Refactored SubscriptionPositionStorageConfig to sealed interface
* Refactored CompetingConsumerSubscriptionModel
* Refactored StartAt to a sealed interface
* Refactored ClassName to a sealed interface
* Refactored RetryStrategy to a sealed interface
* Converted Deadline to a sealed interface
* Converted CompetingConsumer in CompetingConsumerSubscriptionModel to a record
* Converting Backoff to sealed interface
* Converting Condition and WriteCondition to sealed interfaces
* Converting SortBy to a sealed interface
* Refactor MaxAttempts to sealed interface and implementations to records

### 0.15.1 (2022-12-02)
* The spring-boot-starter module now supports Spring Boot 3 (thanks to Kirill Gavrilov for pull request)

### 0.15.0 (2022-11-24)
Introducing deadline scheduling. Scheduling (aka deadlines, alarm clock) is a very handy technique to schedule to commands to be executed in the future or periodically.  
Imagine, for example, a multiplayer game, where we want to game to end automatically after 10 hours of inactivity.  
This means that as soon as a player has made a guess, we’d like to schedule a “timeout game command” to be executed after 10 hours.

The way it works in Occurrent is that you schedule a `org.occurrent.deadline.api.blocking.Deadline` using a `org.occurrent.deadline.api.blocking.DeadlineScheduler` implementation.
The `Deadline` is a date/time in the future when the deadline is up. You also register a `org.occurrent.deadline.api.blocking.DeadlineConsumer` to a 
`org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry` implementation, and it'll be invoked when a deadline is up. For example: 


```java
// In some method we schedule a deadline two hours from now with data "hello world" 
var deadlineId = UUID.randomUUID(); 
var deadlineCategory = "hello-world"; 
var deadline = Deadline.afterHours(2)
deadlineScheduler.schedule(deadlineId, deadlineCategory, deadline, "hello world");

// In some other method, during application startup, we register a deadline consumer to the registry for the "hello-world" deadline category
deadlineConsumerRegistry.register("hello-world", (deadlineId, deadlineCategory, deadline, data) -> System.out.println(data));
```

In the example above, the deadline consumer will print "hello world" after 2 hours.

There are two implementations of `DeadlineScheduler` and `DeadlineConsumerRegistry`, one that uses [JobRunr](https://www.jobrunr.io/) and one in-memory implementation.
Depend on `org.occurrent:deadline-jobrunr:0.15.0` to get the JobRunr implementation, and `org.occurrent:deadline-inmemory:0.15.0` to get the in-memory implementation. 

### 0.14.9 (2022-11-10)
* Upgraded Kotlin from 1.7.10 to 1.7.20
* Upgraded cloudevents from 2.3.0 to 2.4.0
* Upgraded Spring Boot from 2.7.3 to 2.7.5
* Changed toString() on StreamVersionWriteCondition when condition is null from "any stream version" to "any"
* Fixed a bug in SpringMongoEventStore when several writes happened in parallel to the same stream and write condition was "any". 
  This could result in a WriteConditionNotFulfilledException since the underlying MongoDB transaction failed. Now, after the fix, the events are stored as indented.

### 0.14.8 (2022-10-10)
* Fixed NPE issue in the toString() method in `org.occurrent.eventstore.api.StreamVersionWriteCondition` when stream condition was `any`.

### 0.14.7 (2022-09-22)
* Fixed issue in `SpringMongoSubscriptionModel` that prevented restart of subscriptions when MongoDB goes into leader election mode.
* Upgraded spring-boot to 2.7.3
* Upgraded Spring Data MongoDB from 3.3.4 to 3.3.7

### 0.14.6 (2022-08-17)
* InMemoryEventStore now checks for duplicate events. You can no longer write two events with the same cloud event id and source to the same stream.
* Fixed an issue with command composition in Kotlin in which, in version 0.14.5, returned _all_ events in a stream and not only _new_ events. 

### 0.14.5 (2022-07-29)
* Updated Kotlin extension functions for partial function application (`org.occurrent.application.composition.command.PartialExtensions`)
  to work on any type of function instead of just those that has `List` or `Sequence`.
* Fixed an issue in JacksonCloudEventConvert in which it didn't use the CloudEventTypeMapper correctly when calling `toCloudEvent` ([issue 119](https://github.com/johanhaleby/occurrent/issues/119)). 

### 0.14.4 (2022-07-15)
  
* Removed `PartialListCommandApplication`, `PartialStreamCommandApplication` and `PartialApplicationFunctions` in package 
  `org.occurrent.application.composition.command.partial` of module `command-composition`. They have all been replaced by
  `org.occurrent.application.composition.command.partial.PartialFunctionApplication` which is a generic form a partial function
  application that works on all kinds of functions, not only those taking `Stream` and/or `List`. A simple search and replace
  should be enough to migrate.
* Upgraded Jackson from 2.13.2 to 2.13.3
* Upgraded project reactor to 3.4.16 to 3.4.21
* Upgraded Spring Boot from 2.6.7 to 2.7.1
* Upgraded Java MongoDB driver from 4.5.1 to 4.6.1
* Upgraded Kotlin from 1.6.21 to 1.7.10

### 0.14.3 (2022-04-27)

* Upgraded to Kotlin from 1.6.0 to 1.6.21
* Upgraded project reactor to 3.4.12 to 3.4.16
* Upgraded Spring Data MongoDB from 3.3.0 to 3.3.0
* Upgraded Spring Boot from 2.5.6 to 2.6.7
* Upgraded Java MongoDB driver from 4.4.0 to 4.5.1
* Upgraded Java cloudevents SDK from 2.2.0 to 2.3.0 
* Upgraded Jackson from 2.13.0 to 2.13.2 
* Upgraded Jackson Databind from 2.13.0 to 2.13.2.1

### 0.14.2 (2021-12-10)

* Improved `SpringMongoEventStore`, `MongoEventStore` and `ReactorMongoEventStore` so that they never does in-memory filtering of events that we're not interested in.
* Added `oldStreamVersion` to `WriteResult` (that is returned when calling `write(..)` on an event store). The `getStreamVersion()` method has been deprecated in favor of `getNewStreamVersion()`.
* Upgraded to Kotlin 1.6.0
* Upgraded Java MongoDB driver to 4.4.0
* Upgraded Spring Data MongoDB to 3.3.0
* Upgraded Jackson to 2.13.0
* Upgraded amqp-client to 5.14.0

### 0.14.1 (2021-11-12)

* Using `insert` from `MongoTemplate` when writing events in the `SpringMongoEventStore`. Previously, the vanilla `mongoClient` was (accidentally) used for this operation.
* When using the spring boot starter project for MongoDB (`org.occurrent:spring-boot-starter-mongodb`), the transaction manager used by default is now configured to use "majority" read- and write concerns.
  To revert to the "default" settings used by Spring, or change it to your own needs, specify a `MongoTransactionManager` bean. For example:

  ```java                                                                       
  @Bean
  public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
    return new MongoTransactionManager(dbFactory, TransactionOptions.builder(). .. .build());
  }
  ```
* Separating read- and query options configuration so that you can e.g. configure queries made by `EventStoreQueries` and reads from the `EventStore.read(..)` separately.  
  This useful if you want to e.g. allow queries from `EventStoreQueries` to be made to secondary nodes but still force reads from `EventStore.read(..)` to be made from the primary.
  You can configure this by supplying a `readOption` (to configure the reads from the `EventStore`) and `queryOption` (for `EventStoreQueries`) in the `EventStoreConfig`. 
  This has been implemented for `SpringMongoEventStore` and `ReactorMongoEventStore`.

### 0.14.0 (2021-11-06)

* Non-backward compitable change: CloudEventConverter's now has a third method that you must implement:
  ```java
  /**
   * Get the cloud event type from a Java class.
   *
   * @param type The java class that represents a specific domain event type
   * @return The cloud event type of the domain event (cannot be {@code null})
   */
  @NotNull String getCloudEventType(@NotNull Class<? extends T> type);
  ```
  The reason for this is that several components, such as the [subscription dsl](https://occurrent.org/documentation#subscription-dsl), needs to get the cloud event type from the domain event class. And since this is highly related to "cloud event conversion", 
  this method has been added there to avoid complicating the API. 
* Introduced the concept of CloudEventTypeMapper's. A cloud event type mapper is component whose purpose it is to get the [cloud event type](https://occurrent.org/documentation#cloudevents) from a domain event type and vice versa.
  Cloud Event Type mappers are used by certain `CloudEventConverter`'s to define how they should derive the cloud event type from the domain event as well as a way to reconstruct the domain event type from the cloud event type.
  and the new domain queries DSL. You should use the same type mapper instance for all these components. To write a custom type mapper, depend on the `org.occurent:cloudevent-type-mapper-api` module and implement the `org.occurrent.application.converter.typemapper.CloudEventTypeMapper`
  (functional) interface.
* Introduced a blocking Query DSL. It's a small wrapper around the [EventStoreQueries](https://occurrent.org/documentation#eventstore-queries) API that lets you work with domain events instead of CloudEvents. 
  Depend on the `org.occurrent:query-dsl-blocking` module and create an instance of `org.occurrent.dsl.query.blocking.DomainEventQueries`. For example:

  ```java                                                      
  EventStoreQueries eventStoreQueries = .. 
  CloudEventConverter<DomainEvent> cloudEventConverter = ..
  DomainEventQueries<DomainEvent> domainEventQueries = new DomainEventQueries<DomainEvent>(eventStoreQueries, cloudEventConverter);
   
  Stream<DomainEvent> events = domainQueries.query(Filter.subject("someSubject"));
  ```
  
  There's also support for skip, limits and sorting and convenience methods for querying for a single event:

  ```java                                                      
  Stream<DomainEvent> events = domainQueries.query(GameStarted.class, GameEnded.class); // Find only events of this type
  GameStarted event1 = domainQueries.queryOne(GameStarted.class); // Find the first event of this type
  GamePlayed event2 = domainQueries.queryOne(Filter.id("d7542cef-ac20-4e74-9128-fdec94540fda")); // Find event with this id
  ```
  
  There are also some Kotlin extensions that you can use to query for a `Sequence` of events instead of a `Stream`:

  ```kotlin
  val events : Sequence<DomainEvent> = domainQueries.queryForSequence(GamePlayed::class, GameWon::class, skip = 2) // Find only events of this type and skip the first two events
  val event1 = domainQueries.queryOne<GameStarted>() // Find the first event of this type
  val event2 = domainQueries.queryOne<GamePlayed>(Filter.id("d7542cef-ac20-4e74-9128-fdec94540fda")) // Find event with this id
  ```
* Introducing spring boot starter project to easily bootstrap Occurrent if using Spring. Depend on `org.occurrent:spring-boot-starter-mongodb` and create a Spring Boot application annotated with `@SpringBootApplication` as you would normally do.
  Occurrent will then configure the following components automatically:
    * Spring MongoDB Event Store instance (`EventStore`)
    * A Spring `SubscriptionPositionStorage` instance 
    * A durable Spring MongoDB competing consumer subscription model (`SubscriptionModel`)
    * A Jackson-based `CloudEventConverter`
    * A `GenericApplication` instance (`ApplicationService`)
    * A subscription dsl instance (`Subscriptions`)
    * A reflection based type mapper that uses the fully-qualified class name as cloud event type (you _should_ absolutely override this bean for production use cases) (`CloudEventTypeMapper`)
      For example, by doing:
      ```java
      @Bean
      public CloudEventTypeMapper<GameEvent> cloudEventTypeMapper() {
        return ReflectionCloudEventTypeMapper.simple(GameEvent.class);
      }
      ```
      This will use the "simple name" (via reflection) of a domain event as the cloud event type. But since the package name is now lost, the `ReflectionCloudEventTypeMapper` will append the package name of `GameEvent` to when converting back into a domain event. 
      This _only_ works if all your domain events are located in the exact same package as `GameEvent`. If this is not that case you need to implement a more advanced `CloudEventTypeMapper` such as:

      ```kotlin
      class CustomTypeMapper : CloudEventTypeMapper<GameEvent> {
          override fun getCloudEventType(type: Class<out GameEvent>): String = type.simpleName
      
          override fun <E : GameEvent> getDomainEventType(cloudEventType: String): Class<E> = when (cloudEventType) {
              GameStarted::class.simpleName -> GameStarted::class
              GamePlayed::class.simpleName -> GamePlayed::class
              // Add all other events here!!
              ...
              else -> throw IllegalStateException("Event type $cloudEventType is unknown")
          }.java as Class<E>
      }
      ```
  See `org.occurrent.springboot.OccurrentMongoAutoConfiguration` if you want to know exactly what gets configured.
* Upgraded spring-boot from 2.5.4 to 2.5.6.

## 0.13.1 (2021-10-03)

* No longer using transactional reads in `ReactorMongoEventStore`, this also means that the `transactionalReads` configuration property could be removed since it's no longer used. 

## 0.13.0 (2021-10-03)

* Reading event streams from `MongoEventStore` and `SpringMongoEventStore` are now much faster and more reliable. Before there was a bug in both implementation in which
  the stream could be abruptly closed when reading a large number of events. This has now been fixed, and as a consequence, Occurrent doesn't need to start a MongoDB transaction
  when reading an event stream, which improves performance.
* Removed the `transactionalReads` property (introduced in previous release) from `EventStoreConfig` for both `MongoEventStore` and `SpringMongoEventStore` since it's no longer needed.
* Upgraded jackson from version 2.11.1 to 2.12.5

## 0.12.0 (2021-09-24)

* Added ability to map event type to event name in subscriptions DSL from Kotlin
* Upgraded Kotlin to 1.5.31
* Upgraded spring-boot used in examples to 2.5.4
* Upgraded spring-mongodb to 3.2.5
* Upgraded the mongodb java driver to 4.3.2
* Upgraded project reactor to 3.4.10
* Upgrading to cloudevents sdk 2.2.0
* Minor tweak in ApplicationService extension function for Kotlin so that it no longer converts the Java stream into a temporary Kotlin sequence before converting it to a List
* Allow configuring (using the `EventStoreConfig` builder) whether transactional reads should be enabled or disabled for all MongoDB event stores.
  This is an advanced feature, and you almost always want to have it enabled. There are two reasons for disabling it:
  1. There's a bug/limitation on Atlas free tier clusters which yields an exception when reading large number of events in a stream in a transaction.
     To workaround this you could disable transactional reads. The exception takes this form:
     ```
     java.lang.IllegalStateException: state should be: open
     at com.mongodb.assertions.Assertions.isTrue(Assertions.java:79)
     at com.mongodb.internal.session.BaseClientSessionImpl.getServerSession(BaseClientSessionImpl.java:101)
     at com.mongodb.internal.session.ClientSessionContext.getSessionId(ClientSessionContext.java:44)
     at com.mongodb.internal.connection.ClusterClockAdvancingSessionContext.getSessionId(ClusterClockAdvancingSessionContext.java:46)
     at com.mongodb.internal.connection.CommandMessage.getExtraElements(CommandMessage.java:265)
     at com.mongodb.internal.connection.CommandMessage.encodeMessageBodyWithMetadata(CommandMessage.java:155)
     at com.mongodb.internal.connection.RequestMessage.encode(RequestMessage.java:138)
     at com.mongodb.internal.connection.CommandMessage.encode(CommandMessage.java:59)
     at com.mongodb.internal.connection.InternalStreamConnection.sendAndReceive(InternalStreamConnection.java:268)
     at com.mongodb.internal.connection.UsageTrackingInternalConnection.sendAndReceive(UsageTrackingInternalConnection.java:100)
     at com.mongodb.internal.connection.DefaultConnectionPool$PooledConnection.sendAndReceive(DefaultConnectionPool.java:490)
     at com.mongodb.internal.connection.CommandProtocolImpl.execute(CommandProtocolImpl.java:71)
     at com.mongodb.internal.connection.DefaultServer$DefaultServerProtocolExecutor.execute(DefaultServer.java:253)
     at com.mongodb.internal.connection.DefaultServerConnection.executeProtocol(DefaultServerConnection.java:202)
     at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:118)
     at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:110)
     at com.mongodb.internal.operation.QueryBatchCursor.getMore(QueryBatchCursor.java:268)
     at com.mongodb.internal.operation.QueryBatchCursor.hasNext(QueryBatchCursor.java:141)
     at com.mongodb.client.internal.MongoBatchCursorAdapter.hasNext(MongoBatchCursorAdapter.java:54)
     at java.base/java.util.Iterator.forEachRemaining(Iterator.java:132)
     at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801)
     at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
     at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
     at java.base/java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:913)
     at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
     ```
     It's possible that this would work if you enable "no cursor timeout" on the query, but this is not allowed on Atlas free tier.
  2. You're set back by the performance penalty of transactions and are willing to sacrifice read consistency
  
  If you disable transactional reads, you _may_ end up with a mismatch between the version number in the `EventStream` and
  the last event returned from the event stream. This is because Occurrent does two reads to MongoDB when reading an event stream. First it finds the current version number of the stream (A),
  and secondly it queries for all events (B). If you disable transactional reads, then another thread might have written more events before the call to B has been made. Thus, the version number
  received from query A might be stale. This may or may not be a problem for your domain, but it's generally recommended having transactional reads enabled. Configuration example:
  ```java
  EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().transactionalReads(false). .. .build();
  eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
  ```
* Added ability to tweak query options for reads in the event store, for example cursor timeouts, allow reads from slave etc. You can configure this in the `EventStoreConfig` for each event store
  by using the `queryOption` higher-order function. For example:
  ```java
  EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.DATE)
                  .queryOptions(query -> query.noCursorTimeout().allowSecondaryReads()).build();
  var eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
  ```
  Note that you must <i>not</i> use this to change the query itself, i.e. don't use the `Query#with(Sort)` etc. Only use options such as `Query#cursorBatchSize(int)` that doesn't change the actual query or sort order.
  This is an advanced feature and should be used sparingly.
* Added ability to convert a `Stream` of cloud events to domain events and vice versa in the `CloudEventConverter` by overriding the new `toCloudEvents` and/or `toDomainEvents` methods. 
  The reason for overriding any of these methods is to allow adding things such as correlation id that should be the same for all events in a stream.
* Non-backward compatible change: The cloud event converter module name has changed from `org.occurrent:cloudevent-converter` to `org.occurrent:cloudevent-converter-api` 
* Non-backward compatible change: The generic cloud event converter (`org.occurrent.application.converter.generic.GenericCloudEventConverter`) has been moved to its own module, depend on `org.occurrent:cloudevent-converter-generic` to use it.
* Introduced a cloud event converter that uses XStream to (de-)serialize the domain event to cloud event data. Depend on `org.occurrent:cloudevent-converter-xstream` and then use it like this:
    ```java
    XStream xStream = new XStream();
    xStream.allowTypeHierarchy(MyDomainEvent.class);
    XStreamCloudEventConverter<MyDomainEvent> cloudEventConverter = new XStreamCloudEventConverter<>(xStream, URI.create("urn:occurrent:domain"));
    ```                                                                                                                                           
   You can also configure how different attributes of the domain event should be represented in the cloud event by using the builder, `new XStreamCloudEventConverter.Builder<MyDomainEvent>().. build()`. 
* Introduced a cloud event converter that uses Jackson to (de-)serialize the domain event to cloud event data. Depend on `org.occurrent:cloudevent-converter-jackson` and then use it like this:
    ```java
    ObjectMapper objectMapper = new ObjectMapper();
    JacksonCloudEventConverter<MyDomainEvent> cloudEventConverter = new JacksonCloudEventConverter<>(objectMapper, URI.create("urn:occurrent:domain"));
    ```                                                                                                                                           
   You can also configure how different attributes of the domain event should be represented in the cloud event by using the builder, `new JacksonCloudEventConverter.Builder<MyDomainEvent>().. build()`. 

## 0.11.0 (2021-08-13)

* Improved error message and version for write condition not fulfilled that may happen when parallel writers write to the same stream at the same time.
* Upgraded to cloud events java sdk to version 2.1.1
* Upgraded to Kotlin 1.5.21
* Added a `mapRetryPredicate` function to `Retry` that easily allows you to map the current retry predicate into a new one. This is useful if you e.g. want to add an additional predicate to the existing predicate. For example:

    ```java
    // Let's say you have a retry strategy:
    Retry retry = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(WriteConditionNotFulfilledException.class::isInstance);
    // Now you also want to retry if an IllegalArgumentException is thrown:
    retry.mapRetryPredicate(currentRetryPredicate -> currentRetryPredicate.or(IllegalArgument.class::isInstance))
    ```                                                                                                          
* The GenericApplicationService now has a RetryStrategy enabled by default. The default retry strategy uses exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
  each retry, if `WriteConditionNotFulfilledException` is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception. You can override the default strategy 
  by calling `new GenericApplicationService(eventStore, cloudEventConverter, retryStrategy)`. Use `new GenericApplicationService(eventStore, cloudEventConverter, RetryStrategy.none())` to revert to previous
  behavior.
* Upgraded spring-boot used in examples to 2.5.3
* Upgraded spring-mongodb to 3.2.3
* Upgraded the mongodb java driver to 4.3.1
* Added ability to write a single event to the event store instead of a stream. For example:

    ```java          
    CloudEvent event = ...
    eventStore.write("streamId", event);
    ```                                 
  This have been implemented for both the blocking and reactive event stores.

## 0.10.0 (2021-04-16)
                   
* The event store API's now returns an instance of `org.occurrent.eventstore.api.WriteResult` when writing events to the event store (previously `void` was returned). 
  The `WriteResult` instance contains the stream id and the new stream version of the stream. The reason for this change is to make it easier to implement use cases such
  as "read your own writes".
* The blocking ApplicationService `org.occurrent.application.service.blocking.ApplicationService` now returns `WriteResult` instead of `void`.
* Fixed bug in `InMemoryEventStore` that accidentally could skip version numbers when new events were inserted into the database.
* Improved detection of duplicate cloud event's in all MongoDB event stores
* Fixed a bug where `WriteConditionNotFulfilledException` was not thrown when a streams was updated by several threads in parallel (fixed for all mongodb event store implementations)
* Upgraded Spring Boot from 2.4.2 to 2.4.4
* Upgraded reactor from 3.4.2 to 3.4.4
* Upgraded spring-data-mongodb from 3.1.1 to 3.1.7
* Upgraded lettuce-core from 6.0.1 to 6.1.0
* Upgraded mongo java client from 4.1.1 to 4.2.2
* Upgraded spring-aspects from 5.2.9.RELEASE to 5.3.5
* Upgraded spring-retry from 1.3.0 to 1.3.1
* Upgraded kotlin from 1.4.31 to 1.4.32
* Upgraded kotlinx-collections-immutable-jvm from 0.3.2 to 0.3.4

## 0.9.0 (2021-03-19)
                                                                                                                                                                                        
* Fixed a bug in `InMemorySubscription` that accidentally pushed `null` values to subscriptions every 500 millis unless an actual event was received.
* Renamed `org.occurrent.subscription.mongodb.spring.blocking.SpringSubscriptionModelConfig` to `org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig`.
* Upgraded to Kotlin 1.4.31
* All blocking subscriptions now implements the life cycle methods defined in the `org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle` interface. A new interface, `org.occurrent.subscription.api.blocking.Subscribable`
  has been defined, that contains all "subscribe" methods. You can use this interface in your application if all you want to do is start subscriptions.
* Introduced a new default "StartAt" implementation called "default" (`StartAt.subscriptionModelDefault()`). This is different to `StartAt.now()` in that it will allow the subscription model 
  to choose where to start automatically if you don't want to start at an earlier position.
* Removed the ability to pass a supplier returning `StartAt` to the subscribe methods in `org.occurrent.subscription.api.blocking.Subscribable` interface. Instead, use `StartAt.dynamic(supplier)` to achieve the same result.
* Upgraded to CloudEvents Java SDK 2.0.0
* Waiting for internal message listener to be shutdown when stopping `SpringMongoSubscriptionModel`.
* Using a `org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor` as executor in `SpringMongoSubscriptionModel` instead of the default `org.springframework.core.task.SimpleAsyncTaskExecutor`. 
  The reason for this is that the `DefaultMessageListenerContainer` used internally in `SpringMongoSubscriptionModel` will wait for all threads in the `ThreadPoolTaskExecutor` to stop when stopping the
  `SpringMongoSubscriptionModel` instance. Otherwise, a race conditions can occur when stopping and then immediately starting a `SpringMongoSubscriptionModel`.
* Introducing competing consumer support! A competing consumer subscription model wraps another subscription model to allow several subscribers to subscribe to the same subscription. One of the subscribes will get a lock of the subscription
  and receive events from it. If a subscriber looses its lock, another subscriber will take over automatically. To achieve distributed locking, the subscription model uses a `org.occurrent.subscription.api.blocking.CompetingConsumerStrategy` to
  support different algorithms. You can write custom algorithms by implementing this interface yourself. Here's an example of how to use the `CompetingConsumerSubscriptionModel`. First add the `org.occurrent:competing-consumer-subscription` module to 
  classpath. This example uses the `NativeMongoLeaseCompetingConsumerStrategy` from module `org.occurrent:subscription-mongodb-native-blocking-competing-consumer-strategy`. It also wraps the [DurableSubscriptionModel](https://occurrent.org/documentation#durable-subscriptions-blocking) 
  which in turn wraps the [Native MongoDB](https://occurrent.org/documentation#blocking-subscription-using-the-native-java-mongodb-driver) subscription model.
  
  ```java
  MongoDatabase mongoDatabase = mongoClient.getDatabase("some-database");
  SubscriptionPositionStorage positionStorage = NativeMongoSubscriptionPositionStorage(mongoDatabase, "position-storage");
  SubscriptionModel wrappedSubscriptionModel = new DurableSubscriptionModel(new NativeMongoSubscriptionModel(mongoDatabase, "events", TimeRepresentation.DATE), positionStorage);
     // Create the CompetingConsumerSubscriptionModel
  NativeMongoLeaseCompetingConsumerStrategy competingConsumerStrategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(mongoDatabase);
  CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(wrappedSubscriptionModel, competingConsumerStrategy);
     // Now subscribe!
  competingConsumerSubscriptionModel.subscribe("subscriptionId", type("SomeEvent"));
  ```
  
  If the above code is executed on multiple nodes/processes, then only *one* subscriber will receive events.

## 0.8.0 (2021-02-20)

* Only log with "warn" when subscription is restarted due to "ChangeStreamHistoryLost".
* `InMemoryEventStore` now sorts queries by insertion order by default (before "time" was used)
* Added a new default compound index to MongoDB event stores, `{ streamid : 1, streamversion : 1}`. The reason for this is to get the events back in order when reading a stream from the event store _and_ 
  to make this efficient. Previous `$natural` order was used but this would skip the index, making reads slower if you have lots of data.
* Removed the index, `{ streamid : 1, streamversion : -1 }`, from all MongoDB EventStore's. It's no longer needed now that we have `{ streamid : 1, streamversion : 1}`.
* All MongoDB EventStore's now loads the events for a stream by leveraging the new `{ streamid : 1, streamversion : 1}` index.
* `CatchupSubscriptionModel` now sorts by time and then by stream version to allow for a consistent read order (see [MongoDB documentation](https://docs.mongodb.com/manual/reference/method/cursor.sort/#sort-consistency)).
  Note that the above is only true _if_ you supply a `TimeBasedSubscriptionPosition` that is _not_ equal to ``TimeBasedSubscriptionPosition.beginningOfTime()` (which is default if no filter is supplied).
* Major change in how you can sort the result from queries. Before you only had four options, "natural" (ascending/descending) and "time" (ascending/descending), now you can specify any support CloudEvent 
  field. This means that e.g. `SortBy.TIME_ASC` has been removed. It has been replaced with the `SortBy` API (`org.occurrent.eventstore.api.SortBy`), that allows you to do e.g.
  
  ```java
  SortBy.time(ASCENDING)
  ```
  
  Sorting can now be composed, e.g.

  ```java
  SortBy.time(ASCENDING).thenNatural(DESCENDING)  
  ```
  
  This has been implemented for all event stores.
* It's now possible to change how `CatchupSubscriptionModel` sorts events read from the event store during catch-up phase. For example:
  
  ```java
  var subscriptionModel = ...
  var eventStore = ..
  var cfg = new CatchupSubscriptionModelConfig(100).catchupPhaseSortBy(SortBy.descending(TIME));
  var catchupSubscriptionModel = CatchupSubscriptionModel(subscriptionModel, eventStore, cfg);  
  ```

  By default, events are sorted by time and then stream version (if two or more events have the same time).

## 0.7.4 (2021-02-13)

* Added better logging to `SpringMongoSubscriptionModel`, it'll now include the subscription id if an error occurs.
* If there's not enough history available in the mongodb oplog to resume a subscription created from a `SpringMongoSubscriptionModel`, this subscription model now supports restarting the subscription from the current 
  time automatically. This is only of concern when an application is restarted, and the subscriptions are configured to start from a position in the oplog that is no longer available. It's disabled by default since it might not 
  be 100% safe (meaning that you can miss some events when the subscription is restarted). It's not 100% safe if you run subscriptions in a different process than the event store _and_ you have lot's of 
  writes happening to the event store. It's safe if you run the subscription in the same process as the writes to the event store _if_ you make sure that the
  subscription is started _before_ you accept writes to the event store on startup. To enable automatic restart, you can do like this:
  
  ```java
  var subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, SpringSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));
  ```
  
  An alternative approach to restarting automatically is to use a catch-up subscription and restart the subscription from an earlier date.
* Better shutdown handling of all executor services used by subscription models.
* Don't log to error when a `SpringMongoSubscriptionModel` subscription is paused right after it was created, leading to a race condition. This is not an error. It's now logged in "debug" mode instead.

## 0.7.3 (2021-02-11)

* Removed the automatic creation of the "streamid" index in all MongoDB event stores. The reason is that it's not needed since there's another (compound) index (streamid+version) and 
  queries for "streamid" will be covered by that index.

## 0.7.2 (2021-02-05)

* When running MongoDB subscriptions on services like Atlas, it's not possible to get the current time (global subscription position) when starting a new subscription since access is denied. 
  If this happens then the subscription will start at the "current time" instead (`StartAt.now()`). There's a catch however! If processing the very first event fails _and_ the application is restarted,
  then the event cannot be retried. If this is major concern, consider upgrading your MongoDB server to a non-shared environment.

## 0.7.1 (2021-02-04)
                                                                                                                                                   
* Removed `org.occurrent:eventstore-inmemory` as dependency to `org.occurrent:application-service-blocking` (it should have been a test dependency) 
* Including a "details" message in `DuplicateCloudEventException` that adds more details on why this happens (which index etc). This is especially useful
  if you're creating custom, unique, indexes over the events and a write fail due to a duplicate cloud event.
* Upgraded to Kotlin 1.3.40
* Upgraded project-reactor to 3.4.2 (previously 3.4.0 was used)
* When running MongoDB subscriptions on services like Atlas, it's not possible to get the current time (global subscription position) when starting a new subscription since access is denied. 
  If this happens then the local time of the client is used instead.

## 0.7.0 (2021-01-31)
                                 
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
  
## 0.6.0 (2021-01-23)

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

## 0.5.1 (2021-01-07)

* Renamed `org.occurrent.subscription.redis.spring.blocking.SpringSubscriptionPositionStorageForRedis` to `SpringRedisSubscriptionPositionStorage`.
* Renamed `org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscription` to `ReactorMongoSubscriptionModel`.

## 0.5.0 (2021-01-06)

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

## 0.4.1 (2020-12-14)

* Upgraded to Kotlin 1.4.21
* Upgraded to cloud events 2.0.0.RC2

## 0.4.0 (2020-12-04)

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

## 0.3.0 (2020-11-21)
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

## 0.2.1 (2020-11-03)
* Fixed typo in `CatchupSupportingBlockingSubscriptionConfig`, renamed method `dontSubscriptionPositionStorage` to `dontUseSubscriptionPositionStorage`.
* Added `getSubscriptionPosition()` to `PositionAwareCloudEvent` that returns `Optional<SubscriptionPosition>`.
* Removed duplicate `GenericCloudEventConverter` located in the `org.occurrent.application.service.blocking.implementation` package. Use `org.occurrent.application.converter.implementation.CloudEventConverter` instead.
* Handling if the domain model returns a null `Stream<DomainEvent>` in the `GenericApplicationService`. 

## 0.2.0 (2020-10-31)
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

## 0.1.1 (2020-09-26):

* Catchup subscriptions (blocking)
* EveryN for stream persistence (both blocking and reactive)
* Added "count" to EventStoreQueries (both blocking and reactive)
* Added ability to query for "data" attribute in EventStoreQueries and subscriptions