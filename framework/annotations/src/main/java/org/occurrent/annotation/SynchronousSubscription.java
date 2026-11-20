/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.annotation;

import java.lang.annotation.*;

/**
 * Registers a capability-agnostic subscription that is invoked <strong>synchronously</strong> by the application
 * service, on the calling thread, before {@code execute} returns, rather than asynchronously off a change stream. It
 * delivers events of the given types regardless of which write model produced them (both stream-written and
 * DCB-appended events on a store with both capabilities), filtered only by event type. For example:
 *
 * <pre lang="java">
 * &#64;SynchronousSubscription(id = "updateReadModel")
 * void updateReadModel(MyDomainEvent event) {
 *     // runs on the writer's thread, before execute() returns
 * }
 * </pre>
 *
 * <h4>Synchronous, not asynchronous</h4>
 * <p>
 * Unlike {@link Subscription}, {@link StreamSubscription} and {@link DcbSubscription} - which start background,
 * resumable subscriptions off the event store's change stream - a {@code @SynchronousSubscription} handler runs
 * in-process on the thread that produced the events, as part of the write. There is therefore no start position,
 * checkpoint, catch-up, or replay, so this annotation carries none of the async-only attributes ({@code startAt},
 * {@code startAtGlobalPosition}, {@code resumeBehavior}, {@code startupMode}) - only an {@link #id()} and the event
 * types to match.
 * </p>
 *
 * <h4>What a synchronous handler gets</h4>
 * <p>
 * Registered by the Spring Boot starter, a synchronous handler gets the events written through the application service
 * of its own process after it registered. It also gets the events your code passes to the
 * {@code SynchronousSubscriptionModel} directly, with {@code dispatch} on either stack or {@code accept} on the
 * blocking one, for example from an in-memory event store's write listener. It gets no others:
 * </p>
 * <ul>
 * <li>It has no replay, so it doesn't get the events stored before it registered unless your code passes them to the
 * model. A synchronous handler added to an application whose event store already has events starts from nothing. To
 * build a read model from the events already stored, use an asynchronous handler that starts at the beginning.</li>
 * <li>An event written directly to the event store, without going through the application service, doesn't reach it
 * unless your code also passes it to the model.</li>
 * <li>An event that another process writes doesn't reach it unless your code passes it to the model. Every process that
 * writes through an application service must run the same synchronous handlers, so add or remove one with every process
 * stopped, or use an asynchronous handler. During a rolling deploy, a process still running the previous version writes
 * events that a handler only the new version declares never gets, unless your code passes them to the model.</li>
 * <li>Without a transaction executor, delivery is best-effort, as described below.</li>
 * </ul>
 * <p>
 * The Spring Boot starters register every handler in a scan while the application starts, and the application service
 * they configure refuses a write with {@code WriteRefusedException} until the scan is done. The scan can't see a
 * handler on a bean whose declared type hides it, such as a {@code @Bean} method that returns an interface or
 * {@code Object}. When such a bean is created after the scan, a {@code @Lazy} one for example, it fails its creation
 * with an {@code IllegalStateException}, since its handler would have missed the writes before it. To have the scan
 * register it, remove {@code @Lazy}, or declare the {@code @Bean} method with the annotated class as its return type.
 * </p>
 *
 * <h4>There is no free lunch</h4>
 * <p>
 * Enabling synchronous subscriptions is not free. While at least one is registered, every event-producing write pays
 * one extra read: after the write the application service re-reads exactly the just-written events (so handlers see
 * them enriched with stream version and global position) before dispatching. When no synchronous subscription is
 * registered, no per-write cost is incurred.
 * </p>
 *
 * <h4>Best-effort without a transaction executor</h4>
 * <p>
 * Whether a throwing handler undoes the write depends on whether a transaction executor spans the write and the
 * handlers. With one wired (the Spring starters wire a {@code SpringTransactionExecutor} by default), the write and
 * the synchronous handlers commit atomically: a handler that throws rolls the write back. Without one, synchronous
 * subscriptions are best-effort - the write has already committed by the time a handler runs, so a handler that
 * throws after the committed write surfaces as an {@code execute} failure even though the events are already
 * persisted. Do not assume atomicity unless a transaction executor is in place.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * As with {@link Subscription}, the handler may declare a {@link org.occurrent.cloudevents.EventMetadata}
 * parameter in addition to the event parameter to receive the metadata of the delivered event.
 * </p>
 * <p>
 * The Spring Boot starter finds this annotation on a method the bean's class declares, inherits from a superclass, or
 * gets as an interface default method. An override without an annotation of its own runs under the overridden
 * method's annotation, and an override that repeats the annotation uses its own.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SynchronousSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify event types to subscribe to. Works exactly like {@link Subscription#eventTypes()}: when empty, the event
     * type is derived from the handler's event parameter (expanding a sealed type to its permitted subtypes),
     * otherwise the listed types are matched and must be assignable to the declared event parameter.
     */
    Class<?>[] eventTypes() default {};
}
