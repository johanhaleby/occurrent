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
 *
 * @deprecated Replaced by {@link OccurrentSynchronousSubscription}, which marks a factory method returning a
 * {@code Subscription} descriptor rather than a {@code void} handler method, and which is named so that all seven
 * Occurrent annotations share one prefix. The replacement has no {@code eventTypes}, because the descriptor's own
 * handlers say which events it wants. This annotation keeps behaving exactly as it does today until it is removed, so
 * nothing has to change at once. See
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md">the 0.35.0
 * migration guide</a> for how to move a handler over, and for what {@code org.occurrent.UpgradeToOccurrent_0_35} does
 * not rewrite for you.
 */
@Deprecated(forRemoval = true)
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
