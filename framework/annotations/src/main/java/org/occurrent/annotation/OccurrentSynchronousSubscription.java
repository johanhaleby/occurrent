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
 * Marks a no-arg factory method returning a {@code Subscription}, registering it as a capability-agnostic
 * subscription whose handlers are invoked <strong>synchronously</strong> by the application service, on the calling
 * thread, before {@code execute} returns, rather than asynchronously off a change stream. It delivers events
 * regardless of which write model produced them, both stream-written and DCB-appended events on a store with both
 * capabilities. For example:
 *
 * <pre lang="java">
 * &#64;OccurrentSynchronousSubscription(id = "updateReadModel")
 * Subscription&lt;OrderEvent&gt; updateReadModel() {
 *     return Subscription.&lt;OrderEvent&gt;builder()
 *         .on(OrderShipped.class, (metadata, event) -> readModel.shipped(event))
 *         .build();
 * }
 * </pre>
 * The handlers run on the writer's thread, before {@code execute()} returns. There is no synchronous descriptor, the
 * delivery is chosen here on the annotation.
 *
 * <h4>Runs on the writer's thread</h4>
 * <p>
 * {@link OccurrentSubscription}, {@link OccurrentStreamSubscription} and {@link OccurrentDcbSubscription} start
 * background, resumable subscriptions off the event store's change stream. An
 * {@code @OccurrentSynchronousSubscription} handler runs in-process on the thread that produced the events, as part
 * of the write. There is therefore no start position, checkpoint, catch-up, or replay, so this annotation carries
 * none of the async-only attributes ({@code startAt}, {@code startAtGlobalPosition}, {@code resumeBehavior},
 * {@code startupMode}), only an {@link #id()}.
 * </p>
 *
 * <h4>There is no free lunch</h4>
 * <p>
 * Enabling synchronous subscriptions is not free. While at least one is registered, every event-producing write pays
 * one extra read. After the write the application service re-reads exactly the just-written events, so handlers see
 * them enriched with stream version and global position, before dispatching. When no synchronous subscription is
 * registered, no per-write cost is incurred.
 * </p>
 *
 * <h4>Best-effort without a transaction executor</h4>
 * <p>
 * Whether a throwing handler undoes the write depends on whether a transaction executor spans the write and the
 * handlers. With one wired (the Spring starters wire a {@code SpringTransactionExecutor} by default), the write and
 * the synchronous handlers commit atomically, so a handler that throws rolls the write back. Without one, synchronous
 * subscriptions are best-effort, the write has already committed by the time a handler runs, so a handler that
 * throws after the committed write fails the {@code execute} call even though the events are already
 * persisted. Do not assume atomicity unless a transaction executor is in place.
 * </p>
 *
 * <h4>Which events arrive</h4>
 * <p>
 * The descriptor decides that, and this annotation has no say in it. The types the handlers are registered for
 * select the events, and a registered sealed type selects every concrete event it permits.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * Each handler receives the event's {@link org.occurrent.cloudevents.EventMetadata} alongside the event, with the
 * stream id, the stream version and any custom value the event was written with.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OccurrentSynchronousSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();
}
