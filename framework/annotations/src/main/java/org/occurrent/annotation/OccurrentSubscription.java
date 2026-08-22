/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.annotation;

import java.lang.annotation.*;

/**
 * Marks a no-arg factory method returning a {@code Subscription}, registering it as a capability-agnostic,
 * framework-managed subscription. It delivers events regardless of which write model produced them, so on a store
 * that has both the {@code STREAM} and {@code DCB} capabilities it receives both stream-written and DCB-appended
 * events. For example:
 *
 * <pre lang="java">
 * &#64;OccurrentSubscription(id = "notifyCustomer")
 * Subscription&lt;OrderEvent&gt; notifyCustomer() {
 *     return Subscription.&lt;OrderEvent&gt;builder()
 *         .on(OrderShipped.class, (metadata, event) -> mailer.shipped(event))
 *         .on(OrderCancelled.class, (metadata, event) -> mailer.cancelled(event))
 *         .build();
 * }
 * </pre>
 * On the reactor stack the method returns a {@code ReactiveSubscription} instead, whose handlers return
 * {@code Mono<Void>}.
 * <p>
 * The method may live on any Spring bean, a {@code @Bean} in a {@code @Configuration} or a method on a
 * {@code @Component}.
 *
 * <h4>Which events arrive</h4>
 * <p>
 * The descriptor decides that, and this annotation has no say in it. The types the handlers are registered for
 * select the events, a registered sealed type selects every concrete event it permits, and an explicit filter on
 * the descriptor replaces that derived selection. An event the selection admits that no handler handles is ignored.
 * </p>
 *
 * <h4>Which subscription annotation to use</h4>
 * <p>
 * This is the neutral default for a read model or a policy that reacts to events and does not care which write model
 * produced them. Use {@link OccurrentStreamSubscription} or {@link OccurrentDcbSubscription} to scope a subscription
 * to a single capability, that is stream-written events only, or DCB events narrowed by tags. Use
 * {@link OccurrentSynchronousSubscription} to have the handler run on the writer's thread as part of the write
 * instead of off a change stream.
 * </p>
 *
 * <h4>Start Position</h4>
 * <p>
 * You can specify where the subscription should start over the unified global {@code position}:
 * <pre lang="java">
 * &#64;OccurrentSubscription(id = "notifyCustomer", startAt = StartPosition.BEGINNING)
 * Subscription&lt;OrderEvent&gt; notifyCustomer() { .. }
 * </pre>
 * This will first replay all historic events from the beginning of the global position sequence and then continue
 * subscribing to new events continuously. You can also start after a specific global position with
 * {@link #startAtGlobalPosition()}.
 * </p>
 * <p>
 * Note that the example above will <i>start</i> replaying historic events from the beginning when the subscription is
 * started the first time. However, once the subscription is resumed, for example on application restart, it will
 * continue from the last received event. If you want a different behavior, configure a different
 * {@link #resumeBehavior()}.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * Each handler receives the event's {@link org.occurrent.cloudevents.EventMetadata} alongside the event, with the
 * stream id, the stream version and any custom value the event was written with. Note that on this
 * capability-agnostic annotation a DCB-appended event has the internal partition id and per-partition counter there
 * rather than a domain stream id and version, the same values {@code EventMetadata.getStreamId()} and
 * {@code getStreamVersion()} expose for it today.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface OccurrentSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify the start position as one of the predefined {@link StartPosition} values. Mutually exclusive with
     * {@link #startAtGlobalPosition()}, which starts after a specific global position instead of a predefined one.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific global {@code position}, that is deliver events from {@code startAtGlobalPosition + 1}
     * onward, which is useful to rewind a durable read model to a known-good position. The default of {@code -1} means
     * unset, in which case {@link #startAt()} is used. Mutually exclusive with a non-{@link StartPosition#DEFAULT}
     * {@link #startAt()}, and {@link #resumeBehavior()} applies the same way it does to {@link StartPosition#BEGINNING}.
     */
    long startAtGlobalPosition() default -1;

    /**
     * Specify if the resume behavior for the subscription should differ from when it is started. By default
     * ({@link ResumeBehavior#DEFAULT}), a subscription that starts by replaying history (from
     * {@link StartPosition#BEGINNING} or from a {@link #startAtGlobalPosition()}) replays only the first time it is
     * started and then resumes from the last received event on application restart. That is the right behavior for a
     * durable read model that persists what it builds.
     * <p>
     * An in-memory read model is different. It keeps no durable state, so it has to replay the whole history on every
     * boot. For that, combine {@link StartPosition#BEGINNING} with {@link ResumeBehavior#SAME_AS_START_AT}, which
     * replays from the beginning on every restart and keeps no checkpoint. With the default resume behavior an in-memory
     * model would, after a restart, resume mid-sequence and silently miss all history before the stored position.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    StartupMode startupMode() default StartupMode.DEFAULT;
}
