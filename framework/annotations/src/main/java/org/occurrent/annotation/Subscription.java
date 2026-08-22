/*
 *
 *  Copyright 2023 Johan Haleby
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
 * Starts or resumes a capability-agnostic subscription. It delivers events of the given types regardless of which write
 * model produced them, so on a store that has both the {@code STREAM} and {@code DCB} capabilities it receives both
 * stream-written and DCB-appended events, filtered only by event type. For example:
 *
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event) {
 *     System.out.println("Received event: " + event);
 * }
 * </pre>
 *
 * <h4>Which subscription annotation to use</h4>
 * <p>
 * This is the neutral default for a read model or policy that reacts to events by type and does not care which write
 * model produced them. Use {@link StreamSubscription} or {@link DcbSubscription} instead when a subscription should be
 * scoped to a single capability, that is stream-written events only, or DCB events filtered by tags.
 * </p>
 *
 * <h4>Start Position</h4>
 * <p>
 * You can specify where the subscription should start over the unified global {@code position}:
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription", startAt = StartPosition.BEGINNING)
 * void mySubscription(MyDomainEvent event) { .. }
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
 * <p>
 * Note also that if {@code MyDomainEvent} is a sealed interface or class, then all events implementing this interface
 * or class will be received. If you want to receive only some of the events that implement this interface, see
 * {@link #eventTypes()}.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * Sometimes it can be useful to get the metadata associated with the received event. For this reason, you can add a
 * parameter to the method annotated with {@code @Subscription} of type
 * {@link org.occurrent.cloudevents.EventMetadata}. For example:
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event, EventMetadata metadata) {
 *   String streamId = metadata.getStreamId();
 *   long streamVersion = metadata.getStreamVersion();
 *   Object myCustomValue = metadata.get("MyCustomValue");
 *   ..
 * }
 * </pre>
 * <p>
 * When you only need the stream id or stream version, annotate a parameter with {@link StreamId} or
 * {@link StreamVersion} instead of taking the whole {@link org.occurrent.cloudevents.EventMetadata}:
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event, &#64;StreamId String streamId, &#64;StreamVersion long streamVersion) { .. }
 * </pre>
 * These may appear in any order alongside the event and an optional {@code EventMetadata} parameter. Note that on this
 * capability-agnostic annotation a DCB-appended event carries the internal partition id and per-partition counter
 * rather than a domain stream id/version, the same values {@code EventMetadata.getStreamId()}/{@code getStreamVersion()}
 * expose there.
 * </p>
 *
 * @deprecated Replaced by {@link OccurrentSubscription}, which marks a factory method returning a
 * {@code Subscription} descriptor rather than a {@code void} handler method, and which is named so it no longer
 * takes the word a user needs for the {@code Subscription} it marks. The replacement has no {@code eventTypes},
 * because the descriptor's own handlers say which events it wants. This annotation keeps behaving exactly as it does
 * today until it is removed, so nothing has to change at once. See
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md">the 0.35.0
 * migration guide</a> for how to move a handler over, and for what {@code org.occurrent.UpgradeToOccurrent_0_35} does
 * not rewrite for you.
 */
@Deprecated(forRemoval = true)
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Subscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify event types to subscribe to. Useful if you want to, for example, subscribe to two events, MyEvent1 and MyEvent2 and you want them to be received as "MyEvent", which may
     * have other subtypes besides MyEvent1 and MyEvent2. For example let's say you have this event hierarchy:
     *
     * <pre lang="java">
     * public sealed interface Event permits MyEvent1, MyEvent2, MyEvent3, .. {
     * }
     * record MyEvent1(..) implements Event { .. }
     * record MyEvent2(..) implements Event { .. }
     * record MyEvent3(..) implements Event { .. }
     * </pre>
     *
     * <p>
     * Now lets say that you want to create subscription in which you want to receive either MyEvent1 or MyEvent2. You can then do:
     * </p>
     *
     * <pre lang="java">
     * &#64;Subscription(id="mySubscription", eventTypes = {MyEvent1.class, MyEvent2.class})
     * void subscribeToMyEvent1Or2(MyEvent event1Or2) { .. }
     * </pre>
     * <p>
     * This will create a subscription filter that only subscribes to "MyEvent1" or "MyEvent2" and receives it as type "MyEvent".
     * </p>
     */
    Class<?>[] eventTypes() default {};

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
     * An in-memory read model is different: it keeps no durable state, so it has to replay the whole history on every
     * boot. For that, combine {@link StartPosition#BEGINNING} with {@link ResumeBehavior#SAME_AS_START_AT}, which
     * replays from the beginning on every restart and keeps no checkpoint. With the default resume behavior an in-memory
     * model would, after a restart, resume mid-sequence and silently miss all history before the stored position.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    StartupMode startupMode() default StartupMode.DEFAULT;
}