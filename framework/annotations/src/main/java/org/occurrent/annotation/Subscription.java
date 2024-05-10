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
 * An annotation that can be used to start/resume subscriptions. For example:
 *
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event) {
 *     System.out.println("Received event: " + event);
 * }
 * </pre>
 *
 * <h4>Start Position</h4>
 * <p>
 * You can also specify at which time the subscription should start:
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription", startAt = StartPosition.BEGINNING_OF_TIME)
 * void mySubscription(MyDomainEvent event) { .. }
 * </pre>
 * This will first replay all historic events from the beginning of time and then continue subscribing to new events continuously. You can also start at a specific date
 * by using {@link #startAtISO8601()} or {@link #startAtTimeEpochMillis()}.
 * </p>
 * <p>
 * Note that the example above will <i>start</i> replay historic events from the beginning of time when the subscription is started the first time. However, once the subscription is resumed,
 * e.g. on application restart, it'll continue from the last received event. If you want a different behavior, configure a different {@link #resumeBehavior()}.
 * </p>
 * <p>
 * Note also that if {@code MyDomainEvent} is a sealed interface/class, then all events implementing this interface/class will be received. If you want to receive only
 * some of the events that implements this interface, see {@link #eventTypes()}.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * Sometimes it can be useful to get the metadata associated with the received event. For this reason, you can add a parameter to the method annotated with
 * {@code @Subscription} of type {@link org.occurrent.dsl.subscription.blocking.EventMetadata}. For example:
 * <pre lang="java">
 * &#64;Subscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event, EventMetadata metadata) {
 *   String streamId = metadata.getStreamId();
 *   long streamVersion = metadata.getStreamVersion();
 *   Object myCustomValue = metadata.get("MyCustomValue");
 *   ..
 * }
 * </pre>
 */
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
     * Specify the start position to one if the predefined ones in {@link StartPosition}.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Specify the start position as time epoch milliseconds
     */
    long startAtTimeEpochMillis() default -1;

    /**
     * Start a subscription from the specified ISO8601 date/time. Valid dates are e.g.
     * <pre>
     * 2024-05-10T10:48:00.838
     * 2024-05-10T10:48:00.838Z
     * 2024-05-10T15:30:37.123+02:00
     * </pre>
     */
    String startAtISO8601() default "";

    /**
     * Specify if the resume behavior for the subscription should differ from when its started.
     * For example, if you specify {@code startAt=BEGINNING_OF_TIME}, the {@code resumeBehavior}
     * defines how the subscription should behave on restart of the application. By default, if you've
     * specified {@code startAt} (or epoch/iso date), then the subscription will be resumed from the last
     * received event when the application is restarted. I.e. first the subscription is caught-up
     * (by reading the events from the beginning of time in this example) and then it'll continue by listening
     * to new events, <i>without</i>_ starting from the beginning of time when the application is restarted.
     * If you <i>always</i> want to start from the beginning of time, you can set the resume behavior to
     * {@link ResumeBehavior#SAME_AS_START_AT}. This means that the subscription will start the "catching-up"
     * even on application restarts. This can be useful for in-memory projections/read-models where you don't
     * want to maintain any state at all.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * A set of predefined start positions
     */
    enum StartPosition {
        /**
         * Start this subscription from the first event in the event store
         */
        BEGINNING_OF_TIME,
        /**
         * Start this subscription from "NOW"
         */
        NOW,
        /**
         * Start this subscription using the default behavior of the subscription model.
         * Typically, this means that it'll start from "NOW", unless the subscription has already been
         * started before, in which case the subscription will be started from its last know position.
         */
        DEFAULT
    }

    /**
     * Specifies how a subscription should be resumed once the application is restarted.
     */
    enum ResumeBehavior {
        /**
         * Always start at the same position as specified by the {@link StartPosition}. I.e. even if there's a subscription position (checkpoint) stored for the subscription
         * it'll be ignored on application restart and the subscription will resume from the specified {@link StartPosition}.
         */
        SAME_AS_START_AT,
        /**
         * Use the default resume behavior of the underlying subscription model. For example, if the {@link StartPosition} is set to {@link StartPosition#BEGINNING_OF_TIME},
         * and {@code ResumeBehavior} is set to {@link ResumeBehavior#DEFAULT}, then the subscription will <i>start</i> from the beginning of time the first time it's run,
         * then on application restart, it'll continue from the last received event (the subscription position (checkpoint) for the subscription) on restart.
         */
        DEFAULT
    }
}