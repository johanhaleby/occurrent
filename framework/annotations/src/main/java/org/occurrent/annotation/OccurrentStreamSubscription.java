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
 * Marks a no-arg factory method returning a {@code Subscription}, registering it as a framework-managed subscription
 * scoped to stream-written events. For example:
 *
 * <pre lang="java">
 * &#64;OccurrentStreamSubscription(id = "notifyCustomer")
 * Subscription&lt;OrderEvent&gt; notifyCustomer() {
 *     return Subscription.&lt;OrderEvent&gt;builder()
 *         .on(OrderShipped.class, (metadata, event) -> mailer.shipped(event))
 *         .on(OrderCancelled.class, (metadata, event) -> mailer.cancelled(event))
 *         .build();
 * }
 * </pre>
 * On the reactor stack the method returns a {@code ReactiveSubscription} instead, whose handlers return
 * {@code Mono<Void>}. There is no stream-specific descriptor, the capability is chosen here on the annotation.
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
 * <h4>Start Position</h4>
 * <p>
 * You can also specify at which time the subscription should start:
 * <pre lang="java">
 * &#64;OccurrentStreamSubscription(id = "notifyCustomer", startAt = StartPosition.BEGINNING_OF_TIME)
 * Subscription&lt;OrderEvent&gt; notifyCustomer() { .. }
 * </pre>
 * This will first replay all historic events from the beginning of time and then continue subscribing to new events
 * continuously. You can also start at a specific date by using {@link #startAtISO8601()} or
 * {@link #startAtTimeEpochMillis()}.
 * </p>
 * <p>
 * Note that the example above will <i>start</i> replaying historic events from the beginning of time when the
 * subscription is started the first time. However, once the subscription is resumed, for example on application
 * restart, it will continue from the last received event. If you want a different behavior, configure a different
 * {@link #resumeBehavior()}.
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
@Inherited
public @interface OccurrentStreamSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify the start position to one of the predefined ones in {@link StartPosition}.
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
     * Specify if the resume behavior for the subscription should differ from when it's started.
     * For example, if you specify {@code startAt=BEGINNING_OF_TIME}, the {@code resumeBehavior}
     * defines how the subscription should behave on restart of the application. By default, if you've
     * specified {@code startAt} (or epoch/iso date), then the subscription will be resumed from the last
     * received event when the application is restarted. I.e. first the subscription is caught-up
     * (by reading the events from the beginning of time in this example) and then it'll continue by listening
     * to new events, <i>without</i> starting from the beginning of time when the application is restarted.
     * If you <i>always</i> want to start from the beginning of time, you can set the resume behavior to
     * {@link ResumeBehavior#SAME_AS_START_AT}. This means that the subscription will start the "catching-up"
     * even on application restarts. This can be useful for in-memory projections/read-models where you don't
     * want to maintain any state at all.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    StartupMode startupMode() default StartupMode.DEFAULT;

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
         * started before, in which case the subscription will be started from its last known position.
         */
        DEFAULT
    }
}
