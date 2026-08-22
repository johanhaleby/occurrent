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
 * Marks a no-arg factory method returning a {@code DcbSubscription}, registering it as a framework-managed Dynamic
 * Consistency Boundary (DCB) subscription. It is the DCB counterpart to {@link OccurrentStreamSubscription}, where
 * the stream annotation starts at a time, this one starts at a {@code position} and its descriptor selects events by
 * DCB query. For example:
 *
 * <pre lang="java">
 * &#64;OccurrentDcbSubscription(id = "courseDashboard", startAt = StartPosition.BEGINNING)
 * DcbSubscription&lt;CourseEvent&gt; courseDashboard() {
 *     return DcbSubscription.&lt;CourseEvent&gt;builder()
 *         .tags("courseId:java-101")
 *         .on(StudentSubscribed.class, (metadata, event) -> dashboard.subscribed(event))
 *         .on(StudentUnsubscribed.class, (metadata, event) -> dashboard.unsubscribed(event))
 *         .build();
 * }
 * </pre>
 * On the reactor stack the method returns a {@code ReactiveDcbSubscription} instead, whose handlers return
 * {@code Mono<Void>}.
 * <p>
 * The method may live on any Spring bean, a {@code @Bean} in a {@code @Configuration} or a method on a
 * {@code @Component}.
 *
 * <h4>Which DCB subscription API to use</h4>
 * <p>
 * Use this annotation for a persistent, durable subscription managed by the framework, such as a read model that
 * catches up from history on startup. For an ephemeral, per-connection subscription that you start and cancel by hand
 * (for example a Server-Sent-Events feed scoped to one HTTP request), inject and use the {@code DcbSubscriptions} DSL
 * instead. The {@code DcbSubscriptionModel} interface is the lower-level, CloudEvent-level typed view that both build
 * on, and most application code does not use it directly.
 * </p>
 *
 * <h4>Which events arrive</h4>
 * <p>
 * The descriptor decides that, and this annotation has no say in it. The DCB query is built from the types the
 * handlers are registered for, matched as any-of and translated to CloudEvent types through the configured converter,
 * and a registered sealed type expands to its concrete subtypes. Declaring tags on the descriptor narrows those types
 * to a tag boundary, matched as all-of, while giving the descriptor an explicit {@code DcbCriteria} replaces the whole
 * derived query instead. An event the query admits that no handler handles is ignored.
 * </p>
 *
 * <h4>Start Position</h4>
 * <p>
 * {@link #startAt()} selects one of the {@link StartPosition} values. {@link StartPosition#BEGINNING} replays
 * the whole DCB sequence by {@code position} before switching to live delivery, so a read model can be rebuilt
 * from history. As with {@link OccurrentStreamSubscription}, the replay happens the first time the subscription
 * starts, and on later restarts it resumes from the last received event, unless {@link #resumeBehavior()} says
 * otherwise.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * Each handler receives the event's {@code org.occurrent.dsl.dcb.DcbEventMetadata} alongside the event, which also
 * exposes the DCB position and tags.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface OccurrentDcbSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify the start position as one of the predefined {@link StartPosition} values. Mutually exclusive with
     * {@link #startAtDcbPosition()}, which starts from a specific position instead of a predefined one.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific DCB sequence position, that is deliver events from {@code startAtDcbPosition + 1} onward,
     * which is useful to rewind a durable read model to a known-good position. This is the DCB counterpart to
     * {@link OccurrentStreamSubscription#startAtTimeEpochMillis()}. The default of {@code -1} means unset, in which
     * case {@link #startAt()} is used. Mutually exclusive with a non-{@link StartPosition#DEFAULT} {@link #startAt()},
     * and {@link #resumeBehavior()} applies the same way it does to {@link StartPosition#BEGINNING}.
     */
    long startAtDcbPosition() default -1;

    /**
     * Specify if the resume behavior for the subscription should differ from when it is started. By default
     * ({@link ResumeBehavior#DEFAULT}), a subscription that starts by replaying history (from
     * {@link StartPosition#BEGINNING} or from a {@link #startAtDcbPosition()}) replays only the first time it is
     * started and then resumes from the last received event on application restart. That is the right behavior for a
     * durable read model that persists what it builds.
     * <p>
     * An in-memory read model is different. It keeps no durable state, so it has to replay the whole history on every
     * boot. For that, combine {@link StartPosition#BEGINNING} with {@link ResumeBehavior#SAME_AS_START_AT}, which
     * replays from the beginning on every restart and keeps no checkpoint. With the default resume behavior an in-memory
     * model would, after a restart, resume mid-sequence and silently miss all history before the stored position.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * Specify how the subscription should behave during startup.
     */
    StartupMode startupMode() default StartupMode.DEFAULT;
}
