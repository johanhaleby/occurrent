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
 * Starts or resumes a Dynamic Consistency Boundary (DCB) subscription. It is the DCB counterpart to
 * {@link StreamSubscription}: where the stream annotation filters by an Occurrent {@code Filter} and starts at a time,
 * this one filters by a DCB query (event types and tags) and starts at a {@code dcbposition}. For example:
 *
 * <pre lang="java">
 * &#64;DcbSubscription(id = "courseDashboard", startAt = DcbStartPosition.BEGINNING)
 * void onEvent(CourseEvent event) {
 *     dashboard.update(event);
 * }
 * </pre>
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
 * <h4>Query</h4>
 * <p>
 * The subscription delivers the DCB events matching its query. The query is built from {@link #eventTypes()} and
 * {@link #tagsAllOf()}: event types are matched as any-of (translated to CloudEvent types through the configured
 * converter), tags are matched as all-of. When {@link #eventTypes()} is empty the event types are taken from the
 * method's event parameter (a sealed parameter expands to its concrete subtypes), the same way
 * {@link StreamSubscription} does, so the query is always scoped to the resolved event types. Add {@link #tagsAllOf()}
 * to narrow further to a tag boundary.
 * </p>
 *
 * <h4>Start Position</h4>
 * <p>
 * {@link #startAt()} selects one of the {@link DcbStartPosition} values. {@link DcbStartPosition#BEGINNING} replays
 * the whole DCB sequence by {@code dcbposition} before switching to live delivery, so a read model can be rebuilt
 * from history. As with {@link StreamSubscription}, the replay happens the first time the subscription starts, and on
 * later restarts it resumes from the last received event, unless {@link #resumeBehavior()} says otherwise.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * The annotated method may take the metadata associated with the event as a second parameter, either the generic
 * {@link org.occurrent.dsl.subscription.blocking.EventMetadata} or the DCB specific
 * {@code org.occurrent.dsl.dcb.blocking.DcbEventMetadata}, which also exposes the DCB position and tags.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DcbSubscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify the event types to subscribe to. When empty, the event type is taken from the method's event parameter
     * (a sealed type is expanded to its concrete subtypes). The types are matched as any-of and translated to
     * CloudEvent types through the configured converter to build the DCB query.
     */
    Class<?>[] eventTypes() default {};

    /**
     * Specify the DCB tags the events must all carry, the consistency boundary the subscription is scoped to. Matched
     * as all-of. When empty the subscription is not scoped by tags.
     */
    String[] tagsAllOf() default {};

    /**
     * Specify the start position as one of the predefined {@link DcbStartPosition} values. Mutually exclusive with
     * {@link #startAtDcbPosition()}, which starts from a specific position instead of a predefined one.
     */
    DcbStartPosition startAt() default DcbStartPosition.DEFAULT;

    /**
     * Start after a specific DCB sequence position, that is deliver events from {@code startAtDcbPosition + 1} onward,
     * which is useful to rewind a durable read model to a known-good position. This is the DCB counterpart to
     * {@link StreamSubscription#startAtTimeEpochMillis()}. The default of {@code -1} means unset, in which case
     * {@link #startAt()} is used. Mutually exclusive with a non-{@link DcbStartPosition#DEFAULT} {@link #startAt()}, and
     * {@link #resumeBehavior()} applies the same way it does to {@link DcbStartPosition#BEGINNING}.
     */
    long startAtDcbPosition() default -1;

    /**
     * Specify if the resume behavior for the subscription should differ from when it is started. By default
     * ({@link ResumeBehavior#DEFAULT}), a subscription that starts by replaying history (from
     * {@link DcbStartPosition#BEGINNING} or from a {@link #startAtDcbPosition()}) replays only the first time it is
     * started and then resumes from the last received event on application restart. That is the right behavior for a
     * durable read model that persists what it builds.
     * <p>
     * An in-memory read model is different: it keeps no durable state, so it has to replay the whole history on every
     * boot. For that, combine {@link DcbStartPosition#BEGINNING} with {@link ResumeBehavior#SAME_AS_START_AT}, which
     * replays from the beginning on every restart and keeps no checkpoint. With the default resume behavior an in-memory
     * model would, after a restart, resume mid-sequence and silently miss all history before the stored position.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * Specify how the subscription should behave during startup.
     */
    StartupMode startupMode() default StartupMode.DEFAULT;

    /**
     * The predefined DCB start positions.
     */
    enum DcbStartPosition {
        /**
         * Replay the whole DCB sequence from the beginning (by {@code dcbposition}) before switching to live delivery.
         */
        BEGINNING,
        /**
         * Start from "now", delivering only events written after the subscription starts.
         */
        NOW,
        /**
         * Use the default behavior of the subscription model. Typically this resumes from the last stored position if
         * the subscription has run before, otherwise it behaves like {@link #NOW}.
         */
        DEFAULT
    }

    /**
     * Specifies how a subscription should be resumed once the application is restarted.
     */
    enum ResumeBehavior {
        /**
         * Always start at the same position as specified by the {@link DcbStartPosition}. Even if a subscription
         * position (checkpoint) is stored, it is ignored on restart and the subscription resumes from the specified
         * {@link DcbStartPosition}.
         */
        SAME_AS_START_AT,
        /**
         * Use the default resume behavior. For example, if {@link DcbStartPosition} is {@link DcbStartPosition#BEGINNING}
         * and {@code ResumeBehavior} is {@link ResumeBehavior#DEFAULT}, the subscription replays from the beginning the
         * first time it runs, then resumes from the last received event on restart.
         */
        DEFAULT
    }

    /**
     * Specify how the subscription should behave during startup.
     */
    enum StartupMode {
        /**
         * Occurrent determines the startup mode from the other properties of the subscription. It uses
         * {@link #BACKGROUND} when the subscription needs to replay history before subscribing to new events (for
         * example when {@link #startAt()} is {@link DcbStartPosition#BEGINNING}), otherwise {@link #WAIT_UNTIL_STARTED}.
         */
        DEFAULT,
        /**
         * The subscription waits until it has started fully before Spring continues starting the rest of the
         * application. Most of the time this is recommended because otherwise a request could reach the application
         * before the subscription has bootstrapped, which a brand new subscription could miss.
         */
        WAIT_UNTIL_STARTED,
        /**
         * The subscription does not wait until it has started fully, it starts in the background. This is mainly useful
         * when the subscription replays a lot of history (such as from the beginning) and waiting for the replay to
         * finish before the application starts would take too long.
         */
        BACKGROUND
    }
}
