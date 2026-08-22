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
 * this one filters by a DCB query (event types and tags) and starts at a {@code position}. For example:
 *
 * <pre lang="java">
 * &#64;DcbSubscription(id = "courseDashboard", startAt = StartPosition.BEGINNING)
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
 * {@link #tags()}: event types are matched as any-of (translated to CloudEvent types through the configured
 * converter), tags are matched as all-of. When {@link #eventTypes()} is empty the event types are taken from the
 * method's event parameter (a sealed parameter expands to its concrete subtypes), the same way
 * {@link StreamSubscription} does, so the query is always scoped to the resolved event types. Add {@link #tags()}
 * to narrow further to a tag boundary.
 * </p>
 *
 * <h4>Start Position</h4>
 * <p>
 * {@link #startAt()} selects one of the {@link StartPosition} values. {@link StartPosition#BEGINNING} replays
 * the whole DCB sequence by {@code position} before switching to live delivery, so a read model can be rebuilt
 * from history. As with {@link StreamSubscription}, the replay happens the first time the subscription starts, and on
 * later restarts it resumes from the last received event, unless {@link #resumeBehavior()} says otherwise.
 * </p>
 *
 * <h4>Metadata</h4>
 * <p>
 * The annotated method may take the metadata associated with the event as a second parameter, either the generic
 * {@link org.occurrent.cloudevents.EventMetadata} or the DCB specific
 * {@code org.occurrent.dsl.dcb.DcbEventMetadata}, which also exposes the DCB position and tags.
 * </p>
 *
 * @deprecated Replaced by {@link OccurrentDcbSubscription}, which marks a factory method returning a
 * {@code DcbSubscription} descriptor rather than a {@code void} handler method, and which is named so it no longer
 * takes the word a user needs for the {@code DcbSubscription} it marks. The replacement has no {@code eventTypes}
 * and no {@code tags}, because the descriptor's own handlers say which events it wants. This annotation keeps
 * behaving exactly as it does today until it is removed, so nothing has to change at once. See
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md">the 0.35.0
 * migration guide</a> for how to move a handler over, and for what {@code org.occurrent.UpgradeToOccurrent_0_35} does
 * not rewrite for you.
 */
@Deprecated(forRemoval = true)
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
     * <p>
     * Each entry must be in the {@code "key:value"} format (for example {@code "email:foo@bar.com"}). The string is
     * split on the first {@code :} into a tag key and value. A malformed value (missing {@code :}, or a blank key or
     * value) fails fast at application startup.
     * </p>
     */
    String[] tags() default {};

    /**
     * Specify the start position as one of the predefined {@link StartPosition} values. Mutually exclusive with
     * {@link #startAtDcbPosition()}, which starts from a specific position instead of a predefined one.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific DCB sequence position, that is deliver events from {@code startAtDcbPosition + 1} onward,
     * which is useful to rewind a durable read model to a known-good position. This is the DCB counterpart to
     * {@link StreamSubscription#startAtTimeEpochMillis()}. The default of {@code -1} means unset, in which case
     * {@link #startAt()} is used. Mutually exclusive with a non-{@link StartPosition#DEFAULT} {@link #startAt()}, and
     * {@link #resumeBehavior()} applies the same way it does to {@link StartPosition#BEGINNING}.
     */
    long startAtDcbPosition() default -1;

    /**
     * Specify if the resume behavior for the subscription should differ from when it is started. By default
     * ({@link ResumeBehavior#DEFAULT}), a subscription that starts by replaying history (from
     * {@link StartPosition#BEGINNING} or from a {@link #startAtDcbPosition()}) replays only the first time it is
     * started and then resumes from the last received event on application restart. That is the right behavior for a
     * durable read model that persists what it builds.
     * <p>
     * An in-memory read model is different: it keeps no durable state, so it has to replay the whole history on every
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
