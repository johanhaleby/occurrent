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
 * Marks a no-arg factory method returning a {@code Projection} or {@code DcbProjection}, registering it as a
 * persistent, framework-managed read model. For example:
 *
 * <pre lang="java">
 * &#64;Projection(id = "orderStatus")
 * Projection&lt;OrderStatus, OrderEvent, String&gt; orderStatusProjection() {
 *     return Projection.&lt;OrderStatus, OrderEvent, String&gt;builder(OrderStatus.EMPTY)
 *         .id(OrderEvent::orderId)
 *         .on(OrderPlaced.class, (state, event) -> state.placed(event))
 *         .on(OrderShipped.class, (state, event) -> state.shipped(event))
 *         .build();
 * }
 * </pre>
 * The Kotlin equivalent is {@code projection(OrderStatus.EMPTY) { id { it.orderId }; on<OrderPlaced> { s, e -> s.placed(e) } }},
 * and a DCB read model returns a {@code DcbProjection} from {@code dcbProjection { .. }}.
 * <p>
 * The method may live on any Spring bean: a {@code @Bean} in a {@code @Configuration}, or a method on a
 * {@code @Component}. Prefer {@code @Component} for a single dedicated projection, {@code @Bean} to group several in
 * one class.
 *
 * <h4>Projection Descriptor Type</h4>
 * <p>
 * The method must return a descriptor: a {@code Projection} for capability-agnostic or stream-based
 * read models, or a {@code DcbProjection} for DCB-scoped models. The {@link #capability()} attribute
 * is consulted only for non-DCB descriptors; a DCB descriptor enforces the DCB path.
 * </p>
 *
 * <h4>Mode and Startup Behavior</h4>
 * <p>
 * {@link #mode()} chooses asynchronous delivery (the default, eventually consistent, catching up from history) or
 * synchronous (read-your-writes, updated on the write path in the write transaction). Synchronous mode is mutually
 * exclusive with {@link #startAt()}, {@link #startAtPosition()}, and {@link #resumeBehavior()}.
 * </p>
 * <p>
 * {@link #startupMode()} chooses how startup behaves: the default or background mode may replay history before going
 * live, while {@link StartupMode#WAIT_UNTIL_STARTED} blocks until the projection is fully started.
 * </p>
 *
 * <h4>State Store</h4>
 * <p>
 * The {@link #store()} attribute selects the read-model store by the store bean's type (for example
 * {@code ViewStateRepository.class}, {@code MaterializedView.class}, or a {@code CrudRepository}
 * subinterface). Use {@link #storeName()} to select by bean name instead, or alongside {@link #store()}
 * to disambiguate when several beans of that type exist. With both unset, the store resolves by
 * convention (the unique store bean, otherwise the default Mongo implementation on the blocking stack).
 * </p>
 * <p>
 * A document-backed store (the Mongo default, or a {@code CrudRepository}) persists the view state under the state's
 * own id, following the same convention as the view DSL's {@code materialized(...)}, so the materialized state type
 * must carry an id equal to the projection's {@code id}. A {@code ViewStateRepository} bean is the store-agnostic
 * option when you need full control over how the id maps to the stored key.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Projection {
    /**
     * The unique identifier of the projection (required, no default).
     */
    String id();

    /**
     * Specify the start position as one of the predefined StartPosition values. Mutually exclusive
     * with {@link #startAtPosition()}, which starts from a specific position instead. Also mutually
     * exclusive with {@link #mode()} set to {@link Mode#SYNCHRONOUS}.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific global or DCB position, useful to rewind a durable read model to a
     * known-good position. The default of -1 means unset, in which case {@link #startAt()} is used.
     * Mutually exclusive with a non-{@link StartPosition#DEFAULT} {@link #startAt()} and with
     * {@link Mode#SYNCHRONOUS}.
     */
    long startAtPosition() default -1;

    /**
     * Specify if the resume behavior differs from when the projection is started. By default
     * ({@link ResumeBehavior#DEFAULT}), a projection that starts replaying history does so only
     * the first time and resumes from the last received event on restart. For in-memory read models
     * that do not persist state, combine an early start position with {@link ResumeBehavior#SAME_AS_START_AT}
     * to replay from the beginning on every restart.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * Specify how the projection behaves during startup. The default defers to the framework
     * (typically {@link StartupMode#BACKGROUND} if replaying history, otherwise
     * {@link StartupMode#WAIT_UNTIL_STARTED}).
     */
    StartupMode startupMode() default StartupMode.DEFAULT;

    /**
     * The capability scope for the projection. This is consulted only for non-DCB descriptors:
     * {@link Capability#AGNOSTIC} receives events from all capabilities (stream and DCB),
     * while {@link Capability#STREAM} receives only stream-written events. For DCB descriptors,
     * this attribute is ignored and the DCB path is always used.
     */
    Capability capability() default Capability.AGNOSTIC;

    /**
     * The processing mode for events. {@link Mode#ASYNC} (the default) processes events
     * asynchronously and eventually updates the read model. {@link Mode#SYNCHRONOUS} guarantees
     * read-your-writes semantics: the command that triggered an event sees the projection state
     * immediately. Synchronous mode is mutually exclusive with {@link #startAt()},
     * {@link #startAtPosition()}, and {@link #resumeBehavior()}.
     */
    Mode mode() default Mode.ASYNC;

    /**
     * The read-model store to materialize into, given as the store bean's type (for example
     * {@code MaterializedView.class}, {@code ViewStateRepository.class}, or a concrete
     * {@code CrudRepository} subinterface). {@link Void} (the default) leaves the type unset, in which
     * case {@link #storeName()} or the convention-based resolution applies. When several beans of the
     * given type exist, disambiguate with {@link #storeName()}.
     */
    Class<?> store() default Void.class;

    /**
     * Optional Spring bean name of the read-model store. Used on its own to resolve the store by name,
     * or together with {@link #store()} to pick one bean when several of that type exist. An empty
     * string (the default) means unset. With both {@link #store()} and {@link #storeName()} unset, the
     * store resolves by convention (the unique {@code MaterializedView}, {@code ViewStateRepository},
     * or {@code CrudRepository} bean, otherwise the default Mongo implementation on the blocking stack).
     */
    String storeName() default "";

    /**
     * A set of predefined start positions for a projection.
     */
    enum StartPosition {
        /**
         * Replay the whole event sequence from the beginning before switching to live delivery,
         * so the read model can be rebuilt from history.
         */
        BEGINNING,
        /**
         * Start from now, delivering only events written after the projection starts.
         */
        NOW,
        /**
         * Use the default behavior. Typically this resumes from the last stored position if
         * the projection has run before, otherwise it behaves like NOW.
         */
        DEFAULT
    }

    /**
     * Specifies the capability scope for a projection.
     */
    enum Capability {
        /**
         * The projection receives events from all capabilities (both stream and DCB) on a store
         * that supports both. This is the agnostic, neutral default.
         */
        AGNOSTIC,
        /**
         * The projection receives only stream-written events, ignored on a DCB descriptor.
         */
        STREAM
    }

    /**
     * Specifies the processing mode for a projection.
     */
    enum Mode {
        /**
         * Events are processed asynchronously; the read model is eventually consistent with
         * the command model.
         */
        ASYNC,
        /**
         * Events are processed synchronously before the command returns, providing
         * read-your-writes semantics.
         */
        SYNCHRONOUS
    }
}
