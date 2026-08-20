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
 * is consulted only for non-DCB descriptors. A DCB descriptor enforces the DCB path.
 * </p>
 *
 * <h4>Mode and Startup Behavior</h4>
 * <p>
 * {@link #mode()} chooses asynchronous delivery (the default, eventually consistent, catching up from history) or
 * synchronous (read-your-writes, updated on the write path in the write transaction). Synchronous mode is mutually
 * exclusive with {@link #startAt()}, {@link #startAtGlobalPosition()}, {@link #resumeBehavior()}, and {@link #startupMode()}.
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
     * The unique identifier of the projection (required, no default). It is the durable checkpoint key and the
     * namespace for the zero-config store, and must be unique across all subscriptions, projections, and snapshots.
     * <p>
     * For a single-instance projection it is also the key the one view state is stored under, since such a projection
     * has no id function to derive a key from. So a read model declared {@code @Projection(id = "is-username-claimed")}
     * is found under {@code "is-username-claimed"}, not under anything taken from the events.
     */
    String id();

    /**
     * Specify the start position as one of the predefined StartPosition values. Mutually exclusive
     * with {@link #startAtGlobalPosition()}, which starts from a specific position instead. Also mutually
     * exclusive with {@link #mode()} set to {@link Mode#SYNCHRONOUS}.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific global or DCB position, useful to rewind a durable read model to a
     * known-good position. The default of -1 means unset, in which case {@link #startAt()} is used.
     * Mutually exclusive with a non-{@link StartPosition#DEFAULT} {@link #startAt()} and with
     * {@link Mode#SYNCHRONOUS}.
     */
    long startAtGlobalPosition() default -1;

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
     * <p>
     * A {@link Source#PUSH} projection is the exception: its catch-up always replays from the beginning, so reading
     * {@link StartupMode#DEFAULT} as "background because it replays history" would move every push projection off the
     * startup path. {@code DEFAULT} therefore waits there, and only an explicit {@link StartupMode#BACKGROUND} does
     * not.
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
     * {@link #startAtGlobalPosition()}, {@link #resumeBehavior()}, and {@link #startupMode()}.
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
     * Where the projection reads its events from. {@link Source#EVENT_STORE} (the default) uses the framework's
     * asynchronous catch-up and durable subscription models. {@link Source#PUSH} feeds the projection from an external
     * push feed (RabbitMQ, Kafka, ...) instead, wrapped in a replay-then-push catch-up unless {@link #catchup()} is
     * {@link Catchup#NONE}. Select the feed bean with {@link #subscriptionModel()} or {@link #subscriptionModelName()}.
     * Its type decides how live events are delivered (a {@code PushSubscriptionModel} delivers CloudEvents, a
     * {@code DomainEventFeed} delivers domain events directly). A push source is mutually exclusive with
     * {@link Mode#SYNCHRONOUS}, and it rejects {@link #startAt()}, {@link #startAtGlobalPosition()} and
     * {@link #resumeBehavior()}, since the catch-up always replays from the beginning and where the live feed resumes
     * is the broker's responsibility. {@link #startupMode()} <em>is</em> supported under the default {@link #catchup()}:
     * set it to {@link StartupMode#BACKGROUND} to keep that replay off the startup path.
     */
    Source source() default Source.EVENT_STORE;

    /**
     * Whether a {@link Source#PUSH} projection is backfilled from the event store before it goes live.
     * {@link Catchup#FROM_EVENT_STORE} (the default) replays history once and hands over. {@link Catchup#NONE} takes
     * live events only and touches no event store at all, which is what a projection fed by another application's
     * broker needs, since the local event store holds none of those events. With a {@code PushSubscriptionModel} feed
     * that skips the catch-up wrapper entirely, and with a {@code DomainEventFeed} the projection registers and goes
     * live without ever reading history. {@link #startupMode()} is rejected together with {@link Catchup#NONE}, since
     * there is no replay for it to move off the startup path. Setting this on a {@link Source#EVENT_STORE} projection
     * is rejected, since that projection chooses its history with {@link #startAt()} instead.
     */
    Catchup catchup() default Catchup.FROM_EVENT_STORE;

    /**
     * The push feed bean to feed this projection when {@link #source()} is {@link Source#PUSH}, given as the bean's type
     * (a {@code PushSubscriptionModel} for CloudEvents, or a {@code DomainEventFeed} for domain events). {@link Void}
     * (the default) leaves the type unset, in which case {@link #subscriptionModelName()} or the unique push feed bean
     * applies. When several beans of the given type exist, disambiguate with {@link #subscriptionModelName()}. Ignored
     * for {@link Source#EVENT_STORE}.
     */
    Class<?> subscriptionModel() default Void.class;

    /**
     * Optional Spring bean name of the push feed bean, used when {@link #source()} is {@link Source#PUSH}. Used on its
     * own to resolve the feed by name, or together with {@link #subscriptionModel()} to pick one bean when several of
     * that type exist. An empty string (the default) means unset. Ignored for {@link Source#EVENT_STORE}.
     */
    String subscriptionModelName() default "";

    /**
     * Whether this projection records every append it applies into the {@code AppliedAppendStore} bean, so a caller
     * can ask whether this projection has applied a particular append
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
     * {@code false} by default.
     * <p>
     * Refused at startup when no {@code AppliedAppendStore} bean exists, and when combined with
     * {@link Mode#SYNCHRONOUS}, since a synchronous projection already updates inside the write and answers
     * read-your-writes without this. Recording is skipped, quietly, for an event with no {@code appendid} extension:
     * one written before this feature existed, or one delivered through a push feed whose producer supplied none.
     * <p>
     * Nothing is recorded while the projection is replaying. Whether that can be told apart from live delivery, and
     * what happens to previously recorded appends after a replay, depends on the composition; see ADR 132 decisions
     * 6 through 9. Configure the store's retention, the wait's poll pacing, and this feature's own replay-detection
     * poll under {@code occurrent.projection.applied-append}.
     * <p>
     * Whether a rebuilt read model clears its recorded memberships automatically depends on {@link #startAt()} as
     * well as the composition. The default start position never replays, so a projection left there records but
     * never clears on its own, and a startup warning names it. Set {@link #startAt()} to {@link StartPosition#BEGINNING}
     * to have a rebuild replay and clear correctly, or clear stale memberships with an operator step instead.
     * <p>
     * Recording happens after the first handled event that carries an append id, not after every event the append
     * wrote. An append whose events this projection handles across several deliveries can therefore have
     * {@code hasApplied}/{@code waitUntilApplied} answer {@code true} while some of those deliveries are still
     * unapplied. See ADR 132 decision 10 for why, and for the delay's three different sizes.
     */
    boolean recordAppliedAppends() default false;
}
