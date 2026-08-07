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
 * Marks a no-arg factory method returning a {@code Saga} (an event-driven process manager), registering it as a
 * framework-managed saga: the framework subscribes to its events, persists per-instance state, dispatches the commands it
 * issues, and fires its timeouts. For example:
 *
 * <pre lang="java">
 * &#64;Saga(id = "orderFulfillment")
 * Saga&lt;OrderEvent, OrderSagaState, OrderCommand&gt; orderFulfillment() {
 *     return Saga.&lt;OrderEvent, OrderSagaState, OrderCommand&gt;builder(OrderSagaState.EMPTY)
 *         .correlateAll(OrderEvent::orderId)
 *         .startsOn(OrderPlaced.class)
 *         .evolve(OrderPlaced.class, (state, event) -> state.placed(event))
 *         .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.startTimeout("payment", Duration.ofMinutes(30))))
 *         .evolve(PaymentReserved.class, (state, event) -> state.paid())
 *         .react(PaymentReserved.class, (state, event) -> List.of(SagaEffect.cancelTimeout("payment"), SagaEffect.issue(new ShipOrder(event.orderId()))))
 *         .reactOnTimeout("payment", (state, timeout) -> List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId()))))
 *         .build();
 * }
 * </pre>
 * The Kotlin equivalent is the {@code saga(...) { }} / flow {@code saga { }} block.
 * <p>
 * The method may live on any Spring bean: a {@code @Bean} in a {@code @Configuration}, or a method on a
 * {@code @Component}. This is a blocking-stack feature, the reactive starter does not register {@code @Saga}.
 * <p>
 * The two input paths fail differently. A failing event propagates to the subscription, which redelivers and retries,
 * since that subscription is a single ordered channel shared by every instance of this saga, an event that keeps failing
 * blocks the events behind it (head-of-line blocking) until it clears. A failing timeout is caught per instance, logged,
 * and left due for the next poll, so it never blocks other instances. Commands are dispatched before the state is saved
 * and a lost save retries the step, so a receiver must be idempotent and tolerate the same command arriving more than once
 * per input.
 *
 * <h4>State store</h4>
 * <p>
 * The {@link #store()} attribute selects the {@code SagaStateStore} bean by type, {@link #storeName()} by name (or both to
 * disambiguate). With both unset, the store resolves by convention: the unique {@code SagaStateStore} bean, otherwise the
 * default MongoDB implementation (a {@code saga-<id>} collection), whose state type is read from the factory method's
 * generic return type. A core saga's state serializes with the application's {@code MongoConverter}, like the
 * snapshot store. A flow saga's received events serialize as CloudEvents through the {@code CloudEventConverter}, so they
 * persist by their stable {@code CloudEventTypeMapper} type and a domain event can move to a different package without
 * breaking in-flight saga state.
 * </p>
 *
 * <h4>Command dispatcher</h4>
 * <p>
 * The saga issues user command types, so it needs a {@code CommandDispatcher} bean to run them, resolved from
 * {@link #commandDispatcher()}/{@link #commandDispatcherName()} or the unique {@code CommandDispatcher} bean. There is no
 * default: a dispatcher is usually a lambda over an {@code ApplicationService}, with or without a decider.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Saga {
    /**
     * The unique identifier of the saga (required, no default). It is the durable subscription/checkpoint key.
     */
    String id();

    /**
     * Specify the start position as one of the predefined StartPosition values. Mutually exclusive with
     * {@link #startAtGlobalPosition()}.
     */
    StartPosition startAt() default StartPosition.DEFAULT;

    /**
     * Start after a specific global position. The default of -1 means unset, in which case {@link #startAt()} is used.
     * Mutually exclusive with a non-{@link StartPosition#DEFAULT} {@link #startAt()}.
     */
    long startAtGlobalPosition() default -1;

    /**
     * Specify if the resume behavior differs from when the saga is started. By default ({@link ResumeBehavior#DEFAULT}),
     * a saga that starts replaying history does so only the first time and resumes from the last received event on
     * restart.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * Specify how the saga behaves during startup. The default defers to the framework.
     */
    StartupMode startupMode() default StartupMode.DEFAULT;

    /**
     * The capability scope for the saga. {@link Capability#AGNOSTIC} (the default) receives events from all capabilities
     * (stream and DCB), while {@link Capability#STREAM} receives only stream-written events.
     */
    Capability capability() default Capability.AGNOSTIC;

    /**
     * The {@code SagaStateStore} to persist instances into, given as the store bean's type. {@link Void} (the default)
     * leaves the type unset, in which case {@link #storeName()} or convention-based resolution applies. When several
     * beans of the given type exist, disambiguate with {@link #storeName()}.
     */
    Class<?> store() default Void.class;

    /**
     * Optional Spring bean name of the {@code SagaStateStore}. Used on its own to resolve the store by name, or together
     * with {@link #store()} to pick one bean when several of that type exist. An empty string (the default) means unset.
     * With both unset the store resolves by convention (the unique {@code SagaStateStore} bean, otherwise the default
     * MongoDB implementation).
     */
    String storeName() default "";

    /**
     * The {@code CommandDispatcher} that runs the commands the saga issues, given as the bean's type. {@link Void} (the
     * default) leaves the type unset, in which case {@link #commandDispatcherName()} or the unique
     * {@code CommandDispatcher} bean applies.
     */
    Class<?> commandDispatcher() default Void.class;

    /**
     * Optional Spring bean name of the {@code CommandDispatcher}. Used on its own to resolve the dispatcher by name, or
     * together with {@link #commandDispatcher()} to pick one bean when several of that type exist. An empty string (the
     * default) means unset.
     */
    String commandDispatcherName() default "";

    /**
     * Where the saga reads its events from. {@link Source#EVENT_STORE} (the default) uses the framework's asynchronous
     * catch-up and durable subscription models. {@link Source#PUSH} feeds the saga from an external push feed
     * (RabbitMQ, Kafka, ...) instead, wrapped in a replay-then-push catch-up. Select the feed bean with
     * {@link #subscriptionModel()} or {@link #subscriptionModelName()}. Unlike {@link Projection}, only a
     * {@code PushSubscriptionModel} is accepted, since a {@code DomainEventFeed} carries no stream metadata and a saga
     * needs it, see below.
     * <p>
     * A push saga catches up before it goes live: the framework puts a replay in front of the feed, works through the
     * event store's history, and then hands over to the live feed. Set {@link #catchup()} to {@link Catchup#NONE} to
     * skip the replay and take live events only, which is what a saga fed by another application's broker needs, since
     * the local event store holds none of those events.
     * <p>
     * Neither choice takes a start position, so {@link #startAt()}, {@link #startAtGlobalPosition()} and
     * {@link #resumeBehavior()} are rejected rather than silently ignored. The replay always starts at the beginning,
     * and where the live feed resumes after a restart is the broker's business, not Occurrent's.
     * {@link #startupMode()} is the exception, and applies under the default {@link Catchup#FROM_EVENT_STORE}, where
     * {@link StartupMode#BACKGROUND} keeps the replay off the startup path. It is rejected with {@link Catchup#NONE},
     * where there is no replay to wait for.
     * <p>
     * <strong>Forward the Occurrent CloudEvent extensions from your listener.</strong> A saga recognises a redelivered
     * event by its {@code streamid} together with its {@code streamversion}, or by its {@code position}. Push delivery
     * is at-least-once, so an event arriving with none of those would be reacted to a second time and its commands
     * issued a second time. The catch-up leg is safe either way, because it replays from the event store, whose events
     * always carry them. It is the live leg that depends on what the listener forwards, so the saga refuses an event
     * that carries none of them unless {@link #redeliveryDetection()} says otherwise.
     */
    Source source() default Source.EVENT_STORE;

    /**
     * Whether a {@link Source#PUSH} saga is backfilled from the event store before it goes live.
     * {@link Catchup#FROM_EVENT_STORE} (the default) replays history once and hands over,
     * {@link Catchup#NONE} goes straight to the live feed and needs no event store at all. Setting this on a
     * {@link Source#EVENT_STORE} saga is rejected, since that saga chooses its history with {@link #startAt()}.
     */
    Catchup catchup() default Catchup.FROM_EVENT_STORE;

    /**
     * What a {@link Source#PUSH} saga does with an event it cannot recognise a redelivery of, one carrying neither a
     * {@code streamid} with a {@code streamversion} nor a {@code position}. {@link RedeliveryDetection#REQUIRED} (the
     * default) refuses it, so the feed that dropped the metadata announces itself rather than quietly costing the saga
     * its redelivery protection. {@link RedeliveryDetection#BEST_EFFORT} reacts to it anyway, warning once, for a feed
     * that genuinely carries none of it and a saga whose commands are all safe to receive more than once. Setting this on a
     * {@link Source#EVENT_STORE} saga is rejected, since the event store's own events always carry the metadata and
     * there would be nothing for it to change.
     */
    RedeliveryDetection redeliveryDetection() default RedeliveryDetection.REQUIRED;

    /**
     * The {@code PushSubscriptionModel} bean to feed this saga when {@link #source()} is {@link Source#PUSH}, given as
     * the bean's type. {@link Void} (the default) leaves the type unset, in which case {@link #subscriptionModelName()}
     * or the unique {@code PushSubscriptionModel} bean applies. When several beans of the given type exist,
     * disambiguate with {@link #subscriptionModelName()}. Ignored for {@link Source#EVENT_STORE}.
     */
    Class<?> subscriptionModel() default Void.class;

    /**
     * Optional Spring bean name of the {@code PushSubscriptionModel}, used when {@link #source()} is
     * {@link Source#PUSH}. Used on its own to resolve the feed by name, or together with {@link #subscriptionModel()}
     * to pick one bean when several of that type exist. An empty string (the default) means unset. Ignored for
     * {@link Source#EVENT_STORE}.
     */
    String subscriptionModelName() default "";
}
