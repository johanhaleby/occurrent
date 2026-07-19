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
 *         .build();
 * }
 * </pre>
 * The Kotlin equivalent is the {@code saga(...) { }} / flow {@code saga { }} block.
 * <p>
 * The method may live on any Spring bean: a {@code @Bean} in a {@code @Configuration}, or a method on a
 * {@code @Component}. This is a blocking-stack feature, the reactive starter does not register {@code @Saga}.
 *
 * <h4>State store</h4>
 * <p>
 * The {@link #store()} attribute selects the {@code SagaStateStore} bean by type, {@link #storeName()} by name (or both to
 * disambiguate). With both unset, the store resolves by convention: the unique {@code SagaStateStore} bean, otherwise the
 * default MongoDB implementation (a {@code saga-<id>} collection), whose state type is read from the factory method's
 * generic return type. A machine-core saga's state serializes with the application's {@code MongoConverter}, like the
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
}
