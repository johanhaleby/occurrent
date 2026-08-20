/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.push.blocking.PushObserver;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import static java.util.Objects.requireNonNull;

/**
 * The wiring ADR 133 decision 1 requires between a {@link PushSubscriptionModel} and {@link KafkaCloudEventBridge}.
 * The model's {@link PushObserver} is a constructor argument with no way to attach one afterwards, while the bridge
 * is built from a model that already exists, so neither can hand the other its {@link RoutingOutcome} directly.
 * Construct one instance of this, pass it to both:
 *
 * <pre>{@code
 * RoutingOutcomeChannel channel = new RoutingOutcomeChannel();
 * PushSubscriptionModel model = new PushSubscriptionModel(dataFieldReader, channel);
 * KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig, model, channel)
 *         .resolver(resolver)
 *         .build();
 * }</pre>
 * <p>
 * and the bridge reads the outcome of the {@code accept(...)} call it just made off the same instance, rather than
 * needing the model to return one.
 * <p>
 * The outcome is captured per calling thread rather than in a single shared field, so correctness never depends on
 * how the bridge happens to run its poll loop. It only has to hold for the one thread currently inside
 * {@code accept(...)}.
 * <p>
 * Delegates to another {@link PushObserver}, {@link PushObserver#noop()} by default, so an application with its own
 * diagnostics keeps them instead of having to choose between this wiring and its own observer.
 * <p>
 * Logic-identical to {@code org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel}, its package and the
 * transport-specific names in its javadoc aside, rather than a shared class, since the wiring is per-transport by
 * ADR 133 decision 1 and this module has no dependency on the RabbitMQ one to reuse its copy from. Not worth
 * promoting into {@code occurrent-broker-api-blocking} for two transports. Reconsider if a third one needs it too.
 */
public final class RoutingOutcomeChannel implements PushObserver {

    private final PushObserver delegate;
    private final ThreadLocal<@Nullable RoutingOutcome> captured = new ThreadLocal<>();

    /**
     * Creates a channel with no delegate observer of its own.
     */
    public RoutingOutcomeChannel() {
        this(PushObserver.noop());
    }

    /**
     * Creates a channel that also reports every outcome to {@code delegate}, for an application that wants its own
     * {@link PushObserver} diagnostics alongside this wiring.
     */
    public RoutingOutcomeChannel(PushObserver delegate) {
        this.delegate = requireNonNull(delegate, PushObserver.class.getSimpleName() + " cannot be null");
    }

    @Override
    public void observe(CloudEvent cloudEvent, RoutingOutcome outcome) {
        // Captured before delegating, so a delegate that throws (caught and logged by PushSubscriptionModel, see
        // PushObserver) still leaves the real outcome behind for the bridge to read.
        captured.set(outcome);
        delegate.observe(cloudEvent, outcome);
    }

    /**
     * The {@link RoutingOutcome} the most recent {@code accept(...)} call on the calling thread reported, and clears
     * it, so a call made without having just called {@code accept(...)} on this thread reads {@code null} rather
     * than a stale answer left over from an earlier one.
     */
    @Nullable RoutingOutcome takeLastOutcome() {
        RoutingOutcome outcome = captured.get();
        captured.remove();
        return outcome;
    }
}
