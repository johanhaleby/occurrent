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

package org.occurrent.tck.subscription.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Presents a reactive subscription model as a blocking one, so the blocking conformance suites can be run against it
 * unchanged instead of being written a second time in terms of {@code Mono} and {@code Flux}.
 * <p>
 * This is a test bridge and nothing more. It is not a general-purpose adapter and must not be used in production:
 * every wait blocks the calling thread, which is exactly what a reactive model exists to avoid.
 * <p>
 * The translation is small because reactor {@code SubscriptionModelLifeCycle} already returns plain values, a
 * deliberate choice its own javadoc records. What actually differs is the action type,
 * {@code Function<CloudEvent, Mono<Void>>} against {@code Consumer<CloudEvent>}, and the two waits: this bridge wraps
 * the consumer in {@code Mono.fromRunnable(..)} so the handler runs when the model subscribes to the returned
 * {@code Mono}, and blocks on {@code waitUntilStarted(Duration)}.
 * <p>
 * A bridge cannot see everything. Whether the model subscribes to the action's {@code Mono} rather than merely
 * assembling it, whether {@code waitUntilStarted()} answers, and what disposing a wait does are invisible once a
 * result has been blocked on. Those are the reactive contract, not the behavioural one, and they belong to
 * {@link ReactiveSubscriptionModelConformance} rather than here.
 */
@NullMarked
public final class BlockingSubscriptionOverReactive implements SubscriptionModel, IntrospectableSubscriptionModel {

    /**
     * Above this, a timeout is treated as "wait forever". The blocking no-arg {@code waitUntilStarted()} default
     * passes {@code ChronoUnit.FOREVER}'s duration, which overflows the millisecond arithmetic inside reactor's
     * {@code timeout(..)} operator, so a wait that long blocks on the unbounded {@code Mono} instead. A year exceeds
     * any bounded wait a suite is allowed to take.
     */
    private static final Duration PRACTICALLY_FOREVER = Duration.ofDays(365);

    private final org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel;
    private final org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel introspectable;

    private BlockingSubscriptionOverReactive(org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel,
                                             org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel introspectable) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "Reactive subscription model cannot be null");
        this.introspectable = requireNonNull(introspectable, "Reactive introspectable subscription model cannot be null");
    }

    /**
     * Bridges a model that is also introspectable, which every reactive model shipping with Occurrent is and what an
     * out-of-tree model is likely to be too.
     */
    public static <T extends org.occurrent.subscription.api.reactor.SubscriptionModel
            & org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel> BlockingSubscriptionOverReactive of(T subscriptionModel) {
        requireNonNull(subscriptionModel, "Reactive subscription model cannot be null");
        return new BlockingSubscriptionOverReactive(subscriptionModel, subscriptionModel);
    }

    /**
     * Bridges capabilities that live on different objects.
     */
    public static BlockingSubscriptionOverReactive of(org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel,
                                                      org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel introspectable) {
        return new BlockingSubscriptionOverReactive(subscriptionModel, introspectable);
    }

    // Subscribable

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(action, "action cannot be null");
        org.occurrent.subscription.api.reactor.Subscription subscription =
                subscriptionModel.subscribe(subscriptionId, filter, startAt, cloudEvent -> Mono.fromRunnable(() -> action.accept(cloudEvent)));
        return new BlockingSubscriptionOverReactiveSubscription(subscription);
    }

    // SubscriptionModelLifeCycle

    @Override
    public void stop() {
        subscriptionModel.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        subscriptionModel.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return subscriptionModel.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return subscriptionModel.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return subscriptionModel.isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return new BlockingSubscriptionOverReactiveSubscription(subscriptionModel.resumeSubscription(subscriptionId));
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        subscriptionModel.pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        subscriptionModel.cancelSubscription(subscriptionId);
    }

    @Override
    public void shutdown() {
        subscriptionModel.shutdown();
    }

    // IntrospectableSubscriptionModel

    @Override
    public Set<String> subscriptionIds() {
        return introspectable.subscriptionIds();
    }

    private record BlockingSubscriptionOverReactiveSubscription(
            org.occurrent.subscription.api.reactor.Subscription subscription) implements Subscription {

        private BlockingSubscriptionOverReactiveSubscription {
            requireNonNull(subscription, "Reactive subscription cannot be null");
        }

        @Override
        public String id() {
            return subscription.id();
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            requireNonNull(timeout, "timeout cannot be null");
            if (timeout.compareTo(PRACTICALLY_FOREVER) >= 0) {
                subscription.waitUntilStarted().block();
                return true;
            }
            return Boolean.TRUE.equals(subscription.waitUntilStarted(timeout).block());
        }
    }
}
