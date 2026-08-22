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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Presents a reactive subscription model as a blocking one, so the blocking conformance suites can run against it
 * unchanged instead of being rewritten a second time in terms of {@code Mono} and {@code Flux}.
 * <p>
 * This is a test bridge only. It is not a general-purpose adapter and must not be used in production. Every wait
 * blocks the calling thread, which is exactly what a reactive model exists to avoid.
 * <p>
 * The bridge wraps each {@code Consumer<CloudEvent>} action in {@code Mono.fromRunnable(..)}, so the handler runs
 * when the model subscribes to the returned {@code Mono}. It blocks on {@code waitUntilStarted(Duration)} to turn
 * the reactive wait into a blocking one.
 * <p>
 * A bridge cannot see everything. Whether the model actually subscribes to the action's {@code Mono} rather than
 * just assembling it, whether {@code waitUntilStarted()} answers, and what disposing a wait does are invisible
 * once a result has been blocked on. Those parts of the contract are tested separately, by
 * {@link ReactiveSubscriptionModelConformance}.
 */
@NullMarked
public class BlockingSubscriptionOverReactive implements SubscriptionModel, IntrospectableSubscriptions {

    /**
     * Above this, a timeout is treated as "wait forever". The blocking no-arg {@code waitUntilStarted()} default
     * passes {@code ChronoUnit.FOREVER}'s duration, which overflows the millisecond arithmetic inside reactor's
     * {@code timeout(..)} operator, so a wait that long blocks on the unbounded {@code Mono} instead. A year exceeds
     * any bounded wait a suite is allowed to take.
     */
    private static final Duration PRACTICALLY_FOREVER = Duration.ofDays(365);

    /**
     * Bound on blocking for {@code globalCheckpoint()}. The blocking method the bridge implements has no timeout
     * parameter, so the bound has to live here, and an unbounded {@code block()} would hang a suite instead of
     * failing it. Twenty seconds matches the reactive-only suite's budget. The answer is a single command against
     * the store, so a model that has not answered by then is not going to.
     */
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofSeconds(20);

    private final org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel;
    private final org.occurrent.subscription.api.reactor.IntrospectableSubscriptions introspectable;

    private BlockingSubscriptionOverReactive(org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel,
                                             org.occurrent.subscription.api.reactor.IntrospectableSubscriptions introspectable) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "Reactive subscription model cannot be null");
        this.introspectable = requireNonNull(introspectable, "Reactive introspectable subscription model cannot be null");
    }

    /**
     * Bridges a model that is also introspectable, which every reactive model shipping with Occurrent is and what an
     * out-of-tree model is likely to be too.
     */
    public static <T extends org.occurrent.subscription.api.reactor.SubscriptionModel
            & org.occurrent.subscription.api.reactor.IntrospectableSubscriptions> BlockingSubscriptionOverReactive of(T subscriptionModel) {
        requireNonNull(subscriptionModel, "Reactive subscription model cannot be null");
        return new BlockingSubscriptionOverReactive(subscriptionModel, subscriptionModel);
    }

    /**
     * Bridges capabilities that live on different objects.
     */
    public static BlockingSubscriptionOverReactive of(org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel,
                                                      org.occurrent.subscription.api.reactor.IntrospectableSubscriptions introspectable) {
        return new BlockingSubscriptionOverReactive(subscriptionModel, introspectable);
    }

    /**
     * Bridges a model that can also report where the event feed is, so
     * {@code CheckpointAwareSubscriptionModelConformance} can run against it. This is a separate factory rather than
     * behaviour of the plain bridge on purpose. That suite treats implementing the blocking
     * {@link CheckpointAwareSubscriptionModel} as the declaration itself, so a bridge that implemented it
     * unconditionally would drag every bridged model through a suite only checkpoint-aware ones can answer.
     * <p>
     * A reactor model that completes {@code globalCheckpoint()} empty is mapped to the blocking {@code null}, which
     * the blocking interface documents as "an unresolvable problem" and the suite asserts on both branches. That
     * empty completion is a real answer in the wild, not a hypothetical. See issue #517.
     */
    public static <T extends org.occurrent.subscription.api.reactor.SubscriptionModel
            & org.occurrent.subscription.api.reactor.IntrospectableSubscriptions
            & org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel> BlockingSubscriptionOverReactive ofCheckpointAware(T subscriptionModel) {
        requireNonNull(subscriptionModel, "Reactive subscription model cannot be null");
        return new CheckpointAwareBridge(subscriptionModel, subscriptionModel, subscriptionModel);
    }

    private static final class CheckpointAwareBridge extends BlockingSubscriptionOverReactive implements CheckpointAwareSubscriptionModel {

        private final org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel checkpointAware;

        private CheckpointAwareBridge(org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel,
                                      org.occurrent.subscription.api.reactor.IntrospectableSubscriptions introspectable,
                                      org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel checkpointAware) {
            super(subscriptionModel, introspectable);
            this.checkpointAware = checkpointAware;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            // blockOptional rather than block: an empty completion is a documented answer (issue #517), and it maps
            // to the blocking null rather than to an error.
            return checkpointAware.globalCheckpoint().blockOptional(CHECKPOINT_TIMEOUT).orElse(null);
        }
    }

    // Subscribable

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(action, "action cannot be null");
        org.occurrent.subscription.api.reactor.SubscriptionHandle subscription =
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
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
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

    // IntrospectableSubscriptions

    @Override
    public Set<String> subscriptionIds() {
        return introspectable.subscriptionIds();
    }

    private record BlockingSubscriptionOverReactiveSubscription(
            org.occurrent.subscription.api.reactor.SubscriptionHandle subscription) implements SubscriptionHandle {

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
