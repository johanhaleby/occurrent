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

package org.occurrent.dsl.saga.blocking;

import org.occurrent.command.CommandDispatcher;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link Saga} as an asynchronous process manager fed by a subscription. It subscribes to the saga's events,
 * builds and persists per-instance state, dispatches the commands each reaction issues, and polls the state store for
 * due timers. It is the write-side counterpart of the read-side {@code ProjectionRunner}.
 * <p>
 * Pick the capability with the factory. {@link #agnostic(Subscribable, CloudEventConverter) agnostic} delivers both
 * stream-written and DCB-appended events, {@link #stream(Subscribable, CloudEventConverter) stream} delivers only the
 * stream-written ones. So to receive DCB-appended events, use {@code agnostic}. There is no DCB-only factory, because
 * DCB filtering is expressed as a {@code DcbCriteria} rather than the Occurrent {@link Filter} that a saga's handled
 * event types produce. Timers are polled from the {@link SagaStateStore} rather than scheduled through an external
 * scheduler, so a run needs no extra deadline infrastructure. The returned {@link SagaSubscription} owns the timer
 * poller, so close it to stop polling.
 *
 * <h2>Multi-instance timer polling</h2>
 * When the {@link Subscribable} is a competing-consumer model, only one instance handles the event path at a time. The
 * timer poller is not coordinated that way. Without help, every instance polls the shared store on its own interval and
 * multiplies the query load. Set a {@link CompetingConsumerStrategy} with
 * {@link #competingConsumerStrategy(CompetingConsumerStrategy)} and only the instance holding the saga's timer lease
 * polls. The others wake on their interval and do nothing without
 * touching the store. The lease uses its own key, separate from the event subscription's lease, and is released on
 * {@link SagaSubscription#close()} so another instance takes over within roughly one lease period. Without a strategy
 * the poller runs on every instance (still correct through compare-and-set, just not coordinated).
 *
 * <h2>Failure handling differs between the two input paths</h2>
 * A failed input is handled differently on the event path and the timer path, and the difference matters when a
 * reaction or a dispatch throws.
 * <ul>
 *   <li><strong>Event path.</strong> An exception (including a {@link SagaConcurrencyException} once the retries are
 *       exhausted) propagates to the subscription model, which redelivers the event and retries the whole step. The
 *       event is not lost. But the subscription is a single ordered channel shared by every instance this saga handles,
 *       so an input that keeps failing blocks the events queued behind it until it succeeds or someone intervenes. One
 *       poisoned instance can stall the others on the same subscription.</li>
 *   <li><strong>Timer path.</strong> A failing timeout is caught per instance, logged, and left due, so the next poll
 *       retries it while the other instances keep going. A timeout failure does not block the poller and does not
 *       propagate anywhere else, so only the poller ever retries it, never a subscription redelivery.</li>
 * </ul>
 * Commands are dispatched before the save, and a lost compare-and-set retries the step, so a single input can
 * re-dispatch its whole command list several times (up to {@code maxCasAttempts}). Receivers must be idempotent and
 * tolerate that. See {@link SagaRunnerConfig} and {@link SagaConcurrencyException}.
 *
 * @param <E> the domain event type
 * @param <C> the command type
 */
@NullMarked
public final class SagaRunner<E, C> {

    private final Subscribable subscriptionModel;
    private final CloudEventConverter<E> cloudEventConverter;
    private final Function<Filter, SubscriptionFilter> toSubscriptionFilter;
    private final @Nullable CompetingConsumerStrategy competingConsumerStrategy;

    private SagaRunner(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter, Function<Filter, SubscriptionFilter> toSubscriptionFilter,
                       @Nullable CompetingConsumerStrategy competingConsumerStrategy) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.cloudEventConverter = requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        this.toSubscriptionFilter = requireNonNull(toSubscriptionFilter, "toSubscriptionFilter cannot be null");
        this.competingConsumerStrategy = competingConsumerStrategy;
    }

    /** A runner whose subscription is capability-agnostic: it delivers both stream-written and DCB-appended events. */
    public static <E, C> SagaRunner<E, C> agnostic(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new SagaRunner<>(subscriptionModel, cloudEventConverter, AgnosticSubscriptionFilter::filter, null);
    }

    /** A runner whose subscription is scoped to the {@code STREAM} capability, excluding DCB-appended events. */
    public static <E, C> SagaRunner<E, C> stream(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new SagaRunner<>(subscriptionModel, cloudEventConverter, StreamSubscriptionFilter::filter, null);
    }

    /**
     * Returns a copy of this runner whose saga timer poller is coordinated by {@code strategy}. Only the instance that
     * currently holds the saga's timer lease polls the store for due timers, the others wake on their interval and do
     * nothing without querying it. Without a strategy (the default) every instance polls. The lease is released on
     * {@link SagaSubscription#close()}.
     */
    public SagaRunner<E, C> competingConsumerStrategy(CompetingConsumerStrategy strategy) {
        return new SagaRunner<>(subscriptionModel, cloudEventConverter, toSubscriptionFilter,
                requireNonNull(strategy, "competingConsumerStrategy cannot be null"));
    }

    /** Runs {@code saga} with the default configuration, starting at the subscription model's default position. */
    public <S extends @Nullable Object> SagaSubscription run(String subscriptionId, Saga<E, S, C> saga,
                                                             SagaStateStore<S> stateStore, CommandDispatcher<C> commandDispatcher) {
        return run(subscriptionId, saga, stateStore, commandDispatcher, null, SagaRunnerConfig.defaults());
    }

    /** Runs {@code saga} with the default configuration, starting at {@code startAt} ({@code null} means the model's default). */
    public <S extends @Nullable Object> SagaSubscription run(String subscriptionId, Saga<E, S, C> saga,
                                                             SagaStateStore<S> stateStore, CommandDispatcher<C> commandDispatcher,
                                                             @Nullable StartAt startAt) {
        return run(subscriptionId, saga, stateStore, commandDispatcher, startAt, SagaRunnerConfig.defaults());
    }

    /**
     * Runs {@code saga}. It subscribes with a filter derived from the saga's handled event types, builds per-instance
     * state in {@code stateStore}, dispatches issued commands through {@code commandDispatcher}, and starts a timer
     * poller. If a {@link #competingConsumerStrategy(CompetingConsumerStrategy) competing-consumer strategy} was set on
     * this runner, only the instance holding the saga's timer lease polls the store, otherwise every instance polls.
     * The returned {@link SagaSubscription} is already started, and releases the lease on {@link SagaSubscription#close()}.
     */
    public <S extends @Nullable Object> SagaSubscription run(String subscriptionId, Saga<E, S, C> saga,
                                                             SagaStateStore<S> stateStore, CommandDispatcher<C> commandDispatcher,
                                                             @Nullable StartAt startAt, SagaRunnerConfig config) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(stateStore, "stateStore cannot be null");
        requireNonNull(commandDispatcher, "commandDispatcher cannot be null");
        requireNonNull(config, "config cannot be null");

        SagaExecution<E, S, C> execution = new SagaExecution<>(subscriptionId, saga, stateStore, commandDispatcher, cloudEventConverter, config);
        SubscriptionFilter filter = toSubscriptionFilter.apply(SagaFilters.filterFor(cloudEventConverter, saga));
        Consumer<CloudEvent> action = execution::onCloudEvent;
        StartAt effectiveStartAt = startAt != null ? startAt : StartAt.subscriptionModelDefault();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter, effectiveStartAt, action);
        subscription.waitUntilStarted();

        // Register the poller as a competing consumer under its own lease key, then poll only while this instance holds it.
        // hasLock is an in-memory check the strategy's background refresh maintains, so a standby instance costs no query.
        final @Nullable CompetingConsumerStrategy strategy = competingConsumerStrategy;
        final @Nullable String leaseKey;
        final @Nullable String holderId;
        final Runnable pollTask;
        if (strategy != null) {
            String key = timerLeaseKey(subscriptionId);
            String holder = UUID.randomUUID().toString();
            strategy.registerCompetingConsumer(key, holder);
            pollTask = () -> {
                if (strategy.hasLock(key, holder)) {
                    execution.pollTimers();
                }
            };
            leaseKey = key;
            holderId = holder;
        } else {
            leaseKey = null;
            holderId = null;
            pollTask = execution::pollTimers;
        }

        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("occurrent-saga-timer-" + subscriptionId));
        long intervalMillis = config.timerPollInterval().toMillis();
        poller.scheduleWithFixedDelay(pollTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        return new SagaSubscription(subscription, poller, SagaInstances.of(stateStore), strategy, leaseKey, holderId);
    }

    /**
     * The competing-consumer lease key the timer poller uses for {@code subscriptionId}. The {@code saga-timer:} prefix
     * keeps it separate from the event subscription's own lease, which uses the raw subscription id. Without the prefix
     * the two would share a key, and the poller would lose the lock on every instance and never run.
     */
    static String timerLeaseKey(String subscriptionId) {
        return "saga-timer:" + subscriptionId;
    }

    private static ThreadFactory daemonThreadFactory(String namePrefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, namePrefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
