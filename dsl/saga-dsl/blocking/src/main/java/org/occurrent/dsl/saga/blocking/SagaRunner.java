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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.Saga;
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
 * Runs a {@link Saga} as an asynchronous, subscription-fed process manager: it subscribes to the saga's events, folds and
 * persists per-instance state, dispatches the commands each reaction issues, and polls the state store to fire timeouts.
 * The write-side mirror of the read-side {@code ProjectionRunner}.
 * <p>
 * Pick the capability with the factory: {@link #agnostic(Subscribable, CloudEventConverter) agnostic} delivers both
 * stream-written and DCB-appended events, {@link #stream(Subscribable, CloudEventConverter) stream} only stream-written
 * ones. Timers are polled from the {@link SagaStateStore}, not scheduled through an external scheduler, so a run needs no
 * deadline infrastructure. The returned {@link SagaSubscription} owns the timer poller, close it to stop polling.
 *
 * <h2>Multi-instance timer polling</h2>
 * The event path is already single-active across instances when the {@link Subscribable} is a competing-consumer model.
 * The timer poller is not: without coordination every instance polls the shared store on its own interval, multiplying the
 * query load. Pass a {@link CompetingConsumerStrategy} to
 * {@link #run(String, Saga, SagaStateStore, CommandDispatcher, StartAt, SagaRunnerConfig, CompetingConsumerStrategy) run}
 * and only the instance holding the saga's timer lease polls, the others wake and no-op without touching the store. The
 * lease is keyed by {@link #timerLeaseKey(String)}, distinct from the event subscription's own lease, and released on
 * {@link SagaSubscription#close()} so another instance takes over within roughly one lease period. Without a strategy the
 * poller runs on every instance as before (correct via compare-and-set, just not coordinated).
 *
 * <h2>Failure handling differs between the two input paths</h2>
 * The event path and the timer-poll path do not handle a failed input the same way, and the difference matters when a
 * reaction or a dispatch throws:
 * <ul>
 *   <li><strong>Event path.</strong> An exception (including a {@link SagaConcurrencyException} after the retries are
 *       exhausted) propagates to the subscription model, which redelivers the event and retries the whole step. The event
 *       is not lost, but the subscription is a single ordered channel shared by every instance this saga handles, so an
 *       input that keeps failing blocks the events queued behind it (head-of-line blocking) until it succeeds or the
 *       subscription is intervened on. One poisoned instance can stall the others multiplexed onto the same subscription.</li>
 *   <li><strong>Timer path.</strong> A failing timeout is caught per instance, logged, and left due, so the next poll
 *       retries it while other instances keep progressing; a timeout failure does not block the poller. It also does not
 *       propagate anywhere else, so it is only ever retried by the poller, never by a subscription redelivery.</li>
 * </ul>
 * Because commands are dispatched before the save and a lost compare-and-set retries the step, a single input can
 * re-dispatch its whole command list several times (up to {@code maxCasAttempts}); receivers must be idempotent and
 * tolerate that multiplicity. See {@link SagaRunnerConfig} and {@link SagaConcurrencyException}.
 *
 * @param <E> the domain event type
 * @param <C> the command type
 */
@NullMarked
public final class SagaRunner<E, C> {

    private final Subscribable subscriptionModel;
    private final CloudEventConverter<E> cloudEventConverter;
    private final Function<Filter, SubscriptionFilter> toSubscriptionFilter;

    private SagaRunner(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter, Function<Filter, SubscriptionFilter> toSubscriptionFilter) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.cloudEventConverter = requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        this.toSubscriptionFilter = requireNonNull(toSubscriptionFilter, "toSubscriptionFilter cannot be null");
    }

    /** A runner whose subscription is capability-agnostic: it delivers both stream-written and DCB-appended events. */
    public static <E, C> SagaRunner<E, C> agnostic(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new SagaRunner<>(subscriptionModel, cloudEventConverter, AgnosticSubscriptionFilter::filter);
    }

    /** A runner whose subscription is scoped to the {@code STREAM} capability, excluding DCB-appended events. */
    public static <E, C> SagaRunner<E, C> stream(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new SagaRunner<>(subscriptionModel, cloudEventConverter, StreamSubscriptionFilter::filter);
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
     * Runs {@code saga}: subscribes with a filter derived from the saga's handled event types, materializes per-instance
     * state into {@code stateStore}, dispatches issued commands through {@code commandDispatcher}, and starts a timer
     * poller that runs on every instance. The returned {@link SagaSubscription} is already started.
     */
    public <S extends @Nullable Object> SagaSubscription run(String subscriptionId, Saga<E, S, C> saga,
                                                             SagaStateStore<S> stateStore, CommandDispatcher<C> commandDispatcher,
                                                             @Nullable StartAt startAt, SagaRunnerConfig config) {
        return run(subscriptionId, saga, stateStore, commandDispatcher, startAt, config, null);
    }

    /**
     * Runs {@code saga} with the timer poller gated by {@code competingConsumerStrategy}: only the instance that currently
     * holds the saga's timer lease polls the store for due timers, the others wake on their interval and no-op without
     * querying it. Pass {@code null} to poll on every instance (the behavior of the shorter overloads). The lease is keyed
     * by {@link #timerLeaseKey(String)} and released on {@link SagaSubscription#close()}. Everything else matches
     * {@link #run(String, Saga, SagaStateStore, CommandDispatcher, StartAt, SagaRunnerConfig)}.
     */
    public <S extends @Nullable Object> SagaSubscription run(String subscriptionId, Saga<E, S, C> saga,
                                                             SagaStateStore<S> stateStore, CommandDispatcher<C> commandDispatcher,
                                                             @Nullable StartAt startAt, SagaRunnerConfig config,
                                                             @Nullable CompetingConsumerStrategy competingConsumerStrategy) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(stateStore, "stateStore cannot be null");
        requireNonNull(commandDispatcher, "commandDispatcher cannot be null");
        requireNonNull(config, "config cannot be null");

        SagaExecution<E, S, C> execution = new SagaExecution<>(saga, stateStore, commandDispatcher, cloudEventConverter, config);
        SubscriptionFilter filter = toSubscriptionFilter.apply(SagaFilters.filterFor(cloudEventConverter, saga));
        Consumer<CloudEvent> action = execution::onCloudEvent;
        StartAt effectiveStartAt = startAt != null ? startAt : StartAt.subscriptionModelDefault();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter, effectiveStartAt, action);
        subscription.waitUntilStarted();

        // Register the poller as a competing consumer under its own lease key, then poll only while this instance holds it.
        // hasLock is an in-memory check the strategy's background refresh maintains, so a standby instance costs no query.
        final String leaseKey;
        final String holderId;
        final Runnable pollTask;
        if (competingConsumerStrategy != null) {
            leaseKey = timerLeaseKey(subscriptionId);
            holderId = UUID.randomUUID().toString();
            competingConsumerStrategy.registerCompetingConsumer(leaseKey, holderId);
            pollTask = () -> {
                if (competingConsumerStrategy.hasLock(leaseKey, holderId)) {
                    execution.pollTimers();
                }
            };
        } else {
            leaseKey = null;
            holderId = null;
            pollTask = execution::pollTimers;
        }

        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("occurrent-saga-timer-" + subscriptionId));
        long intervalMillis = config.timerPollInterval().toMillis();
        poller.scheduleWithFixedDelay(pollTask, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        return new SagaSubscription(subscription, poller, competingConsumerStrategy, leaseKey, holderId);
    }

    /**
     * The competing-consumer lease key the timer poller uses for {@code subscriptionId}. Namespaced with a {@code saga-timer:}
     * prefix so it never collides with the event subscription's own lease (keyed by the raw subscription id), which would
     * otherwise make the poller lose that lock on every instance and never fire a timer.
     */
    public static String timerLeaseKey(String subscriptionId) {
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
