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

package org.occurrent.application.service.reactor.generic;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ExecuteOptions;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The reactive counterpart of the blocking {@code GenericApplicationService}. It reads a stream, applies the events to a
 * pure domain function, writes the produced events with optimistic concurrency, and runs an optional reactive
 * side-effect after the write, retrying from a fresh read on a {@link WriteConditionNotFulfilledException}.
 *
 * @param <E> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
@NullMarked
public class GenericApplicationService<E> implements ApplicationService<E> {

    private final EventStore eventStore;
    private final CloudEventConverter<E> cloudEventConverter;
    private final Retry retry;
    private final @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher;
    private final ReactiveTransactionExecutor transactionExecutor;

    /**
     * Create a service with the default retry policy for optimistic concurrency conflicts.
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetry());
    }

    /**
     * Create a service with explicit collaborators and a retry policy applied when a {@link WriteConditionNotFulfilledException}
     * is caught.
     * <p>
     * To also configure synchronous subscriptions or a {@link ReactiveTransactionExecutor}, use {@link #builder(EventStore, CloudEventConverter)}.
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, Retry retry) {
        this(eventStore, cloudEventConverter, retry, null, ReactiveTransactionExecutor.noTransaction());
    }

    private GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, Retry retry,
                                      @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher, ReactiveTransactionExecutor transactionExecutor) {
        if (eventStore == null) throw new IllegalArgumentException(EventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retry == null) throw new IllegalArgumentException(Retry.class.getSimpleName() + " cannot be null");
        if (transactionExecutor == null) throw new IllegalArgumentException(ReactiveTransactionExecutor.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.retry = retry;
        this.synchronousEventDispatcher = synchronousEventDispatcher;
        this.transactionExecutor = transactionExecutor;
    }

    /**
     * Start building a reactive {@link GenericApplicationService}. Use this instead of the constructors when you want to
     * configure synchronous subscriptions ({@link Builder#synchronousSubscriptions(ReactiveSynchronousEventDispatcher)})
     * or a {@link ReactiveTransactionExecutor} ({@link Builder#transactionExecutor(ReactiveTransactionExecutor)}).
     *
     * @param eventStore          The event store to use
     * @param cloudEventConverter The cloud event converter
     * @param <E>                 The domain event type
     * @return A new builder.
     */
    public static <E> Builder<E> builder(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        return new Builder<>(eventStore, cloudEventConverter);
    }

    @Override
    public Mono<WriteResult> execute(String streamId, ExecuteOptions<E> executeOptions, Function<List<E>, List<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(executeOptions, "ExecuteOptions cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable StreamReadFilter filter = resolveFilter(executeOptions);
        if (filter != null && !(eventStore instanceof ReadEventStreamWithFilter)) {
            throw new UnsupportedOperationException("The provided EventStore implementation does not support reading with a StreamReadFilter. EventStore must implement ReadEventStreamWithFilter in order to use filters when reading.");
        }
        @Nullable Function<List<E>, Mono<Void>> sideEffect = executeOptions.sideEffect();
        boolean dispatchSynchronously = synchronousEventDispatcher != null && synchronousEventDispatcher.hasSubscriptions();

        // The read, decide, write, and synchronous dispatch run as one unit inside the transaction executor and retry
        // from a fresh read on an optimistic-concurrency conflict, so the decision always runs against the current
        // events. The side-effect is composed after the retry so it runs once on success, not per attempt.
        Mono<Result<E>> readDecideWrite = transactionExecutor.inTransaction(() -> read(streamId, filter).flatMap(eventStream ->
                eventStream.events().collectList().flatMap(currentCloudEvents -> {
                    List<E> domainEvents = cloudEventConverter.toDomainEvents(currentCloudEvents.stream()).toList();
                    List<E> newDomainEvents = functionThatCallsDomainModel.apply(domainEvents);
                    if (newDomainEvents == null) {
                        newDomainEvents = List.of();
                    }
                    List<CloudEvent> newCloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents);
                    List<E> writtenDomainEvents = newDomainEvents;
                    return eventStore.write(streamId, eventStream.version(), Flux.fromIterable(newCloudEvents))
                            .flatMap(writeResult -> {
                                Result<E> result = new Result<>(writeResult, writtenDomainEvents);
                                if (!dispatchSynchronously || newCloudEvents.isEmpty()) {
                                    return Mono.just(result);
                                }
                                // Re-read exactly the just-written tail so synchronous handlers get events enriched by
                                // the store (stream version and global position), then dispatch inside the transaction.
                                return eventStore.read(streamId, Math.toIntExact(writeResult.oldStreamVersion()), newCloudEvents.size())
                                        .flatMap(enrichedStream -> enrichedStream.events().collectList())
                                        .flatMap(enriched -> synchronousEventDispatcher.dispatch(enriched).thenReturn(result));
                            });
                }))).retryWhen(retry);

        return readDecideWrite.flatMap(result -> {
            if (sideEffect == null) {
                return Mono.just(result.writeResult());
            }
            return sideEffect.apply(result.newDomainEvents()).thenReturn(result.writeResult());
        });
    }

    private Mono<EventStream<CloudEvent>> read(String streamId, @Nullable StreamReadFilter filter) {
        if (filter == null) {
            return eventStore.read(streamId);
        }
        return ((ReadEventStreamWithFilter) eventStore).read(streamId, filter);
    }

    private @Nullable StreamReadFilter resolveFilter(ExecuteOptions<E> executeOptions) {
        ExecuteFilter<? extends E> executeFilter = executeOptions.executeFilter();
        if (executeFilter != null) {
            return executeFilter.resolve(cloudEventConverter::getCloudEventType);
        }
        return executeOptions.filter();
    }

    /**
     * Returns the default reactive retry policy for optimistic concurrency conflicts. It retries a
     * {@link WriteConditionNotFulfilledException} up to five times with exponential backoff and rethrows the original
     * failure when the attempts are exhausted.
     */
    public static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .filter(WriteConditionNotFulfilledException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private record Result<E>(WriteResult writeResult, List<E> newDomainEvents) {
    }

    /**
     * Fluent builder for the reactive {@link GenericApplicationService}. Only {@code eventStore} and
     * {@code cloudEventConverter} are required; everything else has a sensible default (default retry, no synchronous
     * subscriptions, and {@link ReactiveTransactionExecutor#noTransaction()}).
     *
     * @param <E> The domain event type.
     */
    public static final class Builder<E> {
        private final EventStore eventStore;
        private final CloudEventConverter<E> cloudEventConverter;
        private Retry retry = defaultRetry();
        private @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher;
        private ReactiveTransactionExecutor transactionExecutor = ReactiveTransactionExecutor.noTransaction();

        private Builder(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
            this.eventStore = eventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        /**
         * Override the reactor {@link Retry} policy (defaults to {@link #defaultRetry()}).
         */
        public Builder<E> retry(Retry retry) {
            this.retry = Objects.requireNonNull(retry, "retry cannot be null");
            return this;
        }

        /**
         * Register the reactive synchronous subscription dispatcher. When set, after every write that produces events
         * the application service composes a dispatch to it into its chain, before {@code execute} completes. Enabling
         * this adds one extra read per event-producing write (to enrich the events with stream version and global
         * position), paid only while at least one synchronous subscription is registered. It is not free.
         */
        public Builder<E> synchronousSubscriptions(ReactiveSynchronousEventDispatcher synchronousEventDispatcher) {
            this.synchronousEventDispatcher = Objects.requireNonNull(synchronousEventDispatcher, "synchronousEventDispatcher cannot be null");
            return this;
        }

        /**
         * Set the {@link ReactiveTransactionExecutor} that spans the write and synchronous subscription handlers
         * (defaults to {@link ReactiveTransactionExecutor#noTransaction()}, i.e. best-effort with no transaction).
         */
        public Builder<E> transactionExecutor(ReactiveTransactionExecutor transactionExecutor) {
            this.transactionExecutor = Objects.requireNonNull(transactionExecutor, "transactionExecutor cannot be null");
            return this;
        }

        public GenericApplicationService<E> build() {
            return new GenericApplicationService<>(eventStore, cloudEventConverter, retry, synchronousEventDispatcher, transactionExecutor);
        }
    }
}
