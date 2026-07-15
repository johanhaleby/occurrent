/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.application.service.blocking.generic;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.application.service.SynchronousEventDispatcher;
import org.occurrent.application.service.TransactionExecutor;
import org.occurrent.application.service.blocking.ExecuteOptions;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A generic application service that works in many scenarios. If you need more complex logic, such as transaction support, you may consider either wrapping it
 * in a custom {@code ApplicationService} implementation, or simply copy and paste the source into your own code base and make changes there.
 *
 * @param <E> The type of the event to store. Normally this would be your custom "DomainEvent" class, but it could also be {@link CloudEvent}.
 */
@NullMarked
public class GenericApplicationService<E> implements ApplicationService<E> {

    private final EventStore eventStore;
    private final CloudEventConverter<E> cloudEventConverter;
    private final RetryStrategy retryStrategy;
    private final @Nullable SynchronousEventDispatcher synchronousEventDispatcher;
    private final TransactionExecutor transactionExecutor;

    /**
     * Create a GenericApplicationService with the supplied {@link EventStore} and {@link CloudEventConverter}.
     * It will use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry, if {@link WriteConditionNotFulfilledException} is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception.
     *
     * @param eventStore          The event store to use
     * @param cloudEventConverter The cloud event converter
     * @see #GenericApplicationService(EventStore, CloudEventConverter, RetryStrategy)
     * @see #builder(EventStore, CloudEventConverter)
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetryStrategy());
    }

    /**
     * Create a GenericApplicationService with the supplied {@link EventStore}, {@link CloudEventConverter} and {@link RetryStrategy}.
     * <p>
     * To also configure synchronous subscriptions or a {@link TransactionExecutor}, use {@link #builder(EventStore, CloudEventConverter)}.
     *
     * @param eventStore          The event store to use
     * @param cloudEventConverter The cloud event converter
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, RetryStrategy retryStrategy) {
        this(eventStore, cloudEventConverter, retryStrategy, null, TransactionExecutor.noTransaction());
    }

    @SuppressWarnings("ConstantValue")
    private GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, RetryStrategy retryStrategy,
                                      @Nullable SynchronousEventDispatcher synchronousEventDispatcher, TransactionExecutor transactionExecutor) {
        if (eventStore == null) throw new IllegalArgumentException(EventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retryStrategy == null) throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");
        if (transactionExecutor == null) throw new IllegalArgumentException(TransactionExecutor.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.retryStrategy = retryStrategy;
        this.synchronousEventDispatcher = synchronousEventDispatcher;
        this.transactionExecutor = transactionExecutor;
    }

    /**
     * Start building a {@link GenericApplicationService}. Use this instead of the constructors when you want to
     * configure synchronous subscriptions ({@link Builder#synchronousSubscriptions(SynchronousEventDispatcher)}) or a
     * {@link TransactionExecutor} ({@link Builder#transactionExecutor(TransactionExecutor)}).
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
    public WriteResult execute(String streamId, ExecuteOptions<E> executeOptions, Function<List<E>, List<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(executeOptions, "ExecuteOptions cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        StreamReadFilter filter = resolveFilter(executeOptions);
        Consumer<List<E>> sideEffect = executeOptions.sideEffect();

        boolean isStreamReadFilterCompatibleEventStore = eventStore instanceof ReadEventStreamWithFilter;
        if (!isStreamReadFilterCompatibleEventStore && filter != null) {
            throw new UnsupportedOperationException("The provided EventStore implementation does not support reading with a StreamReadFilter. EventStore must implement ReadEventStreamWithFilter in order to use filters when reading.");
        }

        // @formatter:off
              record Tuple<T1, T2>(T1 v1, T2 v2) {}
              // @formatter:on

        boolean dispatchSynchronously = synchronousEventDispatcher != null && synchronousEventDispatcher.hasSubscriptions();

        Tuple<WriteResult, List<E>> result = retryStrategy.execute(() -> transactionExecutor.inTransaction(() -> {
            EventStream<CloudEvent> eventStream = filter == null ? eventStore.read(streamId) : ((ReadEventStreamWithFilter) eventStore).read(streamId, filter);
            List<E> eventsInStream = cloudEventConverter.toDomainEvents(eventStream.events()).toList();

            List<E> newDomainEvents = functionThatCallsDomainModel.apply(eventsInStream);
            if (newDomainEvents == null) {
                newDomainEvents = List.of();
            }

            List<CloudEvent> newEvents = cloudEventConverter.toCloudEvents(newDomainEvents);
            WriteResult writeResult = eventStore.write(streamId, eventStream.version(), newEvents);

            if (dispatchSynchronously && !newEvents.isEmpty()) {
                // Re-read exactly the just-written tail so synchronous handlers get events enriched by the store
                // (stream version and global position), then dispatch on this thread, inside the transaction.
                int newEventCount = newEvents.size();
                List<CloudEvent> writtenEnriched = eventStore.read(streamId, Math.toIntExact(writeResult.oldStreamVersion()), newEventCount).events().toList();
                synchronousEventDispatcher.dispatch(writtenEnriched);
            }

            return new Tuple<>(writeResult, newDomainEvents);
        }));

        if (sideEffect != null) {
            sideEffect.accept(result.v2);
        }
        return result.v1;
    }

    private @Nullable StreamReadFilter resolveFilter(ExecuteOptions<E> executeOptions) {
        ExecuteFilter<? extends E> executeFilter = executeOptions.executeFilter();
        if (executeFilter != null) {
            return executeFilter.resolve(cloudEventConverter::getCloudEventType);
        }
        return executeOptions.filter();
    }

    /**
     * @return The default {@link RetryStrategy} using exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time if {@link WriteConditionNotFulfilledException} is caught.
     * It will only retry 5 times before giving up, rethrowing the original exception.
     */
    public static Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(WriteConditionNotFulfilledException.class::isInstance);
    }

    /**
     * Fluent builder for {@link GenericApplicationService}. Only {@code eventStore} and {@code cloudEventConverter} are
     * required; everything else has a sensible default (default retry strategy, no synchronous subscriptions, and
     * {@link TransactionExecutor#noTransaction()}).
     *
     * @param <E> The domain event type.
     */
    public static final class Builder<E> {
        private final EventStore eventStore;
        private final CloudEventConverter<E> cloudEventConverter;
        private RetryStrategy retryStrategy = defaultRetryStrategy();
        private @Nullable SynchronousEventDispatcher synchronousEventDispatcher;
        private TransactionExecutor transactionExecutor = TransactionExecutor.noTransaction();

        private Builder(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
            this.eventStore = eventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        /**
         * Override the {@link RetryStrategy} (defaults to {@link #defaultRetryStrategy()}).
         */
        public Builder<E> retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = Objects.requireNonNull(retryStrategy, "retryStrategy cannot be null");
            return this;
        }

        /**
         * Register the synchronous subscription dispatcher. When set, after every write that produces events the
         * application service dispatches the just-written events to it synchronously, before {@code execute} returns.
         * Enabling this adds one extra read per event-producing write (to enrich the events with stream version and
         * global position), paid only while at least one synchronous subscription is registered. It is not free.
         */
        public Builder<E> synchronousSubscriptions(SynchronousEventDispatcher synchronousEventDispatcher) {
            this.synchronousEventDispatcher = Objects.requireNonNull(synchronousEventDispatcher, "synchronousEventDispatcher cannot be null");
            return this;
        }

        /**
         * Set the {@link TransactionExecutor} that spans the write and synchronous subscription handlers (defaults to
         * {@link TransactionExecutor#noTransaction()}, i.e. best-effort with no cross-cutting transaction).
         */
        public Builder<E> transactionExecutor(TransactionExecutor transactionExecutor) {
            this.transactionExecutor = Objects.requireNonNull(transactionExecutor, "transactionExecutor cannot be null");
            return this;
        }

        public GenericApplicationService<E> build() {
            return new GenericApplicationService<>(eventStore, cloudEventConverter, retryStrategy, synchronousEventDispatcher, transactionExecutor);
        }
    }
}
