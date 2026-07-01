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
import java.util.stream.Stream;

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

    /**
     * Create a service with the default retry policy for optimistic concurrency conflicts.
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetry());
    }

    /**
     * Create a service with explicit collaborators and a retry policy applied when a {@link WriteConditionNotFulfilledException}
     * is caught.
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, Retry retry) {
        if (eventStore == null) throw new IllegalArgumentException(EventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retry == null) throw new IllegalArgumentException(Retry.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.retry = retry;
    }

    @Override
    public Mono<WriteResult> execute(String streamId, ExecuteOptions<E> executeOptions, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(executeOptions, "ExecuteOptions cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable StreamReadFilter filter = resolveFilter(executeOptions);
        if (filter != null && !(eventStore instanceof ReadEventStreamWithFilter)) {
            throw new UnsupportedOperationException("The provided EventStore implementation does not support reading with a StreamReadFilter. EventStore must implement ReadEventStreamWithFilter in order to use filters when reading.");
        }
        @Nullable Function<Stream<E>, Mono<Void>> sideEffect = executeOptions.sideEffect();

        // The read, decide, and write run as one unit and retry from a fresh read on an optimistic-concurrency
        // conflict, so the decision always runs against the current events. The side-effect is composed after the
        // retry so it runs once on success, not per attempt.
        Mono<Result<E>> readDecideWrite = Mono.defer(() -> read(streamId, filter).flatMap(eventStream ->
                eventStream.events().collectList().flatMap(currentCloudEvents -> {
                    Stream<E> domainEvents = cloudEventConverter.toDomainEvents(currentCloudEvents.stream());
                    List<E> newDomainEvents = emptyStreamIfNull(functionThatCallsDomainModel.apply(domainEvents)).toList();
                    List<CloudEvent> newCloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents.stream()).toList();
                    return eventStore.write(streamId, eventStream.version(), Flux.fromIterable(newCloudEvents))
                            .map(writeResult -> new Result<>(writeResult, newDomainEvents));
                }))).retryWhen(retry);

        return readDecideWrite.flatMap(result -> {
            if (sideEffect == null) {
                return Mono.just(result.writeResult());
            }
            return sideEffect.apply(result.newDomainEvents().stream()).thenReturn(result.writeResult());
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

    private static <T> Stream<T> emptyStreamIfNull(@Nullable Stream<T> stream) {
        return stream == null ? Stream.empty() : stream;
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
}
