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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A generic application service that works in many scenarios. If you need more complex logic, such as transaction support, you may consider either wrapping it
 * in a custom {@code ApplicationService} implementation, or simply copy and paste the source into your own code base and make changes there.
 *
 * @param <T> The type of the event to store. Normally this would be your custom "DomainEvent" class, but it could also be {@link CloudEvent}.
 */
public class GenericApplicationService<T> implements ApplicationService<T> {

    private final EventStore eventStore;
    private final CloudEventConverter<T> cloudEventConverter;
    private final RetryStrategy retryStrategy;

    /**
     * Create a GenericApplicationService with the supplied {@link EventStore} and {@link CloudEventConverter}.
     * It will use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry, if {@link WriteConditionNotFulfilledException} is caught. It will, by default, only retry 5 times before giving up, rethrowing the original exception.
     *
     * @param eventStore          The event store to use
     * @param cloudEventConverter The cloud event converter
     * @see #GenericApplicationService(EventStore, CloudEventConverter, RetryStrategy)
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<T> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetryStrategy());
    }

    /**
     * Create a GenericApplicationService with the supplied {@link EventStore}, {@link CloudEventConverter} and {@link RetryStrategy}.
     *
     * @param eventStore          The event store to use
     * @param cloudEventConverter The cloud event converter
     */
    public GenericApplicationService(EventStore eventStore, CloudEventConverter<T> cloudEventConverter, RetryStrategy retryStrategy) {
        if (eventStore == null) throw new IllegalArgumentException(EventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retryStrategy == null) throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");
        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public WriteResult execute(String streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, Consumer<Stream<T>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        Tuple<WriteResult, List<T>> result = retryStrategy.execute(() -> {
            // Read all events from the event store for a particular stream
            EventStream<CloudEvent> eventStream = eventStore.read(streamId);

            // Convert the cloud events into domain events
            Stream<T> eventsInStream = cloudEventConverter.toDomainEvents(eventStream.events());

            // Call a pure function from the domain model which returns a Stream of events
            Stream<T> newDomainEvents = emptyStreamIfNull(functionThatCallsDomainModel.apply(eventsInStream));

            // We need to convert the new domain event stream into a list in order to be able to call side-effects with new events
            // if side effect is defined
            final List<T> newEventsAsList = sideEffect == null ? null : newDomainEvents.collect(Collectors.toList());

            // Convert to cloud events and write the new events to the event store
            Stream<CloudEvent> newEvents = cloudEventConverter.toCloudEvents(sideEffect == null ? newDomainEvents : newEventsAsList.stream());
            WriteResult writeResult = eventStore.write(streamId, eventStream.version(), newEvents);
            return new Tuple<>(writeResult, newEventsAsList);
        });

        // Invoke side-effect
        if (sideEffect != null) {
            sideEffect.accept(result.v2.stream());
        }
        return result.v1;
    }

    private static <T> Stream<T> emptyStreamIfNull(Stream<T> stream) {
        return stream == null ? Stream.empty() : stream;
    }

    /**
     * @return The default {@link RetryStrategy} using exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time if {@link WriteConditionNotFulfilledException} is caught.
     * It will only retry 5 times before giving up, rethrowing the original exception.
     */
    public static Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(WriteConditionNotFulfilledException.class::isInstance);
    }

    private static class Tuple<T1, T2> {
        private final T1 v1;
        private final T2 v2;

        Tuple(T1 v1, T2 v2) {
            this.v1 = v1;
            this.v2 = v2;
        }
    }
}