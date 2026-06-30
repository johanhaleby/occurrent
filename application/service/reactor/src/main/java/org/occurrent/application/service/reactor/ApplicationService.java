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

package org.occurrent.application.service.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.eventstore.api.WriteResult;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The reactive counterpart of the blocking {@code ApplicationService}. It loads the events for a stream, applies them to
 * a pure domain function, writes the new events with optimistic concurrency, and runs an optional side-effect after the
 * write, all as a {@link Mono}.
 *
 * @param <E> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
@NullMarked
public interface ApplicationService<E> {

    /**
     * Execute a function that loads events from the event store and applies them to the domain model using
     * {@link ExecuteOptions}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param executeOptions               Options that control stream read filtering and an optional reactive side-effect.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model.
     * @return A {@link Mono} of the write result.
     */
    Mono<WriteResult> execute(String streamId, ExecuteOptions<E> executeOptions, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel);

    /**
     * Execute a function using an {@link ExecuteFilter} that resolves domain event classes to CloudEvent types when reading.
     */
    default Mono<WriteResult> execute(String streamId, ExecuteFilter<? extends E> executeFilter, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(executeFilter, "ExecuteFilter cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "functionThatCallsDomainModel cannot be null");
        return execute(streamId, ExecuteOptions.<E>options().filter(executeFilter), functionThatCallsDomainModel);
    }

    /**
     * Execute a function with no read filter and no side-effect.
     */
    default Mono<WriteResult> execute(String streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        return execute(streamId, ExecuteOptions.empty(), functionThatCallsDomainModel);
    }

    /**
     * Convenience overload that accepts a {@link UUID} stream id.
     */
    default Mono<WriteResult> execute(UUID streamId, ExecuteOptions<E> executeOptions, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), executeOptions, functionThatCallsDomainModel);
    }

    /**
     * Convenience overload that accepts a {@link UUID} stream id and an {@link ExecuteFilter}.
     */
    default Mono<WriteResult> execute(UUID streamId, ExecuteFilter<? extends E> executeFilter, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), executeFilter, functionThatCallsDomainModel);
    }

    /**
     * Convenience overload that accepts a {@link UUID} stream id.
     */
    default Mono<WriteResult> execute(UUID streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), functionThatCallsDomainModel);
    }
}
