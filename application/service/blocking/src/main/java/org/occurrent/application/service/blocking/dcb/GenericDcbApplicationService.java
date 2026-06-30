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

package org.occurrent.application.service.blocking.dcb;

import org.occurrent.application.service.dcb.TagGenerator;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * Default blocking {@link DcbApplicationService} implementation.
 * <p>
 * It coordinates a {@link DcbEventStore}, a {@link CloudEventConverter}, and a
 * {@link TagGenerator} so application code can keep domain decisions expressed in domain
 * events while DCB metadata is applied at the CloudEvent boundary. Storage stream
 * placement is configured on the {@link DcbEventStore}.
 */
@NullMarked
public class GenericDcbApplicationService<E> implements DcbApplicationService<E> {
    private final DcbEventStore eventStore;
    private final CloudEventConverter<E> cloudEventConverter;
    private final TagGenerator<E> tagGenerator;
    private final RetryStrategy retryStrategy;

    /**
     * Creates a service with the default retry strategy.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator) {
        this(eventStore, cloudEventConverter, tagGenerator, defaultRetryStrategy());
    }

    /**
     * Creates a service with explicit collaborators for event conversion, DCB tagging, and retries after DCB
     * append conflicts. Storage stream placement is configured on the {@link DcbEventStore}, not here.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, RetryStrategy retryStrategy) {
        if (eventStore == null) throw new IllegalArgumentException(DcbEventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (tagGenerator == null) throw new IllegalArgumentException(TagGenerator.class.getSimpleName() + " cannot be null");
        if (retryStrategy == null) throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.tagGenerator = tagGenerator;
        this.retryStrategy = retryStrategy;
    }

    /**
     * Executes a domain function against the current events selected by the DCB query and appends any produced events.
     */
    @Override
    public Optional<DcbAppendResult> execute(DcbQuery query, DcbExecuteOptions<E> options, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(query, "Query cannot be null");
        Objects.requireNonNull(options, DcbExecuteOptions.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable Consumer<Stream<E>> sideEffect = options.sideEffect();

        // @formatter:off
        record Tuple<T1, T2>(T1 v1, T2 v2) {}
        // @formatter:on

        Tuple<Optional<DcbAppendResult>, List<E>> result = retryStrategy.execute(() -> {
            DcbEventStream eventStream = eventStore.read(query);
            Stream<E> domainEvents = cloudEventConverter.toDomainEvents(eventStream.stream());
            List<E> newDomainEvents = emptyStreamIfNull(functionThatCallsDomainModel.apply(domainEvents)).toList();
            if (newDomainEvents.isEmpty()) {
                return new Tuple<>(Optional.empty(), List.of());
            }

            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents.stream()).toList();
            List<CloudEvent> dcbEvents = addTags(newDomainEvents, cloudEvents);
            DcbAppendCondition appendCondition = DcbAppendCondition.failIfEventsMatch(query, eventStream.consistencyToken());
            return new Tuple<>(Optional.of(eventStore.append(dcbEvents, appendCondition)), newDomainEvents);
        });

        // Invoke the side-effect once, after a successful append, with the newly written events. It is not invoked
        // on the no-new-events path, and it is outside the retry so it does not run per attempt.
        if (sideEffect != null && result.v1.isPresent()) {
            sideEffect.accept(result.v2.stream());
        }
        return result.v1;
    }

    private List<CloudEvent> addTags(List<E> domainEvents, List<CloudEvent> cloudEvents) {
        if (domainEvents.size() != cloudEvents.size()) {
            throw new IllegalStateException(CloudEventConverter.class.getSimpleName() + " must preserve the number of events when converting to CloudEvents");
        }
        ArrayList<CloudEvent> dcbEvents = new ArrayList<>(domainEvents.size());
        for (int i = 0; i < domainEvents.size(); i++) {
            dcbEvents.add(DcbCloudEvents.withTags(cloudEvents.get(i), tagGenerator.tags(domainEvents.get(i))));
        }
        return dcbEvents;
    }

    private static <T> Stream<T> emptyStreamIfNull(@Nullable Stream<T> stream) {
        return stream == null ? Stream.empty() : stream;
    }

    /**
     * Returns the default retry policy for optimistic DCB conflicts.
     */
    public static Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(DcbAppendConditionNotFulfilledException.class::isInstance);
    }
}
