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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

/**
 * Default blocking {@link DcbApplicationService} implementation.
 * <p>
 * It coordinates a {@link DcbEventStore}, a {@link CloudEventConverter}, a
 * {@link TagGenerator}, and a {@link DcbStreamIdGenerator} so application code can keep
 * domain decisions expressed in domain events while DCB metadata is applied at the
 * CloudEvent boundary.
 */
@NullMarked
public class GenericDcbApplicationService<E> implements DcbApplicationService<E> {
    private final DcbEventStore eventStore;
    private final CloudEventConverter<E> cloudEventConverter;
    private final TagGenerator<E> tagGenerator;
    private final DcbStreamIdGenerator streamIdGenerator;
    private final RetryStrategy retryStrategy;

    /**
     * Creates a service with the default partitioned storage stream id generator and retry strategy.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator) {
        this(eventStore, cloudEventConverter, tagGenerator, new PartitionedDcbStreamIdGenerator(), defaultRetryStrategy());
    }

    /**
     * Creates a service with the default partitioned storage stream id generator and a custom retry strategy.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, RetryStrategy retryStrategy) {
        this(eventStore, cloudEventConverter, tagGenerator, new PartitionedDcbStreamIdGenerator(), retryStrategy);
    }

    /**
     * Creates a service with explicit collaborators for event conversion, DCB tagging,
     * storage stream placement, and retries after DCB append conflicts.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, DcbStreamIdGenerator streamIdGenerator, RetryStrategy retryStrategy) {
        if (eventStore == null) throw new IllegalArgumentException(DcbEventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (tagGenerator == null) throw new IllegalArgumentException(TagGenerator.class.getSimpleName() + " cannot be null");
        if (streamIdGenerator == null) throw new IllegalArgumentException(DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
        if (retryStrategy == null) throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.tagGenerator = tagGenerator;
        this.streamIdGenerator = streamIdGenerator;
        this.retryStrategy = retryStrategy;
    }

    /**
     * Executes a domain function against the current events selected by the DCB query and appends any produced events.
     */
    @Override
    public Optional<DcbAppendResult> execute(DcbQuery query, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(query, "Query cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        return retryStrategy.execute(() -> {
            DcbEventStream eventStream = eventStore.read(query);
            Stream<E> domainEvents = cloudEventConverter.toDomainEvents(eventStream.stream());
            List<E> newDomainEvents = emptyStreamIfNull(functionThatCallsDomainModel.apply(domainEvents)).toList();
            if (newDomainEvents.isEmpty()) {
                return Optional.empty();
            }

            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents.stream()).toList();
            List<CloudEvent> dcbEvents = addTags(newDomainEvents, cloudEvents);
            DcbAppendCondition appendCondition = DcbAppendCondition.failIfEventsMatch(query, eventStream.lastSequencePosition());
            String streamId = streamIdGenerator.generateStreamId(tagsFromQuery(query));
            return Optional.of(eventStore.append(streamId, dcbEvents, appendCondition));
        });
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

    private static Set<String> tagsFromQuery(DcbQuery query) {
        if (query instanceof DcbQuery.Items items) {
            return items.items().stream()
                    .map(DcbQueryItem::tags)
                    .flatMap(Collection::stream)
                    .collect(toCollection(TreeSet::new));
        }
        return new TreeSet<>();
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
