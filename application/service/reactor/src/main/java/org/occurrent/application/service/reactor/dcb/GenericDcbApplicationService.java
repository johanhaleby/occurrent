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

package org.occurrent.application.service.reactor.dcb;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Default reactive {@link DcbApplicationService} implementation.
 * <p>
 * It coordinates a reactive {@link DcbEventStore}, a {@link CloudEventConverter}, and a {@link TagGenerator} so
 * application code keeps domain decisions expressed in domain events while DCB metadata is applied at the CloudEvent
 * boundary. Storage stream placement is configured on the {@link DcbEventStore}.
 */
@NullMarked
public class GenericDcbApplicationService<E> implements DcbApplicationService<E> {
    private final DcbEventStore eventStore;
    private final CloudEventConverter<E> cloudEventConverter;
    private final TagGenerator<E> tagGenerator;
    private final Retry retry;

    /**
     * Creates a service with the default retry policy for optimistic DCB conflicts.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator) {
        this(eventStore, cloudEventConverter, tagGenerator, defaultRetry());
    }

    /**
     * Creates a service with explicit collaborators for event conversion, DCB tagging, and retries after DCB append
     * conflicts. Storage stream placement is configured on the {@link DcbEventStore}, not here.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, Retry retry) {
        if (eventStore == null) throw new IllegalArgumentException(DcbEventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (tagGenerator == null) throw new IllegalArgumentException(TagGenerator.class.getSimpleName() + " cannot be null");
        if (retry == null) throw new IllegalArgumentException(Retry.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.tagGenerator = tagGenerator;
        this.retry = retry;
    }

    @Override
    public Mono<Optional<DcbAppendResult>> execute(DcbQuery query, DcbExecuteOptions<E> options, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(query, "Query cannot be null");
        Objects.requireNonNull(options, DcbExecuteOptions.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable Function<Stream<E>, Mono<Void>> sideEffect = options.sideEffect();

        // The read, decide, and append run as one unit and retry from a fresh read on a DCB conflict, so the decision
        // always runs against the current events. The side-effect is composed after the retry so it runs once on
        // success, not per attempt.
        Mono<Result<E>> readDecideAppend = Mono.defer(() -> eventStore.read(query).flatMap(eventStream -> {
            Stream<E> domainEvents = cloudEventConverter.toDomainEvents(eventStream.stream());
            List<E> newDomainEvents = emptyStreamIfNull(functionThatCallsDomainModel.apply(domainEvents)).toList();
            if (newDomainEvents.isEmpty()) {
                return Mono.just(new Result<>(Optional.<DcbAppendResult>empty(), List.<E>of()));
            }
            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents.stream()).toList();
            List<CloudEvent> dcbEvents = addTags(newDomainEvents, cloudEvents);
            DcbAppendCondition appendCondition = DcbAppendCondition.failIfEventsMatch(query, eventStream.consistencyToken());
            return eventStore.append(dcbEvents, appendCondition).map(result -> new Result<>(Optional.of(result), newDomainEvents));
        })).retryWhen(retry);

        return readDecideAppend.flatMap(result -> {
            if (sideEffect == null || result.appendResult().isEmpty()) {
                return Mono.just(result.appendResult());
            }
            return sideEffect.apply(result.newDomainEvents().stream()).thenReturn(result.appendResult());
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

    private static <T> Stream<T> emptyStreamIfNull(@Nullable Stream<T> stream) {
        return stream == null ? Stream.empty() : stream;
    }

    /**
     * Returns the default reactive retry policy for optimistic DCB conflicts. It retries a
     * {@link DcbAppendConditionNotFulfilledException} up to five times with exponential backoff and rethrows the
     * original failure when the attempts are exhausted.
     */
    public static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .filter(DcbAppendConditionNotFulfilledException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private record Result<E>(Optional<DcbAppendResult> appendResult, List<E> newDomainEvents) {
    }
}
