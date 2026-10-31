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
import org.occurrent.application.service.internal.FailureAfterWrite;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbAppendConfirmation;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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
    private final @Nullable TagGenerator<E> tagGenerator;
    private final Retry retry;
    private final @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher;
    private final ReactiveTransactionExecutor transactionExecutor;

    /**
     * Creates a service with the default retry policy for optimistic DCB conflicts.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator) {
        this(eventStore, cloudEventConverter, tagGenerator, defaultRetry());
    }

    /**
     * Creates a service with the default retry policy for optimistic DCB conflicts and no global
     * {@link TagGenerator}. Every call to {@code execute} must then supply a {@link TagGenerator} via
     * {@link DcbExecuteOptions#tagGenerator(TagGenerator)}, or use a domain function whose events already carry tags
     * (e.g. a decider that carries its own tags).
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetry());
    }

    /**
     * Creates a service with explicit collaborators for event conversion, DCB tagging, and retries after DCB append
     * conflicts. Storage stream placement is configured on the {@link DcbEventStore}, not here.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, Retry retry) {
        this(eventStore, cloudEventConverter, requireTagGenerator(tagGenerator), retry, null, ReactiveTransactionExecutor.noTransaction());
    }

    private static <E> TagGenerator<E> requireTagGenerator(TagGenerator<E> tagGenerator) {
        if (tagGenerator == null) throw new IllegalArgumentException(TagGenerator.class.getSimpleName() + " cannot be null");
        return tagGenerator;
    }

    private GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, @Nullable TagGenerator<E> tagGenerator, Retry retry,
                                         @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher, ReactiveTransactionExecutor transactionExecutor) {
        if (eventStore == null) throw new IllegalArgumentException(DcbEventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retry == null) throw new IllegalArgumentException(Retry.class.getSimpleName() + " cannot be null");
        if (transactionExecutor == null) throw new IllegalArgumentException(ReactiveTransactionExecutor.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.tagGenerator = tagGenerator;
        this.retry = retry;
        this.synchronousEventDispatcher = synchronousEventDispatcher;
        this.transactionExecutor = transactionExecutor;
    }

    /**
     * Start building a reactive {@link GenericDcbApplicationService}. Use this to configure synchronous subscriptions
     * ({@link Builder#synchronousSubscriptions(ReactiveSynchronousEventDispatcher)}) or a
     * {@link ReactiveTransactionExecutor} ({@link Builder#transactionExecutor(ReactiveTransactionExecutor)}) in
     * addition to the optional global tag generator and retry policy.
     *
     * @param eventStore          The reactive DCB event store to use
     * @param cloudEventConverter The cloud event converter
     * @param <E>                 The domain event type
     * @return A new builder.
     */
    public static <E> Builder<E> builder(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        return new Builder<>(eventStore, cloudEventConverter);
    }

    /**
     * Creates a service with explicit collaborators for event conversion and retries after DCB append conflicts, but
     * no global {@link TagGenerator}. Every call to {@code execute} must then supply a {@link TagGenerator} via
     * {@link DcbExecuteOptions#tagGenerator(TagGenerator)}, or use a domain function whose events already carry tags
     * (e.g. a decider that carries its own tags). Storage stream placement is configured on the
     * {@link DcbEventStore}, not here.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, Retry retry) {
        this(eventStore, cloudEventConverter, (TagGenerator<E>) null, retry, null, ReactiveTransactionExecutor.noTransaction());
    }

    @Override
    public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<E> options, Function<List<E>, List<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(criteria, "Criteria cannot be null");
        Objects.requireNonNull(options, DcbExecuteOptions.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable Function<List<E>, Mono<Void>> sideEffect = options.sideEffect();
        boolean dispatchSynchronously = synchronousEventDispatcher != null && synchronousEventDispatcher.hasSubscriptions();
        @Nullable DcbReadOptions readOptions = readOptions(options);
        boolean versionsRequested = options.versionsRequested();

        // The read and the decision run outside the transaction, since a store reports no marker versions inside one. The
        // append's condition holds this read's token, and a failed condition goes to the retry around this, which can read
        // and decide again. The side-effect is composed after the retry so it runs once on success, not per attempt.
        // An empty Mono here means the domain function produced no new events (a no-op), so nothing is appended and no
        // side-effect runs. The append-produced path carries a Result so the side-effect can fire once after the retry.
        return Mono.defer(() -> {
            FailureAfterWrite failureAfterWrite = new FailureAfterWrite();
            Mono<Result<E>> readDecideAppend = Mono.defer(() -> readOptions == null ? eventStore.read(criteria) : eventStore.read(criteria, readOptions))
                    .doOnNext(eventStream -> refuseEventBelowFoldedPosition(criteria, options, eventStream))
                    .onErrorMap(DcbSnapshotInvalidException.class, refused -> refused.beforeAppend(functionThatCallsDomainModel))
                    .flatMap(eventStream -> {
                List<E> domainEvents = cloudEventConverter.toDomainEvents(eventStream.stream()).toList();
                List<E> newDomainEvents = functionThatCallsDomainModel.apply(domainEvents);
                if (newDomainEvents == null || newDomainEvents.isEmpty()) {
                    return Mono.empty();
                }
                List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents);
                List<CloudEvent> dcbEvents = addTags(options.tagGenerator(), newDomainEvents, cloudEvents);
                DcbAppendCondition appendCondition = DcbAppendCondition.failIfEventsMatch(eventStream.consistencyToken());
                // A transaction already open here counts as joined, so the retry doesn't see a failure after the write
                return reportsTransaction(dispatchSynchronously).flatMap(joined -> {
                    // Asked inside the transaction, since the answer depends on what's open, and before the append, so a failure here undoes nothing
                    Supplier<Mono<DcbAppendResult>> appendAndDispatch = () -> Mono.defer(() -> {
                        failureAfterWrite.refuseIfHeld();
                        return !dispatchSynchronously ? eventStore.append(dcbEvents, appendCondition) : reportsTransaction(dispatchSynchronously).flatMap(transactional ->
                                eventStore.append(dcbEvents, appendCondition).flatMap(appendResult ->
                                        // A successful append is assigned a contiguous global-position block, so re-read exactly that block to
                                        // get the events enriched with their positions, then dispatch inside the transaction.
                                        Mono.defer(() -> eventStore.read(DcbCriteria.all(), DcbReadOptions.between(appendResult.firstSequencePosition() - 1, appendResult.lastSequencePosition())))
                                                .flatMap(enrichedStream -> synchronousEventDispatcher.dispatch(enrichedStream.events(), transactional))
                                                .onErrorMap(failure -> !transactional || joined, failureAfterWrite::hold)
                                                .thenReturn(appendResult)));
                    });
                    // Enter the transaction executor only when synchronous dispatch must commit atomically with the append.
                    // Otherwise the append keeps exactly its prior semantics and no transaction overhead is added.
                    return dispatchSynchronously ? transactionExecutor.inTransaction(appendAndDispatch) : Mono.defer(appendAndDispatch);
                }).map(appendResult -> new Result<>(withCheckedVersions(appendResult, versionsRequested, eventStream), newDomainEvents));
            });
            // Nothing says a transaction of this attempt undid the write, so running the command again could write the events twice. The retry must not see this.
            return readDecideAppend.onErrorResume(failureAfterWrite::isHeldAfter, failure -> Mono.empty())
                    .retryWhen(retry)
                    // An executor can swallow the stand-in and return a value, which says nothing about the failure
                    .filter(result -> !failureAfterWrite.isHeld())
                    .switchIfEmpty(Mono.defer(() -> failureAfterWrite.isHeld() ? Mono.error(failureAfterWrite.failure()) : Mono.empty()))
                    .flatMap(result -> {
                        if (sideEffect == null) {
                            return Mono.just(result.appendResult());
                        }
                        return Mono.defer(() -> sideEffect.apply(result.newDomainEvents())).thenReturn(result.appendResult());
                    });
        });
    }

    private Mono<Boolean> reportsTransaction(boolean dispatchSynchronously) {
        return dispatchSynchronously ? transactionExecutor.isTransactional().defaultIfEmpty(false) : Mono.just(false);
    }

    @Override
    public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
        Objects.requireNonNull(appendResult, "appendResult cannot be null");
        return eventStore instanceof DcbAppendConfirmation confirmation ? confirmation.isConfirmed(appendResult) : Mono.just(false);
    }

    private static void refuseEventBelowFoldedPosition(DcbCriteria criteria, DcbExecuteOptions<?> options, DcbEventStream eventStream) {
        @Nullable DcbMarkerVersions fromVersions = options.fromVersions();
        if (fromVersions != null && eventStream.hasEventBelow(options.highestFoldedPosition())) {
            throw DcbSnapshotInvalidException.eventBelowFoldedPosition(criteria, fromVersions, options.highestFoldedPosition());
        }
    }

    @SuppressWarnings("removal")
    private static @Nullable DcbReadOptions readOptions(DcbExecuteOptions<?> options) {
        @Nullable Long fromPosition = options.fromPosition();
        @Nullable DcbMarkerVersions fromVersions = options.fromVersions();
        if (fromPosition != null) {
            return DcbReadOptions.afterPosition(fromPosition);
        } else if (fromVersions != null) {
            return DcbReadOptions.afterVersions(fromVersions);
        } else if (options.versionsRequested()) {
            return DcbReadOptions.fromBeginning().withVersions();
        }
        return null;
    }

    // The append's versions are kept only when the read of the same attempt reported every marker key with the same
    // epoch, since a marker pruned and created again in between can leave the consistency token unchanged
    private static DcbAppendResult withCheckedVersions(DcbAppendResult appendResult, boolean versionsRequested, DcbEventStream eventStream) {
        if (appendResult.markerVersions().isEmpty()) {
            return appendResult;
        }
        DcbMarkerVersions appended = appendResult.markerVersions().get();
        boolean sameEpochs = versionsRequested && eventStream.markerVersions()
                .map(read -> appended.markers().entrySet().stream().allMatch(entry -> read.get(entry.getKey())
                        .map(readVersion -> readVersion.epoch().equals(entry.getValue().epoch()))
                        .orElse(false)))
                .orElse(false);
        return sameEpochs ? appendResult : new DcbAppendResult(appendResult.firstSequencePosition(), appendResult.lastSequencePosition(),
                appendResult.eventCount(), appendResult.appendId(), Optional.empty());
    }

    private List<CloudEvent> addTags(@Nullable TagGenerator<E> perExecuteTagger, List<E> domainEvents, List<CloudEvent> cloudEvents) {
        if (domainEvents.size() != cloudEvents.size()) {
            throw new IllegalStateException(CloudEventConverter.class.getSimpleName() + " must preserve the number of events when converting to CloudEvents");
        }
        TagGenerator<E> effective = perExecuteTagger != null ? perExecuteTagger : this.tagGenerator;
        if (effective == null) {
            throw new IllegalStateException("No TagGenerator available to tag DCB events. Supply one when constructing GenericDcbApplicationService, pass DcbExecuteOptions.tagGenerator(...), or use a DcbDecider that carries its tags.");
        }
        ArrayList<CloudEvent> dcbEvents = new ArrayList<>(domainEvents.size());
        for (int i = 0; i < domainEvents.size(); i++) {
            dcbEvents.add(DcbCloudEvents.withTags(cloudEvents.get(i), effective.tags(domainEvents.get(i))));
        }
        return dcbEvents;
    }

    /**
     * Returns the default reactive retry policy for optimistic DCB conflicts. It makes up to five attempts in total
     * (the initial attempt plus up to four retries) for a {@link DcbAppendConditionNotFulfilledException}, with
     * exponential backoff and no jitter, and rethrows the original failure when the attempts are exhausted. This
     * matches the blocking counterpart's {@code defaultRetryStrategy}, which also allows five attempts in total with
     * the same backoff and no jitter.
     */
    public static Retry defaultRetry() {
        return Retry.backoff(4, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .jitter(0.0)
                .filter(DcbAppendConditionNotFulfilledException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private record Result<E>(DcbAppendResult appendResult, List<E> newDomainEvents) {
    }

    /**
     * Fluent builder for the reactive {@link GenericDcbApplicationService}. Only {@code eventStore} and
     * {@code cloudEventConverter} are required; the global {@link TagGenerator} is optional, and the retry policy,
     * synchronous subscriptions, and {@link ReactiveTransactionExecutor} default sensibly.
     *
     * @param <E> The domain event type.
     */
    public static final class Builder<E> {
        private final DcbEventStore eventStore;
        private final CloudEventConverter<E> cloudEventConverter;
        private @Nullable TagGenerator<E> tagGenerator;
        private Retry retry = defaultRetry();
        private @Nullable ReactiveSynchronousEventDispatcher synchronousEventDispatcher;
        private ReactiveTransactionExecutor transactionExecutor = ReactiveTransactionExecutor.noTransaction();

        private Builder(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
            this.eventStore = eventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        /**
         * Set the global {@link TagGenerator} (optional; if omitted, tags must come per-execute or from a decider).
         */
        public Builder<E> tagGenerator(TagGenerator<E> tagGenerator) {
            this.tagGenerator = Objects.requireNonNull(tagGenerator, "tagGenerator cannot be null");
            return this;
        }

        /**
         * Override the reactor {@link Retry} policy (defaults to {@link #defaultRetry()}).
         */
        public Builder<E> retry(Retry retry) {
            this.retry = Objects.requireNonNull(retry, "retry cannot be null");
            return this;
        }

        /**
         * Register the reactive synchronous subscription dispatcher. When set, after every successful append the
         * service re-reads the just-appended global-position block (enriched with positions) and composes a dispatch to
         * it before {@code execute} completes. Adds one read per append while at least one synchronous subscription is
         * registered. It is not free.
         */
        public Builder<E> synchronousSubscriptions(ReactiveSynchronousEventDispatcher synchronousEventDispatcher) {
            this.synchronousEventDispatcher = Objects.requireNonNull(synchronousEventDispatcher, "synchronousEventDispatcher cannot be null");
            return this;
        }

        /**
         * Set the {@link ReactiveTransactionExecutor} spanning the append and synchronous subscription handlers
         * (defaults to {@link ReactiveTransactionExecutor#noTransaction()}).
         */
        public Builder<E> transactionExecutor(ReactiveTransactionExecutor transactionExecutor) {
            this.transactionExecutor = Objects.requireNonNull(transactionExecutor, "transactionExecutor cannot be null");
            return this;
        }

        public GenericDcbApplicationService<E> build() {
            return new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator, retry, synchronousEventDispatcher, transactionExecutor);
        }
    }
}
