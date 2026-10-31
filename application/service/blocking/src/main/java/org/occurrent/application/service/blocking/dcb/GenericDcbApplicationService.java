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
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.internal.FailureAfterWrite;
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
import java.util.function.Supplier;


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
    private final @Nullable TagGenerator<E> tagGenerator;
    private final RetryStrategy retryStrategy;
    private final @Nullable SynchronousEventDispatcher synchronousEventDispatcher;
    private final TransactionExecutor transactionExecutor;

    /**
     * Creates a service with the default retry strategy.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator) {
        this(eventStore, cloudEventConverter, tagGenerator, defaultRetryStrategy());
    }

    /**
     * Creates a service with the default retry strategy and no global {@link TagGenerator}. Every call to
     * {@code execute} must then supply a {@link TagGenerator} via {@link DcbExecuteOptions#tagGenerator(TagGenerator)},
     * or use a domain function whose events already carry tags (e.g. a decider that carries its own tags).
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter) {
        this(eventStore, cloudEventConverter, defaultRetryStrategy());
    }

    /**
     * Creates a service with explicit collaborators for event conversion, DCB tagging, and retries after DCB
     * append conflicts. Storage stream placement is configured on the {@link DcbEventStore}, not here.
     */
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, TagGenerator<E> tagGenerator, RetryStrategy retryStrategy) {
        this(eventStore, cloudEventConverter, requireTagGenerator(tagGenerator), retryStrategy, null, TransactionExecutor.noTransaction());
    }

    private static <E> TagGenerator<E> requireTagGenerator(TagGenerator<E> tagGenerator) {
        if (tagGenerator == null) throw new IllegalArgumentException(TagGenerator.class.getSimpleName() + " cannot be null");
        return tagGenerator;
    }

    private GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, @Nullable TagGenerator<E> tagGenerator, RetryStrategy retryStrategy,
                                         @Nullable SynchronousEventDispatcher synchronousEventDispatcher, TransactionExecutor transactionExecutor) {
        if (eventStore == null) throw new IllegalArgumentException(DcbEventStore.class.getSimpleName() + " cannot be null");
        if (cloudEventConverter == null) throw new IllegalArgumentException(CloudEventConverter.class.getSimpleName() + " cannot be null");
        if (retryStrategy == null) throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");
        if (transactionExecutor == null) throw new IllegalArgumentException(TransactionExecutor.class.getSimpleName() + " cannot be null");

        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
        this.tagGenerator = tagGenerator;
        this.retryStrategy = retryStrategy;
        this.synchronousEventDispatcher = synchronousEventDispatcher;
        this.transactionExecutor = transactionExecutor;
    }

    /**
     * Start building a {@link GenericDcbApplicationService}. Use this to configure synchronous subscriptions
     * ({@link Builder#synchronousSubscriptions(SynchronousEventDispatcher)}) or a {@link TransactionExecutor}
     * ({@link Builder#transactionExecutor(TransactionExecutor)}) in addition to the optional global tag generator and
     * retry strategy.
     *
     * @param eventStore          The DCB event store to use
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
    public GenericDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, RetryStrategy retryStrategy) {
        this(eventStore, cloudEventConverter, (TagGenerator<E>) null, retryStrategy, null, TransactionExecutor.noTransaction());
    }

    /**
     * Executes a domain function against the events the DCB query reads and appends any produced events.
     */
    @Override
    public Optional<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<E> options, Function<List<E>, List<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(criteria, "Criteria cannot be null");
        Objects.requireNonNull(options, DcbExecuteOptions.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");

        @Nullable Consumer<List<E>> sideEffect = options.sideEffect();
        boolean dispatchSynchronously = synchronousEventDispatcher != null && synchronousEventDispatcher.hasSubscriptions();
        @Nullable DcbReadOptions readOptions = readOptions(options);
        boolean versionsRequested = options.versionsRequested();

        // @formatter:off
        record Attempt<T1, T2>(T1 v1, T2 v2) {}
        // @formatter:on

        FailureAfterWrite failureAfterWrite = new FailureAfterWrite();

        Attempt<Optional<DcbAppendResult>, List<E>> result = retryStrategy.execute(() -> {
            // The read and the decision run outside the transaction, since a store reports no marker versions inside one.
            // The append's condition holds this read's token, and a failed condition goes to the retry strategy around this.
            final DcbEventStream eventStream;
            try {
                eventStream = readOptions == null ? eventStore.read(criteria) : eventStore.read(criteria, readOptions);
                refuseEventBelowFoldedPosition(criteria, options, eventStream);
            } catch (DcbSnapshotInvalidException refused) {
                throw refused.beforeAppend(functionThatCallsDomainModel);
            }
            List<E> domainEvents = cloudEventConverter.toDomainEvents(eventStream.stream()).toList();
            List<E> newDomainEvents = functionThatCallsDomainModel.apply(domainEvents);
            if (newDomainEvents == null || newDomainEvents.isEmpty()) {
                return new Attempt<>(Optional.empty(), List.of());
            }

            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(newDomainEvents);
            List<CloudEvent> dcbEvents = addTags(options.tagGenerator(), newDomainEvents, cloudEvents);
            DcbAppendCondition appendCondition = DcbAppendCondition.failIfEventsMatch(eventStream.consistencyToken());
            // A transaction already open here counts as joined, so the retry doesn't see a failure after the write
            boolean joined = dispatchSynchronously && transactionExecutor.isTransactional();
            Supplier<DcbAppendResult> appendAndDispatch = () -> {
                failureAfterWrite.refuseIfHeld();
                // Asked inside the transaction, since the answer depends on what's open, and before the append, so a failure here undoes nothing
                boolean transactional = dispatchSynchronously && transactionExecutor.isTransactional();
                DcbAppendResult appendResult = eventStore.append(dcbEvents, appendCondition);

                if (dispatchSynchronously) {
                    try {
                        // A successful append is assigned a contiguous global-position block, so re-read exactly that block
                        // to get the events enriched with their positions, then dispatch on this thread, inside the transaction.
                        List<CloudEvent> writtenEnriched = eventStore.read(DcbCriteria.all(),
                                DcbReadOptions.between(appendResult.firstSequencePosition() - 1, appendResult.lastSequencePosition())).events();
                        synchronousEventDispatcher.dispatch(writtenEnriched, transactional);
                    } catch (Throwable failure) {
                        throw failureAfterWrite.propagate(failure, transactional && !joined);
                    }
                }

                return appendResult;
            };
            // Only open the transaction executor when synchronous dispatch must commit atomically with the append.
            // Without synchronous subscriptions the append keeps exactly its prior semantics and no transaction overhead.
            // The store joins whatever transaction this opens and so cannot retry a transient conflict itself. Retrying
            // that belongs to whoever owns the transaction, which is the TransactionExecutor, not this service. See ADR 0074.
            try {
                DcbAppendResult appendResult = dispatchSynchronously ? transactionExecutor.inTransaction(appendAndDispatch) : appendAndDispatch.get();
                return new Attempt<>(Optional.of(withCheckedVersions(appendResult, versionsRequested, eventStream)), newDomainEvents);
            } catch (Throwable failure) {
                if (!failureAfterWrite.isHeldAfter(failure)) {
                    throw failure;
                }
                // Nothing says a transaction of this attempt undid the write, so running the command again could write the events twice. The retry must not see this.
                return new Attempt<>(Optional.empty(), List.of());
            }
        });

        failureAfterWrite.rethrowIfHeld();

        // Invoke the side-effect once, after a successful append, with the newly written events. It is not invoked
        // on the no-new-events path, and it is outside the retry so it does not run per attempt.
        if (sideEffect != null && result.v1.isPresent()) {
            sideEffect.accept(result.v2);
        }
        return result.v1;
    }

    @Override
    public boolean isAppendConfirmed(DcbAppendResult appendResult) {
        Objects.requireNonNull(appendResult, "appendResult cannot be null");
        return eventStore instanceof DcbAppendConfirmation confirmation && confirmation.isConfirmed(appendResult);
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
     * Returns the default retry policy for optimistic DCB conflicts. It makes up to five attempts in total for a
     * {@link DcbAppendConditionNotFulfilledException}, with exponential backoff and no jitter. This matches the
     * reactive counterpart's {@code defaultRetry}, which also allows five attempts in total with the same backoff
     * and no jitter.
     */
    public static Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(5).retryIf(DcbAppendConditionNotFulfilledException.class::isInstance);
    }

    /**
     * Fluent builder for {@link GenericDcbApplicationService}. Only {@code eventStore} and {@code cloudEventConverter}
     * are required; the global {@link TagGenerator} is optional (supply one per-execute instead), and the retry
     * strategy, synchronous subscriptions, and {@link TransactionExecutor} all default sensibly.
     *
     * @param <E> The domain event type.
     */
    public static final class Builder<E> {
        private final DcbEventStore eventStore;
        private final CloudEventConverter<E> cloudEventConverter;
        private @Nullable TagGenerator<E> tagGenerator;
        private RetryStrategy retryStrategy = defaultRetryStrategy();
        private @Nullable SynchronousEventDispatcher synchronousEventDispatcher;
        private TransactionExecutor transactionExecutor = TransactionExecutor.noTransaction();

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
         * Override the {@link RetryStrategy} (defaults to {@link #defaultRetryStrategy()}).
         */
        public Builder<E> retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = Objects.requireNonNull(retryStrategy, "retryStrategy cannot be null");
            return this;
        }

        /**
         * Register the synchronous subscription dispatcher. When set, after every successful append the service
         * re-reads the just-appended global-position block (enriched with positions) and dispatches to it synchronously
         * before {@code execute} returns. Adds one read per append while at least one synchronous subscription is
         * registered. It is not free.
         */
        public Builder<E> synchronousSubscriptions(SynchronousEventDispatcher synchronousEventDispatcher) {
            this.synchronousEventDispatcher = Objects.requireNonNull(synchronousEventDispatcher, "synchronousEventDispatcher cannot be null");
            return this;
        }

        /**
         * Set the {@link TransactionExecutor} spanning the append and synchronous subscription handlers (defaults to
         * {@link TransactionExecutor#noTransaction()}).
         */
        public Builder<E> transactionExecutor(TransactionExecutor transactionExecutor) {
            this.transactionExecutor = Objects.requireNonNull(transactionExecutor, "transactionExecutor cannot be null");
            return this;
        }

        public GenericDcbApplicationService<E> build() {
            return new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator, retryStrategy, synchronousEventDispatcher, transactionExecutor);
        }
    }
}
