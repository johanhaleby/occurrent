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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;

/**
 * Reproduction for issue 1230 against the reactive store: the global position block is reserved before the append
 * transaction starts, so an append that reserved a lower position can commit after an append that reserved a higher one.
 * <p>
 * Append A reserves its position block and is then held, without blocking a thread, where the store wraps its transaction
 * in the {@link TransactionalOperator}. Append B reserves the next block and commits. The invariant under test: an event
 * that commits after a reader observed head {@code p} has a position above {@code p}.
 */
@Testcontainers
@Timeout(120)
@DisplayName("ReactorMongoEventStoreUncommittedPosition")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreUncommittedPositionTest {

    private static final URI SOURCE = URI.create("urn:test:uncommitted-position");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String ACCOUNT_TAG = "account:a1";
    private static final DcbCriteria ACCOUNT_BOUNDARY = DcbCriteria.types("Deposited", "Withdrawn").tags(Tag.parse(ACCOUNT_TAG));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient client;

    @AfterEach
    void close_client() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void an_event_that_commits_after_a_reader_observed_the_head_has_a_position_above_that_head() throws Exception {
        // Given
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        String databaseName = requireNonNull(connectionString.getDatabase());
        client = MongoClients.create(connectionString);
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(client, databaseName);
        TransactionalOperator transactionalOperator = TransactionalOperator.create(
                new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(client, databaseName)));
        HoldingTransactionalOperator holdingOperator = new HoldingTransactionalOperator(transactionalOperator);
        ReactorMongoEventStore lateAppender = newEventStore(template, holdingOperator);
        ReactorMongoEventStore reader = newEventStore(template, transactionalOperator);

        CloudEvent deposited = taggedEvent("Deposited", ACCOUNT_TAG);

        // When
        CompletableFuture<DcbAppendResult> lateAppend = lateAppender.append(List.of(deposited)).toFuture();
        long observedHead;
        try {
            assertThat(holdingOperator.held.await(30, TimeUnit.SECONDS))
                    .as("the late append did not reach its held transaction")
                    .isTrue();
            long counterAfterLateReservation = requireNonNull(reader.currentPosition().block(TIMEOUT));
            assertThat(counterAfterLateReservation)
                    .as("the late append must have reserved its position before it was held")
                    .isEqualTo(1);

            DcbEventStream boundary = requireNonNull(reader.read(ACCOUNT_BOUNDARY).block(TIMEOUT));
            DcbAppendCondition condition = failIfEventsMatch(ACCOUNT_BOUNDARY, boundary.consistencyToken());
            long readerPosition = requireNonNull(reader.append(List.of(taggedEvent("Withdrawn", ACCOUNT_TAG)), condition).block(TIMEOUT)).firstSequencePosition();
            assertThat(readerPosition)
                    .as("the reader's append must have reserved its position after the held late append")
                    .isEqualTo(counterAfterLateReservation + 1);

            DcbEventStream headRead = requireNonNull(reader.read(ACCOUNT_BOUNDARY).block(TIMEOUT));
            observedHead = headRead.lastSequencePosition();
            assertThat(headRead.stream().map(CloudEvent::getId))
                    .as("the late event is uncommitted and must not be visible yet")
                    .doesNotContain(deposited.getId());

            holdingOperator.release.complete(null);
            lateAppend.get(30, TimeUnit.SECONDS);
        } finally {
            holdingOperator.release.complete(null);
        }
        long lateEventPosition = requireNonNull(reader.read(ACCOUNT_BOUNDARY).block(TIMEOUT)).stream()
                .filter(e -> e.getId().equals(deposited.getId()))
                .mapToLong(OccurrentCloudEventExtension::getPosition)
                .findFirst()
                .orElseThrow(() -> new AssertionError("the late event was never committed"));
        List<String> idsAfterObservedHead = requireNonNull(reader.read(ACCOUNT_BOUNDARY, DcbReadOptions.afterPosition(observedHead)).block(TIMEOUT)).stream()
                .map(CloudEvent::getId)
                .toList();

        // Then
        long head = observedHead;
        assertAll(
                () -> assertThat(lateEventPosition)
                        .as("The Deposited event committed after a reader had observed head %d, so its position must be above that head, but it was committed at position %d", head, lateEventPosition)
                        .isGreaterThan(head),
                () -> assertThat(idsAfterObservedHead)
                        .as("A read after the observed head %d (what a snapshot saved at that position resumes from) must contain the Deposited event that committed afterwards", head)
                        .contains(deposited.getId())
        );
    }

    private static ReactorMongoEventStore newEventStore(ReactiveMongoTemplate template, TransactionalOperator transactionalOperator) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionalOperator)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new ReactorMongoEventStore(template, config);
    }

    /**
     * A {@link TransactionalOperator} that holds the first transaction it is asked to wrap, before the transaction begins,
     * until released. The store has reserved its position block by then, and every later transaction runs normally.
     */
    private static final class HoldingTransactionalOperator implements TransactionalOperator {
        private final TransactionalOperator delegate;
        private final CountDownLatch held = new CountDownLatch(1);
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final AtomicBoolean holdNext = new AtomicBoolean(true);

        HoldingTransactionalOperator(TransactionalOperator delegate) {
            this.delegate = delegate;
        }

        private Mono<Void> holdOnce() {
            return Mono.defer(() -> {
                if (!holdNext.compareAndSet(true, false)) {
                    return Mono.empty();
                }
                held.countDown();
                return Mono.fromFuture(release).timeout(TIMEOUT);
            });
        }

        @Override
        public <T> Mono<T> transactional(Mono<T> mono) {
            return holdOnce().then(delegate.transactional(mono));
        }

        @Override
        public <T> Flux<T> transactional(Flux<T> flux) {
            return holdOnce().thenMany(delegate.transactional(flux));
        }

        @Override
        public <T> Flux<T> execute(TransactionCallback<T> action) {
            return holdOnce().thenMany(delegate.execute(action));
        }
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList());
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
