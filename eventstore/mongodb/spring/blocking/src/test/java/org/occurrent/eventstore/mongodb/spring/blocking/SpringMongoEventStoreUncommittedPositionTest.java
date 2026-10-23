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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;

/**
 * Reproduction for issue 1230: the global position block is reserved before the append transaction starts, so an append
 * that reserved a lower position can commit after an append that reserved a higher one.
 * <p>
 * Append A reserves its position block and is then held before its transaction begins, which is where the first
 * attempt of an append that backs off on a transient transaction error sits. Append B reserves the next block and
 * commits. A reader that has observed the head after B (a snapshot saved at B's position, a read from that
 * position) is then behind a position that A's event later commits below.
 * <p>
 * The invariant under test: an event that commits after a reader observed head {@code p} has a position above
 * {@code p}.
 */
@Testcontainers
@Timeout(120)
@DisplayName("SpringMongoEventStoreUncommittedPosition")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreUncommittedPositionTest {

    private static final URI SOURCE = URI.create("urn:test:uncommitted-position");
    private static final String COLLECTION = "events";
    private static final String ACCOUNT_TAG = "account:a1";
    private static final DcbCriteria ACCOUNT_BOUNDARY = DcbCriteria.types("Deposited", "Withdrawn").tags(Tag.parse(ACCOUNT_TAG));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient lateAppenderClient;
    private MongoClient readerClient;

    @AfterEach
    void close_clients() {
        if (lateAppenderClient != null) {
            lateAppenderClient.close();
        }
        if (readerClient != null) {
            readerClient.close();
        }
    }

    @Nested
    @DisplayName("when a dcb append is held after reserving its position while a later dcb append commits")
    class When_a_dcb_append_is_held_after_reserving_its_position_while_a_later_dcb_append_commits {

        @Test
        void an_event_that_commits_after_a_reader_observed_the_head_has_a_position_above_that_head() throws Exception {
            // Given
            CloudEvent deposited = taggedEvent("Deposited", ACCOUNT_TAG);

            // When
            LateCommit lateCommit = commitLateEventBehindAnObservedHead(
                    lateAppender -> lateAppender.append(List.of(deposited)),
                    reader -> {
                        DcbEventStream boundary = reader.read(ACCOUNT_BOUNDARY);
                        DcbAppendCondition condition = failIfEventsMatch(ACCOUNT_BOUNDARY, boundary.consistencyToken());
                        return reader.append(List.of(taggedEvent("Withdrawn", ACCOUNT_TAG)), condition);
                    },
                    new Reads(
                            store -> store.read(ACCOUNT_BOUNDARY).lastSequencePosition(),
                            store -> store.read(ACCOUNT_BOUNDARY).stream().toList(),
                            (store, head) -> store.read(ACCOUNT_BOUNDARY, DcbReadOptions.afterPosition(head)).stream().toList()),
                    deposited.getId());

            // Then
            assertAll(
                    () -> assertThat(lateCommit.lateEventPosition())
                            .as("The Deposited event committed after a reader had observed head %d, so its position must be above that head, but it was committed at position %d", lateCommit.observedHead(), lateCommit.lateEventPosition())
                            .isGreaterThan(lateCommit.observedHead()),
                    () -> assertThat(lateCommit.idsOfEventsAfterObservedHead())
                            .as("A read after the observed head %d (what a snapshot saved at that position resumes from) must contain the Deposited event that committed afterwards", lateCommit.observedHead())
                            .contains(deposited.getId())
            );
        }
    }

    @Nested
    @DisplayName("when a stream write is held after reserving its position while a later dcb append commits")
    class When_a_stream_write_is_held_after_reserving_its_position_while_a_later_dcb_append_commits {

        @Test
        void an_event_that_commits_after_a_reader_observed_the_head_has_a_position_above_that_head() throws Exception {
            // Given
            CloudEvent deposited = event("Deposited");

            // When
            LateCommit lateCommit = commitLateEventBehindAnObservedHead(
                    lateAppender -> lateAppender.write("stream-1", List.of(deposited)),
                    reader -> reader.append(List.of(taggedEvent("Withdrawn", ACCOUNT_TAG))),
                    new Reads(
                            SpringMongoEventStore::currentPosition,
                            store -> store.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()).toList(),
                            (store, head) -> store.readInPositionOrder(Filter.all(), PositionRange.afterPosition(head)).toList()),
                    deposited.getId());

            // Then
            assertAll(
                    () -> assertThat(lateCommit.lateEventPosition())
                            .as("The stream event committed after a reader had observed head %d, so its position must be above that head, but it was committed at position %d", lateCommit.observedHead(), lateCommit.lateEventPosition())
                            .isGreaterThan(lateCommit.observedHead()),
                    () -> assertThat(lateCommit.idsOfEventsAfterObservedHead())
                            .as("A read after the observed head %d must contain the stream event that committed afterwards", lateCommit.observedHead())
                            .contains(deposited.getId())
            );
        }
    }

    /**
     * Runs {@code lateAppend} on a store whose transaction is held until the reader's append has committed, so the late
     * append reserves its position block first and commits last. Between the two it records the head the reader observes.
     */
    private LateCommit commitLateEventBehindAnObservedHead(Consumer<SpringMongoEventStore> lateAppend,
                                                           Function<SpringMongoEventStore, DcbAppendResult> readerAppend,
                                                           Reads reads,
                                                           String lateEventId) throws Exception {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        String databaseName = requireNonNull(connectionString.getDatabase());

        lateAppenderClient = MongoClients.create(connectionString);
        HoldingTransactionTemplate holdingTransaction = new HoldingTransactionTemplate(
                new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(lateAppenderClient, databaseName)));
        SpringMongoEventStore lateAppender = newEventStore(new MongoTemplate(lateAppenderClient, databaseName), holdingTransaction);

        readerClient = MongoClients.create(connectionString);
        SpringMongoEventStore reader = newEventStore(new MongoTemplate(readerClient, databaseName),
                new TransactionTemplate(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(readerClient, databaseName))));

        CompletableFuture<Void> lateAppendFuture = CompletableFuture.runAsync(() -> lateAppend.accept(lateAppender));
        try {
            assertThat(holdingTransaction.held.await(30, TimeUnit.SECONDS))
                    .as("the late append did not reach its held transaction")
                    .isTrue();
            long counterAfterLateReservation = reader.currentPosition();
            assertThat(counterAfterLateReservation)
                    .as("the late append must have reserved its position before it was held")
                    .isEqualTo(1);

            DcbAppendResult readerAppendResult = readerAppend.apply(reader);
            assertThat(readerAppendResult.firstSequencePosition())
                    .as("the reader's append must have reserved its position after the held late append")
                    .isEqualTo(counterAfterLateReservation + 1);

            long observedHead = reads.head().apply(reader);
            assertThat(reads.all().apply(reader).stream().map(CloudEvent::getId))
                    .as("the late event is uncommitted and must not be visible yet")
                    .doesNotContain(lateEventId);

            holdingTransaction.release.countDown();
            lateAppendFuture.get(30, TimeUnit.SECONDS);

            long lateEventPosition = reads.all().apply(reader).stream()
                    .filter(e -> e.getId().equals(lateEventId))
                    .mapToLong(OccurrentCloudEventExtension::getPosition)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("the late event was never committed"));
            List<String> idsAfterObservedHead = reads.after().apply(reader, observedHead).stream()
                    .map(CloudEvent::getId)
                    .toList();
            return new LateCommit(observedHead, lateEventPosition, idsAfterObservedHead);
        } finally {
            holdingTransaction.release.countDown();
        }
    }

    private record Reads(Function<SpringMongoEventStore, Long> head,
                         Function<SpringMongoEventStore, List<CloudEvent>> all,
                         BiFunction<SpringMongoEventStore, Long, List<CloudEvent>> after) {
    }

    private record LateCommit(long observedHead, long lateEventPosition, List<String> idsOfEventsAfterObservedHead) {
    }

    private static SpringMongoEventStore newEventStore(MongoTemplate template, TransactionTemplate transactionTemplate) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(transactionTemplate)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new SpringMongoEventStore(template, config);
    }

    /**
     * A {@link TransactionTemplate} that holds its first {@code execute} before the transaction begins, which is after the
     * store has reserved the position block, and runs every later one normally.
     */
    private static final class HoldingTransactionTemplate extends TransactionTemplate {
        private final transient CountDownLatch held = new CountDownLatch(1);
        private final transient CountDownLatch release = new CountDownLatch(1);
        private final transient AtomicBoolean holdNext = new AtomicBoolean(true);

        HoldingTransactionTemplate(MongoTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            if (holdNext.compareAndSet(true, false)) {
                held.countDown();
                try {
                    if (!release.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to be released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return super.execute(action);
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
