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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tagsAllOf;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;

/**
 * Reproduction for the read-watermark overshoot lost-conflict bug (finding 2).
 * <p>
 * {@code read()} reports {@code lastSequencePosition() == currentDcbPosition()}, i.e. the global position counter. That
 * counter is incremented by {@code reserveDcbPositions} <em>outside</em> the append transaction, before the events
 * commit. So a reader that runs while an append holds reserved-but-uncommitted positions inherits a watermark that
 * points past data it cannot see. The reader's command then derives {@code failIfEventsMatch(query, watermark)}, and the
 * append-time check {@code position > watermark} structurally excludes the very events the appender reserved — once they
 * commit, the conflict is silently lost and the reader's conditional append wrongly succeeds.
 * <p>
 * The interleaving is made deterministic by pausing appender A at the start of its transaction (after it has reserved
 * its positions) until reader R has read. R then observes an empty boundary but a watermark of 1, A commits its matching
 * event at position 1, and R appends conditionally on watermark 1. The correct DCB outcome is
 * {@link DcbAppendConditionNotFulfilledException}; the bug lets R succeed.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbReadWatermarkTest {

    private static final URI SOURCE = URI.create("urn:test:read-watermark");
    private static final String COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(
            new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_read_watermark"));

    @Test
    void conditional_append_detects_an_event_reserved_before_but_committed_after_the_read() throws Exception {
        String tag = "shared-tag";

        CountDownLatch appenderReserved = new CountDownLatch(1);
        CountDownLatch readerHasRead = new CountDownLatch(1);

        // Appender A: pauses at the start of its transaction (its positions are already reserved by then).
        ConnectionString appenderCs = connectionString();
        MongoClient appenderClient = MongoClients.create(appenderCs);
        String databaseName = requireNonNull(appenderCs.getDatabase());
        MongoTemplate appenderTemplate = new MongoTemplate(appenderClient, databaseName);
        PausingTransactionTemplate pausingTx = new PausingTransactionTemplate(
                new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(appenderClient, databaseName)),
                appenderReserved, readerHasRead);
        SpringMongoEventStore appenderStore = newEventStore(appenderTemplate, pausingTx);

        // Reader R: a plain store over the same collection.
        MongoClient readerClient = MongoClients.create(connectionString());
        MongoTemplate readerTemplate = new MongoTemplate(readerClient, databaseName);
        SpringMongoEventStore readerStore = newEventStore(readerTemplate,
                new TransactionTemplate(new MongoTransactionManager(
                        new SimpleMongoClientDatabaseFactory(readerClient, databaseName))));

        // A reads an empty boundary and appends a matching event conditionally.
        DcbAppendCondition appenderCondition = failIfEventsMatch(tagsAllOf(tag), DcbConsistencyToken.of(0));
        Thread appender = new Thread(
                () -> appenderStore.append(List.of(taggedEvent("AppenderEvent", tag)), appenderCondition),
                "appender");
        appender.start();

        // A has reserved its position block (counter advanced) and is paused before committing.
        assertThat(appenderReserved.await(30, TimeUnit.SECONDS)).as("appender did not reach its paused transaction").isTrue();

        // R reads the same boundary. A's event is uncommitted, so R observes nothing, and the consistency token it
        // captures reflects only committed appends (A's marker version is not bumped until A commits).
        DcbEventStream readerBoundary = readerStore.read(tagsAllOf(tag));
        DcbConsistencyToken readerToken = readerBoundary.consistencyToken();
        assertThat(readerBoundary.stream().toList())
                .as("A's event is uncommitted, so R must observe an empty boundary")
                .isEmpty();

        readerHasRead.countDown();
        appender.join(TimeUnit.SECONDS.toMillis(30));

        // R appends conditionally on the token it read. A's matching event is now committed, which bumped the marker
        // version, so the token has changed and R's append must be rejected.
        DcbAppendCondition readerCondition = failIfEventsMatch(tagsAllOf(tag), readerToken);
        Throwable thrown = catchThrowable(() ->
                readerStore.append(List.of(taggedEvent("ReaderEvent", tag)), readerCondition));

        assertThat(thrown)
                .as("R observed an empty boundary at consistency token %s, but A committed a matching event after R's "
                        + "read (bumping the marker version). The token-based check must reject R's append; a "
                        + "position-based check would miss it (silent lost conflict).", readerToken)
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    // ---------------------------------------------------------------------------
    // Infrastructure
    // ---------------------------------------------------------------------------

    private SpringMongoEventStore newEventStore(MongoTemplate template, TransactionTemplate transactionTemplate) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(transactionTemplate)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new SpringMongoEventStore(template, config);
    }

    private ConnectionString connectionString() {
        return new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_read_watermark");
    }

    /**
     * A {@link TransactionTemplate} that, on its first {@code execute}, signals that the caller has entered the
     * transaction (its DCB positions are already reserved by then, since reservation happens before the transaction) and
     * waits until released before running the real transaction. Subsequent calls run normally.
     */
    private static final class PausingTransactionTemplate extends TransactionTemplate {
        private final transient CountDownLatch entered;
        private final transient CountDownLatch release;
        private final AtomicBoolean paused = new AtomicBoolean(false);

        PausingTransactionTemplate(MongoTransactionManager txManager, CountDownLatch entered, CountDownLatch release) {
            super(txManager);
            this.entered = entered;
            this.release = release;
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            if (paused.compareAndSet(false, true)) {
                entered.countDown();
                await(release);
            }
            return super.execute(action);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
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
