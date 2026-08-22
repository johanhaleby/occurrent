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

package org.occurrent.dsl.projection.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The acceptance criterion issue #740 states, proven at the membership level
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>):
 * "Two concurrent appends that commit in the reverse order of their positions. The wait for the stalled writer's
 * append must not answer true until that writer's event has actually been applied."
 * <p>
 * Writer A reserves a stream position (outside its transaction, per ADR 84) and is then held paused before its
 * transaction commits. Writer B, on a different stream over the same collection, reserves a strictly later position
 * and commits immediately, so the change stream delivers B before A even though A's position is lower. A
 * position-based watermark ("everything at or below the highest applied position is applied", the design ADR 122
 * withdrew) would misreport A as applied the moment B is recorded, since B's position is higher than A's. The
 * membership design tracks each append's own identity instead, so it must not make that mistake: {@code hasApplied}
 * for A's append stays false while only B has been delivered, and only turns true once A's own event actually
 * commits and is delivered.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingMaterializedViewMembershipInversionTest {

    private static final URI SOURCE = URI.create("urn:occurrent:membership-inversion");
    private static final String PROJECTION_ID = "membership-inversion";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    record Ticked(String id) {
    }

    @Test
    void the_wait_for_a_stalled_writers_append_does_not_answer_true_until_that_writers_event_is_actually_applied() throws Exception {
        String databaseName = "membership_inversion";
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(databaseName));
        String collection = "events";

        CountDownLatch reserved = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // All four clients are try-with-resources, so a failure anywhere below (including a subscription that never
        // starts) still closes every one of them, rather than leaking whichever were already open at that point.
        try (MongoClient writerAClient = MongoClients.create(connectionString);
             MongoClient writerBClient = MongoClients.create(connectionString);
             MongoClient observerClient = MongoClients.create(connectionString);
             MongoClient recorderClient = MongoClients.create(connectionString)) {

            // Writer A: paused right after its position is reserved (reservation happens outside the transaction,
            // ADR 84), before its transaction commits.
            MongoTemplate writerATemplate = new MongoTemplate(writerAClient, requireNonNull(connectionString.getDatabase()));
            PausingTransactionTemplate pausingTx = new PausingTransactionTemplate(
                    new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(writerAClient, requireNonNull(connectionString.getDatabase()))),
                    reserved, release);
            SpringMongoEventStore writerAStore = newEventStore(writerATemplate, pausingTx, collection);

            // Writer B: a plain store over the same collection, commits fully and immediately.
            MongoTemplate writerBTemplate = new MongoTemplate(writerBClient, requireNonNull(connectionString.getDatabase()));
            SpringMongoEventStore writerBStore = newEventStore(writerBTemplate,
                    new TransactionTemplate(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(writerBClient, requireNonNull(connectionString.getDatabase())))),
                    collection);

            // A raw, side-channel subscription (independent of the recording wrapper under test) that records the
            // real delivery order and the real position of every event, proving the inversion genuinely happened
            // rather than being assumed.
            MongoTemplate observerTemplate = new MongoTemplate(observerClient, requireNonNull(connectionString.getDatabase()));
            SpringMongoSubscriptionModel observerModel = new SpringMongoSubscriptionModel(observerTemplate, collection, TimeRepresentation.RFC_3339_STRING);

            // The recording projection under test: a real RecordingMaterializedView over a real live subscription,
            // the shipped production path (Projections.recordingAppliedAppends), never a hand-rolled record call.
            MongoTemplate recorderTemplate = new MongoTemplate(recorderClient, requireNonNull(connectionString.getDatabase()));
            SpringMongoSubscriptionModel recorderModel = new SpringMongoSubscriptionModel(recorderTemplate, collection, TimeRepresentation.RFC_3339_STRING);

            // Both models' shutdown lives in one finally covering both waitUntilStarted assertions below, so either
            // one failing to start still shuts down whichever model(s) were already subscribed.
            try {
                CloudEventConverter<Ticked> converter = new JacksonCloudEventConverter<>(new ObjectMapper(), SOURCE);
                List<EventMetadata> deliveryOrder = new CopyOnWriteArrayList<>();
                SubscriptionHandle observerSubscription = observerModel.subscribe("observer", StartAt.now(),
                        cloudEvent -> deliveryOrder.add(EventMetadata.from(cloudEvent)));
                assertThat(observerSubscription.waitUntilStarted(Duration.ofSeconds(20))).as("the observer subscription never started").isTrue();

                ConcurrentHashMap<String, Integer> state = new ConcurrentHashMap<>();
                ViewStateRepository<Integer, String> repository = ViewStateRepository.create(state::get, state::put);
                Projection<Integer, Ticked, String> projection = Projection.<Integer, Ticked>singletonBuilder(0)
                        .on(Ticked.class, (count, event) -> count + 1)
                        .build();
                AppliedAppendStore appliedAppendStore = AppliedAppendStore.inMemory();
                List<AppendId> recordedInOrder = new CopyOnWriteArrayList<>();
                AppliedAppendStore observedStore = recordingSpy(appliedAppendStore, recordedInOrder);
                MaterializedView<Ticked> view = Projections.materializedView(projection, repository, PROJECTION_ID);
                MaterializedView<Ticked> recordingView = Projections.recordingAppliedAppends(view, PROJECTION_ID, observedStore);
                ProjectionRunner<Ticked> runner = ProjectionRunner.stream(recorderModel, converter);
                SubscriptionHandle recordingSubscription = runner.project(PROJECTION_ID, projection, recordingView, StartAt.now());
                assertThat(recordingSubscription.waitUntilStarted(Duration.ofSeconds(20))).as("the recording subscription never started").isTrue();

                Thread appender = new Thread(() -> writerAStore.write("stream-a", List.of(converter.toCloudEvent(new Ticked("a")))), "writer-a");
                try {
                    // A starts writing and pauses, its position already reserved.
                    appender.start();
                    assertThat(reserved.await(30, TimeUnit.SECONDS)).as("writer A did not reach its paused transaction").isTrue();

                    // B commits fully, on a different stream, and reserves a strictly higher position than A's already-reserved one.
                    WriteResult resultB = writerBStore.write("stream-b", List.of(converter.toCloudEvent(new Ticked("b"))));
                    AppendId appendIdB = resultB.appendId().orElseThrow();

                    await().atMost(Duration.ofSeconds(20)).until(() -> appliedAppendStore.hasApplied(PROJECTION_ID, appendIdB));

                    // The crux of the acceptance criterion: at the moment B's (higher-position) append is recorded as
                    // applied, A's event has not committed yet, so nothing else has been recorded. A position-based
                    // watermark would have called this "applied up to B's position", wrongly including A's lower one.
                    assertThat(recordedInOrder).as("only B's append should be recorded while A is still paused before commit").containsExactly(appendIdB);
                    // A's own append id is not known yet (uncommitted inside A's still-paused transaction, so it
                    // cannot be read from anywhere outside it), which is why this cannot call hasApplied with A's
                    // real id directly. A fresh, unrelated id stands in to exercise the read path itself rather than
                    // relying only on the recordApplied spy above: hasApplied is a plain membership lookup against
                    // the same set recordApplied writes to, so an id nothing has recorded for answers false, exactly
                    // as A's real id must until its own commit reaches this projection.
                    assertThat(appliedAppendStore.hasApplied(PROJECTION_ID, AppendId.mint())).isFalse();

                    // Release A: it commits last, with the lower position.
                    release.countDown();
                    appender.join(TimeUnit.SECONDS.toMillis(30));
                    assertThat(appender.isAlive()).isFalse();

                    // Read A's append id back from what was actually delivered (not minted ahead of time), and prove
                    // the wait now (and only now) answers true for it.
                    await().atMost(Duration.ofSeconds(20)).until(() -> deliveryOrder.size() >= 2);
                    EventMetadata aMetadata = deliveryOrder.stream()
                            .filter(metadata -> "stream-a".equals(metadata.getStreamId()))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("A's event was never observed as delivered"));
                    AppendId appendIdA = AppendId.from(aMetadata).orElseThrow();

                    assertThat(appliedAppendStore.waitUntilApplied(PROJECTION_ID, appendIdA, Duration.ofSeconds(20))).isTrue();
                    assertThat(recordedInOrder).as("B was recorded first, A only after its own commit").containsExactly(appendIdB, appendIdA);

                    // The real, independently observed positions confirm the inversion actually happened: A's
                    // position is lower than B's even though A committed and was delivered after B.
                    EventMetadata bMetadata = deliveryOrder.stream()
                            .filter(metadata -> "stream-b".equals(metadata.getStreamId()))
                            .findFirst()
                            .orElseThrow();
                    assertThat(deliveryOrder.get(0).getStreamId()).as("B is delivered first, in real commit order").isEqualTo("stream-b");
                    assertThat(requireNonNull(aMetadata.getPosition())).as("but A's reserved position is lower than B's")
                            .isLessThan(requireNonNull(bMetadata.getPosition()));
                } finally {
                    // release unconditionally, not just after the happy path: if an assertion above throws before
                    // release.countDown() runs, writer A stays parked on its latch forever, and this join would hang
                    // the whole Maven fork rather than let the failure surface. CountDownLatch.countDown() is a
                    // no-op once the count has already reached zero, so releasing twice on the happy path is
                    // harmless.
                    release.countDown();
                    appender.join(TimeUnit.SECONDS.toMillis(30));
                }
            } finally {
                recorderModel.shutdown();
                observerModel.shutdown();
            }
        }
    }

    private static AppliedAppendStore recordingSpy(AppliedAppendStore delegate, List<AppendId> recordedInOrder) {
        return new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                // recordedInOrder updates before the delegate, not after: this test polls hasApplied() (reading the
                // delegate) and then immediately asserts recordedInOrder, on a different thread. Updating the list
                // first, in program order before the delegate write a waiter can observe, means a waiter that has
                // just seen hasApplied() turn true is guaranteed to see this list entry too.
                recordedInOrder.add(appendId);
                delegate.recordApplied(projectionId, appendId);
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                return delegate.hasApplied(projectionId, appendId);
            }

            @Override
            public void clear(String projectionId) {
                delegate.clear(projectionId);
            }
        };
    }

    private static SpringMongoEventStore newEventStore(MongoTemplate template, TransactionTemplate transactionTemplate, String collection) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(collection)
                .transactionConfig(transactionTemplate)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        return new SpringMongoEventStore(template, config);
    }

    /**
     * A {@link TransactionTemplate} that, on its first {@code execute}, signals that the caller has entered the
     * transaction (its stream position is already reserved by then, since reservation happens outside the
     * transaction, ADR 84) and waits until released before running the real transaction. Subsequent calls run
     * normally. Adapted from {@code SpringMongoEventStoreDcbReadWatermarkTest}'s identical helper.
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
    }
}
