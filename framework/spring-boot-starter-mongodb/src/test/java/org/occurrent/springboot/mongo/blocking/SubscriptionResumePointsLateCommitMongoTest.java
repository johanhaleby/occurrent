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

package org.occurrent.springboot.mongo.blocking;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A DCB event store reserves an event's position before its transaction commits, so an event at a lower position can
 * become visible after one at a higher position. An application that stores the checkpoint each event was delivered
 * with, and answers it from a {@link SubscriptionResumePoints} bean after a restart, still gets the event that
 * committed late.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(240)
class SubscriptionResumePointsLateCommitMongoTest {

    private static final String HELD_WRITER = "writer-A-held";
    private static final String RESUMED = "resumed-after-late-commit";

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_checkpoint_delivered_by_a_replay_that_passed_a_late_commit_resumes_a_now_subscription_at_the_event_that_committed_late() throws Exception {
        // Given
        String database = "late-commit-" + UUID.randomUUID().toString().substring(0, 8);
        ConfigurableApplicationContext first = run(database, null);
        String checkpointOfC;
        try {
            Thread writerA = holdWriterAInItsCommit(first);
            DcbAppendResult appendedB = append(first, new Plain("B"), "name:B");
            DcbAppendResult appendedC = append(first, new Plain("C"), "name:C");

            // When
            Map<String, String> checkpoints = aReplayDelivers(first, "B", "C");
            checkpointOfC = checkpoints.get("C");
            first.getBean(HoldControl.class).releaseA.countDown();
            writerA.join(20_000);
            assertThat(writerA.isAlive()).as("writer A still committing").isFalse();
            assertThat(writerAResult.get().firstSequencePosition()).as("A reserved its position before B did").isLessThan(appendedB.firstSequencePosition());
            assertThat(appendedC.lastSequencePosition()).isGreaterThan(writerAResult.get().firstSequencePosition());
        } finally {
            first.getBean(HoldControl.class).releaseA.countDown();
            first.close();
        }

        // Then
        ConfigurableApplicationContext second = run(database, checkpointOfC);
        try {
            Delivered delivered = second.getBean(Delivered.class);
            append(second, new Plain("D"), "name:D");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(delivered.names()).as("what the subscription resumed from the checkpoint of C delivered").containsExactly("A", "D"));
        } finally {
            second.close();
        }
    }

    @Test
    void a_checkpoint_delivered_live_before_a_late_commit_resumes_a_now_subscription_at_the_event_that_committed_late() throws Exception {
        // Given
        String database = "late-commit-" + UUID.randomUUID().toString().substring(0, 8);
        ConfigurableApplicationContext first = run(database, null);
        String checkpointOfC;
        try {
            DcbSubscriptions<TestEvent> dcbSubscriptions = dcbSubscriptionsOf(first);
            Map<String, String> checkpoints = new ConcurrentHashMap<>();
            CopyOnWriteArrayList<String> names = new CopyOnWriteArrayList<>();
            dcbSubscriptions.subscribeWithMetadata("recorder", DcbCriteria.all(), DcbStartAt.now(), true, (metadata, event) -> {
                names.add(event.name());
                checkpoints.put(event.name(), metadata.checkpoint().orElse(""));
            });
            Thread writerA = holdWriterAInItsCommit(first);
            DcbAppendResult appendedB = append(first, new Plain("B"), "name:B");
            append(first, new Plain("C"), "name:C");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(names).as("what the live subscription delivered").containsExactly("B", "C"));
            checkpointOfC = checkpoints.get("C");
            assertThat(checkpointOfC).as("the checkpoint C was delivered with").isNotEmpty();

            // When
            first.getBean(HoldControl.class).releaseA.countDown();
            writerA.join(20_000);
            assertThat(writerA.isAlive()).as("writer A still committing").isFalse();
            assertThat(writerAResult.get().firstSequencePosition()).as("A reserved its position before B did").isLessThan(appendedB.firstSequencePosition());
        } finally {
            first.getBean(HoldControl.class).releaseA.countDown();
            first.close();
        }

        // Then
        ConfigurableApplicationContext second = run(database, checkpointOfC);
        try {
            Delivered delivered = second.getBean(Delivered.class);
            append(second, new Plain("D"), "name:D");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(delivered.names()).as("what the subscription resumed from the checkpoint of C delivered").containsExactly("A", "D"));
        } finally {
            second.close();
        }
    }

    private final AtomicReference<DcbAppendResult> writerAResult = new AtomicReference<>();

    // Writer A reserves a position and holds its transaction open, so the events that take higher positions commit first
    private Thread holdWriterAInItsCommit(ConfigurableApplicationContext ctx) throws InterruptedException {
        Thread writerA = new Thread(() -> writerAResult.set(append(ctx, new Held("A"), "name:A")), HELD_WRITER);
        writerA.start();
        assertThat(ctx.getBean(HoldControl.class).aInCommit.await(20, TimeUnit.SECONDS)).as("writer A reached its commit").isTrue();
        return writerA;
    }

    private static Map<String, String> aReplayDelivers(ConfigurableApplicationContext ctx, String... names) {
        DcbSubscriptions<TestEvent> dcbSubscriptions = dcbSubscriptionsOf(ctx);
        Map<String, String> checkpoints = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        dcbSubscriptions.subscribeWithMetadata("recorder", DcbCriteria.all(), DcbStartAt.beginning(), true, (metadata, event) -> {
            delivered.add(event.name());
            checkpoints.put(event.name(), metadata.checkpoint().orElse(""));
        });
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(delivered).as("what the replay delivered while A was held").containsExactly(names));
        assertThat(checkpoints.values()).as("the checkpoint each replayed event was delivered with").doesNotContain("");
        return checkpoints;
    }

    @SuppressWarnings("unchecked")
    private static DcbSubscriptions<TestEvent> dcbSubscriptionsOf(ConfigurableApplicationContext ctx) {
        return ctx.getBean(DcbSubscriptions.class);
    }

    private static ConfigurableApplicationContext run(String database, @Nullable String resumePoint) {
        List<String> args = new ArrayList<>(List.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database));
        if (resumePoint != null) {
            args.add("--test.resume-point=" + resumePoint);
        }
        return SpringApplication.run(LateCommitApplication.class, args.toArray(String[]::new));
    }

    @SuppressWarnings("unchecked")
    private static DcbAppendResult append(ConfigurableApplicationContext ctx, TestEvent event, String tag) {
        CloudEventConverter<TestEvent> converter = ctx.getBean(CloudEventConverter.class);
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event)).stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse(tag))))
                .toList();
        return ctx.getBean(DcbEventStore.class).append(cloudEvents);
    }

    static class HoldControl {
        final CountDownLatch aInCommit = new CountDownLatch(1);
        final CountDownLatch releaseA = new CountDownLatch(1);
    }

    // Holds the commit of whichever transaction runs on the HELD_WRITER thread, after the store reserved its position
    // and inserted its documents inside the transaction, so they stay invisible to every other reader
    static class HoldingTransactionManager extends MongoTransactionManager {
        private final HoldControl hold;

        HoldingTransactionManager(MongoDatabaseFactory factory, TransactionOptions options, HoldControl hold) {
            super(factory, options);
            this.hold = hold;
        }

        @Override
        protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
            if (Thread.currentThread().getName().equals(HELD_WRITER)) {
                hold.aInCommit.countDown();
                assertThat(hold.releaseA.await(60, TimeUnit.SECONDS)).isTrue();
            }
            super.doCommit(transactionObject);
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class LateCommitApplication {

        @Bean
        HoldControl holdControl() {
            return new HoldControl();
        }

        @Bean
        @Primary
        MongoTransactionManager holdingTransactionManager(MongoDatabaseFactory dbFactory, HoldControl holdControl) {
            return new HoldingTransactionManager(dbFactory, TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY).build(), holdControl);
        }

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:late-commit"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        @ConditionalOnProperty(name = "test.resume-point")
        SubscriptionResumePoints resumePoints(Environment environment) {
            return subscriptionId -> RESUMED.equals(subscriptionId) ? environment.getProperty("test.resume-point") : null;
        }

        @Bean
        @ConditionalOnProperty(name = "test.resume-point")
        Delivered delivered() {
            return new Delivered();
        }
    }

    static class Delivered {
        private final CopyOnWriteArrayList<String> names = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = RESUMED, eventTypes = {Held.class, Plain.class}, startAt = StartPosition.NOW)
        void on(TestEvent event, DcbEventMetadata metadata) {
            names.add(event.name());
        }

        List<String> names() {
            return names;
        }
    }

    interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Held(String eventId, Date timestamp, String name) implements TestEvent {
        Held(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record Plain(String eventId, Date timestamp, String name) implements TestEvent {
        Plain(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
