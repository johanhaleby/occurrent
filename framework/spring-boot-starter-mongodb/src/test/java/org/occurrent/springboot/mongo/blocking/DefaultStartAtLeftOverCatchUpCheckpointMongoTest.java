/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * A blocking {@code @Subscription}, {@code @StreamSubscription} or {@code @Projection} with {@code startAt} unset is delivered the events written
 * while it was down when the checkpoint stored for its id is a catch-up checkpoint.
 * <p>
 * A first boot at the present delivers {@code historic-1} and stores a live checkpoint. A test can't stop a catch-up
 * halfway, so the catch-up checkpoint a replay would have left is written in its place, positioned at {@code historic-1}
 * and resuming live from the checkpoint the first boot stored. {@code while-down-1} is then written before the second boot
 * subscribes and {@code live-2} after it started.
 * <p>
 * Booted with {@link SpringApplication#run} rather than {@code @SpringBootTest}, so there is no
 * {@code @ServiceConnection} and the url is passed as a boot argument.
 */
@DisplayName("DEFAULT start position with a catch-up checkpoint in storage")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(240)
class DefaultStartAtLeftOverCatchUpCheckpointMongoTest {

    @Container
    static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final String ID = "default-left-over";

    @Test
    void subscription_without_start_at_is_delivered_what_was_written_while_it_was_down_when_a_catch_up_checkpoint_is_stored() {
        List<String> handled = restartWithCatchUpCheckpoint("subscription", SubscriptionDefault.class);

        assertThat(handled).containsExactly("while-down-1", "live-2");
    }

    @Test
    void projection_without_start_at_is_delivered_what_was_written_while_it_was_down_when_a_catch_up_checkpoint_is_stored() {
        List<String> handled = restartWithCatchUpCheckpoint("projection", ProjectionDefault.class);

        assertThat(handled).containsExactly("while-down-1", "live-2");
    }

    @Test
    void stream_subscription_without_start_at_is_delivered_what_was_written_while_it_was_down_when_a_catch_up_checkpoint_is_stored() {
        List<String> handled = restartWithCatchUpCheckpoint("stream-subscription", StreamSubscriptionDefault.class);

        assertThat(handled).containsExactly("while-down-1", "live-2");
    }

    // The standby is paused through its own subscription model, which stops it from competing (ADR 112), so it does not
    // take over when the active instance closes. Resuming it after the checkpoint is planted and an event is written makes
    // it win the lease and subscribe for the first time, which is the takeover.
    @Test
    void standby_without_start_at_is_delivered_what_was_written_before_it_took_over_when_a_catch_up_checkpoint_is_stored() {
        String database = "default-left-over-standby";
        try (ConfigurableApplicationContext active = boot(database, "instance-a", SubscriptionDefault.class, "")) {
            Checkpoint beforeLive1 = active.getBean(CheckpointStorage.class).read(ID);
            write(active, "live-1");
            Checkpoint afterLive1 = awaitLiveCheckpointOtherThan(active.getBean(CheckpointStorage.class), beforeLive1);

            try (ConfigurableApplicationContext standby = boot(database, "instance-b", SubscriptionDefault.class, "")) {
                SubscriptionModel standbyModel = standby.getBean(SubscriptionModel.class);
                standbyModel.pauseSubscription(ID);
                await().atMost(ofSeconds(10)).pollInterval(ofMillis(50)).until(() -> standbyModel.isPaused(ID));
                active.close();

                CheckpointStorage storage = standby.getBean(CheckpointStorage.class);
                // live-1 is global position 1
                storage.save(ID, GlobalCheckpoint.of(1, afterLive1, 0, 1));
                assertThat(isCatchUpCheckpoint(storage.read(ID))).as("catch-up checkpoint").isTrue();
                write(standby, "while-down-1");
                HistoryBeforeThePresent.awaitedWith(standby.getBean(MongoTemplate.class));
                standbyModel.resumeSubscription(ID).waitUntilStarted(ofSeconds(30));
                write(standby, "live-2");
                Handled handler = standby.getBean(SubscriptionDefault.class);
                awaitHandled(handler, "live-2");

                assertThat(handler.names()).containsExactly("while-down-1", "live-2");
            }
        }
    }

    @Test
    void subscription_without_start_at_is_not_delivered_what_was_written_before_it_booted_when_nothing_is_stored() {
        try (ConfigurableApplicationContext context = boot("default-left-over-nothing-stored", "restart", SubscriptionDefault.class, "historic-1,historic-2")) {
            write(context, "live-2");
            Handled handler = context.getBean(SubscriptionDefault.class);
            awaitHandled(handler, "live-2");

            assertThat(handler.names()).containsExactly("live-2");
        }
    }

    private static List<String> restartWithCatchUpCheckpoint(String name, Class<? extends Handled> handlerType) {
        String database = "default-left-over-" + name;
        Checkpoint live = firstBootStoringALiveCheckpoint(database, handlerType);
        AtomicReference<ConfigurableApplicationContext> started = new AtomicReference<>();
        assertThatCode(() -> started.set(boot(database, "restart", handlerType, "while-down-1"))).as("the second boot").doesNotThrowAnyException();
        try (ConfigurableApplicationContext secondBoot = started.get()) {
            write(secondBoot, "live-2");
            Handled handler = secondBoot.getBean(handlerType);
            awaitHandled(handler, "live-2");
            return handler.names();
        }
    }

    // Closed before it returns, leaving historic-1 as global position 1 and a catch-up checkpoint over it in storage.
    private static Checkpoint firstBootStoringALiveCheckpoint(String database, Class<? extends Handled> handlerType) {
        try (ConfigurableApplicationContext firstBoot = boot(database, "restart", handlerType, "")) {
            CheckpointStorage storage = firstBoot.getBean(CheckpointStorage.class);
            Checkpoint beforeHistoric1 = storage.read(ID);
            write(firstBoot, "historic-1");
            Checkpoint live = awaitLiveCheckpointOtherThan(storage, beforeHistoric1);
            storage.save(ID, GlobalCheckpoint.of(1, live, 0, 1));
            assertThat(isCatchUpCheckpoint(storage.read(ID))).as("catch-up checkpoint").isTrue();
            return live;
        }
    }

    // A checkpoint is stored after the handler returned, so one that differs from the one before shows that the event is checkpointed.
    private static Checkpoint awaitLiveCheckpointOtherThan(CheckpointStorage storage, Checkpoint before) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).untilAsserted(() -> {
            Checkpoint stored = storage.read(ID);
            assertThat(stored).isNotNull().isNotEqualTo(before);
            assertThat(isCatchUpCheckpoint(stored)).as("catch-up checkpoint %s", stored.asString()).isFalse();
        });
        return storage.read(ID);
    }

    private static boolean isCatchUpCheckpoint(Checkpoint checkpoint) {
        return GlobalCheckpoint.isGlobalCheckpoint(checkpoint) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(checkpoint);
    }

    @SuppressWarnings("unchecked")
    private static void write(ConfigurableApplicationContext context, String name) {
        ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
        applicationService.execute(name, __ -> List.of(new TestEvent(name)));
    }

    private static void awaitHandled(Handled handler, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(handler.names()).contains(marker));
    }

    private static ConfigurableApplicationContext boot(String database, String applicationName, Class<? extends Handled> handlerType, String writtenBeforeSubscribing) {
        SpringApplication application = new SpringApplication(TestApplication.class);
        application.addInitializers(context -> ((GenericApplicationContext) context).registerBean(handlerType));
        return application.run(
                "--spring.application.name=" + applicationName,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database,
                "--test.before-subscribe=" + writtenBeforeSubscribing);
    }

    @SpringBootApplication
    @EnableOccurrent
    static class TestApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:default-start-at-left-over"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService, MongoTemplate mongoTemplate, @Value("${test.before-subscribe}") String beforeSubscribe) {
            return new EventWriter(applicationService, mongoTemplate, beforeSubscribe);
        }
    }

    static class EventWriter {
        private final ApplicationService<TestEvent> applicationService;
        private final MongoTemplate mongoTemplate;
        private final List<String> beforeSubscribe;

        EventWriter(ApplicationService<TestEvent> applicationService, MongoTemplate mongoTemplate, String beforeSubscribe) {
            this.applicationService = applicationService;
            this.mongoTemplate = mongoTemplate;
            this.beforeSubscribe = Stream.of(beforeSubscribe.split(",")).filter(name -> !name.isBlank()).toList();
        }

        // Subscriptions start once every singleton exists, so these are written before any of them does.
        @PostConstruct
        void writeBeforeSubscribe() {
            if (beforeSubscribe.isEmpty()) {
                return;
            }
            beforeSubscribe.forEach(name -> applicationService.execute(name, __ -> List.of(new TestEvent(name))));
            HistoryBeforeThePresent.awaitedWith(mongoTemplate);
        }
    }

    interface Handled {
        List<String> names();
    }

    abstract static class Recording implements Handled {
        private final CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();

        void record(String name) {
            handled.add(name);
        }

        @Override
        public List<String> names() {
            return List.copyOf(handled);
        }
    }

    static class SubscriptionDefault extends Recording {
        @Subscription(id = ID)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamSubscriptionDefault extends Recording {
        @StreamSubscription(id = ID)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class ProjectionDefault extends Recording {
        @Projection(id = ID)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Seen, TestEvent, String>builder(new Seen("none"))
                    .id(TestEvent::name)
                    .on(TestEvent.class, (state, event) -> {
                        record(event.name());
                        return new Seen(event.name());
                    })
                    .build();
        }
    }

    record Seen(@Id String id) {
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
