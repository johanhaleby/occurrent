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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code startAt = NOW} combined with {@code resumeBehavior} on the stream-backed annotations that share
 * one start-position computation ({@link Subscription}, {@link org.occurrent.annotation.Projection},
 * {@link Saga} and {@link Snapshot}) behaves as documented across an actual application restart. The context is
 * closed and a fresh one is booted against the same MongoDB data, with an event written while nothing is subscribed.
 * <p>
 * Each boot writes the events named by {@code test.before-subscribe} before any subscription registers and, from an
 * {@link ApplicationRunner}, the events named by {@code test.after-start}. The last of those is a marker: it is
 * written after everything else, so once an annotation has handled it, whatever it was going to handle for the events
 * before it has arrived and the whole handled list can be asserted.
 */
@DisplayName("NOW start position with resumeBehavior on annotations across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class NowStartAtResumeBehaviorAnnotationMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    enum Annotated {
        SUBSCRIPTION(SubscriptionNowDefault.class, "now-sub-default", SubscriptionNowSameAsStartAt.class, "now-sub-same", false),
        PROJECTION(ProjectionNowDefault.class, "now-projection-default", ProjectionNowSameAsStartAt.class, "now-projection-same", false),
        SAGA(SagaNowDefault.class, "now-saga-default", SagaNowSameAsStartAt.class, "now-saga-same", false),
        // A snapshot is stored in MongoDB, so what the first boot handled is still there on the second.
        SNAPSHOT(SnapshotNowDefault.class, "now-snapshot-default", SnapshotNowSameAsStartAt.class, "now-snapshot-same", true);

        final Class<? extends Handled> withDefault;
        final String defaultId;
        final Class<? extends Handled> withSameAsStartAt;
        final String sameAsStartAtId;
        final boolean survivesRestart;

        Annotated(Class<? extends Handled> withDefault, String defaultId, Class<? extends Handled> withSameAsStartAt, String sameAsStartAtId, boolean survivesRestart) {
            this.withDefault = withDefault;
            this.defaultId = defaultId;
            this.withSameAsStartAt = withSameAsStartAt;
            this.sameAsStartAtId = sameAsStartAtId;
            this.survivesRestart = survivesRestart;
        }

        List<String> afterRestart(List<String> handledSinceRestart) {
            return survivesRestart ? Stream.concat(Stream.of("live-1"), handledSinceRestart.stream()).toList() : handledSinceRestart;
        }
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void now_with_default_resume_behavior_ignores_history_on_first_boot_and_handles_events_written_while_down_after_restart(Annotated annotated) {
        restart("now-default-" + annotated.name().toLowerCase(), annotated.withDefault,
                List.of("live-1"),
                annotated.afterRestart(List.of("while-down-1", "live-2")));
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void now_with_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down(Annotated annotated) {
        restart("now-same-as-start-at-" + annotated.name().toLowerCase(), annotated.withSameAsStartAt,
                List.of("live-1"),
                annotated.afterRestart(List.of("live-2")));
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void now_with_same_as_start_at_stores_no_checkpoint_where_default_stores_one(Annotated annotated) {
        try (ConfigurableApplicationContext context = SpringApplication.run(RestartApplication.class,
                bootArgs("now-checkpoint-" + annotated.name().toLowerCase(), "historic-1", "live-1"))) {
            Handled withDefault = context.getBean(annotated.withDefault);
            Handled withSameAsStartAt = context.getBean(annotated.withSameAsStartAt);
            awaitHandled(withDefault, "live-1");
            awaitHandled(withSameAsStartAt, "live-1");
            CheckpointStorage checkpointStorage = context.getBean(CheckpointStorage.class);

            // A checkpoint is stored after the handler returned, so the DEFAULT one appearing shows that the storage
            // is where checkpoints land and that the same event has been checkpointed for the subscription beside it.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(checkpointStorage.exists(annotated.defaultId)).as("checkpoint of %s", annotated.defaultId).isTrue());
            assertThat(checkpointStorage.exists(annotated.sameAsStartAtId)).as("checkpoint of %s", annotated.sameAsStartAtId).isFalse();
        }
    }

    // A saga and a snapshot keep their state in MongoDB, shared by the instances, so what an instance handled is not
    // what its subscription was delivered: a saga skips an event another instance already advanced it past, and a
    // snapshot is read from the shared store. Only the subscription and the projection record what they were delivered.
    @ParameterizedTest
    @EnumSource(value = Annotated.class, names = {"SAGA", "SNAPSHOT"}, mode = EnumSource.Mode.EXCLUDE)
    void now_with_same_as_start_at_runs_on_every_instance_instead_of_competing(Annotated annotated) {
        String databaseName = "now-every-instance-" + annotated.name().toLowerCase();
        try (ConfigurableApplicationContext first = SpringApplication.run(RestartApplication.class, bootArgs(databaseName, "instance-a", "", ""));
             ConfigurableApplicationContext second = SpringApplication.run(RestartApplication.class, bootArgs(databaseName, "instance-b", "", ""))) {
            Handled onFirst = first.getBean(annotated.withSameAsStartAt);
            Handled onSecond = second.getBean(annotated.withSameAsStartAt);

            write(first, "live-1");

            awaitHandled(onFirst, "live-1");
            awaitHandled(onSecond, "live-1");
            assertThat(onFirst.names()).containsExactly("live-1");
            assertThat(onSecond.names()).containsExactly("live-1");
        }
    }

    // The control for the test above: the same two instances and the same events, but a DEFAULT subscription competes,
    // so one instance handles every event and the other none. Without it, "both instances received the event" could
    // be true of any setup in which the two contexts happen to see the same stream.
    @ParameterizedTest
    @EnumSource(value = Annotated.class, names = {"SAGA", "SNAPSHOT"}, mode = EnumSource.Mode.EXCLUDE)
    void now_with_default_competes_so_one_instance_handles_each_event_and_the_other_none(Annotated annotated) {
        String databaseName = "now-competing-" + annotated.name().toLowerCase();
        try (ConfigurableApplicationContext first = SpringApplication.run(RestartApplication.class, bootArgs(databaseName, "instance-a", "", ""));
             ConfigurableApplicationContext second = SpringApplication.run(RestartApplication.class, bootArgs(databaseName, "instance-b", "", ""))) {
            Handled competingOnFirst = first.getBean(annotated.withDefault);
            Handled competingOnSecond = second.getBean(annotated.withDefault);

            write(first, "live-1");
            write(first, "live-2");

            // The SAME_AS_START_AT subscription beside it is delivered to both instances, so once both have the last
            // event, both instances are live and the competing one has had every chance to be delivered on both.
            awaitHandled(first.getBean(annotated.withSameAsStartAt), "live-2");
            awaitHandled(second.getBean(annotated.withSameAsStartAt), "live-2");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(Stream.concat(competingOnFirst.names().stream(), competingOnSecond.names().stream())).contains("live-2"));
            List<List<String>> receivers = Stream.of(competingOnFirst.names(), competingOnSecond.names()).filter(names -> !names.isEmpty()).toList();
            assertThat(receivers).containsExactly(List.of("live-1", "live-2"));
        }
    }

    private static void restart(String databaseName, Class<? extends Handled> handlerType,
                                List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        ConfigurableApplicationContext firstBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(databaseName, "historic-1,historic-2", "live-1"));
        try {
            Handled handler = firstBoot.getBean(handlerType);
            awaitHandled(handler, "live-1");
            assertThat(handler.names()).containsExactlyElementsOf(expectedFirstBoot);
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(databaseName, "while-down-1", "live-2"));
        try {
            Handled handler = secondBoot.getBean(handlerType);
            awaitHandled(handler, "live-2");
            assertThat(handler.names()).containsExactlyElementsOf(expectedSecondBoot);
        } finally {
            secondBoot.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static void write(ConfigurableApplicationContext context, String name) {
        ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
        applicationService.execute(name, __ -> List.of(new TestEvent(name)));
    }

    private static void awaitHandled(Handled handler, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(handler.names()).contains(marker));
    }

    // Booted with SpringApplication.run rather than @SpringBootTest, so there is no @ServiceConnection and the url is
    // passed as an argument.
    private static String[] bootArgs(String databaseName, String beforeSubscribe, String afterStart) {
        return bootArgs(databaseName, "restart", beforeSubscribe, afterStart);
    }

    private static String[] bootArgs(String databaseName, String applicationName, String beforeSubscribe, String afterStart) {
        return new String[]{
                "--spring.application.name=" + applicationName,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName,
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart
        };
    }

    @SpringBootApplication
    @EnableOccurrent
    static class RestartApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:now-start-at-resume-behavior"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(applicationService, beforeSubscribe, afterStart);
        }

        @Bean
        RecordingDispatcher recordingDispatcher() {
            return new RecordingDispatcher();
        }

        @Bean
        SubscriptionNowDefault subscriptionNowDefault() {
            return new SubscriptionNowDefault();
        }

        @Bean
        SubscriptionNowSameAsStartAt subscriptionNowSameAsStartAt() {
            return new SubscriptionNowSameAsStartAt();
        }

        @Bean
        ProjectionNowDefault projectionNowDefault() {
            return new ProjectionNowDefault();
        }

        @Bean
        ProjectionNowSameAsStartAt projectionNowSameAsStartAt() {
            return new ProjectionNowSameAsStartAt();
        }

        @Bean
        SagaNowDefault sagaNowDefault(RecordingDispatcher dispatcher) {
            return new SagaNowDefault(dispatcher);
        }

        @Bean
        SagaNowSameAsStartAt sagaNowSameAsStartAt(RecordingDispatcher dispatcher) {
            return new SagaNowSameAsStartAt(dispatcher);
        }

        @Bean
        SnapshotNowDefault snapshotNowDefault(MongoOperations mongoOperations) {
            return new SnapshotNowDefault(mongoOperations);
        }

        @Bean
        SnapshotNowSameAsStartAt snapshotNowSameAsStartAt(MongoOperations mongoOperations) {
            return new SnapshotNowSameAsStartAt(mongoOperations);
        }
    }

    static class EventWriter implements ApplicationRunner {
        private final ApplicationService<TestEvent> applicationService;
        private final List<String> beforeSubscribe;
        private final List<String> afterStart;

        EventWriter(ApplicationService<TestEvent> applicationService, String beforeSubscribe, String afterStart) {
            this.applicationService = applicationService;
            this.beforeSubscribe = names(beforeSubscribe);
            this.afterStart = names(afterStart);
        }

        private static List<String> names(String commaSeparated) {
            return Stream.of(commaSeparated.split(",")).filter(name -> !name.isBlank()).toList();
        }

        // Subscriptions register once every singleton exists, so these are written before any of them starts. A
        // subscription starting at the present can still receive what the same MongoClient wrote up to a second
        // earlier, so the pause keeps them out of reach of the NOW subscribers.
        @PostConstruct
        void writeBeforeSubscribe() throws InterruptedException {
            beforeSubscribe.forEach(this::write);
            Thread.sleep(1500);
        }

        @Override
        public void run(ApplicationArguments args) {
            afterStart.forEach(this::write);
        }

        // One stream per event, named after it, so a snapshot (which is kept per stream) is found by the event name.
        private void write(String name) {
            applicationService.execute(name, __ -> List.of(new TestEvent(name)));
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

    // --- @Subscription ---

    static class SubscriptionNowDefault extends Recording {
        @Subscription(id = "now-sub-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class SubscriptionNowSameAsStartAt extends Recording {
        @Subscription(id = "now-sub-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    // --- @Projection ---

    static class ProjectionNowDefault extends Recording {
        @org.occurrent.annotation.Projection(id = "now-projection-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class ProjectionNowSameAsStartAt extends Recording {
        @org.occurrent.annotation.Projection(id = "now-projection-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    // --- @Saga ---

    static class SagaNowDefault implements Handled {
        private final RecordingDispatcher dispatcher;

        SagaNowDefault(RecordingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Saga(id = "now-saga-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return issuingSaga("now-saga-default");
        }

        @Override
        public List<String> names() {
            return dispatcher.issuedBy("now-saga-default");
        }
    }

    static class SagaNowSameAsStartAt implements Handled {
        private final RecordingDispatcher dispatcher;

        SagaNowSameAsStartAt(RecordingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Saga(id = "now-saga-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return issuingSaga("now-saga-same");
        }

        @Override
        public List<String> names() {
            return dispatcher.issuedBy("now-saga-same");
        }
    }

    // --- @Snapshot ---

    static class SnapshotNowDefault extends SnapshotHandled {
        SnapshotNowDefault(MongoOperations mongoOperations) {
            super(mongoOperations, "now-snapshot-default");
        }

        @Snapshot(id = "now-snapshot-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        SnapshotView<Named, TestEvent> snapshot() {
            return snapshotView();
        }
    }

    static class SnapshotNowSameAsStartAt extends SnapshotHandled {
        SnapshotNowSameAsStartAt(MongoOperations mongoOperations) {
            super(mongoOperations, "now-snapshot-same");
        }

        @Snapshot(id = "now-snapshot-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        SnapshotView<Named, TestEvent> snapshot() {
            return snapshotView();
        }
    }

    // A snapshot is stored per stream and each event is written to a stream named after it, so the events a snapshot
    // handled are the streams that have one. Listed in the order they are written.
    abstract static class SnapshotHandled implements Handled {
        private static final List<String> WRITTEN = List.of("historic-1", "historic-2", "live-1", "while-down-1", "live-2");
        private final SpringMongoSnapshotStore<Named> store;

        SnapshotHandled(MongoOperations mongoOperations, String snapshotId) {
            this.store = new SpringMongoSnapshotStore<>(mongoOperations, Named.class, "occurrent-snapshot-" + snapshotId);
        }

        @Override
        public List<String> names() {
            return WRITTEN.stream().filter(name -> store.findLatest(name).isPresent()).toList();
        }
    }

    private static SnapshotView<Named, TestEvent> snapshotView() {
        return SnapshotView.<Named, TestEvent>builder(new Named("none"))
                .on(TestEvent.class, (state, event) -> new Named(event.name()))
                .build();
    }

    private static org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> recordingProjection(Consumer<String> onEvent) {
        return org.occurrent.dsl.projection.Projection.<Seen, TestEvent, String>builder(new Seen("none"))
                .id(TestEvent::name)
                .on(TestEvent.class, (state, event) -> {
                    onEvent.accept(event.name());
                    return new Seen(event.name());
                })
                .build();
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> issuingSaga(String sagaId) {
        return org.occurrent.dsl.saga.Saga.<TestEvent, Named, Observed>builder(new Named("none"))
                .correlateAll(TestEvent::name)
                .startsOn(TestEvent.class)
                .evolve(TestEvent.class, (state, event) -> new Named(event.name()))
                .react(TestEvent.class, (state, event) -> List.of(SagaEffect.issue(new Observed(sagaId, event.name()))))
                .build();
    }

    static class RecordingDispatcher implements CommandDispatcher<Observed> {
        private final CopyOnWriteArrayList<Observed> issued = new CopyOnWriteArrayList<>();

        @Override
        public void dispatch(Observed command) {
            issued.add(command);
        }

        List<String> issuedBy(String sagaId) {
            return issued.stream().filter(command -> command.sagaId().equals(sagaId)).map(Observed::name).toList();
        }
    }

    record Observed(String sagaId, String name) {
    }

    record Seen(@Id String id) {
    }

    record Named(String name) {
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
