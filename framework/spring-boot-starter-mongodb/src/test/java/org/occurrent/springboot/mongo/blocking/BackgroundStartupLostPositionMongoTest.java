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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.Capability;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * What a blocking annotation with {@code startupMode = BACKGROUND} does when the live checkpoint stored for its id is one
 * that MongoDB can no longer resume from, and what that does to a healthy subscription beside it.
 * <p>
 * Each kind of annotation has a database of its own. A first boot runs the id at {@code BEGINNING}, hands over and
 * stores a live checkpoint. MongoDB then loses the change stream history past all of those checkpoints with a single
 * drop, since a drop takes about a minute. A second boot runs the id at {@code BEGINNING} next to a healthy subscription
 * with an id of its own. With {@code startupMode} unset it has to fail the start, and with {@code BACKGROUND} it has to
 * hold that one subscription paused. Deleting the checkpoint and booting again lets it deliver.
 * <p>
 * Booted with {@link SpringApplication#run} rather than {@code @SpringBootTest}, so there is no
 * {@code @ServiceConnection} and the url is passed as a boot argument.
 */
@DisplayName("startupMode BACKGROUND with a stored position MongoDB has lost")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(600)
class BackgroundStartupLostPositionMongoTest {

    @Container
    static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private static final String BROKEN = "broken";
    private static final String HEALTHY = "healthy";
    private static final String DCB_TAG = "lost-position";
    private static final Map<Kind, String> STORED = new EnumMap<>(Kind.class);

    enum Kind {
        SUBSCRIPTION(SubscriptionInBackground.class, SubscriptionUnset.class),
        STREAM_SUBSCRIPTION(StreamSubscriptionInBackground.class, StreamSubscriptionUnset.class),
        DCB_SUBSCRIPTION(DcbSubscriptionInBackground.class, DcbSubscriptionUnset.class),
        STREAM_PROJECTION(StreamProjectionInBackground.class, StreamProjectionUnset.class),
        AGNOSTIC_PROJECTION(AgnosticProjectionInBackground.class, AgnosticProjectionUnset.class),
        DCB_PROJECTION(DcbProjectionInBackground.class, DcbProjectionUnset.class),
        SAGA(SagaInBackground.class, SagaUnset.class),
        STREAM_SNAPSHOT(StreamSnapshotInBackground.class, StreamSnapshotUnset.class),
        AGNOSTIC_SNAPSHOT(AgnosticSnapshotInBackground.class, AgnosticSnapshotUnset.class),
        DCB_SNAPSHOT(DcbSnapshotInBackground.class, DcbSnapshotUnset.class),
        // startAt left at its default. A catch-up reads the stored live token, so a lost one is refused as for BEGINNING
        SUBSCRIPTION_DEFAULT(SubscriptionDefaultInBackground.class, SubscriptionDefaultUnset.class, false),
        STREAM_SUBSCRIPTION_DEFAULT(StreamSubscriptionDefaultInBackground.class, StreamSubscriptionDefaultUnset.class, false),
        AGNOSTIC_PROJECTION_DEFAULT(AgnosticProjectionDefaultInBackground.class, AgnosticProjectionDefaultUnset.class, false),
        STREAM_PROJECTION_DEFAULT(StreamProjectionDefaultInBackground.class, StreamProjectionDefaultUnset.class, false),
        SAGA_DEFAULT(SagaDefaultInBackground.class, SagaDefaultUnset.class, false),
        AGNOSTIC_SNAPSHOT_DEFAULT(AgnosticSnapshotDefaultInBackground.class, AgnosticSnapshotDefaultUnset.class, false);

        private final Class<? extends Broken> inBackground;
        private final Class<? extends Broken> unset;
        // A start at the beginning replays the earlier events once the checkpoint is gone, a start at the default does not
        private final boolean replaysHistoryWithoutACheckpoint;

        Kind(Class<? extends Broken> inBackground, Class<? extends Broken> unset) {
            this(inBackground, unset, true);
        }

        Kind(Class<? extends Broken> inBackground, Class<? extends Broken> unset, boolean replaysHistoryWithoutACheckpoint) {
            this.inBackground = inBackground;
            this.unset = unset;
            this.replaysHistoryWithoutACheckpoint = replaysHistoryWithoutACheckpoint;
        }

        String database() {
            return "lost-position-" + name().toLowerCase().replace('_', '-');
        }
    }

    // A first boot for every kind, then one truncation of the oplog past all of them
    @BeforeAll
    static void store_a_live_checkpoint_for_every_kind_and_make_mongodb_lose_the_history_it_resumes_from() {
        for (Kind kind : Kind.values()) {
            try (ConfigurableApplicationContext firstBoot = boot(kind.database(), kind.unset, false, "live-1")) {
                Broken broken = firstBoot.getBean(Broken.class);
                CheckpointStorage storage = firstBoot.getBean(CheckpointStorage.class);
                await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
                    assertThat(broken.received()).as("first boot of " + kind).contains("live-1");
                    assertThat(storage.read(BROKEN)).as("the checkpoint stored for " + kind).isNotNull();
                    assertThat(storage.read(BROKEN).asString()).as("a live checkpoint, not a position a catch-up replays from").doesNotStartWith("position:");
                });
            }
            STORED.put(kind, storedCheckpoint(kind.database()));
        }
        BsonTimestamp now;
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(Kind.SUBSCRIPTION.database()))) {
            now = client.getDatabase(Kind.SUBSCRIPTION.database()).runCommand(new Document("ping", 1)).get("operationTime", BsonTimestamp.class);
        }
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, Kind.SUBSCRIPTION.database(), now);
        for (Kind kind : Kind.values()) {
            assertThat(canResumeFrom(kind.database(), STORED.get(kind))).as("canResumeFrom the checkpoint stored for " + kind).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Kind.class)
    void a_lost_position_fails_the_start_unless_the_startup_mode_is_background_and_repairing_it_lets_the_subscription_deliver(Kind kind) {
        // Unset startupMode keeps failing the start
        Throwable failure = failureOfTheStartWithStartupModeUnset(kind);
        assertThat(failure).as("the start with startupMode unset").isNotNull();
        assertThat(causes(failure)).as("the start with startupMode unset").anySatisfy(cause -> assertThat(cause).isInstanceOf(CatchupCannotResumeException.class));
        assertThat(storedCheckpoint(kind.database())).as("the checkpoint after the refused start").isEqualTo(STORED.get(kind));

        // BACKGROUND holds that one subscription paused and the application starts
        AtomicReference<ConfigurableApplicationContext> started = new AtomicReference<>();
        assertThat(catchThrowable(() -> started.set(boot(kind.database(), kind.inBackground, true, "live-2")))).as("the start with startupMode BACKGROUND").isNull();
        try (ConfigurableApplicationContext secondBoot = started.get()) {
            Broken broken = secondBoot.getBean(Broken.class);
            Healthy healthy = secondBoot.getBean(Healthy.class);
            SubscriptionModel model = secondBoot.getBean(SubscriptionModel.class);
            Logs logs = secondBoot.getBean(Logs.class);

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(healthy.received()).as("the healthy subscription").contains("live-2"));
            assertThat(model.isPaused(BROKEN)).as("the broken subscription is paused").isTrue();
            assertThat(model.isRunning(BROKEN)).as("the broken subscription is running").isFalse();
            assertThat(broken.received()).as("what the broken subscription received").isEmpty();
            assertThat(secondBoot.getBean(CheckpointStorage.class).read(BROKEN).asString()).as("the checkpoint stored for the broken subscription").isEqualTo(STORED.get(kind));
            assertThat(logs.errorsFor(BROKEN)).as("errors logged for the broken subscription").isNotEmpty();
            assertThat(logs.errorsFor(HEALTHY)).as("errors logged for the healthy subscription").isEmpty();
        }

        // Deleting the checkpoint and restarting lets the subscription deliver
        deleteStoredCheckpoint(kind.database());
        try (ConfigurableApplicationContext repairedBoot = boot(kind.database(), kind.inBackground, true, "live-3")) {
            Broken broken = repairedBoot.getBean(Broken.class);
            SubscriptionModel model = repairedBoot.getBean(SubscriptionModel.class);

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(broken.received()).as("the repaired subscription")
                    .contains(kind.replaysHistoryWithoutACheckpoint ? new String[]{"live-2", "live-3"} : new String[]{"live-3"}));
            assertThat(model.isPaused(BROKEN)).as("the repaired subscription is paused").isFalse();
        }
    }

    @Test
    void start_true_in_manual_mode_returns_holds_the_lost_subscription_paused_with_one_error_and_lets_the_healthy_one_deliver() {
        Kind kind = Kind.SUBSCRIPTION;
        // The parameterized test repairs the checkpoint, so the lost position is stored again, and MongoDB still has no history back to it
        withTemplate(kind.database(), template -> new SpringMongoCheckpointStorage(template, "subscriptions").save(BROKEN, new StringBasedCheckpoint(STORED.get(kind))));
        assertThat(canResumeFrom(kind.database(), STORED.get(kind))).as("canResumeFrom the checkpoint stored again").isFalse();

        try (ConfigurableApplicationContext manualBoot = boot(kind.database(), kind.inBackground, true, "live-4", "--occurrent.subscription.mode=manual")) {
            Broken broken = manualBoot.getBean(Broken.class);
            Healthy healthy = manualBoot.getBean(Healthy.class);
            SubscriptionModel model = manualBoot.getBean(SubscriptionModel.class);
            Logs logs = manualBoot.getBean(Logs.class);

            Throwable thrown = catchThrowable(() -> model.start(true));

            assertThat(thrown).as("thrown by start(true)").isNull();
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(healthy.received()).as("the healthy subscription").contains("live-4"));
            assertThat(model.isPaused(BROKEN)).as("the broken subscription is paused").isTrue();
            assertThat(model.isRunning(BROKEN)).as("the broken subscription is running").isFalse();
            assertThat(broken.received()).as("what the broken subscription received").isEmpty();
            assertThat(logs.errorsFor(BROKEN)).as("errors logged for the broken subscription").hasSize(1);
            assertThat(logs.errorsFor(HEALTHY)).as("errors logged for the healthy subscription").isEmpty();
        }
    }

    // A start that never finishes is a failure of the test and not a hang, since a refused start has to fail within seconds
    private static Throwable failureOfTheStartWithStartupModeUnset(Kind kind) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ConfigurableApplicationContext> start = executor.submit(() -> boot(kind.database(), kind.unset, true, "live-2"));
        try {
            start.get(60, SECONDS).close();
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            start.cancel(true);
            return fail("Expected the start with startupMode unset to fail with CatchupCannotResumeException, but it neither finished nor failed within 60 s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static ConfigurableApplicationContext boot(String database, Class<? extends Broken> broken, boolean healthy, String afterStart, String... additionalArguments) {
        SpringApplication application = new SpringApplication(LostPositionApplication.class);
        application.addInitializers(context -> ((GenericApplicationContext) context).registerBean(broken));
        return application.run(Stream.concat(Stream.of(
                "--spring.application.name=lost-position",
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream,dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database,
                "--test.after-start=" + afterStart,
                "--test.healthy=" + healthy), Stream.of(additionalArguments)).toArray(String[]::new));
    }

    private static String storedCheckpoint(String database) {
        return withTemplate(database, template -> new SpringMongoCheckpointStorage(template, "subscriptions").read(BROKEN).asString());
    }

    private static void deleteStoredCheckpoint(String database) {
        withTemplate(database, template -> {
            new SpringMongoCheckpointStorage(template, "subscriptions").delete(BROKEN);
            return null;
        });
    }

    private static boolean canResumeFrom(String database, String checkpoint) {
        return withTemplate(database, template -> {
            SpringMongoSubscriptionModel model = new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
            try {
                return model.canResumeFrom(new StringBasedCheckpoint(checkpoint));
            } finally {
                model.shutdown();
            }
        });
    }

    private static <T> T withTemplate(String database, Function<MongoTemplate, T> use) {
        SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(mongoDBContainer.getReplicaSetUrl(database));
        try {
            return use.apply(new MongoTemplate(factory));
        } finally {
            try {
                factory.destroy();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static List<Throwable> causes(Throwable failure) {
        return Stream.iterate(failure, Objects::nonNull, Throwable::getCause).toList();
    }

    @SpringBootApplication
    @EnableOccurrent
    static class LostPositionApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:lost-position"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService, DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(applicationService, dcbEventStore, converter, afterStart);
        }

        @Bean
        Logs logs() {
            return new Logs();
        }

        @Bean
        RecordingDispatcher recordingDispatcher() {
            return new RecordingDispatcher();
        }

        @Bean
        @ConditionalOnProperty(name = "test.healthy", havingValue = "true")
        Healthy healthy() {
            return new Healthy();
        }
    }

    // Writes every event twice, as a stream event and as a DCB event, so each kind has events of its own capability
    static class EventWriter implements ApplicationRunner {
        private final ApplicationService<TestEvent> applicationService;
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> converter;
        private final String afterStart;

        EventWriter(ApplicationService<TestEvent> applicationService, DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, String afterStart) {
            this.applicationService = applicationService;
            this.dcbEventStore = dcbEventStore;
            this.converter = converter;
            this.afterStart = afterStart;
        }

        @Override
        public void run(ApplicationArguments args) {
            applicationService.execute(afterStart, __ -> List.of(new TestEvent(afterStart)));
            CloudEvent dcbEvent = DcbCloudEvents.withTags(converter.toCloudEvent(new TestEvent(afterStart)), List.of(Tag.parse(DCB_TAG)));
            dcbEventStore.append(List.of(dcbEvent));
        }
    }

    // Attached while the context starts, before any subscription does, so it holds whatever a subscription logs on start
    static class Logs {
        private final Logger logger = (Logger) LoggerFactory.getLogger("org.occurrent.subscription");
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        Logs() {
            appender.start();
            logger.addAppender(appender);
        }

        List<ILoggingEvent> errorsFor(String subscriptionId) {
            synchronized (appender) {
                return appender.list.stream()
                        .filter(event -> event.getLevel() == Level.ERROR)
                        .filter(event -> event.getFormattedMessage().contains(subscriptionId))
                        .toList();
            }
        }

        @PreDestroy
        void detach() {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    static class Healthy {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @Subscription(id = HEALTHY, startAt = StartPosition.BEGINNING)
        void on(TestEvent event) {
            received.add(event.name());
        }

        List<String> received() {
            return List.copyOf(received);
        }
    }

    abstract static class Broken {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        void record(String name) {
            received.add(name);
        }

        List<String> received() {
            return List.copyOf(received);
        }
    }

    static class SubscriptionInBackground extends Broken {
        @Subscription(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class SubscriptionUnset extends Broken {
        @Subscription(id = BROKEN, startAt = StartPosition.BEGINNING)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamSubscriptionInBackground extends Broken {
        @StreamSubscription(id = BROKEN, startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamSubscriptionUnset extends Broken {
        @StreamSubscription(id = BROKEN, startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class DcbSubscriptionInBackground extends Broken {
        @DcbSubscription(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class DcbSubscriptionUnset extends Broken {
        @DcbSubscription(id = BROKEN, startAt = StartPosition.BEGINNING)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamProjectionInBackground extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND, capability = Capability.STREAM)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class StreamProjectionUnset extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING, capability = Capability.STREAM)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class AgnosticProjectionInBackground extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND, capability = Capability.AGNOSTIC)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class AgnosticProjectionUnset extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING, capability = Capability.AGNOSTIC)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class DcbProjectionInBackground extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        DcbProjection<Seen, TestEvent, String> projection() {
            return new DcbProjection<>(recordingProjection(this::record), DcbCriteria.tags(Tag.parse(DCB_TAG)));
        }
    }

    static class DcbProjectionUnset extends Broken {
        @Projection(id = BROKEN, startAt = StartPosition.BEGINNING)
        DcbProjection<Seen, TestEvent, String> projection() {
            return new DcbProjection<>(recordingProjection(this::record), DcbCriteria.tags(Tag.parse(DCB_TAG)));
        }
    }

    static class SagaInBackground extends Broken {
        @Saga(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return recordingSaga(this::record);
        }
    }

    static class SagaUnset extends Broken {
        @Saga(id = BROKEN, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return recordingSaga(this::record);
        }
    }

    static class StreamSnapshotInBackground extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND, capability = Capability.STREAM)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
    }

    static class StreamSnapshotUnset extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING, capability = Capability.STREAM)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
    }

    static class AgnosticSnapshotInBackground extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND, capability = Capability.AGNOSTIC)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
    }

    static class AgnosticSnapshotUnset extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING, capability = Capability.AGNOSTIC)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
    }

    static class DcbSnapshotInBackground extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        DcbSnapshotView<Named, TestEvent> snapshot() {
            return new DcbSnapshotView<>(recordingSnapshot(this::record), DcbCriteria.tagsAnyOf(Tag.parse(DCB_TAG)));
        }
    }

    static class DcbSnapshotUnset extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.BEGINNING)
        DcbSnapshotView<Named, TestEvent> snapshot() {
            return new DcbSnapshotView<>(recordingSnapshot(this::record), DcbCriteria.tagsAnyOf(Tag.parse(DCB_TAG)));
        }
    }

    static class SubscriptionDefaultInBackground extends Broken {
        @Subscription(id = BROKEN, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class SubscriptionDefaultUnset extends Broken {
        @Subscription(id = BROKEN)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamSubscriptionDefaultInBackground extends Broken {
        @StreamSubscription(id = BROKEN, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class StreamSubscriptionDefaultUnset extends Broken {
        @StreamSubscription(id = BROKEN)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class AgnosticProjectionDefaultInBackground extends Broken {
        @Projection(id = BROKEN, startupMode = StartupMode.BACKGROUND, capability = Capability.AGNOSTIC)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class AgnosticProjectionDefaultUnset extends Broken {
        @Projection(id = BROKEN, capability = Capability.AGNOSTIC)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class StreamProjectionDefaultInBackground extends Broken {
        @Projection(id = BROKEN, startupMode = StartupMode.BACKGROUND, capability = Capability.STREAM)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class StreamProjectionDefaultUnset extends Broken {
        @Projection(id = BROKEN, capability = Capability.STREAM)
        org.occurrent.dsl.projection.Projection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class SagaDefaultInBackground extends Broken {
        @Saga(id = BROKEN, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return recordingSaga(this::record);
        }
    }

    static class SagaDefaultUnset extends Broken {
        @Saga(id = BROKEN)
        org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> saga() {
            return recordingSaga(this::record);
        }
    }

    // A snapshot's startAt defaults to BEGINNING, so DEFAULT has to be asked for
    static class AgnosticSnapshotDefaultInBackground extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.DEFAULT, startupMode = StartupMode.BACKGROUND, capability = Capability.AGNOSTIC)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
    }

    static class AgnosticSnapshotDefaultUnset extends Broken {
        @Snapshot(id = BROKEN, startAt = StartPosition.DEFAULT, capability = Capability.AGNOSTIC)
        SnapshotView<Named, TestEvent> snapshot() {
            return recordingSnapshot(this::record);
        }
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

    private static SnapshotView<Named, TestEvent> recordingSnapshot(Consumer<String> onEvent) {
        return SnapshotView.<Named, TestEvent>builder(new Named("none"))
                .on(TestEvent.class, (state, event) -> {
                    onEvent.accept(event.name());
                    return new Named(event.name());
                })
                .build();
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, Named, Observed> recordingSaga(Consumer<String> onEvent) {
        return org.occurrent.dsl.saga.Saga.<TestEvent, Named, Observed>builder(new Named("none"))
                .correlateAll(TestEvent::name)
                .startsOn(TestEvent.class)
                .evolve(TestEvent.class, (state, event) -> {
                    onEvent.accept(event.name());
                    return new Named(event.name());
                })
                .react(TestEvent.class, (state, event) -> List.of(SagaEffect.issue(new Observed(event.name()))))
                .build();
    }

    static class RecordingDispatcher implements CommandDispatcher<Observed> {
        @Override
        public void dispatch(Observed command) {
        }
    }

    record Observed(String name) {
    }

    record Seen(@org.springframework.data.annotation.Id String id) {
    }

    record Named(String name) {
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
