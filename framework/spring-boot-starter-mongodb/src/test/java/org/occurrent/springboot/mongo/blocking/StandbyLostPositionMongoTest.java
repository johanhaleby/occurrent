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
import jakarta.annotation.PreDestroy;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What a standby node does with a subscription whose stored position MongoDB can no longer resume from, once the node
 * that held the lease stops and the standby is granted it.
 * <p>
 * Each scenario has a database of its own. A first boot at {@code BEGINNING} stores a live checkpoint, which is kept, and
 * MongoDB then loses the change stream history past all of them with a single drop, since a drop takes about a
 * minute. Node A then runs the subscription from a checkpoint it stores after that, which MongoDB can resume from, and
 * holds the lease. The
 * subscription is quiet while the stored checkpoint is overwritten with the one that was kept, which is now lost. Node B
 * runs the same subscription next to A, either before or after that overwrite, so it either boots with a position that is
 * fine or one that is already lost. A then stops and B is granted the lease.
 * <p>
 * Booted with {@link SpringApplication#run} rather than {@code @SpringBootTest}, so there is no
 * {@code @ServiceConnection} and the url is passed as a boot argument. The lease is two seconds so a standby
 * competes for it often enough to be seen repeating a refusal.
 */
@DisplayName("A standby granted the lease for a subscription whose stored position MongoDB has lost")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(600)
class StandbyLostPositionMongoTest {

    @Container
    static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private static final String BROKEN = "broken";
    private static final String COMPETING_CONSUMERS = "org.occurrent.subscription.blocking.competingconsumers";
    private static final String LEASE_STRATEGY = "org.occurrent.subscription.mongodb.blocking.ccs";
    private static final Map<Scenario, String> LOST = new EnumMap<>(Scenario.class);

    enum Scenario {
        IN_BACKGROUND_WITH_THE_POSITION_FINE_AT_BOOT(InBackground.class, false),
        IN_BACKGROUND_WITH_THE_POSITION_LOST_AT_BOOT(InBackground.class, true),
        UNSET_WITH_THE_POSITION_FINE_AT_BOOT(Unset.class, false),
        UNSET_WITH_THE_POSITION_LOST_AT_BOOT(Unset.class, true);

        private final Class<? extends Broken> subscription;
        private final boolean lostAtBoot;

        Scenario(Class<? extends Broken> subscription, boolean lostAtBoot) {
            this.subscription = subscription;
            this.lostAtBoot = lostAtBoot;
        }

        String database() {
            return "standby-" + ordinal();
        }
    }

    @BeforeAll
    static void keep_a_live_checkpoint_for_every_scenario_and_make_mongodb_lose_the_history_it_resumes_from() {
        for (Scenario scenario : Scenario.values()) {
            try (ConfigurableApplicationContext firstBoot = boot(scenario.database(), Unset.class, "live-1")) {
                Broken broken = firstBoot.getBean(Broken.class);
                await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
                    assertThat(broken.received()).as("first boot of " + scenario).contains("live-1");
                    assertThat(storedCheckpoint(scenario.database())).as("the checkpoint stored for " + scenario).isNotNull().doesNotStartWith("position:");
                });
            }
            LOST.put(scenario, storedCheckpoint(scenario.database()));
        }
        BsonTimestamp now;
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(Scenario.values()[0].database()))) {
            now = client.getDatabase(Scenario.values()[0].database()).runCommand(new Document("ping", 1)).get("operationTime", BsonTimestamp.class);
        }
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, Scenario.values()[0].database(), now);
        for (Scenario scenario : Scenario.values()) {
            assertThat(canResumeFrom(scenario.database(), LOST.get(scenario))).as("canResumeFrom the checkpoint kept for " + scenario).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Scenario.class)
    void the_standby_holds_the_subscription_paused_once_granted_the_lease_and_does_not_try_it_again(Scenario scenario) {
        String database = scenario.database();
        String lost = LOST.get(scenario);
        deleteStoredCheckpoint(database);

        try (ConfigurableApplicationContext nodeA = boot(database, scenario.subscription, "")) {
            Broken brokenOnA = nodeA.getBean(Broken.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
                assertThat(brokenOnA.received()).as("node A replays the history").contains("live-1");
                assertThat(storedCheckpoint(database)).as("the checkpoint node A stored").isNotNull().isNotEqualTo(lost).doesNotStartWith("position:");
            });
            assertThat(canResumeFrom(database, storedCheckpoint(database))).as("node A runs from a position MongoDB can resume from").isTrue();

            ConfigurableApplicationContext nodeB = null;
            try {
                if (!scenario.lostAtBoot) {
                    nodeB = boot(database, scenario.subscription, "");
                    overwriteStoredCheckpoint(database, lost);
                } else {
                    overwriteStoredCheckpoint(database, lost);
                    nodeB = boot(database, scenario.subscription, "");
                }
                assertThat(nodeB.isActive()).as("node B started as a standby, with the position " + (scenario.lostAtBoot ? "already lost" : "fine")).isTrue();
                Broken brokenOnB = nodeB.getBean(Broken.class);
                SubscriptionModel modelOnB = nodeB.getBean(SubscriptionModel.class);
                Logs logsOnB = nodeB.getBean(Logs.class);
                assertThat(modelOnB.isPaused(BROKEN)).as("the standby is not paused while node A holds the lease").isFalse();

                nodeA.close();
                assertThat(storedCheckpoint(database)).as("the checkpoint after node A stopped").isEqualTo(lost);

                await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(modelOnB.isPaused(BROKEN)).as("the standby, granted the lease, is paused").isTrue());
                assertThat(modelOnB.isRunning(BROKEN)).as("the standby is running").isFalse();
                assertThat(logsOnB.errors()).as("errors logged by the competing-consumer model and the lease strategy").hasSize(1).allSatisfy(error -> assertThat(error.getFormattedMessage()).contains(BROKEN));

                // Several lease periods, in which a standby that gave the lease back would be granted it and refused again
                await().during(ofSeconds(10)).atMost(ofSeconds(15)).pollInterval(ofMillis(250)).untilAsserted(() -> {
                    assertThat(logsOnB.errors()).as("errors logged by the competing-consumer model and the lease strategy").hasSize(1);
                    assertThat(logsOnB.warnings()).as("warnings logged by the competing-consumer model and the lease strategy").isEmpty();
                    assertThat(modelOnB.isPaused(BROKEN)).as("the standby is paused").isTrue();
                    assertThat(storedCheckpoint(database)).as("the checkpoint stored").isEqualTo(lost);
                });
                assertThat(brokenOnB.received()).as("what the standby received").isEmpty();
            } finally {
                if (nodeB != null) {
                    nodeB.close();
                }
            }
        }
    }

    private static ConfigurableApplicationContext boot(String database, Class<? extends Broken> broken, String afterStart) {
        SpringApplication application = new SpringApplication(StandbyApplication.class);
        application.addInitializers(context -> ((GenericApplicationContext) context).registerBean(broken));
        return application.run(
                "--spring.application.name=standby-lost",
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database,
                "--test.after-start=" + afterStart);
    }

    private static String storedCheckpoint(String database) {
        return withTemplate(database, template -> {
            Checkpoint checkpoint = new SpringMongoCheckpointStorage(template, "subscriptions").read(BROKEN);
            return checkpoint == null ? null : checkpoint.asString();
        });
    }

    private static void deleteStoredCheckpoint(String database) {
        withTemplate(database, template -> {
            new SpringMongoCheckpointStorage(template, "subscriptions").delete(BROKEN);
            return null;
        });
    }

    private static void overwriteStoredCheckpoint(String database, String checkpoint) {
        withTemplate(database, template -> new SpringMongoCheckpointStorage(template, "subscriptions").save(BROKEN, new StringBasedCheckpoint(checkpoint)));
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

    @SpringBootApplication
    @EnableOccurrent
    static class StandbyApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:standby-lost"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        SpringMongoLeaseCompetingConsumerStrategy shortLeaseCompetingConsumerStrategy(MongoTemplate mongoTemplate) {
            return new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(ofSeconds(2)).build();
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService, @Value("${test.after-start}") String afterStart) {
            return new EventWriter(applicationService, afterStart);
        }

        @Bean
        Logs logs() {
            return new Logs();
        }
    }

    static class EventWriter implements ApplicationRunner {
        private final ApplicationService<TestEvent> applicationService;
        private final String afterStart;

        EventWriter(ApplicationService<TestEvent> applicationService, String afterStart) {
            this.applicationService = applicationService;
            this.afterStart = afterStart;
        }

        @Override
        public void run(ApplicationArguments args) {
            if (!afterStart.isBlank()) {
                applicationService.execute(afterStart, __ -> List.of(new TestEvent(afterStart)));
            }
        }
    }

    // Attached while the context starts, before any subscription does
    static class Logs {
        private final List<Logger> loggers = List.of((Logger) LoggerFactory.getLogger(COMPETING_CONSUMERS), (Logger) LoggerFactory.getLogger(LEASE_STRATEGY));
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        Logs() {
            appender.start();
            loggers.forEach(logger -> logger.addAppender(appender));
        }

        List<ILoggingEvent> errors() {
            return logged(Level.ERROR);
        }

        List<ILoggingEvent> warnings() {
            return logged(Level.WARN);
        }

        private List<ILoggingEvent> logged(Level level) {
            synchronized (appender) {
                return appender.list.stream().filter(event -> event.getLevel() == level).toList();
            }
        }

        @PreDestroy
        void detach() {
            loggers.forEach(logger -> logger.detachAppender(appender));
            appender.stop();
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

    static class InBackground extends Broken {
        @Subscription(id = BROKEN, startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    static class Unset extends Broken {
        @Subscription(id = BROKEN, startAt = StartPosition.BEGINNING)
        void on(TestEvent event) {
            record(event.name());
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
