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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import jakarta.annotation.PreDestroy;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An application that takes longer to start than MongoDB keeps its change stream history. The live start the starter
 * reads when the event store bean is created is pinned as the first checkpoint of a handler at the default start
 * position, an operation time, and by the time the handler subscribes MongoDB has dropped the history past it. The
 * catch-up refuses to start from it rather than skip what was written in between.
 * <p>
 * Both applications boot in a single boot each, at the same time, against databases of their own. Each is held where
 * the starter turns the live start it read into the checkpoint it pins, and MongoDB drops the history past both with
 * one drop, since a drop takes about a minute. One application leaves {@code startupMode} unset, the other sets
 * {@code BACKGROUND}.
 */
@DisplayName("A pinned live start that MongoDB has dropped from its change stream history")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(600)
class PinnedLiveStartLostFromTheOplogMongoTest {

    @Container
    static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private static final String UNSET_DATABASE = "pinned-live-start-unset";
    private static final String BACKGROUND_DATABASE = "pinned-live-start-background";
    private static final String UNSET = "pinnedWithStartupModeUnset";
    private static final String IN_BACKGROUND = "pinnedInBackground";
    private static final long WAIT_SECONDS = 180;

    private static final Pin unsetPin = new Pin();
    private static final Pin backgroundPin = new Pin();
    private static @Nullable Throwable unsetFailure;
    private static @Nullable Throwable backgroundFailure;
    private static @Nullable ConfigurableApplicationContext backgroundContext;

    @BeforeAll
    static void boot_both_applications_and_make_mongodb_drop_the_history_past_the_live_starts_they_pin() throws Exception {
        Hold hold = new Hold();
        unsetPin.hold = hold;
        backgroundPin.hold = hold;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<ConfigurableApplicationContext> unsetBoot = pool.submit(() -> boot(UNSET_DATABASE, UnsetHandler.class, unsetPin));
            Future<ConfigurableApplicationContext> backgroundBoot = pool.submit(() -> boot(BACKGROUND_DATABASE, InBackgroundHandler.class, backgroundPin));
            assertThat(hold.bothApplicationsReadTheirLiveStart.await(WAIT_SECONDS, SECONDS))
                    .as("both applications read their live start and are about to pin it").isTrue();

            ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, UNSET_DATABASE, operationTimeNow());
            hold.historyDropped.countDown();

            unsetFailure = failureOf(unsetBoot);
            backgroundFailure = failureOf(backgroundBoot);
            backgroundContext = backgroundFailure == null ? backgroundBoot.get() : null;
        } finally {
            hold.historyDropped.countDown();
            pool.shutdownNow();
        }
    }

    @AfterAll
    static void close_the_application_that_started() {
        if (backgroundContext != null) {
            backgroundContext.close();
        }
    }

    @Test
    void with_startup_mode_unset_fails_the_start_with_catchup_cannot_resume_and_leaves_the_pinned_checkpoint_as_it_was() {
        String pinned = pinnedOperationTime(unsetPin);

        assertThat(causes(unsetFailure)).as("the start with startupMode unset")
                .anySatisfy(cause -> assertThat(cause).isInstanceOf(CatchupCannotResumeException.class));
        assertThat(storedCheckpoint(UNSET_DATABASE, UNSET)).as("the checkpoint after the refused start").isEqualTo(pinned);
        assertThat(canResumeFrom(UNSET_DATABASE, pinned)).as("canResumeFrom the pinned checkpoint").isFalse();
    }

    @Test
    void with_startup_mode_background_starts_the_application_and_holds_the_subscription_paused_with_an_error_naming_it() {
        String pinned = pinnedOperationTime(backgroundPin);

        assertThat(backgroundFailure).as("the start with startupMode BACKGROUND").isNull();
        ConfigurableApplicationContext context = requireNonNull(backgroundContext);
        SubscriptionModel model = context.getBean(SubscriptionModel.class);
        InBackgroundHandler handler = context.getBean(InBackgroundHandler.class);
        Logs logs = context.getBean(Logs.class);

        applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("written-once-started")));

        assertThat(model.isPaused(IN_BACKGROUND)).as("the subscription is paused").isTrue();
        assertThat(model.isRunning(IN_BACKGROUND)).as("the subscription is running").isFalse();
        await().during(ofSeconds(3)).atMost(ofSeconds(5)).pollInterval(ofMillis(100))
                .untilAsserted(() -> assertThat(handler.received()).as("what the subscription received").isEmpty());
        assertThat(storedCheckpoint(BACKGROUND_DATABASE, IN_BACKGROUND)).as("the checkpoint stored for the subscription").isEqualTo(pinned);
        assertThat(logs.errorsFor(IN_BACKGROUND)).as("errors logged for the subscription").isNotEmpty();
        assertThat(canResumeFrom(BACKGROUND_DATABASE, pinned)).as("canResumeFrom the pinned checkpoint").isFalse();
    }

    // The checkpoint the starter made of the live start, in the form MongoOperationTimeCheckpoint stores, so neither
    // test can pass on a resume token
    private static String pinnedOperationTime(Pin pin) {
        Checkpoint checkpoint = pin.checkpoint;
        assertThat(checkpoint).as("the checkpoint the starter pinned").isInstanceOf(MongoOperationTimeCheckpoint.class);
        String pinned = requireNonNull(checkpoint).asString();
        BsonTimestamp operationTime = OperationTimeLiveStart.parse(pinned);
        assertThat(operationTime).as("the operation time the pinned checkpoint holds").isNotNull();
        assertThat(pinned).as("the pinned checkpoint").isEqualTo(OperationTimeLiveStart.format(requireNonNull(operationTime)));
        return pinned;
    }

    private static ConfigurableApplicationContext boot(String database, Class<?> handler, Pin pin) {
        SpringApplication application = new SpringApplication(PinnedLiveStartApplication.class);
        application.addInitializers(context -> {
            ((GenericApplicationContext) context).registerBean(handler);
            context.getBeanFactory().registerSingleton("pin", pin);
        });
        return application.run(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database);
    }

    private static @Nullable Throwable failureOf(Future<?> boot) throws InterruptedException {
        try {
            boot.get(WAIT_SECONDS, SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            boot.cancel(true);
            return e;
        }
    }

    private static BsonTimestamp operationTimeNow() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(UNSET_DATABASE));
        try (MongoClient client = MongoClients.create(connectionString)) {
            return client.getDatabase(requireNonNull(connectionString.getDatabase())).runCommand(new Document("ping", 1)).get("operationTime", BsonTimestamp.class);
        }
    }

    private static @Nullable String storedCheckpoint(String database, String subscriptionId) {
        return withTemplate(database, template -> {
            @Nullable Checkpoint stored = new SpringMongoCheckpointStorage(template, "subscriptions").read(subscriptionId);
            return stored == null ? null : stored.asString();
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

    private static List<Throwable> causes(@Nullable Throwable failure) {
        return Stream.iterate(failure, Objects::nonNull, Throwable::getCause).toList();
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<TestEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    /**
     * What the two applications share, which is how both are held until MongoDB has dropped the history.
     */
    static final class Hold {
        final CountDownLatch bothApplicationsReadTheirLiveStart = new CountDownLatch(2);
        final CountDownLatch historyDropped = new CountDownLatch(1);
    }

    static final class Pin {
        volatile @Nullable Hold hold;
        volatile @Nullable Checkpoint checkpoint;

        @Nullable Checkpoint pinned(@Nullable Checkpoint checkpoint) {
            this.checkpoint = checkpoint;
            Hold hold = requireNonNull(this.hold);
            hold.bothApplicationsReadTheirLiveStart.countDown();
            try {
                if (!hold.historyDropped.await(WAIT_SECONDS * 2, SECONDS)) {
                    throw new IllegalStateException("MongoDB never dropped the history, so the live start was not lost");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return checkpoint;
        }
    }

    // Only a start at the default or at NOW turns the live start into a checkpoint, so the one handler of each
    // application is the only one held
    static class PinHooks implements BeanPostProcessor, BeanFactoryAware {
        private @Nullable BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (bean instanceof LiveStartCheckpoints starter) {
                Pin pin = requireNonNull(beanFactory).getBean(Pin.class);
                return (LiveStartCheckpoints) liveStart -> pin.pinned(starter.checkpointOf(liveStart));
            }
            return bean;
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

    static class UnsetHandler {
        @Subscription(id = UNSET)
        void on(TestEvent event) {
        }
    }

    static class InBackgroundHandler {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @Subscription(id = IN_BACKGROUND, startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            received.add(event.name());
        }

        List<String> received() {
            return List.copyOf(received);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    static class PinnedLiveStartApplication {

        @Bean
        static PinHooks pinHooks() {
            return new PinHooks();
        }

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:pinned-live-start"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        Logs logs() {
            return new Logs();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
