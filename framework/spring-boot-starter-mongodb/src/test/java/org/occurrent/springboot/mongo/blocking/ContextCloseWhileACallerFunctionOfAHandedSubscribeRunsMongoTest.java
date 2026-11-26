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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Closing the application context while a {@code StartAt.dynamic} function, which a subscribe handed over by a live
 * action is asked on a thread of its own, {@code occurrent-catchup-handed-call}, has not returned. The function runs without the catch-up model lock, so the
 * close doesn't wait for it. The {@code shutdown} destroy method Spring calls once completes, the nested models are
 * stopped when it returns, and the late subscribe once the function returns changes nothing.
 * <p>
 * Booted with {@link SpringApplication#run} rather than {@code @SpringBootTest}, so there is no
 * {@code @ServiceConnection} and the url is passed as a boot argument.
 */
@DisplayName("Closing the context while a function a handed subscribe asked is still running")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(240)
class ContextCloseWhileACallerFunctionOfAHandedSubscribeRunsMongoTest {

    @Container
    static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final String DATABASE = "context-close-caller-code";
    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    // Spring finds shutdown() through its @PreDestroy and calls it there, and doesn't call the destroyMethod of the same name again
    private static final String DESTROY_LOGGER = "org.springframework.context.annotation.CommonAnnotationBeanPostProcessor";
    private static final Duration BOUND = ofSeconds(60);

    private final CountDownLatch functionAsked = new CountDownLatch(1);
    private final CountDownLatch functionMayReturn = new CountDownLatch(1);
    private final AtomicBoolean handedOverZ = new AtomicBoolean();
    private final AtomicInteger deliveriesToY = new AtomicInteger();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();

    @Test
    void the_close_completes_while_the_function_of_a_handed_subscribe_is_still_running_and_stops_the_nested_models_and_the_late_subscribe_of_z_changes_nothing() throws Exception {
        ConfigurableApplicationContext context = boot(DATABASE, "closed");
        // After the boot, since Spring Boot sets the logging system up again while it boots and drops appenders attached before
        ListAppender<ILoggingEvent> destroyLog = captureWhatSpringLogsWhenADestroyMethodThrows();
        try {
            // Given the nested models of the bean that Spring closes, and y running in the Mongo model
            SubscriptionModel bean = context.getBean(SubscriptionModel.class);
            CatchupSubscriptionModel catchup = nested(bean, CatchupSubscriptionModel.class);
            SpringMongoSubscriptionModel mongo = (SpringMongoSubscriptionModel) nested(bean, DurableSubscriptionModel.class).getWrappedSubscriptionModel();
            StartAt function = StartAt.dynamic(() -> {
                functionAsked.countDown();
                awaitLatch(functionMayReturn);
                return StartAt.now();
            });
            catchup.subscribe(X, null, StartAt.now(), event -> {
            });
            catchup.pauseSubscription(X);
            // The action of y resumes x, which is handed to the lifecycle thread, and then subscribes z paused behind it, so the
            // lifecycle thread hands that subscribe to a thread of its own, which asks the function there without the model lock
            catchup.subscribe(Y, null, StartAt.now(), event -> {
                deliveriesToY.incrementAndGet();
                if (handedOverZ.compareAndSet(false, true)) {
                    try {
                        catchup.resumeSubscription(X);
                        catchup.subscribePaused(Z, null, function, ignored -> {
                        });
                    } catch (Throwable e) {
                        thrownByTheAction.set(e);
                    }
                }
            });
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).until(() -> mongo.isRunning(Y));
            write(context, "first");
            assertThat(functionAsked.await(30, TimeUnit.SECONDS)).as("whether the handed subscribe asked the function on its own thread").isTrue();
            assertThat(thrownByTheAction.get()).as("what the action of y threw").isNull();

            // When the context is closed on another thread, while the function is still running
            long closeStarted = System.nanoTime();
            CompletableFuture<Void> closed = CompletableFuture.runAsync(context::close, closeOnAThreadOfItsOwn());
            assertThat(closed).as("the close of the context").succeedsWithin(BOUND);
            Duration closeTook = Duration.ofNanos(System.nanoTime() - closeStarted);

            // Then the close didn't wait for the function, Spring logged no destroy failure, and the nested models are stopped, with no shutdown() from the test
            List<String> destroyWarnings = destroyLog.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(closeTook).as("how long the close took, which doesn't wait for the function").isLessThan(ofSeconds(9));
            assertThat(destroyWarnings).as("what Spring logged about destroy methods that threw")
                    .noneSatisfy(message -> assertThat(message).contains("occurrentCompetingDurableSubscriptionModel"));
            assertThat(mongo.isRunning()).as("whether the Mongo model runs after the close returned while the function was running").isFalse();
            assertThat(mongo.isRunning(Y)).as("whether y runs in the Mongo model after the close returned").isFalse();
            assertThat(functionMayReturn.getCount()).as("whether the function is still running after the close returned").isEqualTo(1);

            // And an event written afterwards, through another application over the same database, is not delivered to y
            int before = deliveriesToY.get();
            try (ConfigurableApplicationContext writer = boot(DATABASE, "writer")) {
                write(writer, "after-the-close");
                HistoryBeforeThePresent.awaitedWith(writer.getBean(org.springframework.data.mongodb.core.MongoTemplate.class));
                await().during(ofSeconds(2)).atMost(ofSeconds(10)).untilAsserted(() -> assertThat(deliveriesToY.get()).as("the events delivered to y after the close").isEqualTo(before));
            }

            // And once the function returns, the late subscribe of z changes nothing, so z isn't running in the Mongo model
            functionMayReturn.countDown();
            await().during(ofSeconds(2)).atMost(ofSeconds(15)).untilAsserted(() -> {
                assertThat(mongo.isRunning(Z)).as("whether z runs in the Mongo model once the function returned").isFalse();
                assertThat(mongo.isRunning()).as("whether the Mongo model runs once the function returned").isFalse();
                assertThat(catchup.isRunning()).as("whether the catch-up model runs once the function returned").isFalse();
            });
        } finally {
            functionMayReturn.countDown();
            context.close();
            detach(destroyLog);
        }
    }

    // Walks down the wrapped models of the bean, whichever of them the starter put on top
    private static <T extends SubscriptionModel> T nested(SubscriptionModel top, Class<T> type) {
        SubscriptionModel current = top;
        while (!type.isInstance(current)) {
            assertThat(current).as("the model that wraps the " + type.getSimpleName()).isInstanceOf(SubscriptionModelWrapper.class);
            current = ((SubscriptionModelWrapper) current).getWrappedSubscriptionModel();
        }
        return type.cast(current);
    }

    // What Spring logs when a destroy method throws, since closing the context doesn't rethrow it
    private static ListAppender<ILoggingEvent> captureWhatSpringLogsWhenADestroyMethodThrows() {
        Logger logger = (Logger) LoggerFactory.getLogger(DESTROY_LOGGER);
        logger.setLevel(Level.WARN);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private static void detach(ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(DESTROY_LOGGER)).detachAppender(appender);
        appender.stop();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(120, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void write(ConfigurableApplicationContext context, String name) {
        ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
        applicationService.execute(name + "-" + UUID.randomUUID(), __ -> List.of(new TestEvent(name)));
    }

    private static ConfigurableApplicationContext boot(String database, String applicationName) {
        return new SpringApplication(TestApplication.class).run(
                "--spring.application.name=" + applicationName,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database);
    }

    private static Executor closeOnAThreadOfItsOwn() {
        return runnable -> {
            Thread thread = new Thread(runnable, "context-close");
            thread.setDaemon(true);
            thread.start();
        };
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
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:context-close-caller-code"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
