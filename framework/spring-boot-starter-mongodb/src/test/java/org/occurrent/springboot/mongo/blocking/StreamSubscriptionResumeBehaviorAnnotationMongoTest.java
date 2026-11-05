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
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code resumeBehavior} on {@link StreamSubscription} behaves differently across an actual application
 * restart, not just a pause, mirroring {@code DcbSubscriptionResumeBehaviorAnnotationMongoTest} for the stream side.
 * See that class's javadoc for why a restart means closing and re-booting the Spring context against the same
 * durable MongoDB backing data, rather than pausing and resuming the same running subscription model.
 * <p>
 * A third scenario below uses only the annotation's plain defaults (no explicit {@code startAt}/{@code resumeBehavior}),
 * which is the most common real-world usage and had no restart coverage at all before this test.
 * <p>
 * The {@code NOW} scenarios boot a separate application, {@link NowRestartApplication}, because they need events
 * written while nothing is subscribed and a marker written after the subscribers are up. Each boot writes the events
 * named by {@code test.before-subscribe} before any subscription registers and, from an {@link ApplicationRunner},
 * the events named by {@code test.after-start}. The last of those is a marker: it is written after everything else, so
 * once a subscriber has received it, whatever it was going to deliver for the events before it has arrived and the
 * whole delivered list can be asserted.
 */
@DisplayName("StreamSubscription resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class StreamSubscriptionResumeBehaviorAnnotationMongoTest {

    // Booted directly with SpringApplication.run (not @SpringBootTest), so there is no @ServiceConnection to resolve
    // the container for the context. The url is passed as a boot argument instead, and getReplicaSetUrl() reports the
    // mapped host port, so nothing has to be pinned.
    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void default_resume_behavior_resumes_from_the_stored_position_after_restart_while_same_as_start_at_replays_from_the_beginning_again() {
        String[] args = bootArgs("stream-resume-behavior");

        ConfigurableApplicationContext ctx1 = SpringApplication.run(FirstBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber1 = ctx1.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtSubscriber sameAsStartAtSubscriber1 = ctx1.getBean(SameAsStartAtSubscriber.class);
            PlainDefaultsSubscriber plainDefaultsSubscriber1 = ctx1.getBean(PlainDefaultsSubscriber.class);

            // First boot: the two BEGINNING_OF_TIME subscribers replay the pre-existing history.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));

            // The plain-defaults subscriber never replays history, but is live and receives a fresh event appended
            // while it is running.
            ApplicationService<TestEvent> applicationService = ctx1.getBean(ApplicationService.class);
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("plain-live-1")));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(plainDefaultsSubscriber1.received()).extracting(TestEvent::name).containsExactly("plain-live-1"));
        } finally {
            ctx1.close();
        }

        // The application is "down": a new event is appended while nothing is subscribed.
        ConfigurableApplicationContext ctx2 = SpringApplication.run(SecondBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber2 = ctx2.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtSubscriber sameAsStartAtSubscriber2 = ctx2.getBean(SameAsStartAtSubscriber.class);
            PlainDefaultsSubscriber plainDefaultsSubscriber2 = ctx2.getBean(PlainDefaultsSubscriber.class);

            // DEFAULT: the fresh subscriber instance resumes from the durably stored position, it never sees the
            // historic events again, only the one event appended while the application was down.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));

            // SAME_AS_START_AT: the fresh subscriber instance replays the entire history again, from the beginning,
            // exactly as it did on the first boot, plus the new event.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber2.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "while-down-1"));

            // Plain defaults: the fresh subscriber instance resumes from the durably stored position too, it never
            // sees "plain-live-1" again, only the event appended while the application was down.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(plainDefaultsSubscriber2.received()).extracting(TestEvent::name).containsExactly("plain-while-down-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(plainDefaultsSubscriber2.received()).extracting(TestEvent::name).containsExactly("plain-while-down-1"));
        } finally {
            ctx2.close();
        }
    }

    @Test
    void now_with_default_resume_behavior_ignores_history_on_first_boot_and_receives_events_written_while_down_after_restart() {
        restartNow("stream-resume-now-default", NowDefaultSubscriber.class,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void now_with_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        restartNow("stream-resume-now-same-as-start-at", NowSameAsStartAtSubscriber.class,
                List.of("live-1"),
                List.of("live-2"));
    }

    private static <S extends RecordingSubscriber> void restartNow(String databaseName, Class<S> subscriberType,
                                                                    List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        ConfigurableApplicationContext firstBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "historic-1,historic-2", "live-1"));
        try {
            S subscriber = firstBoot.getBean(subscriberType);
            awaitDelivered(subscriber, "live-1");
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedFirstBoot);
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "while-down-1", "live-2"));
        try {
            S subscriber = secondBoot.getBean(subscriberType);
            awaitDelivered(subscriber, "live-2");
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedSecondBoot);
        } finally {
            secondBoot.close();
        }
    }

    private static void awaitDelivered(RecordingSubscriber subscriber, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(subscriber.names()).contains(marker));
    }

    private static String[] nowBootArgs(String databaseName, String beforeSubscribe, String afterStart) {
        return Stream.concat(Arrays.stream(bootArgs(databaseName)), Stream.of(
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart)).toArray(String[]::new);
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    // --- application boot classes ---

    @SpringBootApplication
    @EnableOccurrent
    static class FirstBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the two historic events before any subscriber starts, so BEGINNING_OF_TIME subscriptions replay them.
        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService) {
            return new HistoryAppender(applicationService);
        }

        @Bean
        @DependsOn("historyAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        SameAsStartAtSubscriber sameAsStartAtSubscriber() {
            return new SameAsStartAtSubscriber();
        }

        @Bean
        PlainDefaultsSubscriber plainDefaultsSubscriber() {
            return new PlainDefaultsSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SecondBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the "while the application was down" events before any subscriber (re)starts.
        @Bean
        OfflineAppender offlineAppender(ApplicationService<TestEvent> applicationService) {
            return new OfflineAppender(applicationService);
        }

        @Bean
        @DependsOn("offlineAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        SameAsStartAtSubscriber sameAsStartAtSubscriber() {
            return new SameAsStartAtSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        PlainDefaultsSubscriber plainDefaultsSubscriber() {
            return new PlainDefaultsSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class NowRestartApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        NowEventWriter nowEventWriter(ApplicationService<TestEvent> applicationService,
                                      @Value("${test.before-subscribe}") String beforeSubscribe,
                                      @Value("${test.after-start}") String afterStart) {
            return new NowEventWriter(applicationService, beforeSubscribe, afterStart);
        }

        @Bean
        NowDefaultSubscriber nowDefaultSubscriber() {
            return new NowDefaultSubscriber();
        }

        @Bean
        NowSameAsStartAtSubscriber nowSameAsStartAtSubscriber() {
            return new NowSameAsStartAtSubscriber();
        }
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:stream-resume-behavior"))
                .typeMapper(typeMapper)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    // --- appenders ---

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;

        HistoryAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendHistory() {
            append(new TestEvent("historic-1"));
            append(new TestEvent("historic-2"));
        }

        private void append(TestEvent event) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(event));
        }
    }

    static class OfflineAppender {
        private final ApplicationService<TestEvent> applicationService;

        OfflineAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendWhileDown() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("while-down-1")));
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("plain-while-down-1")));
        }
    }

    static class NowEventWriter implements ApplicationRunner {
        private final ApplicationService<TestEvent> applicationService;
        private final List<String> beforeSubscribe;
        private final List<String> afterStart;

        NowEventWriter(ApplicationService<TestEvent> applicationService, String beforeSubscribe, String afterStart) {
            this.applicationService = applicationService;
            this.beforeSubscribe = List.of(beforeSubscribe.split(","));
            this.afterStart = List.of(afterStart.split(","));
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

        private void write(String name) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name)));
        }
    }

    // --- subscribers (one per scenario, since the annotation attributes are compile-time constants) ---

    static class DefaultResumeSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "stream-resume-default", startAt = StartPosition.BEGINNING_OF_TIME, resumeBehavior = ResumeBehavior.DEFAULT)
        void on(TestEvent event) {
            if (event.name().startsWith("historic-") || event.name().equals("while-down-1")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class SameAsStartAtSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "stream-resume-same-as-start-at", startAt = StartPosition.BEGINNING_OF_TIME, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event) {
            if (event.name().startsWith("historic-") || event.name().equals("while-down-1")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class PlainDefaultsSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // No explicit startAt/resumeBehavior: the everyday usage, exercised here for restart durability specifically.
        @StreamSubscription(id = "stream-resume-plain-defaults")
        void on(TestEvent event) {
            if (event.name().startsWith("plain-")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    abstract static class RecordingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        void record(TestEvent event) {
            received.add(event);
        }

        List<String> names() {
            return received.stream().map(TestEvent::name).toList();
        }
    }

    static class NowDefaultSubscriber extends RecordingSubscriber {
        @StreamSubscription(id = "stream-resume-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        void on(TestEvent event) {
            record(event);
        }
    }

    static class NowSameAsStartAtSubscriber extends RecordingSubscriber {
        @StreamSubscription(id = "stream-resume-now-same-as-start-at", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event) {
            record(event);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
