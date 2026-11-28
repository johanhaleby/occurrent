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

package org.occurrent.springboot.mongo.reactor;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Fills the reactive DCB start-position gap: {@code ReactiveDcbSubscriptionAnnotationMongoTest} only exercises
 * {@code startAt = BEGINNING}. This proves {@code DEFAULT} and {@code NOW} never replay history written more than a
 * second before they subscribe, that {@code startAtDcbPosition = 0} replays the whole history, and that
 * a {@code startAtDcbPosition} above 0 is refused, mirroring the blocking
 * {@code DcbSubscriptionDefaultAndNowStartPositionAnnotationMongoTest} and
 * {@code DcbSubscriptionStartAtPositionAnnotationMongoTest}.
 */
@DisplayName("Reactive DcbSubscription startAt matrix")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveDcbSubscriptionStartPositionAnnotationMongoTest {

    static final String TAG = "test:reactive-start-position";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-dcb-start-position-test");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static ConfigurableApplicationContext context;

    private DcbEventStore dcbEventStore;
    private CloudEventConverter<TestEvent> cloudEventConverter;
    private DefaultPositionSubscriber defaultPositionSubscriber;
    private NowPositionSubscriber nowPositionSubscriber;
    private ExplicitPositionSubscriber explicitPositionSubscriber;
    private RefusedPositionSubscriber refusedPositionSubscriber;
    private CatchupSupportLog catchupSupportLog;

    // The start of a subscription at the present is read when the event store bean is created, so only what is written
    // before the context starts is history. A context with subscriptions off writes it and is closed again, and the
    // history is given a second to age, since a live start opens at the start of the current second.
    @BeforeAll
    static void bootAfterTheHistoryIsWritten() throws InterruptedException {
        SpringApplication.run(DcbOnlyApplication.class, bootArgs("--occurrent.subscription.enabled=false", "--test.history=true")).close();
        Thread.sleep(2100);
        context = SpringApplication.run(DcbOnlyApplication.class, bootArgs());
    }

    @AfterAll
    static void closeContext() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void wire() {
        dcbEventStore = context.getBean(DcbEventStore.class);
        cloudEventConverter = context.getBean(CloudEventConverter.class);
        defaultPositionSubscriber = context.getBean(DefaultPositionSubscriber.class);
        nowPositionSubscriber = context.getBean(NowPositionSubscriber.class);
        explicitPositionSubscriber = context.getBean(ExplicitPositionSubscriber.class);
        refusedPositionSubscriber = context.getBean(RefusedPositionSubscriber.class);
        catchupSupportLog = context.getBean(CatchupSupportLog.class);
    }

    private static String[] bootArgs(String... more) {
        return Stream.concat(Stream.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("reactive-dcb-start-position"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE), Stream.of(more)).toArray(String[]::new);
    }

    @Test
    void default_and_now_never_replay_history_older_than_a_second_while_an_explicit_position_0_replays_all_of_it() {
        // Neither DEFAULT nor NOW ever sees the history, written more than a second before they subscribed.
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(defaultPositionSubscriber.invocationCount()).isZero();
            assertThat(defaultPositionSubscriber.received()).isEmpty();
        });
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(nowPositionSubscriber.invocationCount()).isZero();
            assertThat(nowPositionSubscriber.received()).isEmpty();
        });

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(explicitPositionSubscriber.received()).extracting(TestEvent::name).containsExactlyInAnyOrder("event-1", "event-2", "event-3"));

        append(new TestEvent("live-default"));
        append(new TestEvent("live-now"));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(defaultPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-default"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(nowPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-now"));
    }

    @Test
    void an_explicit_position_above_0_is_refused_and_delivers_nothing() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(catchupSupportLog.events()).anySatisfy(event -> {
                    assertThat(event.getThrowableProxy()).isNotNull();
                    assertThat(event.getThrowableProxy().getClassName()).isEqualTo(CatchupCannotResumeException.class.getName());
                    assertThat(event.getThrowableProxy().getMessage())
                            .contains("Cannot start catch-up subscription reactive-dcb-sp-refused-position after position 1");
                }));

        assertThat(refusedPositionSubscriber.received()).isEmpty();
    }

    private void append(TestEvent event) {
        List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents).block();
    }

    // --- inner application and configuration classes ---

    @SpringBootApplication
    @EnableOccurrentReactive
    static class DcbOnlyApplication {

        @Bean
        CatchupSupportLog catchupSupportLog() {
            return new CatchupSupportLog();
        }

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // Appends three distinguishable events in the boot with subscriptions off, which is the one given test.history.
        // DCB positions are assigned sequentially from 1 on a fresh store, so they get 1, 2, 3.
        @Bean
        @ConditionalOnProperty(name = "test.history", havingValue = "true")
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        DefaultPositionSubscriber defaultPositionSubscriber() {
            return new DefaultPositionSubscriber();
        }

        @Bean
        NowPositionSubscriber nowPositionSubscriber() {
            return new NowPositionSubscriber();
        }

        @Bean
        ExplicitPositionSubscriber explicitPositionSubscriber() {
            return new ExplicitPositionSubscriber();
        }

        @Bean
        RefusedPositionSubscriber refusedPositionSubscriber() {
            return new RefusedPositionSubscriber();
        }
    }

    static class HistoryAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        HistoryAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendHistory() {
            List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(
                            new TestEvent("event-1"), new TestEvent("event-2"), new TestEvent("event-3")))
                    .stream()
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                    .toList();
            dcbEventStore.append(cloudEvents).block();
        }
    }

    // Attached as the context starts, before any subscription does, so it holds the refusal however soon after startup a
    // catch-up thread logs it
    static class CatchupSupportLog {
        private final Logger logger = (Logger) LoggerFactory.getLogger("org.occurrent.subscription.reactor.durable.catchup.NamedCatchupSupport");
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        CatchupSupportLog() {
            appender.start();
            logger.addAppender(appender);
        }

        List<ILoggingEvent> events() {
            synchronized (appender) {
                return List.copyOf(appender.list);
            }
        }

        @PreDestroy
        void detach() {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    // --- subscribers ---

    static class DefaultPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @DcbSubscription(id = "reactive-dcb-sp-default")
        Mono<Void> onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-default")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class NowPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @DcbSubscription(id = "reactive-dcb-sp-now", startAt = StartPosition.NOW)
        Mono<Void> onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-now")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class ExplicitPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "reactive-dcb-sp-explicit-position", startAtDcbPosition = 0)
        Mono<Void> onEvent(TestEvent event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class RefusedPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "reactive-dcb-sp-refused-position", startAtDcbPosition = 1)
        Mono<Void> onEvent(TestEvent event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
