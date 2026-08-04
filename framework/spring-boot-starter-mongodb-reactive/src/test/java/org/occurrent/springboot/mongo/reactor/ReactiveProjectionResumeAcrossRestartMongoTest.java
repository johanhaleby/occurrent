/*
 *
 *  Copyright 2024 Johan Haleby
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

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
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
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves durable resume for a reactive {@code @Projection}: with resumeBehavior DEFAULT, the projection replays history
 * on the first boot, then on a fresh boot resumes from the durable checkpoint rather than replaying, so each event is
 * folded exactly once across the restart. The store is a static, process-wide {@link ViewStateRepository} so it survives
 * the context restart (standing in for a persistent store), and the final count equals the number of distinct events.
 */
@DisplayName("Reactive Projection annotation resumes from the checkpoint across a restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveProjectionResumeAcrossRestartMongoTest {

    static final String TAG = "dashboard:resume";
    private static final String VIEW_ID = "dashboard";

    // Process-wide, so it persists across the two application contexts, standing in for a durable read-model store.
    private static final ConcurrentHashMap<String, CountState> STORE = new ConcurrentHashMap<>();

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void default_resume_folds_each_event_once_across_a_restart() {
        STORE.clear();
        String[] args = bootArgs("reactive-projection-resume");

        ConfigurableApplicationContext ctx1 = SpringApplication.run(FirstBootApplication.class, args);
        try {
            // Two historic events were appended before the projection started, so BEGINNING replays them.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(STORE.get(VIEW_ID)).isNotNull().extracting(CountState::count).isEqualTo(2));
        } finally {
            ctx1.close();
        }

        ConfigurableApplicationContext ctx2 = SpringApplication.run(SecondBootApplication.class, args);
        try {
            // One event was appended while down. DEFAULT resume continues from the checkpoint and folds only that one,
            // so the persistent count becomes 3 (not 5, which a full replay of the two historic events would produce).
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(STORE.get(VIEW_ID)).isNotNull().extracting(CountState::count).isEqualTo(3));
            await().during(ofSeconds(2)).atMost(ofSeconds(6)).untilAsserted(() ->
                    assertThat(STORE.get(VIEW_ID)).isNotNull().extracting(CountState::count).isEqualTo(3));
        } finally {
            ctx2.close();
        }
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:reactive-projection-resume"))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    private static void append(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents).block();
    }

    private static ViewStateRepository<CountState, String> persistentStore() {
        return ViewStateRepository.create(STORE::get, STORE::put);
    }

    private static DcbProjection<CountState, TestEvent, String> countingProjection() {
        Projection<CountState, TestEvent, String> projection = Projection.<CountState, TestEvent, String>builder(new CountState(0))
                .id(event -> VIEW_ID)
                .on(Included.class, (state, event) -> new CountState(state.count() + 1))
                .build();
        return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse(TAG)));
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class FirstBootApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        ViewStateRepository<CountState, String> store() {
            return persistentStore();
        }

        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            return new HistoryAppender(dcbEventStore, converter);
        }

        @Bean
        @DependsOn("historyAppender")
        DashboardProjection dashboardProjection() {
            return new DashboardProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class SecondBootApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        ViewStateRepository<CountState, String> store() {
            return persistentStore();
        }

        @Bean
        OfflineAppender offlineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            return new OfflineAppender(dcbEventStore, converter);
        }

        @Bean
        @DependsOn("offlineAppender")
        DashboardProjection dashboardProjection() {
            return new DashboardProjection();
        }
    }

    static class HistoryAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> converter;

        HistoryAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            this.dcbEventStore = dcbEventStore;
            this.converter = converter;
        }

        @PostConstruct
        void appendHistory() {
            append(dcbEventStore, converter, new Included("historic-1"), new Included("historic-2"));
        }
    }

    static class OfflineAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> converter;

        OfflineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            this.dcbEventStore = dcbEventStore;
            this.converter = converter;
        }

        @PostConstruct
        void appendWhileDown() {
            append(dcbEventStore, converter, new Included("while-down-1"));
        }
    }

    @Component
    static class DashboardProjection {
        @org.occurrent.annotation.Projection(id = "reactive-projection-resume-dashboard", startAt = org.occurrent.annotation.StartPosition.BEGINNING, resumeBehavior = org.occurrent.annotation.ResumeBehavior.DEFAULT)
        DcbProjection<CountState, TestEvent, String> dashboard() {
            return countingProjection();
        }
    }

    record CountState(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Included(String eventId, Date timestamp, String name) implements TestEvent {
        Included(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
