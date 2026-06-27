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

import io.cloudevents.CloudEvent;
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
import org.occurrent.application.service.blocking.dcb.TagGenerator;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that in a STREAM-and-DCB combined-capability context both a stream subscription and a DCB
 * subscription can replay history written before they subscribe, from the single auto-configured
 * {@link SubscriptionModel}. Prior to the dual-mode {@code CatchupSubscriptionModel} constructor a
 * combined-capability app only got stream catch-up; DCB subscriptions started live.
 */
@DisplayName("Dual-mode catch-up auto-configuration (STREAM + DCB)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbDualModeCatchupAutoConfigurationMongoTest.CombinedModeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream,dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dual-mode-catchup-test"
        }
)
@Import(DcbDualModeCatchupAutoConfigurationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbDualModeCatchupAutoConfigurationMongoTest {

    private static final String STREAM_TAG = "kind:stream";
    private static final String DCB_TAG = "kind:dcb";
    private static final URI SOURCE = URI.create("urn:occurrent:dual-mode-catchup-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private DcbSubscriptions<TestEvent> dcbSubscriptions;

    @Autowired
    private SubscriptionModel subscriptionModel;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Test
    void stream_subscription_replays_historic_stream_events_appended_before_subscribe() {
        // Given - stream events written before the subscription starts
        TestEvent historic1 = streamEvent("s-historic-1");
        TestEvent historic2 = streamEvent("s-historic-2");
        appendStream(historic1, historic2);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - stream subscription catches up from the beginning of time
        subscriptionModel.subscribe(
                        "stream-catchup-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> {
                            TestEvent event = cloudEventConverter.toDomainEvent(ce);
                            if (STREAM_TAG.equals(ce.getSubject())) {
                                received.add(event);
                            }
                        })
                .waitUntilStarted();

        // Then - historic stream events are delivered via catch-up
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic1, historic2));
    }

    @Test
    void dcb_subscription_replays_historic_dcb_events_appended_before_subscribe() {
        // Given - DCB events written before the subscription starts
        TestEvent historic1 = dcbEvent("d-historic-1");
        TestEvent historic2 = dcbEvent("d-historic-2");
        TestEvent historic3 = dcbEvent("d-historic-3");
        appendDcb(historic1, historic2, historic3);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - DCB subscription catches up from position 0
        dcbSubscriptions
                .subscribe(
                        "dcb-catchup-" + UUID.randomUUID(),
                        DcbQuery.tagsAllOf(DCB_TAG),
                        DcbStartAt.beginning(),
                        received::add)
                .waitUntilStarted();

        // Then - historic DCB events are delivered via DCB catch-up
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic1, historic2, historic3));
    }

    @Test
    void both_stream_and_dcb_subscriptions_replay_their_respective_histories_from_one_wired_model() {
        // Given - events of both kinds written before any subscription starts
        TestEvent streamHistoric = streamEvent("both-stream-historic");
        TestEvent dcbHistoric = dcbEvent("both-dcb-historic");
        appendStream(streamHistoric);
        appendDcb(dcbHistoric);

        CopyOnWriteArrayList<TestEvent> streamReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<TestEvent> dcbReceived = new CopyOnWriteArrayList<>();

        // When - both subscriptions start after the events are already written
        subscriptionModel.subscribe(
                        "both-stream-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> {
                            if (STREAM_TAG.equals(ce.getSubject())) {
                                streamReceived.add(cloudEventConverter.toDomainEvent(ce));
                            }
                        })
                .waitUntilStarted();

        dcbSubscriptions
                .subscribe(
                        "both-dcb-" + UUID.randomUUID(),
                        DcbQuery.tagsAllOf(DCB_TAG),
                        DcbStartAt.beginning(),
                        dcbReceived::add)
                .waitUntilStarted();

        // Then - each subscription receives its own historical events, not the other kind
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(streamReceived).contains(streamHistoric);
            assertThat(dcbReceived).contains(dcbHistoric);
        });
    }

    @Test
    void dcb_subscription_does_not_receive_stream_only_events() {
        // Given
        TestEvent streamOnly = streamEvent("noise-stream");
        TestEvent dcbOnly = dcbEvent("signal-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        CopyOnWriteArrayList<TestEvent> dcbReceived = new CopyOnWriteArrayList<>();

        // When
        dcbSubscriptions
                .subscribe(
                        "isolation-dcb-" + UUID.randomUUID(),
                        DcbQuery.tagsAllOf(DCB_TAG),
                        DcbStartAt.beginning(),
                        dcbReceived::add)
                .waitUntilStarted();

        // Then - the DCB subscription receives only the DCB event, not the stream-only one
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(dcbReceived).contains(dcbOnly).doesNotContain(streamOnly));
    }

    @Test
    void dcb_subscription_delivers_live_event_after_catch_up_completes() {
        // Given - one historic event before subscribe, one live event after
        TestEvent historic = dcbEvent("live-test-historic");
        appendDcb(historic);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        dcbSubscriptions
                .subscribe(
                        "live-dcb-" + UUID.randomUUID(),
                        DcbQuery.tagsAllOf(DCB_TAG),
                        DcbStartAt.beginning(),
                        received::add)
                .waitUntilStarted();

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic));

        // When - a live event arrives after catch-up
        TestEvent live = dcbEvent("live-test-live");
        appendDcb(live);

        // Then - the live event is delivered without duplication
        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(received).contains(historic, live);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    // --- helpers ---

    private TestEvent streamEvent(String name) {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), STREAM_TAG, name);
    }

    private TestEvent dcbEvent(String name) {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), DCB_TAG, name);
    }

    private void appendStream(TestEvent... events) {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, __ -> Stream.of(events));
    }

    private void appendDcb(TestEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(DCB_TAG)))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- inner application and configuration classes ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class CombinedModeApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .subjectMapper(TestEvent::kind)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        TagGenerator<TestEvent> testEventTagGenerator() {
            return event -> Set.of(event.kind());
        }
    }

    record TestEvent(String eventId, Date timestamp, String kind, String name) {
    }
}
