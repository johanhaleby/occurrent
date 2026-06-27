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
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionPosition;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that DCB catch-up is correctly wired in DCB-only mode ({@code occurrent.event-store.capabilities=dcb}).
 * <p>
 * In DCB-only mode the auto-configured {@code SubscriptionModel} wraps a {@link org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel}
 * in DCB mode. A subscription started at a {@link DcbSubscriptionPosition} must (a) replay all matching
 * history appended before it started, and (b) seamlessly deliver new events written after the catch-up phase
 * completes, with no duplicates at the seam. This test verifies that end-to-end contract using only
 * auto-configured beans -- no manual construction of the subscription model.
 */
@DisplayName("DcbCatchupSubscription auto-configuration (DCB-only mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbCatchupSubscriptionAutoConfigurationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-catchup-test"
        }
)
@Import(DcbCatchupSubscriptionAutoConfigurationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbCatchupSubscriptionAutoConfigurationMongoTest {

    private static final String TAG = "type:NameDefined";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-catchup-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private DcbSubscriptions<TestEvent> dcbSubscriptions;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Test
    void replays_dcb_history_then_delivers_live_events_without_duplicates() {
        // Given – historic events appended before the subscription is created
        TestEvent historic1 = newEvent("historic-1");
        TestEvent historic2 = newEvent("historic-2");
        TestEvent historic3 = newEvent("historic-3");
        appendTagged(TAG, historic1, historic2, historic3);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When – subscribe from position 0 (replay from the very beginning of the DCB sequence)
        dcbSubscriptions
                .subscribe(
                        "test-catchup-" + UUID.randomUUID(),
                        DcbQuery.tagsAllOf(TAG),
                        DcbStartAt.beginning(),
                        received::add)
                .waitUntilStarted();

        // Then – catch-up delivers all historic events
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).containsExactly(historic1, historic2, historic3));

        // When – a live event is appended after the catch-up phase completes
        TestEvent live1 = newEvent("live-1");
        appendTagged(TAG, live1);

        // Then – the live event is delivered through the change stream, in order, with no duplicates
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, historic3, live1);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    // --- helpers ---

    private TestEvent newEvent(String name) {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), "user", name);
    }

    private void appendTagged(String tag, TestEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(tag)))
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
    static class DcbOnlyApplication {

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
    }

    record TestEvent(String eventId, Date timestamp, String userId, String name) {
    }
}
