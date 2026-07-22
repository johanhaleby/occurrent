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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that the {@link StreamId} and {@link StreamVersion} parameter annotations inject the delivered event's stream
 * id and stream version into a subscription handler, for {@link StreamSubscription} and the capability-agnostic
 * {@link Subscription}, in any parameter order. The rejection of these annotations on {@code @DcbSubscription} is
 * covered as a unit test in {@code SubscriptionAnnotationsTest}.
 */
@DisplayName("StreamId and StreamVersion parameter injection")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = StreamIdVersionInjectionMongoTest.StreamIdVersionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:stream-id-version-injection-test"
        }
)
@Import(StreamIdVersionInjectionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class StreamIdVersionInjectionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:stream-id-version-injection-test");

    @Autowired
    private ApplicationService<MyEvent> applicationService;

    @Autowired
    private StreamAccessorReceiver streamAccessorReceiver;

    @Autowired
    private AgnosticMixedOrderReceiver agnosticMixedOrderReceiver;

    @Test
    void stream_subscription_injects_stream_id_and_version() {
        String streamId = "stream-injection-" + UUID.randomUUID();
        applicationService.execute(streamId, __ -> List.of(new MyEvent("id-version-1")));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(streamAccessorReceiver.received()).hasSize(1));
        StreamAccessorReceiver.Received received = streamAccessorReceiver.received().get(0);
        assertThat(received.streamId()).isEqualTo(streamId);
        assertThat(received.streamVersion()).isEqualTo(1L);
    }

    @Test
    void agnostic_subscription_injects_accessors_regardless_of_parameter_order() {
        String streamId = "agnostic-injection-" + UUID.randomUUID();
        applicationService.execute(streamId, __ -> List.of(new MyEvent("id-version-2")));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(agnosticMixedOrderReceiver.received()).hasSize(1));
        AgnosticMixedOrderReceiver.Received received = agnosticMixedOrderReceiver.received().get(0);
        assertThat(received.streamId()).isEqualTo(streamId);
        assertThat(received.streamVersion()).isEqualTo(1L);
        assertThat(received.metadataStreamId()).isEqualTo(streamId);
    }

    // --- container configuration ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    // --- application under test ---

    @SpringBootApplication
    @EnableOccurrent
    static class StreamIdVersionApplication {

        @Bean
        CloudEventTypeMapper<MyEvent> myEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<MyEvent> myEventCloudEventConverter(CloudEventTypeMapper<MyEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<MyEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        StreamAccessorReceiver streamAccessorReceiver() {
            return new StreamAccessorReceiver();
        }

        @Bean
        AgnosticMixedOrderReceiver agnosticMixedOrderReceiver() {
            return new AgnosticMixedOrderReceiver();
        }
    }

    // --- receivers ---

    static class StreamAccessorReceiver {
        record Received(String streamId, long streamVersion) {
        }

        private final CopyOnWriteArrayList<Received> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "stream-id-version-injection")
        void on(MyEvent event, @StreamId String streamId, @StreamVersion long streamVersion) {
            if ("id-version-1".equals(event.name())) {
                received.add(new Received(streamId, streamVersion));
            }
        }

        List<Received> received() { return received; }
    }

    static class AgnosticMixedOrderReceiver {
        record Received(long streamVersion, String streamId, String metadataStreamId) {
        }

        private final CopyOnWriteArrayList<Received> received = new CopyOnWriteArrayList<>();

        // Accessors and metadata declared in a deliberately scrambled order to prove binding is by kind, not position.
        @Subscription(id = "agnostic-id-version-injection")
        void on(@StreamVersion long streamVersion, MyEvent event, @StreamId String streamId, EventMetadata metadata) {
            if ("id-version-2".equals(event.name())) {
                received.add(new Received(streamVersion, streamId, metadata.getStreamId()));
            }
        }

        List<Received> received() { return received; }
    }

    record MyEvent(String eventId, Date timestamp, String name) {
        MyEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
