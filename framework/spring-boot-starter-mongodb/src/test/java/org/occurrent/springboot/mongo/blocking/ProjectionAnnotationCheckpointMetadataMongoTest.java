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

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code @Projection} whose event handler takes the event's {@code EventMetadata} sees the checkpoint the
 * replay delivered the event with.
 */
@DisplayName("Projection annotation (checkpoint in the metadata of a replayed event)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ProjectionAnnotationCheckpointMetadataMongoTest.CheckpointApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:projection-checkpoint-metadata-test"
        }
)
@Import(ProjectionAnnotationCheckpointMetadataMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationCheckpointMetadataMongoTest {

    static final String TAG = "counter:checkpoint";
    static final String NONE = "none";
    private static final URI SOURCE = URI.create("urn:occurrent:projection-checkpoint-metadata-test");

    @Autowired
    private MongoOperations mongoOperations;

    @Test
    void a_metadata_aware_fold_sees_the_checkpoint_the_replayed_event_was_delivered_with() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            LastCheckpoint last = mongoOperations.findById("last", LastCheckpoint.class);
            assertThat(last).isNotNull();
            assertThat(last.checkpoint()).isNotBlank().isNotEqualTo(NONE);
        });
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class CheckpointApplication {
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

        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        LastCheckpointProjection lastCheckpointProjection() {
            return new LastCheckpointProjection();
        }
    }

    static class LastCheckpointProjection {
        @Projection(id = "last-checkpoint", startAt = org.occurrent.annotation.StartPosition.BEGINNING)
        DcbProjection<LastCheckpoint, TestEvent, String> lastCheckpoint() {
            var projection = org.occurrent.dsl.projection.Projection.<LastCheckpoint, TestEvent, String>builder(new LastCheckpoint("last", NONE))
                    .id(event -> "last")
                    .on(OrderPlaced.class, (state, metadata, event) -> new LastCheckpoint("last", metadata.getCheckpoint().orElse(NONE)))
                    .build();
            return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse(TAG)));
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
            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new OrderPlaced("historic-1"), new OrderPlaced("historic-2"))).stream()
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                    .toList();
            dcbEventStore.append(cloudEvents);
        }
    }

    record LastCheckpoint(@Id String id, String checkpoint) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record OrderPlaced(String eventId, Date timestamp, String name) implements TestEvent {
        OrderPlaced(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
