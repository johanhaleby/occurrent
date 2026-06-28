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

import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.TagGenerator;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots the real {@link OccurrentMongoAutoConfiguration} against a MongoDB replica-set testcontainer
 * with both STREAM and DCB capabilities enabled, and exercises a real append + read on each path.
 */
@SpringBootTest(
        classes = OccurrentMongoAutoConfigurationCombinedModeTest.CombinedModeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream,dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:combined-mode-test"
        }
)
@Import(OccurrentMongoAutoConfigurationCombinedModeTest.MongoDbContainerConfiguration.class)
@Testcontainers
class OccurrentMongoAutoConfigurationCombinedModeTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private DcbApplicationService<TestEvent> dcbApplicationService;

    @Autowired
    private DomainEventQueries<TestEvent> domainEventQueries;

    @Autowired
    private DcbDomainEventQueries<TestEvent> dcbDomainEventQueries;

    @Test
    void combined_mode_auto_configures_both_the_stream_and_dcb_application_services() {
        assertThat(applicationContext.getBeansOfType(ApplicationService.class)).isNotEmpty();
        assertThat(applicationContext.getBeansOfType(DcbApplicationService.class)).isNotEmpty();
    }

    @Test
    void stream_application_service_appends_an_event_that_the_stream_query_reads_back() {
        String streamId = UUID.randomUUID().toString();
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(), "stream-name", "stream-subject");

        applicationService.execute(streamId, __ -> Stream.of(event));

        try (Stream<TestEvent> read = domainEventQueries.query(TestEvent.class)) {
            assertThat(read).contains(event);
        }
    }

    @Test
    void dcb_application_service_appends_a_tagged_event_that_the_dcb_query_reads_back() {
        String tag = "tenant:" + UUID.randomUUID();
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(), "dcb-name", tag);

        dcbApplicationService.execute(DcbQuery.tags(tag), __ -> Stream.of(event));

        try (Stream<TestEvent> read = dcbDomainEventQueries.query(DcbQuery.tags(tag))) {
            assertThat(read).containsExactly(event);
        }
    }

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

        // The default TimeRepresentation is DATE, which rejects sub-millisecond precision, so map the event
        // timestamp (millisecond precision) instead of relying on the fallback converter's OffsetDateTime.now().
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:combined-mode-test"))
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        TagGenerator<TestEvent> testEventTagGenerator() {
            return event -> Set.of(event.subject());
        }
    }

    record TestEvent(String eventId, Date timestamp, String name, String subject) {
    }
}
