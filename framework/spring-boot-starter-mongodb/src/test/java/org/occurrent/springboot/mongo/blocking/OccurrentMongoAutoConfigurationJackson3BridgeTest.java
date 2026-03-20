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
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.net.URI;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OccurrentMongoAutoConfigurationJackson3BridgeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withUserConfiguration(EnabledOccurrentConfiguration.class, TestEventTypeMapperConfiguration.class)
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.subscription.enabled=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:jackson3-test"
            );

    @Test
    void jackson3_mapper_is_supported_by_the_starter() {
        contextRunner
                .withBean(tools.jackson.databind.ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(CloudEventConverter.class);

                    CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
                    TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(1_632_482_491_299L), "name", "subject");
                    CloudEvent cloudEvent = converter.toCloudEvent(event);

                    assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:occurrent:jackson3-test"));
                    assertThat(cloudEvent.getType()).isEqualTo(TestEvent.class.getName());
                    assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(event);
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class EnabledOccurrentConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class TestEventTypeMapperConfiguration {
        @Bean
        CloudEventTypeMapper testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name, String subject) {}
}
