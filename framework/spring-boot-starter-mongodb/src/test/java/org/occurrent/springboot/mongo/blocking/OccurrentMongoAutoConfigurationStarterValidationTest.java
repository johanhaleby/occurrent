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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.net.URI;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OccurrentMongoAutoConfigurationStarterValidationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withUserConfiguration(EnabledOccurrentConfiguration.class, TestEventTypeMapperConfiguration.class)
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.subscription.enabled=false",
                    "occurrent.event-store.collection=events-v2",
                    "occurrent.subscription.collection=subscriptions-v2",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                    "occurrent.application-service.enable-default-retry-strategy=false"
            );

    @Test
    void user_provided_cloud_event_converter_takes_precedence() {
        CloudEventConverter<TestEvent> cloudEventConverter = new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(TestEvent domainEvent) {
                return CloudEventBuilder.v1()
                        .withId("custom-" + domainEvent.eventId())
                        .withSource(URI.create("urn:custom"))
                        .withType("custom-type")
                        .build();
            }

            @Override
            public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                throw new AssertionError("not expected");
            }

            @Override
            public String getCloudEventType(Class<? extends TestEvent> type) {
                return "custom-type";
            }
        };

        contextRunner.withBean(CloudEventConverter.class, () -> cloudEventConverter).run(context -> {
            assertThat(context).hasSingleBean(CloudEventConverter.class);
            assertThat(context.getBean(CloudEventConverter.class)).isSameAs(cloudEventConverter);

            CloudEvent cloudEvent = context.getBean(CloudEventConverter.class).toCloudEvent(sampleEvent());
            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:custom"));
            assertThat(cloudEvent.getType()).isEqualTo("custom-type");
        });
    }

    @Test
    void library_provided_cloud_event_converter_takes_precedence_when_enable_occurrent_is_composed() {
        contextRunner.withUserConfiguration(ComposedLibraryConfiguration.class).run(context -> {
            assertThat(context).hasBean("occurrentCloudEventConverter");
            assertThat(context).hasBean("cloudEventConverter");
            assertThat(context.getBean(CloudEventConverter.class)).isSameAs(context.getBean("cloudEventConverter"));

            CloudEvent cloudEvent = context.getBean(CloudEventConverter.class).toCloudEvent(sampleEvent());
            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:custom"));
            assertThat(cloudEvent.getType()).isEqualTo("custom-type");
        });
    }

    @Test
    void user_jackson3_mapper_creates_jackson3_converter() {
        contextRunner.withBean(tools.jackson.databind.ObjectMapper.class, tools.jackson.databind.ObjectMapper::new).run(context -> {
            assertThat(context).hasSingleBean(CloudEventConverter.class);
            assertThat(context.getBean(CloudEventConverter.class).getClass().getName()).startsWith("org.occurrent.application.converter.jackson3.");

            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            CloudEvent cloudEvent = converter.toCloudEvent(sampleEvent());

            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:occurrent:test"));
            assertThat(cloudEvent.getType()).isEqualTo(TestEvent.class.getName());
            assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(sampleEvent());
        });
    }

    @Test
    void default_path_creates_jackson3_converter() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(CloudEventConverter.class);
            assertThat(context.getBean(CloudEventConverter.class).getClass().getName()).startsWith("org.occurrent.application.converter.jackson3.");

            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            CloudEvent cloudEvent = converter.toCloudEvent(sampleEvent());

            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:occurrent:test"));
            assertThat(cloudEvent.getType()).isEqualTo(TestEvent.class.getName());
            assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(sampleEvent());
        });
    }

    @Test
    void user_jackson3_mapper_still_creates_jackson3_converter() {
        contextRunner.withBean(tools.jackson.databind.ObjectMapper.class, tools.jackson.databind.ObjectMapper::new).run(context -> {
            assertThat(context).hasSingleBean(CloudEventConverter.class);
            assertThat(context.getBean(CloudEventConverter.class).getClass().getName()).startsWith("org.occurrent.application.converter.jackson3.");

            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            CloudEvent cloudEvent = converter.toCloudEvent(sampleEvent());

            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:occurrent:test"));
            assertThat(cloudEvent.getType()).isEqualTo(TestEvent.class.getName());
            assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(sampleEvent());
        });
    }

    @Test
    void dependency_alone_does_not_activate_occurrents() {
        new ApplicationContextRunner().run(context -> {
            assertThat(context).doesNotHaveBean(CloudEventConverter.class);
            assertThat(context).doesNotHaveBean(OccurrentProperties.class);
        });
    }

    private static TestEvent sampleEvent() {
        return new TestEvent(UUID.fromString("11111111-2222-3333-4444-555555555555"), new Date(1_632_482_491_299L), "name", "subject");
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

    @Configuration(proxyBeanMethods = false)
    @Import(ComposedLibraryOccurrentConfiguration.class)
    static class ComposedLibraryConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1()
                            .withId("library-" + domainEvent.eventId())
                            .withSource(URI.create("urn:custom"))
                            .withType("custom-type")
                            .build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    throw new AssertionError("not expected");
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return "custom-type";
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ComposedLibraryOccurrentConfiguration {
    }

    record TestEvent(UUID eventId, Date timestamp, String name, String subject) {
    }
}
