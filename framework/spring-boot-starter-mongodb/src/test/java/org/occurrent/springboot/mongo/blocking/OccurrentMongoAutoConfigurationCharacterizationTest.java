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
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability;
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

class OccurrentMongoAutoConfigurationCharacterizationTest {

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
    void enabling_occurrents_creates_expected_beans_and_binds_properties() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(CloudEventConverter.class);
            assertThat(context).hasSingleBean(OccurrentProperties.class);

            OccurrentProperties properties = context.getBean(OccurrentProperties.class);
            assertThat(properties.getEventStore().getCollection()).isEqualTo("events-v2");
            assertThat(properties.getEventStore().getCapabilities()).containsExactly(SpringMongoEventStoreCapability.STREAM);
            assertThat(properties.getSubscription().getCollection()).isEqualTo("subscriptions-v2");
            assertThat(properties.getCloudEventConverter().getCloudEventSource()).isEqualTo(URI.create("urn:occurrent:test"));
            assertThat(properties.getApplicationService().isEnableDefaultRetryStrategy()).isFalse();

            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(1_632_482_491_299L), "name", "subject");
            CloudEvent cloudEvent = converter.toCloudEvent(event);

            assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:occurrent:test"));
            assertThat(cloudEvent.getType()).isEqualTo(TestEvent.class.getName());
            assertThat(cloudEvent.getSubject()).isNull();
            assertThat(cloudEvent.getDataContentType()).isEqualTo("application/json");
            assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(event);
        });
    }

    @Test
    void binds_composed_event_store_capabilities() {
        contextRunner.withPropertyValues("occurrent.event-store.capabilities=stream,dcb").run(context -> {
            OccurrentProperties properties = context.getBean(OccurrentProperties.class);

            assertThat(properties.getEventStore().getCapabilities())
                    .containsExactlyInAnyOrder(SpringMongoEventStoreCapability.STREAM, SpringMongoEventStoreCapability.DCB);
        });
    }

    @Test
    void propagates_default_capabilities_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner().run(context -> {
            EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

            assertThat(eventStoreConfig.eventStoreCapabilities).containsExactly(SpringMongoEventStoreCapability.STREAM);
        });
    }

    @Test
    void propagates_composed_capabilities_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=stream,dcb")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.eventStoreCapabilities)
                            .containsExactlyInAnyOrder(SpringMongoEventStoreCapability.STREAM, SpringMongoEventStoreCapability.DCB);
                });
    }

    @Test
    void propagates_dcb_only_capability_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.eventStoreCapabilities).containsExactly(SpringMongoEventStoreCapability.DCB);
                });
    }

    @Test
    void binds_dcb_only_event_store_capability() {
        contextRunner.withPropertyValues("occurrent.event-store.capabilities=dcb").run(context -> {
            OccurrentProperties properties = context.getBean(OccurrentProperties.class);

            assertThat(properties.getEventStore().getCapabilities()).containsExactly(SpringMongoEventStoreCapability.DCB);
        });
    }

    @Test
    void dependency_alone_does_not_activate_occurrents() {
        new ApplicationContextRunner().run(context -> {
            assertThat(context).doesNotHaveBean(CloudEventConverter.class);
            assertThat(context).doesNotHaveBean(OccurrentProperties.class);
        });
    }

    private ApplicationContextRunner eventStoreConfigContextRunner() {
        return contextRunner
                .withPropertyValues("occurrent.event-store.enabled=true")
                .withBean(SpringMongoEventStore.class, () -> mock(SpringMongoEventStore.class));
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
