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
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
            assertThat(properties.getEventStore().getCapabilities()).containsExactly(EventStoreCapability.STREAM);
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
    void cloud_event_time_is_truncated_to_millis_by_default_for_date_time_representation() {
        // TimeRepresentation defaults to DATE, which cannot store sub-millisecond precision, so the converter should
        // default to truncating the time to milliseconds.
        contextRunner.run(context -> {
            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            CloudEvent cloudEvent = converter.toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), "name", "subject"));
            assertThat(cloudEvent.getTime().getNano() % 1_000_000).isZero();
        });
    }

    @Test
    void explicit_time_precision_property_is_honored() {
        contextRunner.withPropertyValues("occurrent.cloud-event-converter.time-precision=seconds").run(context -> {
            CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
            CloudEvent cloudEvent = converter.toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), "name", "subject"));
            assertThat(cloudEvent.getTime().getNano()).isZero();
        });
    }

    @Test
    void binds_composed_event_store_capabilities() {
        contextRunner.withPropertyValues("occurrent.event-store.capabilities=stream,dcb").run(context -> {
            OccurrentProperties properties = context.getBean(OccurrentProperties.class);

            assertThat(properties.getEventStore().getCapabilities())
                    .containsExactlyInAnyOrder(EventStoreCapability.STREAM, EventStoreCapability.DCB);
        });
    }

    @Test
    void propagates_default_capabilities_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner().run(context -> {
            EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

            assertThat(eventStoreConfig.eventStoreCapabilities).containsExactly(EventStoreCapability.STREAM);
        });
    }

    @Test
    void propagates_composed_capabilities_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=stream,dcb")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.eventStoreCapabilities)
                            .containsExactlyInAnyOrder(EventStoreCapability.STREAM, EventStoreCapability.DCB);
                });
    }

    @Test
    void propagates_dcb_only_capability_to_auto_configured_event_store_config() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.eventStoreCapabilities).containsExactly(EventStoreCapability.DCB);
                });
    }

    @Test
    void leaves_stream_position_at_its_default_when_the_position_property_is_unset() {
        eventStoreConfigContextRunner().run(context -> {
            EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

            assertThat(eventStoreConfig.streamPositionEnabled).isTrue();
            assertThat(eventStoreConfig.streamPositionExplicitlyEnabled).isFalse();
        });
    }

    @Test
    void enables_stream_position_explicitly_when_the_position_property_is_true() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.stream.position=true")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.streamPositionEnabled).isTrue();
                    assertThat(eventStoreConfig.streamPositionExplicitlyEnabled).isTrue();
                });
    }

    @Test
    void opts_a_stream_store_out_of_position_when_the_position_property_is_false() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.stream.position=false")
                .run(context -> {
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    assertThat(eventStoreConfig.streamPositionEnabled).isFalse();
                });
    }

    @Test
    void binds_the_catch_up_then_live_tunables_and_leaves_them_unset_when_no_property_is_given() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties tunables =
                    context.getBean(OccurrentProperties.class).getSubscription().getCatchupThenLive();

            assertThat(tunables.getDedupCacheSize()).isNull();
            assertThat(tunables.getMaxBufferedEvents()).isNull();
        });
    }

    @Test
    void binds_the_catch_up_then_live_tunables_from_kebab_case_properties() {
        contextRunner
                .withPropertyValues(
                        "occurrent.subscription.catchup-then-live.dedup-cache-size=50000",
                        "occurrent.subscription.catchup-then-live.max-buffered-events=200000")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties tunables =
                            context.getBean(OccurrentProperties.class).getSubscription().getCatchupThenLive();

                    assertThat(tunables.getDedupCacheSize()).isEqualTo(50_000);
                    assertThat(tunables.getMaxBufferedEvents()).isEqualTo(200_000);
                });
    }

    @Test
    void a_non_positive_catch_up_then_live_tunable_fails_the_context_instead_of_silently_using_the_default() {
        contextRunner
                .withPropertyValues("occurrent.subscription.catchup-then-live.max-buffered-events=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("occurrent.subscription.catchup-then-live.max-buffered-events");
                });
    }

    @Test
    void ignores_a_false_position_property_when_dcb_is_enabled_so_the_context_still_loads() {
        eventStoreConfigContextRunner()
                .withPropertyValues(
                        "occurrent.event-store.capabilities=stream,dcb",
                        "occurrent.event-store.stream.position=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    EventStoreConfig eventStoreConfig = context.getBean(EventStoreConfig.class);

                    // withoutStreamPosition() is rejected with DCB, so a false property is skipped and the default stands.
                    assertThat(eventStoreConfig.streamPositionEnabled).isTrue();
                });
    }

    @Test
    void binds_dcb_only_event_store_capability() {
        contextRunner.withPropertyValues("occurrent.event-store.capabilities=dcb").run(context -> {
            OccurrentProperties properties = context.getBean(OccurrentProperties.class);

            assertThat(properties.getEventStore().getCapabilities()).containsExactly(EventStoreCapability.DCB);
        });
    }

    @Test
    void dcb_only_auto_configures_domain_event_queries_and_dcb_subscription_catchup_but_not_stream_application_service() {
        contextRunner
                .withPropertyValues(
                        "occurrent.event-store.enabled=true",
                        "occurrent.subscription.enabled=true",
                        "occurrent.event-store.capabilities=dcb"
                )
                .withBean(SpringMongoEventStore.class, () -> mock(SpringMongoEventStore.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ApplicationService.class);
                    assertThat(context).hasSingleBean(DomainEventQueries.class);
                    // Two beans satisfy SubscriptionModel, because the register-only SynchronousSubscriptionModel
                    // declares it too. What the old hasSingleBean check was really standing for is that a by-type
                    // injection point still reaches the asynchronous model, which is what @Primary is there for, so
                    // assert that directly.
                    assertThat(context).hasBean("occurrentCompetingDurableSubscriptionModel");
                    assertThat(context.getBean(SubscriptionModel.class))
                            .isSameAs(context.getBean("occurrentCompetingDurableSubscriptionModel"));

                    // In DCB-only mode the subscription model now wraps a DCB-mode CatchupSubscriptionModel, so a
                    // subscription started at a GlobalCheckpoint can replay history by position.
                    SubscriptionModel subscriptionModel = context.getBean(SubscriptionModel.class);
                    assertThat(subscriptionModel).isInstanceOf(DelegatingSubscriptionModel.class);
                    SubscriptionModel delegated = ((DelegatingSubscriptionModel) subscriptionModel).getDelegatedSubscriptionModel();
                    assertThat(delegated).isInstanceOf(CatchupSubscriptionModel.class);
                    assertThat(((DelegatingSubscriptionModel) delegated).getDelegatedSubscriptionModel())
                            .isInstanceOf(DurableSubscriptionModel.class);
                });
    }

    @Test
    void virtual_thread_property_configures_the_blocking_subscription_executor() {
        eventStoreConfigContextRunner()
                .withPropertyValues(
                        "occurrent.subscription.enabled=true",
                        "spring.threads.virtual.enabled=true"
                )
                .run(context -> {
                    SpringMongoSubscriptionModel springMongoSubscriptionModel = findDelegate(context.getBean(SubscriptionModel.class), SpringMongoSubscriptionModel.class);
                    DefaultMessageListenerContainer container = getField(springMongoSubscriptionModel, "messageListenerContainer", DefaultMessageListenerContainer.class);
                    ThreadPoolTaskExecutor executor = getField(container, "taskExecutor", ThreadPoolTaskExecutor.class);
                    CountDownLatch executed = new CountDownLatch(1);
                    AtomicBoolean virtual = new AtomicBoolean(false);

                    try {
                        executor.execute(() -> {
                            virtual.set(Thread.currentThread().isVirtual());
                            executed.countDown();
                        });

                        assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
                        assertThat(virtual).isTrue();
                    } finally {
                        executor.shutdown();
                    }
                });
    }

    @Test
    void stream_capability_auto_configures_stream_application_service_only_by_default() {
        eventStoreConfigContextRunner().run(context -> {
            assertThat(context).hasSingleBean(ApplicationService.class);
            assertThat(context).doesNotHaveBean(DcbApplicationService.class);
        });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_when_tag_generator_exists() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ApplicationService.class);
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                });
    }

    @Test
    void stream_and_dcb_capabilities_auto_configure_both_application_services_when_tag_generator_exists() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=stream,dcb")
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .run(context -> {
                    assertThat(context).hasSingleBean(ApplicationService.class);
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_with_annotation_tag_generator_default_when_no_user_tag_generator_is_defined() {
        // dcb-annotation-taggenerator is an optional starter dependency and therefore present on this module's own
        // test classpath, so with no user-defined TagGenerator bean the auto-configured AnnotationTagGenerator kicks
        // in and the DcbApplicationService is created, unlike before this default existed.
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ApplicationService.class);
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                    assertThat(context.getBean(TagGenerator.class)).isInstanceOf(AnnotationTagGenerator.class);
                });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_without_tag_generator_even_when_annotation_tag_generator_module_is_absent() {
        // A global TagGenerator is now optional. A DcbDecider carries the tags for the events it emits, so the service
        // is auto-configured even with no TagGenerator bean and no AnnotationTagGenerator fallback. Decider-less
        // execution with no tagger of any kind fails loudly at append time instead.
        eventStoreConfigContextRunner()
                .withClassLoader(new FilteredClassLoader(AnnotationTagGenerator.class))
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(TagGenerator.class);
                    assertThat(context).doesNotHaveBean(ApplicationService.class);
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                });
    }

    @Test
    void user_defined_tag_generator_takes_precedence_over_the_annotation_tag_generator_default() {
        TagGenerator<TestEvent> userTagGenerator = tagsForTestEvent();

        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> userTagGenerator)
                .run(context -> assertThat(context.getBean(TagGenerator.class)).isSameAs(userTagGenerator));
    }

    @Test
    void custom_dcb_application_service_is_not_replaced() {
        DcbApplicationService customApplicationService = mock(DcbApplicationService.class);

        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .withBean(DcbApplicationService.class, () -> customApplicationService)
                .run(context -> assertThat(context.getBean(DcbApplicationService.class)).isSameAs(customApplicationService));
    }

    @Test
    void dcb_capability_auto_configures_dcb_query_and_subscription_dsl() {
        eventStoreConfigContextRunner()
                .withPropertyValues(
                        "occurrent.subscription.enabled=true",
                        "occurrent.event-store.capabilities=dcb"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(DcbDomainEventQueries.class);
                    assertThat(context).hasSingleBean(DcbSubscriptions.class);
                });
    }

    @Test
    void stream_only_does_not_auto_configure_dcb_query_or_subscription_dsl() {
        eventStoreConfigContextRunner()
                .withPropertyValues("occurrent.subscription.enabled=true")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DcbDomainEventQueries.class);
                    assertThat(context).doesNotHaveBean(DcbSubscriptions.class);
                });
    }

    @Test
    void custom_dcb_query_and_subscription_dsl_beans_are_not_replaced() {
        DcbDomainEventQueries<?> customQueries = mock(DcbDomainEventQueries.class);
        DcbSubscriptions<?> customSubscriptions = mock(DcbSubscriptions.class);

        eventStoreConfigContextRunner()
                .withPropertyValues(
                        "occurrent.subscription.enabled=true",
                        "occurrent.event-store.capabilities=dcb"
                )
                .withBean(DcbDomainEventQueries.class, () -> customQueries)
                .withBean(DcbSubscriptions.class, () -> customSubscriptions)
                .run(context -> {
                    assertThat(context.getBean(DcbDomainEventQueries.class)).isSameAs(customQueries);
                    assertThat(context.getBean(DcbSubscriptions.class)).isSameAs(customSubscriptions);
                });
    }

    @Test
    void disabling_application_service_disables_stream_and_dcb_application_services() {
        eventStoreConfigContextRunner()
                .withPropertyValues(
                        "occurrent.event-store.capabilities=stream,dcb",
                        "occurrent.application-service.enabled=false"
                )
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ApplicationService.class);
                    assertThat(context).doesNotHaveBean(DcbApplicationService.class);
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

    private TagGenerator<TestEvent> tagsForTestEvent() {
        return event -> Set.of(Tag.of("subject", event.subject()));
    }

    private static <T> T findDelegate(SubscriptionModel subscriptionModel, Class<T> type) {
        SubscriptionModel current = subscriptionModel;
        while (true) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            if (current instanceof DelegatingSubscriptionModel delegatingSubscriptionModel) {
                current = delegatingSubscriptionModel.getDelegatedSubscriptionModel();
            } else {
                throw new IllegalStateException("Could not find delegate of type " + type.getName());
            }
        }
    }

    private static <T> T getField(Object target, String fieldName, Class<T> type) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return type.cast(field.get(target));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read field " + fieldName + " from " + target.getClass().getName(), e);
        }
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
