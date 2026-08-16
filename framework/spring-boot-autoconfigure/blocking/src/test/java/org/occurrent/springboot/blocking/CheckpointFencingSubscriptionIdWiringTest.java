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

package org.occurrent.springboot.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link CheckpointStorage#evaluatesWriteConditions()} answering true is not the end of what
 * {@link CheckpointFencingConfigurationCheck} asks. It also asks
 * {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} for every subscription id whose own registration
 * path reaches {@link CheckpointStorage}, before {@link OccurrentBlockingAnnotationBeanPostProcessor} registers
 * anything. Neither fixture projection's factory method is ever invoked in the tests below, since the fencing check
 * throws first, so this needs none of the reader, converter or store beans
 * {@link ProjectionAnnotationFencingWiringTest} wires for a projection that actually catches up.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointFencingSubscriptionIdWiringTest {

    private static final String SUBSCRIPTION_ID_A = "proj-fenced-a";
    private static final String SUBSCRIPTION_ID_B = "proj-fenced-b";
    private static final String SUBSCRIPTION_ID_SYNC = "proj-sync";
    private static final String SUBSCRIPTION_ID_NO_CATCHUP = "proj-no-catchup";
    private static final String SUBSCRIPTION_ID_DOMAIN_FEED = "proj-domain-feed";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(TwoFencedProjectionsConfiguration.class);

    @Test
    void a_storage_that_evaluates_conditions_globally_but_refuses_one_declared_id_fails_startup_naming_the_storage_and_the_id() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(SUBSCRIPTION_ID_A)).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(SUBSCRIPTION_ID_B)).thenReturn(false);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(CheckpointStorageCannotFenceSubscriptionException.class)
                            .hasMessageContaining("CheckpointStorage$MockitoMock")
                            .hasMessageContaining(SUBSCRIPTION_ID_B)
                            .hasMessageNotContaining(SUBSCRIPTION_ID_A);
                    // Neither projection's factory method was ever invoked, since the check throws before either is
                    // registered, so a checkpoint write for the id that IS supported never happens either.
                    verify(checkpointStorage, never()).save(any(), any(), any());
                });
    }

    @Test
    void every_declared_id_the_storage_refuses_is_collected_into_one_exception() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(false);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(CheckpointStorageCannotFenceSubscriptionException.class)
                            .hasMessageContaining(SUBSCRIPTION_ID_A)
                            .hasMessageContaining(SUBSCRIPTION_ID_B);
                    verify(checkpointStorage, never()).save(any(), any(), any());
                });
    }

    @Test
    void a_synchronous_projections_id_is_never_asked_about_even_when_the_storage_would_refuse_it() {
        // mode = SYNCHRONOUS registers directly with the synchronous subscription model and never reaches
        // CheckpointStorage at all, so evaluatesWriteConditionsFor must never be asked about its id, whatever
        // happens to registration once the fencing check has passed. A runner of its own, not the shared "runner"
        // field, since TwoFencedProjectionsConfiguration would otherwise ride along and its own refused ids would
        // carry the assertion instead of this one.
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(false);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SynchronousProjectionConfiguration.class)
                .withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> verify(checkpointStorage, never()).evaluatesWriteConditionsFor(SUBSCRIPTION_ID_SYNC));
    }

    @Test
    void a_push_projections_id_is_never_asked_about_when_catchup_is_none() {
        // catchup = NONE uses the bare push feed directly, and no CatchupThenPushSubscriptionModel, the one that
        // resolves CheckpointStorage, is ever built, so this id must never reach the fencing check either.
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(false);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(NoCatchupPushProjectionConfiguration.class)
                .withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> verify(checkpointStorage, never()).evaluatesWriteConditionsFor(SUBSCRIPTION_ID_NO_CATCHUP));
    }

    @Test
    void a_push_projections_id_fed_by_a_domainEventFeed_is_never_asked_about_even_with_the_default_catchup() {
        // registerDomainPushProjection never resolves CheckpointStorage at all, whatever catchup says, unlike the
        // PushSubscriptionModel path, which does exactly that once catchup defaults to FROM_EVENT_STORE. So an
        // id-sensitive storage that refuses every id, as SpringRedisCheckpointStorage's Cluster-safe mode does for
        // one id shape, must not fail startup here, since this projection never reaches that storage.
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(false);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(DomainEventFeedProjectionConfiguration.class)
                .withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage, never()).evaluatesWriteConditionsFor(SUBSCRIPTION_ID_DOMAIN_FEED);
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SynchronousProjectionConfiguration {
        @Bean
        SynchronousProjection synchronousProjection() {
            return new SynchronousProjection();
        }
    }

    static class SynchronousProjection {
        @Projection(id = SUBSCRIPTION_ID_SYNC, mode = Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class NoCatchupPushProjectionConfiguration {
        @Bean
        NoCatchupPushProjection noCatchupPushProjection() {
            return new NoCatchupPushProjection();
        }
    }

    static class NoCatchupPushProjection {
        @Projection(id = SUBSCRIPTION_ID_NO_CATCHUP, source = Source.PUSH, catchup = org.occurrent.annotation.Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainEventFeedProjectionConfiguration {
        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> store.put(id, value));
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId(domainEvent.eventId()).withSource(URI.create("urn:test")).withType("TestEvent").build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent(cloudEvent.getId());
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        // An empty reader replays no history, which the fencing check running before this feed's catch-up makes
        // enough to let the context refresh complete, with no event store bean needed.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(reader, converter, TestEvent::eventId);
        }

        @Bean
        DomainFeedProjection domainFeedProjection() {
            return new DomainFeedProjection();
        }
    }

    static class DomainFeedProjection {
        @Projection(id = SUBSCRIPTION_ID_DOMAIN_FEED, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class TwoFencedProjectionsConfiguration {
        @Bean
        TwoFencedProjections twoFencedProjections() {
            return new TwoFencedProjections();
        }
    }

    static class TwoFencedProjections {
        @Projection(id = SUBSCRIPTION_ID_A, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionA() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }

        @Projection(id = SUBSCRIPTION_ID_B, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionB() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    record TestEvent(String eventId) {
    }
}
