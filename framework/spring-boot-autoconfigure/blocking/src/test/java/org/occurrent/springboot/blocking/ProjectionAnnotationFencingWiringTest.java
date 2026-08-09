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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code @Projection(source = PUSH)} with the default catch-up: {@link ProjectionAnnotationRegistrar} builds a
 * {@code CatchupThenPushSubscriptionModel} and has to reach it with a {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource}
 * built over a lazily resolved {@link CompetingConsumerStrategy} bean (ADR 116). The finite fake reader's one event
 * completes the catch-up synchronously, since the default {@code startupMode} blocks registration until it does,
 * and the marker write that follows is what carries the condition under test. Container-free, the same way
 * {@link BackgroundCatchupFailureTest} is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationFencingWiringTest {

    private static final String SUBSCRIPTION_ID = "proj-fenced";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(FencedPushProjectionConfiguration.class);

    @Test
    void one_strategy_bean_stamps_the_catch_up_marker_write_not_older_than_its_token() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(42L));
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage).save(eq(SUBSCRIPTION_ID), any(), eq(CheckpointWriteCondition.notOlderThan(42L)));
                });
    }

    @Test
    void two_strategy_beans_leave_the_catch_up_marker_write_unconditional() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        // Stubbed to prove the ambiguity is what suppresses the fence, not this strategy simply having no token.
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(7L));
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);

        runner.withBean("primaryStrategy", CompetingConsumerStrategy.class, () -> strategy)
                .withBean("rivalStrategy", CompetingConsumerStrategy.class, RivalCompetingConsumerStrategy::new)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage).save(eq(SUBSCRIPTION_ID), any(), eq(CheckpointWriteCondition.any()));
                });
    }

    @Test
    void no_strategy_bean_leaves_the_catch_up_marker_write_unconditional() {
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);

        runner.withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage).save(eq(SUBSCRIPTION_ID), any(), eq(CheckpointWriteCondition.any()));
                });
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String eventId) {
    }

    private static final class RivalCompetingConsumerStrategy implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FencedPushProjectionConfiguration {

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        Map<String, Integer> fencedProjectionRepo() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository(Map<String, Integer> fencedProjectionRepo) {
            return ViewStateRepository.create(fencedProjectionRepo::get, fencedProjectionRepo::put);
        }

        // One event, so the catch-up (which the default startupMode waits for) reaches the end of the reader and
        // writes its one-shot marker, the write this test class observes the condition of.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.of(cloudEvent("history-1"));
                }

                @Override
                public long currentPosition() {
                    return 1;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return cloudEvent(domainEvent.eventId());
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

        @Bean
        FencedPushProjection fencedPushProjection() {
            return new FencedPushProjection();
        }
    }

    static class FencedPushProjection {
        @Projection(id = SUBSCRIPTION_ID, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
