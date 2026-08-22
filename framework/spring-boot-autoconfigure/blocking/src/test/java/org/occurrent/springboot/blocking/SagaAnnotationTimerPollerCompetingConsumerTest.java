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
import org.occurrent.annotation.Saga;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link SagaAnnotationRegistrar#resolveSagaCompetingConsumerStrategy()} gates the saga timer poller on a
 * {@link CompetingConsumerStrategy} bean. Several of them with no {@code @Primary} refuse to start. Picking one would
 * gate the poller on a lease the application did not choose, and standing the gate down would run a poller on every
 * instance while the configuration looks like the gate is on. A {@code @Primary} bean says which lease to use.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationTimerPollerCompetingConsumerTest {

    @Test
    void several_strategy_beans_refuse_to_start_rather_than_leaving_the_timer_poller_ungated() {
        CompetingConsumerStrategy first = mock(CompetingConsumerStrategy.class);
        CompetingConsumerStrategy rival = mock(CompetingConsumerStrategy.class);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TwoStrategySagaConfiguration.class)
                .withBean("firstStrategy", CompetingConsumerStrategy.class, () -> first)
                .withBean("rivalStrategy", CompetingConsumerStrategy.class, () -> rival)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(AmbiguousCompetingConsumerStrategyException.class)
                            .hasMessageContaining("firstStrategy")
                            .hasMessageContaining("rivalStrategy")
                            .hasMessageContaining("@Primary");
                    verifyNoInteractions(first);
                    verifyNoInteractions(rival);
                });
    }

    @Test
    void a_primary_strategy_bean_starts_the_saga_and_gates_its_timer_poller_on_that_lease() {
        CompetingConsumerStrategy chosen = mock(CompetingConsumerStrategy.class);
        CompetingConsumerStrategy rival = mock(CompetingConsumerStrategy.class);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TwoStrategySagaConfiguration.class)
                .withBean("chosenStrategy", CompetingConsumerStrategy.class, () -> chosen, definition -> definition.setPrimary(true))
                .withBean("rivalStrategy", CompetingConsumerStrategy.class, () -> rival)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // The poller registers itself as a competing consumer for the timer key, and asks that same
                    // strategy whether it holds the lock before each round.
                    verify(chosen).registerCompetingConsumer(any(), any());
                    verifyNoInteractions(rival);
                });
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> newSaga() {
        return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                .correlateAll(event -> "k")
                .startsOn(TestEvent.class)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class TwoStrategySagaConfiguration {

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("TestEvent").build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent();
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        Subscribable subscribable() {
            Subscribable subscribable = mock(Subscribable.class);
            when(subscribable.subscribe(any(), any(), any(), any())).thenReturn(mock(SubscriptionHandle.class));
            return subscribable;
        }

        @SuppressWarnings("unchecked")
        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            return mock(SagaStateStore.class);
        }

        @Bean
        CommandDispatcher<TestCommand> commandDispatcher() {
            return command -> {
            };
        }

        @Bean
        TwoStrategySaga twoStrategySaga() {
            return new TwoStrategySaga();
        }
    }

    static class TwoStrategySaga {
        @Saga(id = "saga-two-strategy-beans")
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return newSaga();
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }

    record TestCommand() {
    }
}
