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
import org.occurrent.subscription.api.blocking.Subscription;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link SagaAnnotationRegistrar#resolveSagaCompetingConsumerStrategy()} gates the saga timer poller on a
 * {@link CompetingConsumerStrategy} bean, resolved with {@link org.springframework.beans.factory.ObjectProvider#getIfUnique()}.
 * Two strategy beans make the bean ambiguous, and {@code getIfUnique()} answers {@code null} for that rather than
 * throwing {@link org.springframework.beans.factory.NoUniqueBeanDefinitionException}, so the application still
 * starts and the poller runs on every instance, unfenced, the same stand-down
 * {@link CompetingConsumerCheckpointWriteVersionSource} already has for the checkpoint-write side of ADR 116. Before
 * this, {@code getIfAvailable()} threw and a {@code @Saga} in such an application failed to start (#684).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationTimerPollerCompetingConsumerTest {

    @Test
    void two_strategy_beans_start_the_saga_and_leave_its_timer_poller_ungated() {
        CompetingConsumerStrategy primary = mock(CompetingConsumerStrategy.class);
        CompetingConsumerStrategy rival = mock(CompetingConsumerStrategy.class);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TwoStrategySagaConfiguration.class)
                .withBean("primaryStrategy", CompetingConsumerStrategy.class, () -> primary)
                .withBean("rivalStrategy", CompetingConsumerStrategy.class, () -> rival)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // Neither bean is registered as the timer's lease holder: an ambiguous strategy stands the fence
                    // down instead of picking one, so the poller runs unconditionally.
                    verifyNoInteractions(primary);
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
            when(subscribable.subscribe(any(), any(), any(), any())).thenReturn(mock(Subscription.class));
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
