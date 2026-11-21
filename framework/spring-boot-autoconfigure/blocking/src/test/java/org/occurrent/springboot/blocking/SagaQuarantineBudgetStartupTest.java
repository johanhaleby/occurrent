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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.annotation.Saga;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * {@code occurrent.saga.quarantine-after} reaches the runner of an {@code @Saga} at startup. A positive value turns
 * quarantine on with that budget, leaving it out gives the default of five minutes, and
 * {@code occurrent.saga.quarantine-enabled=false} turns it off. Blank, zero and negative values refuse startup, because a saga
 * that quietly ran without the protection its configuration appears to ask for is worse than one that does not start.
 */
@DisplayName("The saga quarantine budget at startup")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineBudgetStartupTest {

    @Test
    void refuses_to_start_when_the_property_is_blank_and_names_how_to_turn_quarantine_off() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-after=").run(context -> {
            assertThat(context).getFailure()
                    .hasMessageContaining("occurrent.saga.quarantine-after")
                    .hasMessageContaining("occurrent.saga.quarantine-enabled=false");
        });
    }

    @Test
    void refuses_to_start_when_the_property_is_only_whitespace_and_names_the_property() {
        contextRunner().withInitializer(context -> context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("whitespace", Map.of("occurrent.saga.quarantine-after", "   ")))).run(context -> {
            assertThat(context).getFailure().hasStackTraceContaining("occurrent.saga.quarantine-after");
        });
    }

    @Test
    void starts_with_a_budget_that_would_be_refused_when_quarantine_is_disabled_since_it_has_no_budget_to_keep() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-enabled=false", "occurrent.saga.quarantine-after=0s").run(context -> {
            assertThat(context).hasNotFailed();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"0s", "-1s", "-5m"})
    @DisplayName("refuses to start when the property is zero or negative")
    void refuses_to_start_when_the_property_is_zero_or_negative(String value) {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-after=" + value).run(context -> {
            assertThat(context).getFailure()
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("occurrent.saga.quarantine-after must be a positive duration")
                    .hasMessageContaining("Leave the property out");
        });
    }

    @Test
    void refuses_to_start_a_saga_that_quarantines_when_its_state_store_cannot_record_skipped_inputs_naming_the_capability_and_the_way_out() {
        contextRunnerOn(StoreThatDoesNotRecordSkippedInputs.class)
                .run(context -> assertThat(context).getFailure()
                        .hasStackTraceContaining("@Saga 'saga-quarantine-budget'")
                        .hasStackTraceContaining("SagaStateStoreSkippedInputs")
                        .hasStackTraceContaining("occurrent.saga.quarantine-enabled=false"));
    }

    @Test
    void starts_a_saga_whose_state_store_cannot_record_skipped_inputs_when_quarantine_is_off() {
        contextRunnerOn(StoreThatDoesNotRecordSkippedInputs.class).withPropertyValues("occurrent.saga.quarantine-enabled=false")
                .run(context -> assertThat(context).hasNotFailed());
    }

    private static ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaConfiguration.class, StoreThatRecordsSkippedInputs.class, PlainModel.class);
    }

    private static ApplicationContextRunner contextRunnerOn(Class<?> storeConfiguration) {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaConfiguration.class, storeConfiguration, PlainModel.class);
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> newSaga() {
        return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                .correlateAll(event -> "k")
                .startsOn(TestEvent.class)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SagaConfiguration {

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
        CommandDispatcher<TestCommand> commandDispatcher() {
            return command -> {
            };
        }

        @Bean
        QuarantineBudgetSaga quarantineBudgetSaga() {
            return new QuarantineBudgetSaga();
        }
    }

    // The store of a saga that can record the inputs it skips and be released, as the in-memory and MongoDB stores are
    @Configuration(proxyBeanMethods = false)
    static class StoreThatRecordsSkippedInputs {
        @SuppressWarnings("unchecked")
        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            SagaStateStore<TestState> store = mock(SagaStateStore.class, withSettings().extraInterfaces(SagaStateStoreSkippedInputs.class));
            when(store.stateCodec()).thenReturn(SagaStateCodec.identity());
            return store;
        }
    }

    // A store of some other implementation, which cannot record the inputs a quarantined instance skips
    @Configuration(proxyBeanMethods = false)
    static class StoreThatDoesNotRecordSkippedInputs {
        @SuppressWarnings("unchecked")
        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            SagaStateStore<TestState> store = mock(SagaStateStore.class);
            when(store.stateCodec()).thenReturn(SagaStateCodec.identity());
            return store;
        }
    }

    // A model that cannot say whether it holds the events it delivers
    @Configuration(proxyBeanMethods = false)
    static class PlainModel {
        @Bean
        Subscribable subscribable() {
            Subscribable subscribable = mock(Subscribable.class);
            when(subscribable.subscribe(any(), any(), any(), any())).thenReturn(mock(Subscription.class));
            return subscribable;
        }
    }

    static class QuarantineBudgetSaga {
        @Saga(id = "saga-quarantine-budget")
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
