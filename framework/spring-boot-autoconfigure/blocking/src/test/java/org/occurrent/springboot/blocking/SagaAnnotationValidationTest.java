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
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes the {@code @Saga} validation branches that fail fast before the saga runner is started, so they
 * reproduce without a running store (no Docker): {@code startAt} combined with {@code startAtGlobalPosition} is
 * rejected, a missing {@code CommandDispatcher} bean fails with actionable guidance, and an ambiguous store type fails
 * fast. Each must fail fast at context startup with the exact message.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationValidationTest {

    @Test
    void saga_with_both_start_at_and_start_at_global_position_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConflictingStartConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("Specify either startAt or startAtGlobalPosition for @Saga");
                });
    }

    @Test
    void saga_without_a_command_dispatcher_bean_fails_fast_with_guidance() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, SubscribableConfiguration.class, StoreConfiguration.class, MissingDispatcherSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("needs a CommandDispatcher bean to run the commands it issues");
                });
    }

    @Test
    void saga_with_an_ambiguous_store_type_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, SubscribableConfiguration.class, AmbiguousStoreSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("Disambiguate with storeName");
                });
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> newSaga() {
        return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                .correlateAll(event -> "k")
                .startsOn(TestEvent.class)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
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
    }

    @Configuration(proxyBeanMethods = false)
    static class SubscribableConfiguration {
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StoreConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            return mock(SagaStateStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ConflictingStartConfiguration {
        @Bean
        ConflictingStartSaga conflictingStartSaga() {
            return new ConflictingStartSaga();
        }
    }

    static class ConflictingStartSaga {
        @Saga(id = "saga-conflicting-start", startAt = StartPosition.NOW, startAtGlobalPosition = 0)
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return newSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MissingDispatcherSagaConfiguration {
        @Bean
        MissingDispatcherSaga missingDispatcherSaga() {
            return new MissingDispatcherSaga();
        }
    }

    static class MissingDispatcherSaga {
        @Saga(id = "saga-missing-dispatcher")
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return newSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AmbiguousStoreSagaConfiguration {
        @Bean
        AmbiguousStoreSaga ambiguousStoreSaga() {
            return new AmbiguousStoreSaga();
        }

        // Two beans of the referenced store type so resolution cannot pick one.
        @Bean
        String storeA() {
            return "a";
        }

        @Bean
        String storeB() {
            return "b";
        }
    }

    static class AmbiguousStoreSaga {
        @Saga(id = "saga-ambiguous-store", store = String.class)
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
