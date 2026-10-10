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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code occurrent.saga.quarantine-after} reaches the runner of an {@code @Saga} at startup. A positive value turns
 * quarantine on and leaving it out keeps quarantine off. Zero and negative values refuse startup, because a saga that
 * quietly ran without the protection its configuration appears to ask for is worse than one that does not start.
 * <p>
 * The subscription model here cannot say whether it still holds an event, so a runner that was asked for quarantine
 * says so at startup when it switches quarantine off again. That line is how these tests tell a runner that was asked
 * for quarantine from one that was not.
 */
@DisplayName("The saga quarantine budget at startup")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineBudgetStartupTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger runnerLog;
    private @Nullable Level originalLevel;

    @BeforeEach
    void startCapturing() {
        appender = new ListAppender<>();
        appender.start();
        runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
        runnerLog.addAppender(appender);
        originalLevel = runnerLog.getLevel();
        runnerLog.setLevel(Level.WARN);
    }

    @AfterEach
    void stopCapturing() {
        runnerLog.setLevel(originalLevel);
        runnerLog.detachAppender(appender);
        appender.stop();
    }

    @Test
    void asks_for_quarantine_when_the_property_is_a_positive_duration() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-after=5m").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(quarantineSwitchedOffWarnings()).hasSize(1);
        });
    }

    @Test
    void keeps_quarantine_off_when_the_property_is_left_out() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(quarantineSwitchedOffWarnings()).isEmpty();
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
            assertThat(quarantineSwitchedOffWarnings()).isEmpty();
        });
    }

    private static ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaConfiguration.class);
    }

    private List<String> quarantineSwitchedOffWarnings() {
        return new ArrayList<>(appender.list).stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("quarantine is switched off for this saga"))
                .toList();
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
        QuarantineBudgetSaga quarantineBudgetSaga() {
            return new QuarantineBudgetSaga();
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
