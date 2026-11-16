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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
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
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.MapPropertySource;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * {@code occurrent.saga.quarantine-after} reaches the runner of an {@code @Saga} at startup. A positive value turns
 * quarantine on with that budget, leaving it out gives the default of five minutes, and
 * {@code occurrent.saga.quarantine-enabled=false} turns it off. Blank, zero and negative values refuse startup, because a saga
 * that quietly ran without the protection its configuration appears to ask for is worse than one that does not start.
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
        runnerLog.setLevel(Level.DEBUG);
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
            assertThat(quarantineSwitchedOffWarnings()).as("the lines at WARN").hasSize(1);
            assertThat(quarantineSwitchedOffDebugLines()).as("the lines at DEBUG").isEmpty();
        });
    }

    @Test
    void says_at_debug_and_not_at_warn_that_quarantine_is_switched_off_when_the_property_is_left_out() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(quarantineSwitchedOffWarnings()).as("the lines at WARN").isEmpty();
            assertThat(quarantineSwitchedOffDebugLines()).as("the lines at DEBUG").hasSize(1);
        });
    }

    @Test
    void refuses_to_start_when_the_property_is_blank_and_names_how_to_turn_quarantine_off() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-after=").run(context -> {
            assertThat(context).getFailure()
                    .hasMessageContaining("occurrent.saga.quarantine-after")
                    .hasMessageContaining("occurrent.saga.quarantine-enabled=false");
            assertThat(quarantineSwitchedOffWarnings()).isEmpty();
        });
    }

    @Test
    void refuses_to_start_when_the_property_is_only_whitespace_and_names_the_property() {
        contextRunner().withInitializer(context -> context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("whitespace", Map.of("occurrent.saga.quarantine-after", "   ")))).run(context -> {
            assertThat(context).getFailure().hasStackTraceContaining("occurrent.saga.quarantine-after");
            assertThat(quarantineSwitchedOffWarnings()).isEmpty();
        });
    }

    @Test
    void keeps_quarantine_off_when_it_is_disabled() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-enabled=false", "occurrent.saga.quarantine-after=5m").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(quarantineSwitchedOffWarnings()).isEmpty();
        });
    }

    @Test
    void gives_the_saga_the_event_store_queries_bean_to_release_from() {
        contextRunner().withBean(EventStoreQueries.class, () -> mock(EventStoreQueries.class)).run(context -> {
            assertThat(context).hasNotFailed();
            SagaSubscription subscription = context.getBean("sagaSubscription-saga-quarantine-budget", SagaSubscription.class);

            assertThat(subscription.release("no-such-instance")).isEqualTo(SagaSubscription.ReleaseResult.NOT_FOUND);
        });
    }

    @Test
    void refuses_to_release_and_names_EventStoreQueries_when_there_is_no_such_bean() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            SagaSubscription subscription = context.getBean("sagaSubscription-saga-quarantine-budget", SagaSubscription.class);

            assertThatThrownBy(() -> subscription.release("no-such-instance"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("EventStoreQueries");
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

    @Test
    void refuses_to_start_a_saga_that_quarantines_on_a_model_that_holds_every_event_when_there_is_no_event_store_queries_bean_naming_the_saga_and_both_ways_out() {
        retainingContextRunner().run(context -> assertThat(context).getFailure()
                .hasStackTraceContaining("@Saga 'saga-quarantine-budget'")
                .hasStackTraceContaining("no EventStoreQueries bean exists")
                .hasStackTraceContaining("Define the event store as a bean")
                .hasStackTraceContaining("occurrent.saga.quarantine-enabled=false"));
    }

    @Test
    void refuses_to_start_a_saga_that_quarantines_on_a_model_that_holds_every_event_when_its_state_store_cannot_record_skipped_inputs_naming_the_capability_and_the_way_out() {
        retainingContextRunnerOn(StoreThatDoesNotRecordSkippedInputs.class).withBean(EventStoreQueries.class, () -> mock(EventStoreQueries.class))
                .run(context -> assertThat(context).getFailure()
                        .hasStackTraceContaining("@Saga 'saga-quarantine-budget'")
                        .hasStackTraceContaining("SagaStateStoreSkippedInputs")
                        .hasStackTraceContaining("occurrent.saga.quarantine-enabled=false"));
    }

    @Test
    void starts_a_saga_whose_state_store_cannot_record_skipped_inputs_when_quarantine_is_off() {
        retainingContextRunnerOn(StoreThatDoesNotRecordSkippedInputs.class).withPropertyValues("occurrent.saga.quarantine-enabled=false")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void starts_on_a_model_that_holds_every_event_when_quarantine_is_off_without_an_event_store_queries_bean() {
        retainingContextRunner().withPropertyValues("occurrent.saga.quarantine-enabled=false").run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void starts_on_a_model_that_holds_every_event_with_one_event_store_queries_bean() {
        retainingContextRunner().withBean(EventStoreQueries.class, () -> mock(EventStoreQueries.class)).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void refuses_to_start_when_several_event_store_queries_beans_are_none_primary_naming_the_saga_the_beans_and_both_ways_out() {
        retainingContextRunner().withUserConfiguration(TwoEventStoresWithNoPrimary.class).run(context -> assertThat(context).getFailure()
                .hasStackTraceContaining("@Saga 'saga-quarantine-budget'")
                .hasStackTraceContaining("found 2 EventStoreQueries beans")
                .hasStackTraceContaining("firstStore")
                .hasStackTraceContaining("secondStore")
                .hasStackTraceContaining("Mark the one the saga reads from @Primary")
                .hasStackTraceContaining("occurrent.saga.quarantine-enabled=false"));
    }

    @Test
    void starts_with_several_event_store_queries_beans_when_one_is_primary() {
        retainingContextRunner().withUserConfiguration(TwoEventStoresWithAPrimary.class).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void starts_with_several_event_store_queries_beans_none_primary_when_quarantine_is_off() {
        retainingContextRunner().withUserConfiguration(TwoEventStoresWithNoPrimary.class)
                .withPropertyValues("occurrent.saga.quarantine-enabled=false")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void starts_with_several_event_store_queries_beans_none_primary_on_a_model_that_cannot_hold_every_event() {
        contextRunner().withPropertyValues("occurrent.saga.quarantine-after=5m").withUserConfiguration(TwoEventStoresWithNoPrimary.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(quarantineSwitchedOffWarnings()).hasSize(1);
        });
    }

    private static ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaConfiguration.class, StoreThatRecordsSkippedInputs.class, PlainModel.class);
    }

    private static ApplicationContextRunner retainingContextRunner() {
        return retainingContextRunnerOn(StoreThatRecordsSkippedInputs.class);
    }

    private static ApplicationContextRunner retainingContextRunnerOn(Class<?> storeConfiguration) {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaConfiguration.class, storeConfiguration, RetainingModel.class);
    }

    private List<String> quarantineSwitchedOffDebugLines() {
        return new ArrayList<>(appender.list).stream()
                .filter(event -> event.getLevel() == Level.DEBUG)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("quarantine is switched off for this saga"))
                .toList();
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

    // A model that holds every event it delivers, which is what lets a saga quarantine and so needs an EventStoreQueries.
    // A class rather than a mock, since a mock answers the capability lookup with nothing
    @Configuration(proxyBeanMethods = false)
    static class RetainingModel {
        @Bean
        Subscribable subscribable() {
            return new RetainingSubscribable();
        }
    }

    static class RetainingSubscribable implements Subscribable, HistoryRetainingSubscriptions {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return mock(Subscription.class);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoEventStoresWithNoPrimary {
        @Bean
        EventStoreQueries firstStore() {
            return mock(EventStoreQueries.class);
        }

        @Bean
        EventStoreQueries secondStore() {
            return mock(EventStoreQueries.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoEventStoresWithAPrimary {
        @Bean
        @Primary
        EventStoreQueries firstStore() {
            return mock(EventStoreQueries.class);
        }

        @Bean
        EventStoreQueries secondStore() {
            return mock(EventStoreQueries.class);
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
