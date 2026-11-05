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

package org.occurrent.springboot.reactor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.slf4j.LoggerFactory;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * A {@code DcbSnapshotView} over {@code DcbCriteria.all()} has no marker versions to save a snapshot at, so the
 * registrar says so once at startup and subscribes to nothing. Container-free, and there is no event store bean on
 * purpose, since the registrar returns before it looks one up.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotAnnotationMatchAllTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;
    private boolean loggerWasAdditive;

    @BeforeEach
    void attachAppender() {
        logger = (Logger) LoggerFactory.getLogger(SnapshotAnnotationRegistrar.class);
        loggerWasAdditive = logger.isAdditive();
        logger.setAdditive(false);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
        logger.setAdditive(loggerWasAdditive);
    }

    @Test
    void a_snapshot_over_all_events_warns_once_and_starts_no_subscription() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndStoreConfiguration.class, MatchAllSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    List<String> warnings = appender.list.stream().filter(event -> event.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
                    assertThat(warnings).singleElement().satisfies(warning -> assertThat(warning).contains("match-all-snapshot").contains("DcbCriteria.all()"));
                    verifyNoInteractions(context.getBean(DcbSubscriptions.class));
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterAndStoreConfiguration {
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

        @SuppressWarnings("unchecked")
        @Bean
        ReactiveSnapshotStore<TestState> snapshotStore() {
            return mock(ReactiveSnapshotStore.class);
        }

        // The bean post processor only scans for @Snapshot methods once at least one Subscribable bean exists
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        DcbSubscriptions<TestEvent> dcbSubscriptions() {
            return mock(DcbSubscriptions.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MatchAllSnapshotConfiguration {
        @Bean
        MatchAllSnapshot matchAllSnapshot() {
            return new MatchAllSnapshot();
        }
    }

    static class MatchAllSnapshot {
        @Snapshot(id = "match-all-snapshot")
        DcbSnapshotView<TestState, TestEvent> snapshot() {
            return new DcbSnapshotView<>(
                    SnapshotView.<TestState, TestEvent>builder(new TestState())
                            .on(TestEvent.class, (state, event) -> state)
                            .build(),
                    DcbCriteria.all());
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
