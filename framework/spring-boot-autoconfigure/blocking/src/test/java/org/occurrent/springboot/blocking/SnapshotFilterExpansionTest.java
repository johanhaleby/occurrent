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
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A {@code @Snapshot} factory method's derived filter goes through {@code EventTypeExpansion.deriveFilter} (ADR 126),
 * so a handler registered on a sealed event type reopened below its declared level is refused at context startup,
 * before any store or subscription bean is resolved, the same way a saga or a subscription registration is refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotFilterExpansionTest {

    sealed interface ReopenedEvent permits ReopenedBase {
    }

    // Sealed above, plain abstract here, so nothing below this class can be found.
    abstract static non-sealed class ReopenedBase implements ReopenedEvent {
    }

    @Test
    void a_snapshot_registered_on_a_sealed_event_type_reopened_below_its_declared_level_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, StoreConfiguration.class, ReopenedSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining(ReopenedEvent.class.getName());
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
        @Bean
        CloudEventConverter<ReopenedEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(ReopenedEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType(domainEvent.getClass().getSimpleName()).build();
                }

                @Override
                public ReopenedEvent toDomainEvent(CloudEvent cloudEvent) {
                    throw new UnsupportedOperationException("not needed for this test");
                }

                @Override
                public String getCloudEventType(Class<? extends ReopenedEvent> type) {
                    return type.getSimpleName();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StoreConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReopenedSnapshotConfiguration {
        @Bean
        ReopenedSnapshot reopenedSnapshot() {
            return new ReopenedSnapshot();
        }
    }

    static class ReopenedSnapshot {
        @Snapshot(id = "reopened-snapshot")
        SnapshotView<TestState, ReopenedEvent> snapshot() {
            return SnapshotView.<TestState, ReopenedEvent>builder(new TestState())
                    .on(ReopenedEvent.class, (state, event) -> state)
                    .build();
        }
    }

    record TestState() {
    }
}
