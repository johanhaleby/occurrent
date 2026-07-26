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
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes the {@code @Snapshot} rejection of a {@code DcbSnapshotView} declared with {@code mode=SYNCHRONOUS}:
 * a DCB snapshot cannot be maintained synchronously, so it must fail fast at context startup with the exact message.
 * The rejection happens before any subscription is started, and a {@code SnapshotStore} bean is provided so the store
 * resolution succeeds without the zero-config default, keeping the test container-free (no Docker).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotAnnotationValidationTest {

    @Test
    void dcb_snapshot_with_synchronous_mode_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, StoreConfiguration.class, DcbSynchronousSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("returns a DcbSnapshotView with mode = SYNCHRONOUS, which is not supported");
                });
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
    static class StoreConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DcbSynchronousSnapshotConfiguration {
        @Bean
        DcbSynchronousSnapshot dcbSynchronousSnapshot() {
            return new DcbSynchronousSnapshot();
        }
    }

    static class DcbSynchronousSnapshot {
        @Snapshot(id = "dcb-sync-snapshot", mode = Mode.SYNCHRONOUS)
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
