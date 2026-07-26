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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes what the reactive stack does when the store-neutral module is used without a store starter: the
 * {@link DefaultReactiveSnapshotStoreProvider} seam must fail fast naming the {@code @Snapshot} id, rather than NPE on a
 * missing provider or silently drop the store.
 * <p>
 * The factory declares a concrete generic return type on purpose. A raw return type is rejected one step earlier by
 * state-type reflection, which {@code ReactiveAnnotationFailFastTest} covers, so it never reaches the provider lookup.
 * <p>
 * Container-free, since no store is ever created.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DefaultReactiveSnapshotStoreProviderSeamTest {

    @Test
    void snapshot_without_a_store_bean_and_without_a_default_store_provider_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, SnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("reactive-snapshot-default-store")
                            .hasMessageContaining("found no ReactiveSnapshotStore bean and this starter contributes no zero-config default");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterAndSubscribableConfiguration {
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
            return mock(Subscribable.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotConfiguration {
        @Bean
        DefaultStoreSnapshot defaultStoreSnapshot() {
            return new DefaultStoreSnapshot();
        }
    }

    static class DefaultStoreSnapshot {
        @Snapshot(id = "reactive-snapshot-default-store")
        SnapshotView<TestState, TestEvent> snapshot() {
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
