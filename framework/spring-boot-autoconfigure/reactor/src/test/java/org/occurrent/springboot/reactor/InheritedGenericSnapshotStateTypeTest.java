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
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A snapshot factory a superclass declares with a state type variable the bean's class binds gets the zero-config
 * default store for the state type the class binds. The variable stays unbound on the superclass, so reading the return
 * type there finds no state type to create a store for. Container-free, over {@code DcbCriteria.all()}, which the
 * registrar returns on after it has resolved the store, so no event store bean is needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InheritedGenericSnapshotStateTypeTest {

    @Test
    void a_dcb_snapshot_factory_declared_with_a_type_variable_on_a_superclass_gets_the_default_store_for_the_state_type_the_bean_class_binds() {
        List<Class<?>> storesCreatedFor = new CopyOnWriteArrayList<>();
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withBean(DefaultReactiveSnapshotStoreProvider.class, () -> recordingProvider(storesCreatedFor))
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, ConcreteSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(storesCreatedFor).containsExactly(BoundState.class);
                });
    }

    @SuppressWarnings("unchecked")
    private static DefaultReactiveSnapshotStoreProvider recordingProvider(List<Class<?>> storesCreatedFor) {
        return new DefaultReactiveSnapshotStoreProvider() {
            @Override
            public <S> ReactiveSnapshotStore<S> createDefaultSnapshotStore(String snapshotId, Class<S> stateType) {
                storesCreatedFor.add(stateType);
                return mock(ReactiveSnapshotStore.class);
            }
        };
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
    static class ConcreteSnapshotConfiguration {
        @Bean
        Concrete concrete() {
            return new Concrete();
        }
    }

    abstract static class Base<S> {
        private final S initialState;

        Base(S initialState) {
            this.initialState = initialState;
        }

        @Snapshot(id = "generic-state-snapshot")
        DcbSnapshotView<S, TestEvent> snapshot() {
            return new DcbSnapshotView<>(
                    SnapshotView.<S, TestEvent>builder(initialState)
                            .on(TestEvent.class, (state, event) -> state)
                            .build(),
                    DcbCriteria.all());
        }
    }

    static class Concrete extends Base<BoundState> {
        Concrete() {
            super(new BoundState());
        }
    }

    record BoundState() {
    }

    record TestEvent() {
    }
}
