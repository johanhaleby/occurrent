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
import kotlin.jvm.functions.Function2;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A static {@code @Snapshot} factory that hides the static factory of its superclass with the same name and
 * parameters and repeats the annotation is a factory of its own, so both register, and an id they share fails startup
 * with {@link DuplicateSubscriptionIdException}. The tests use {@code public} and {@code protected} factories, since
 * the tests of the scan in {@code SubscriptionAnnotationsTest} use package-private ones.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HiddenStaticFactoryRegistrationTest {

    private static final List<String> RUN = new CopyOnWriteArrayList<>();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class);

    @BeforeEach
    void clearRun() {
        RUN.clear();
    }

    @Test
    void the_same_id_on_two_public_static_factories_one_hiding_the_other_fails_startup() {
        runner.withBean(PublicHidingWithSameId.class, PublicHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_public_static_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        runner.withBean(PublicHidingWithOtherId.class, PublicHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("PublicBase", "PublicHidingWithOtherId");
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("base-id", "hiding-id");
        });
    }

    @Test
    void the_same_id_on_two_protected_static_factories_one_hiding_the_other_fails_startup() {
        runner.withBean(ProtectedHidingWithSameId.class, ProtectedHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_protected_static_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        runner.withBean(ProtectedHidingWithOtherId.class, ProtectedHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("ProtectedBase", "ProtectedHidingWithOtherId");
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("base-id", "hiding-id");
        });
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    private static SnapshotView<TestState, TestEvent> view() {
        return SnapshotView.<TestState, TestEvent>builder(new TestState())
                .on(TestEvent.class, (state, event) -> state)
                .build();
    }

    static class PublicBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicBase");
            return view();
        }
    }

    static class PublicHidingWithSameId extends PublicBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithSameId");
            return view();
        }
    }

    static class PublicHidingWithOtherId extends PublicBase {
        @Snapshot(id = "hiding-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithOtherId");
            return view();
        }
    }

    static class ProtectedBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedBase");
            return view();
        }
    }

    static class ProtectedHidingWithSameId extends ProtectedBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedHidingWithSameId");
            return view();
        }
    }

    static class ProtectedHidingWithOtherId extends ProtectedBase {
        @Snapshot(id = "hiding-id", startAt = StartPosition.NOW)
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedHidingWithOtherId");
            return view();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        ReactiveSnapshotStore<TestState> snapshotStore() {
            return mock(ReactiveSnapshotStore.class);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription subscription = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
            when(subscriptions.subscribe(any(String.class), any(AgnosticSubscriptionFilter.class),
                    any(StartAt.class), any(Function2.class))).thenReturn(subscription);
            return subscriptions;
        }
    }

    static class TestEventConverter implements CloudEventConverter<TestEvent> {
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
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
