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
import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.NestedExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A bean in a scope that hands out a new instance later, such as a refresh scope, is delivered every event through
 * its scoped proxy, so each event reaches the instance the scope holds at that moment (issue #1492). The handlers
 * register once, under the proxy's name, from the instance the scope held when the scan read it. Reproduced without a
 * running store (no Docker), reading the ids and handlers off a mocked {@link Subscriptions}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ScopedBeanDeliveryTest {

    private final RefreshableScope scope = new RefreshableScope();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class)
            .withInitializer(scope::registerIn);

    @Test
    void a_target_class_scoped_proxy_delivers_each_event_to_the_instance_its_scope_holds_then_and_registers_the_id_once() {
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "target-class-handler");

            handler.invoke(null, new TestEvent());
            scope.refresh();
            handler.invoke(null, new TestEvent());

            assertThat(context.getBean(Deliveries.class).ranOn()).containsExactly("instance-1", "instance-2");
            assertThat(subscribedIds(subscriptions)).containsExactly("target-class-handler");
        });
    }

    @Test
    void an_interfaces_scoped_proxy_delivers_a_handler_the_interface_declares_to_the_instance_its_scope_holds_then() {
        runner.withUserConfiguration(InterfacesConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "interface-declared-handler");

            handler.invoke(null, new TestEvent());
            scope.refresh();
            handler.invoke(null, new TestEvent());

            assertThat(context.getBean(Deliveries.class).ranOn()).containsExactly("instance-1", "instance-2");
            assertThat(subscribedIds(subscriptions)).containsExactly("interface-declared-handler");
        });
    }

    @Test
    void an_interfaces_scoped_proxy_fails_startup_for_a_handler_only_the_class_declares_because_the_proxy_does_not_implement_it() {
        runner.withUserConfiguration(InterfacesClassOnlyHandlerConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining(ClassOnlyHandler.class.getName() + ".on")
                    .hasMessageContaining("The proxy does not implement it");
        });
    }

    @Test
    void a_scoped_proxy_in_a_scope_that_is_inactive_at_startup_fails_startup_when_its_class_has_a_handler() {
        scope.deactivate();
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Scope refreshable is not active");
        });
    }

    @Test
    void a_scoped_bean_without_an_occurrent_annotation_in_a_scope_that_is_inactive_at_startup_does_not_fail_startup() {
        scope.deactivate();
        runner.withUserConfiguration(UnannotatedScopedConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
        });
    }

    @Test
    void a_scope_creating_a_new_target_after_startup_does_not_register_the_handler_a_second_time() {
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "target-class-handler");

            scope.refresh();
            assertThatCode(() -> handler.invoke(null, new TestEvent())).doesNotThrowAnyException();
            scope.refresh();
            assertThatCode(() -> handler.invoke(null, new TestEvent())).doesNotThrowAnyException();

            assertThat(context).hasNotFailed();
            assertThat(subscribedIds(subscriptions)).containsExactly("target-class-handler");
            assertThat(context.getBean(Deliveries.class).ranOn()).containsExactly("instance-2", "instance-3");
        });
    }

    @Test
    void a_prototype_bean_without_a_scoped_proxy_runs_every_event_on_the_one_instance_the_scan_built() {
        runner.withUserConfiguration(PrototypeWithoutProxyConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "prototype-handler");
            int instancesAfterStartup = context.getBean(InstanceCounter.class).count().get();

            handler.invoke(null, new TestEvent());
            handler.invoke(null, new TestEvent());

            List<String> ranOn = context.getBean(Deliveries.class).ranOn();
            assertThat(ranOn).hasSize(2).containsOnly(ranOn.get(0));
            assertThat(context.getBean(InstanceCounter.class).count()).hasValue(instancesAfterStartup);
        });
    }

    @Test
    void a_delivery_after_the_scope_went_inactive_fails_instead_of_running_on_a_stale_instance() {
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(context.getBean(Subscriptions.class), "target-class-handler");

            scope.deactivate();

            assertThatThrownBy(() -> handler.invoke(null, new TestEvent()))
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Scope refreshable is not active");
            assertThat(context.getBean(Deliveries.class).ranOn()).isEmpty();
        });
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Function2<EventMetadata, TestEvent, Unit> handlerFor(Subscriptions<?> subscriptions, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe") && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> (Function2<EventMetadata, TestEvent, Unit>) invocation.getRawArguments()[invocation.getRawArguments().length - 1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No handler was subscribed under id " + id));
    }

    record TestEvent() {
    }

    record Deliveries(List<String> ranOn) {
    }

    record InstanceCounter(AtomicInteger count) {
        String nextName() {
            return "instance-" + count.incrementAndGet();
        }
    }

    public static class TargetClassHandler {
        private final String name;
        private final Deliveries deliveries;

        TargetClassHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Subscription(id = "target-class-handler")
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }
    }

    interface InterfaceDeclaredPort {
        @Subscription(id = "interface-declared-handler")
        void on(TestEvent event);
    }

    static class InterfaceDeclaredHandler implements InterfaceDeclaredPort {
        private final String name;
        private final Deliveries deliveries;

        InterfaceDeclaredHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Override
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }
    }

    interface PlainMarker {
    }

    static class ClassOnlyHandler implements PlainMarker {
        @Subscription(id = "class-only-handler")
        public void on(TestEvent event) {
        }
    }

    public static class UnannotatedScopedBean {
    }

    public static class PrototypeHandler {
        private final String name;
        private final Deliveries deliveries;

        PrototypeHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Subscription(id = "prototype-handler")
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TargetClassConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.TARGET_CLASS)
        TargetClassHandler targetClassHandler(InstanceCounter counter, Deliveries deliveries) {
            return new TargetClassHandler(counter.nextName(), deliveries);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        InterfaceDeclaredHandler interfaceDeclaredHandler(InstanceCounter counter, Deliveries deliveries) {
            return new InterfaceDeclaredHandler(counter.nextName(), deliveries);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesClassOnlyHandlerConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        ClassOnlyHandler classOnlyHandler() {
            return new ClassOnlyHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnannotatedScopedConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.TARGET_CLASS)
        UnannotatedScopedBean unannotatedScopedBean() {
            return new UnannotatedScopedBean();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeWithoutProxyConfiguration {
        @Bean
        @Scope("prototype")
        PrototypeHandler prototypeHandler(InstanceCounter counter, Deliveries deliveries) {
            return new PrototypeHandler(counter.nextName(), deliveries);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return null;
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return null;
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        @Primary
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> synchronousSubscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        Deliveries deliveries() {
            return new Deliveries(new CopyOnWriteArrayList<>());
        }

        @Bean
        InstanceCounter instanceCounter() {
            return new InstanceCounter(new AtomicInteger());
        }
    }
}
