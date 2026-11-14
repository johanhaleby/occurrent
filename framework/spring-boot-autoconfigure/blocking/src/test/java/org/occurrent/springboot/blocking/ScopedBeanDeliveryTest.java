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
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.support.ScopeNotActiveException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.NestedExceptionUtils;

import java.util.ArrayList;
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
    void a_scoped_proxy_in_a_scope_that_is_inactive_at_startup_fails_startup_naming_the_bean_and_its_scoped_target() {
        scope.deactivate();
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(causeChain(context.getStartupFailure()))
                    .filteredOn(cause -> cause instanceof IllegalStateException && String.valueOf(cause.getMessage()).startsWith("Bean 'targetClassHandler' has a scoped proxy"))
                    .singleElement()
                    .satisfies(cause -> assertThat(cause)
                            .hasMessageStartingWith("Bean 'targetClassHandler' has a scoped proxy, and the scan asks for its scoped target 'scopedTarget.targetClassHandler' at startup to read its Occurrent handlers, but the target's scope isn't active then.")
                            .hasCauseInstanceOf(ScopeNotActiveException.class));
        });
    }

    @Test
    void a_handler_only_the_built_class_declares_is_registered_and_delivered_through_the_proxy_when_the_target_was_built_before_the_scan() {
        runner.withUserConfiguration(EarlyBuiltConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(subscribedIds(subscriptions)).containsExactly("early-built-handler");
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "early-built-handler");

            handler.invoke(null, new TestEvent());

            assertThat(context.getBean(Deliveries.class).ranOn()).containsExactly("instance-1");
        });
    }

    @Test
    void a_prototype_scoped_proxy_builds_exactly_one_target_during_startup_and_a_new_one_for_each_delivery() {
        runner.withUserConfiguration(PrototypeScopedProxyConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(InstanceCounter.class).count()).hasValue(1);
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(subscribedIds(subscriptions)).containsExactly("scoped-prototype-handler");
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "scoped-prototype-handler");

            handler.invoke(null, new TestEvent());
            handler.invoke(null, new TestEvent());

            assertThat(context.getBean(Deliveries.class).ranOn()).containsExactly("instance-2", "instance-3");
        });
    }

    @Test
    void a_lazy_scoped_proxy_whose_target_class_cannot_be_predicted_registers_its_handler_from_the_target_the_scan_asks_for() {
        List<String> ranOn = new CopyOnWriteArrayList<>();
        List<Class<?>> predictedTargetTypes = new ArrayList<>();
        runner.withInitializer(context -> {
            BeanDefinitionRegistry registry = (BeanDefinitionRegistry) context;
            RootBeanDefinition target = new RootBeanDefinition();
            target.setScope(RefreshableScope.NAME);
            target.setInstanceSupplier(() -> new SuppliedHandler("supplied", ranOn));
            BeanDefinitionHolder proxy = ScopedProxyUtils.createScopedProxy(new BeanDefinitionHolder(target, "suppliedHandler"), registry, true);
            ((AbstractBeanDefinition) proxy.getBeanDefinition()).setLazyInit(true);
            registry.registerBeanDefinition(proxy.getBeanName(), proxy.getBeanDefinition());
            context.addBeanFactoryPostProcessor(beanFactory -> predictedTargetTypes.add(beanFactory.getType(ScopedProxyUtils.getTargetBeanName("suppliedHandler"))));
        }).run(context -> {
            assertThat(predictedTargetTypes).hasSize(1).containsOnlyNulls();
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(subscribedIds(subscriptions)).containsExactly("supplied-handler");
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "supplied-handler");

            handler.invoke(null, new TestEvent());

            assertThat(ranOn).containsExactly("supplied");
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

    @Test
    void a_scoped_proxy_declared_as_an_unannotated_interface_registers_the_handler_its_class_declares_once_a_call_through_the_proxy_builds_the_target() {
        runner.withUserConfiguration(UnannotatedInterfaceConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(InstanceCounter.class).count()).hasValue(0);
            UnannotatedPort proxy = context.getBean(UnannotatedPort.class);

            assertThat(proxy.name()).isEqualTo("instance-1");
            assertThat(proxy.name()).isEqualTo("instance-2");

            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(subscribedIds(subscriptions)).containsExactly("unannotated-interface-handler");
            Function2<EventMetadata, TestEvent, Unit> handler = handlerFor(subscriptions, "unannotated-interface-handler");
            handler.invoke(null, new TestEvent());
            assertThat(context.getBean(Deliveries.class).ranOn()).hasSize(1);
        });
    }

    private static List<Throwable> causeChain(Throwable failure) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable cause = failure; cause != null && !chain.contains(cause); cause = cause.getCause()) {
            chain.add(cause);
        }
        return chain;
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

    interface EarlyBuiltPort {
        void on(TestEvent event);

        String name();
    }

    static class EarlyBuiltHandler implements EarlyBuiltPort {
        private final String name;
        private final Deliveries deliveries;

        EarlyBuiltHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Override
        @Subscription(id = "early-built-handler")
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }

        @Override
        public String name() {
            return name;
        }
    }

    static class EarlyBuiltCaller {
        EarlyBuiltCaller(EarlyBuiltPort port) {
            port.name();
        }
    }

    public static class SuppliedHandler {
        private final String name;
        private final List<String> ranOn;

        SuppliedHandler(String name, List<String> ranOn) {
            this.name = name;
            this.ranOn = ranOn;
        }

        @Subscription(id = "supplied-handler")
        public void on(TestEvent event) {
            ranOn.add(name);
        }
    }

    public static class ScopedPrototypeHandler {
        private final String name;
        private final Deliveries deliveries;

        ScopedPrototypeHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Subscription(id = "scoped-prototype-handler")
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }
    }

    interface UnannotatedPort {
        void on(TestEvent event);

        String name();
    }

    static class UnannotatedPortHandler implements UnannotatedPort {
        private final String name;
        private final Deliveries deliveries;

        UnannotatedPortHandler(String name, Deliveries deliveries) {
            this.name = name;
            this.deliveries = deliveries;
        }

        @Override
        @Subscription(id = "unannotated-interface-handler")
        public void on(TestEvent event) {
            deliveries.ranOn().add(name);
        }

        @Override
        public String name() {
            return name;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnannotatedInterfaceConfiguration {
        @Bean
        @Scope(value = "prototype", proxyMode = ScopedProxyMode.INTERFACES)
        UnannotatedPort unannotatedPortHandler(InstanceCounter counter, Deliveries deliveries) {
            return new UnannotatedPortHandler(counter.nextName(), deliveries);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EarlyBuiltConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.TARGET_CLASS)
        EarlyBuiltPort earlyBuiltHandler(InstanceCounter counter, Deliveries deliveries) {
            return new EarlyBuiltHandler(counter.nextName(), deliveries);
        }

        @Bean
        EarlyBuiltCaller earlyBuiltCaller(EarlyBuiltPort port) {
            return new EarlyBuiltCaller(port);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeScopedProxyConfiguration {
        @Bean
        @Scope(value = "prototype", proxyMode = ScopedProxyMode.TARGET_CLASS)
        ScopedPrototypeHandler scopedPrototypeHandler(InstanceCounter counter, Deliveries deliveries) {
            return new ScopedPrototypeHandler(counter.nextName(), deliveries);
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
