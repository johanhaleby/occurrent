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
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.core.PriorityOrdered;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * A descriptor factory ({@code @Projection} or {@code @Snapshot} on a no-argument method) declared on
 * a superclass and inherited unchanged by the bean's class runs and registers, the way one the class declares itself
 * does (issue #983). {@link SubscriptionAnnotations#invokeDescriptorFactory} refuses a target that overrides the
 * method or is not the scanned class at all, which {@code DescriptorFactorySwitchingPrototypeTest} and
 * {@code SubscriptionAnnotationsTest} cover. Reproduced without a running store (no Docker), each with the
 * container-free setup the corresponding {@code ...JdkProxyTest} uses.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InheritedDescriptorFactoryTest {

    private static final AtomicInteger PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger SNAPSHOT_FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void a_projection_factory_declared_on_a_superclass_runs_for_a_bean_that_inherits_it() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, InheritedProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(PROJECTION_FACTORY_INVOCATIONS).hasValue(1);
                });
    }

    @Test
    void a_snapshot_factory_declared_on_a_superclass_runs_and_subscribes_for_a_bean_that_inherits_it() {
        SNAPSHOT_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotInfrastructureConfiguration.class, InheritedSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(SNAPSHOT_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(mockingDetails(context.getBean(Subscriptions.class)).getInvocations())
                            .anyMatch(invocation -> invocation.getMethod().getName().equals("subscribe") && "inherited-snapshot".equals(invocation.getRawArguments()[0]));
                });
    }

    // ------------------------------------------------------------------------------------------------------
    // The state type of an inherited generic factory is read against the bean's class
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_snapshot_factory_inherited_from_a_generic_superclass_gives_the_default_store_the_state_type_the_bean_binds() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotCollaboratorsConfiguration.class)
                .withBean(RecordingSnapshotStoreProvider.class, () -> new RecordingSnapshotStoreProvider(new CopyOnWriteArrayList<>()))
                .withBean(BoundSnapshotFactory.class, BoundSnapshotFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RecordingSnapshotStoreProvider.class).stateTypes()).containsExactly(ViewState.class);
                });
    }

    @Test
    void a_snapshot_factory_inherited_through_a_raw_supertype_keeps_the_state_type_error() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotCollaboratorsConfiguration.class)
                .withBean(RecordingSnapshotStoreProvider.class, () -> new RecordingSnapshotStoreProvider(new CopyOnWriteArrayList<>()))
                .withBean(RawSnapshotFactory.class, RawSnapshotFactory::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("@Snapshot 'generic-snapshot'")
                            .hasMessageContaining("with a concrete state type");
                });
    }

    record ViewState() {
    }

    abstract static class GenericSnapshotFactory<S> {
        private final S initial;

        GenericSnapshotFactory(S initial) {
            this.initial = initial;
        }

        @Snapshot(id = "generic-snapshot", startAt = StartPosition.NOW)
        SnapshotView<S, TestEvent> snapshot() {
            return SnapshotView.<S, TestEvent>builder(initial)
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class BoundSnapshotFactory extends GenericSnapshotFactory<ViewState> {
        BoundSnapshotFactory() {
            super(new ViewState());
        }
    }

    @SuppressWarnings("rawtypes")
    static class RawSnapshotFactory extends GenericSnapshotFactory {
        RawSnapshotFactory() {
            super(null);
        }
    }

    // No store bean of any kind, so the factory's state type is what the zero-config default store is created for
    // and the provider records it.
    record RecordingSnapshotStoreProvider(List<Class<?>> stateTypes) implements DefaultReactiveSnapshotStoreProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <S> ReactiveSnapshotStore<S> createDefaultSnapshotStore(String snapshotId, Class<S> stateType) {
            stateTypes.add(stateType);
            return mock(ReactiveSnapshotStore.class);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // A factory the bean's class declares for an interface, behind a JDK proxy a post-processor adds
    // ------------------------------------------------------------------------------------------------------

    private static final AtomicInteger INTERFACE_SNAPSHOT_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger INTERFACE_SNAPSHOT_FACTORY_ADVICE_CALLS = new AtomicInteger();

    @Test
    void a_snapshot_factory_the_beans_class_declares_for_an_interface_runs_and_subscribes_through_a_jdk_proxy_a_post_processor_adds_over_a_target_source_that_is_not_fixed() {
        INTERFACE_SNAPSHOT_FACTORY_INVOCATIONS.set(0);
        INTERFACE_SNAPSHOT_FACTORY_ADVICE_CALLS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withBean(NonFixedTargetJdkProxyingPostProcessor.class, NonFixedTargetJdkProxyingPostProcessor::new)
                .withUserConfiguration(SnapshotInfrastructureConfiguration.class)
                .withBean("snapshotFactory", InterfaceSnapshotFactoryImpl.class, InterfaceSnapshotFactoryImpl::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Proxy.isProxyClass(context.getBean("snapshotFactory").getClass())).describedAs("the bean is a JDK proxy").isTrue();
                    assertThat(INTERFACE_SNAPSHOT_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(INTERFACE_SNAPSHOT_FACTORY_ADVICE_CALLS).describedAs("the factory ran through the proxy").hasValue(1);
                    assertThat(mockingDetails(context.getBean(Subscriptions.class)).getInvocations())
                            .anyMatch(invocation -> invocation.getMethod().getName().equals("subscribe") && "interface-declared-snapshot".equals(invocation.getRawArguments()[0]));
                });
    }

    interface InterfaceSnapshotFactory {
        @Snapshot(id = "interface-declared-snapshot", startAt = StartPosition.NOW)
        SnapshotView<TestState, TestEvent> snapshot();
    }

    // Public, so invoking this class's method on the proxy fails on the receiver and not on the access check
    public static class InterfaceSnapshotFactoryImpl implements InterfaceSnapshotFactory {
        @Override
        public SnapshotView<TestState, TestEvent> snapshot() {
            INTERFACE_SNAPSHOT_FACTORY_INVOCATIONS.incrementAndGet();
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    // Wraps the factory bean in a JDK proxy of its interface over a target source that is not static, so nothing
    // unwraps the proxy to a target. The scan reads the class the bean had before this post-processor ran.
    static class NonFixedTargetJdkProxyingPostProcessor implements BeanPostProcessor, PriorityOrdered {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof InterfaceSnapshotFactoryImpl)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.addInterface(InterfaceSnapshotFactory.class);
            proxyFactory.setTargetSource(new TargetSource() {
                @Override
                public Class<?> getTargetClass() {
                    return bean.getClass();
                }

                @Override
                public boolean isStatic() {
                    return false;
                }

                @Override
                public Object getTarget() {
                    return bean;
                }

                @Override
                public void releaseTarget(Object target) {
                }
            });
            proxyFactory.addAdvice((MethodInterceptor) invocation -> {
                INTERFACE_SNAPSHOT_FACTORY_ADVICE_CALLS.incrementAndGet();
                return invocation.proceed();
            });
            return proxyFactory.getProxy();
        }

        @Override
        public int getOrder() {
            return HIGHEST_PRECEDENCE;
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // A factory declared on an interface, on a FactoryBean product that is a JDK proxy
    // ------------------------------------------------------------------------------------------------------

    private static final AtomicInteger INTERFACE_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger INTERFACE_FACTORY_ADVICE_CALLS = new AtomicInteger();
    private static final AtomicInteger OVERRIDING_INTERFACE_FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void an_interface_declared_projection_factory_runs_through_a_jdk_proxy_product_whose_target_source_is_not_fixed() {
        INTERFACE_FACTORY_INVOCATIONS.set(0);
        INTERFACE_FACTORY_ADVICE_CALLS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, JdkProxyProductConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(INTERFACE_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(INTERFACE_FACTORY_ADVICE_CALLS).describedAs("the factory ran through the proxy").hasValue(1);
                });
    }

    @Test
    void an_interface_declared_projection_factory_on_a_prototype_that_switches_to_an_overriding_implementation_is_refused() {
        INTERFACE_FACTORY_INVOCATIONS.set(0);
        OVERRIDING_INTERFACE_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, SwitchingJdkProxyPrototypeConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("@Projection factory")
                            .hasMessageContaining(OverridingInterfaceFactoryImpl.class.getName() + " does not run the factory the scan read from " + InterfaceFactoryImpl.class.getName());
                    assertThat(OVERRIDING_INTERFACE_FACTORY_INVOCATIONS).hasValue(0);
                });
    }

    interface InterfaceFactory {
        @Projection(id = "interface-declared-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection();
    }

    static class InterfaceFactoryImpl implements InterfaceFactory {
        @Override
        public org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            INTERFACE_FACTORY_INVOCATIONS.incrementAndGet();
            return countingProjection();
        }
    }

    static class OverridingInterfaceFactoryImpl extends InterfaceFactoryImpl {
        @Override
        public org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            OVERRIDING_INTERFACE_FACTORY_INVOCATIONS.incrementAndGet();
            return countingProjection();
        }
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countingProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    // Every product is a new proxy over a target source that is not static, so the proxy cannot be unwrapped to a
    // target and the factory has to run through the proxy.
    static class JdkProxyProductFactoryBean implements FactoryBean<InterfaceFactory> {
        @Override
        public InterfaceFactory getObject() {
            InterfaceFactoryImpl target = new InterfaceFactoryImpl();
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.addInterface(InterfaceFactory.class);
            proxyFactory.setTargetSource(new TargetSource() {
                @Override
                public Class<?> getTargetClass() {
                    return InterfaceFactoryImpl.class;
                }

                @Override
                public boolean isStatic() {
                    return false;
                }

                @Override
                public Object getTarget() {
                    return target;
                }

                @Override
                public void releaseTarget(Object released) {
                }
            });
            proxyFactory.addAdvice((MethodInterceptor) invocation -> {
                INTERFACE_FACTORY_ADVICE_CALLS.incrementAndGet();
                return invocation.proceed();
            });
            return (InterfaceFactory) proxyFactory.getProxy();
        }

        @Override
        public Class<?> getObjectType() {
            return InterfaceFactory.class;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class JdkProxyProductConfiguration {
        @Bean
        JdkProxyProductFactoryBean interfaceFactory() {
            return new JdkProxyProductFactoryBean();
        }
    }

    record InterfaceFactoryHolder(InterfaceFactory factory) {
    }

    // Same shape as DescriptorFactorySwitchingPrototypeTest, over a JDK proxy with a fixed target. A singleton asking
    // for the prototype during startup is what makes the scan record the first instance's class, and the registrar's
    // own lookup then gets the second.
    @Configuration(proxyBeanMethods = false)
    static class SwitchingJdkProxyPrototypeConfiguration {
        private final AtomicInteger instances = new AtomicInteger();

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        InterfaceFactory interfaceFactory() {
            InterfaceFactory target = instances.getAndIncrement() == 0 ? new InterfaceFactoryImpl() : new OverridingInterfaceFactoryImpl();
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setTarget(target);
            proxyFactory.setInterfaces(InterfaceFactory.class);
            proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
            return (InterfaceFactory) proxyFactory.getProxy();
        }

        @Bean
        InterfaceFactoryHolder interfaceFactoryHolder(InterfaceFactory factory) {
            return new InterfaceFactoryHolder(factory);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // A bean whose class overrides an inherited factory
    // ------------------------------------------------------------------------------------------------------

    @Test
    void an_inherited_projection_factory_on_a_prototype_that_switches_to_a_subclass_overriding_it_is_refused() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        OVERRIDING_PROJECTION_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, SwitchingToOverridingSubclassConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("@Projection factory")
                            .hasMessageContaining(OverridingProjectionFactory.class.getName() + " does not run the factory the scan read from " + BaseProjectionFactory.class.getName());
                    assertThat(OVERRIDING_PROJECTION_FACTORY_INVOCATIONS).hasValue(0);
                });
    }

    private static final AtomicInteger OVERRIDING_PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();

    static class OverridingProjectionFactory extends BaseProjectionFactory {
        @Override
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            OVERRIDING_PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return countingProjection();
        }
    }

    record ProjectionFactoryHolder(BaseProjectionFactory factory) {
    }

    @Configuration(proxyBeanMethods = false)
    static class SwitchingToOverridingSubclassConfiguration {
        private final AtomicInteger instances = new AtomicInteger();

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        BaseProjectionFactory projectionFactory() {
            BaseProjectionFactory target = instances.getAndIncrement() == 0 ? new InheritingProjectionFactory() : new OverridingProjectionFactory();
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setTarget(target);
            proxyFactory.setProxyTargetClass(true);
            proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
            return (BaseProjectionFactory) proxyFactory.getProxy();
        }

        @Bean
        ProjectionFactoryHolder projectionFactoryHolder(BaseProjectionFactory factory) {
            return new ProjectionFactoryHolder(factory);
        }
    }


    abstract static class BaseProjectionFactory {
        @Projection(id = "inherited-projection-reactive", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class InheritingProjectionFactory extends BaseProjectionFactory {
    }

    abstract static class BaseSnapshotFactory {
        @Snapshot(id = "inherited-snapshot", startAt = StartPosition.NOW)
        SnapshotView<TestState, TestEvent> snapshot() {
            SNAPSHOT_FACTORY_INVOCATIONS.incrementAndGet();
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class InheritingSnapshotFactory extends BaseSnapshotFactory {
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritedProjectionConfiguration {
        @Bean
        InheritingProjectionFactory projectionFactory() {
            return new InheritingProjectionFactory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritedSnapshotConfiguration {
        @Bean
        InheritingSnapshotFactory snapshotFactory() {
            return new InheritingSnapshotFactory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionInfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        // The reactive post processor skips annotation processing without a Subscribable bean, and a DomainEventFeed is not one.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.empty();
                }

                @Override
                public Mono<Long> currentPosition() {
                    return Mono.just(0L);
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(emptyReader, converter, event -> "k");
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> store.put(id, value));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(SnapshotCollaboratorsConfiguration.class)
    static class SnapshotInfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        ReactiveSnapshotStore<TestState> reactiveSnapshotStore() {
            return mock(ReactiveSnapshotStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotCollaboratorsConfiguration {
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

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
        }

        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription subscription = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
            when(subscriptions.subscribe(any(String.class), any(org.occurrent.subscription.AgnosticSubscriptionFilter.class),
                    any(org.occurrent.subscription.StartAt.class), any(kotlin.jvm.functions.Function2.class))).thenReturn(subscription);
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
