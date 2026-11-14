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
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.SingletonTargetSource;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.SmartFactoryBean;
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

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockingDetails;

/**
 * A descriptor factory ({@code @Projection}, {@code @Snapshot} or {@code @Saga} on a no-argument method) declared on
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
    private static final AtomicInteger SAGA_FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void a_projection_factory_declared_on_a_superclass_runs_for_a_bean_that_inherits_it() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
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
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotInfrastructureConfiguration.class, InheritedSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(SNAPSHOT_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(mockingDetails(context.getBean(Subscriptions.class)).getInvocations())
                            .anyMatch(invocation -> invocation.getMethod().getName().equals("subscribe") && "inherited-snapshot".equals(invocation.getRawArguments()[0]));
                });
    }

    @Test
    void a_saga_factory_declared_on_a_superclass_runs_and_registers_for_a_bean_that_inherits_it() {
        SAGA_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaInfrastructureConfiguration.class, InheritedSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(SAGA_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("inherited-saga"));
                });
    }

    // ------------------------------------------------------------------------------------------------------
    // The state type of an inherited generic factory is read against the bean's class
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_projection_factory_inherited_from_a_generic_superclass_gives_the_default_store_the_state_type_the_bean_binds() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionFeedConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(BoundProjectionFactory.class, BoundProjectionFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RecordingProjectionStoreProvider.class).stateTypes()).containsExactly(ViewState.class);
                });
    }

    @Test
    void a_snapshot_factory_inherited_from_a_generic_superclass_gives_the_default_store_the_state_type_the_bean_binds() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotCollaboratorsConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(BoundSnapshotFactory.class, BoundSnapshotFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RecordingSnapshotStoreProvider.class).stateTypes()).containsExactly(ViewState.class);
                });
    }

    @Test
    void a_saga_factory_inherited_from_a_generic_superclass_gives_the_default_store_the_state_type_at_index_one() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaCollaboratorsConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(BoundSagaFactory.class, BoundSagaFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RecordingSagaStateStoreProvider.class).stateTypes()).containsExactly(OrderState.class);
                });
    }

    @Test
    void a_snapshot_factory_inherited_from_a_generic_superclass_behind_a_jdk_proxy_over_a_fixed_target_gives_the_default_store_the_state_type_the_bean_binds() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotCollaboratorsConfiguration.class, RecordingProviderConfiguration.class)
                .withBean("snapshotFactory", UnwrappedFactoryMarker.class, () -> jdkProxyOverFixedTarget(new BoundSnapshotFactoryBehindProxy(), UnwrappedFactoryMarker.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Proxy.isProxyClass(context.getBean("snapshotFactory").getClass())).describedAs("the bean is a JDK proxy").isTrue();
                    assertThat(context.getBean(RecordingSnapshotStoreProvider.class).stateTypes()).containsExactly(ViewState.class);
                });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("projectionPathsThatReadTheStateType")
    void a_projection_factory_inherited_from_a_generic_superclass_gives_the_default_store_the_state_type_the_bean_binds_on_every_path(String path, Class<?> collaborators, Class<?> factoryClass) {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(collaborators, RecordingProviderConfiguration.class)
                .withBean(factoryClass)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RecordingProjectionStoreProvider.class).stateTypes()).containsExactly(ViewState.class);
                });
    }

    static Stream<Arguments> projectionPathsThatReadTheStateType() {
        return Stream.of(
                Arguments.of("push subscription model", PushModelCollaboratorsConfiguration.class, BoundPushModelProjectionFactory.class),
                Arguments.of("dcb projection", DcbCollaboratorsConfiguration.class, BoundDcbProjectionFactory.class),
                Arguments.of("polled projection", PolledCollaboratorsConfiguration.class, BoundPolledProjectionFactory.class));
    }

    @Test
    void a_projection_factory_inherited_through_a_raw_supertype_keeps_the_state_type_error() {
        assertProjectionStateTypeRefused(RawProjectionFactory.class);
    }

    @Test
    void a_projection_factory_whose_state_type_the_bean_leaves_unbound_keeps_the_state_type_error() {
        assertProjectionStateTypeRefused(UnboundProjectionFactory.class);
    }

    @Test
    void a_projection_factory_whose_state_type_is_a_method_level_type_variable_keeps_the_state_type_error() {
        assertProjectionStateTypeRefused(MethodLevelProjectionFactory.class);
    }

    private static void assertProjectionStateTypeRefused(Class<?> factoryClass) {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionFeedConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(factoryClass)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("@Projection 'generic-projection'")
                            .hasMessageContaining("with a concrete state type");
                });
    }

    @Test
    void a_snapshot_factory_inherited_through_a_raw_supertype_keeps_the_state_type_error() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotCollaboratorsConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(RawSnapshotFactory.class, RawSnapshotFactory::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("@Snapshot 'generic-snapshot'")
                            .hasMessageContaining("with a concrete state type");
                });
    }

    @Test
    void a_saga_factory_inherited_through_a_raw_supertype_keeps_the_state_type_error() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaCollaboratorsConfiguration.class, RecordingProviderConfiguration.class)
                .withBean(RawSagaFactory.class, RawSagaFactory::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("@Saga 'generic-saga'")
                            .hasMessageContaining("with a concrete state type");
                });
    }

    record ViewState() {
    }

    abstract static class GenericProjectionFactory<S> {
        private final S initial;

        GenericProjectionFactory(S initial) {
            this.initial = initial;
        }

        @Projection(id = "generic-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<S, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<S, TestEvent, String>builder(initial)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class BoundProjectionFactory extends GenericProjectionFactory<ViewState> {
        BoundProjectionFactory() {
            super(new ViewState());
        }
    }

    @SuppressWarnings("rawtypes")
    static class RawProjectionFactory extends GenericProjectionFactory {
        RawProjectionFactory() {
            super(null);
        }
    }

    static class UnboundProjectionFactory<T> extends GenericProjectionFactory<T> {
        UnboundProjectionFactory() {
            super(null);
        }
    }

    static class MethodLevelProjectionFactory {
        @Projection(id = "generic-projection", source = Source.PUSH)
        @SuppressWarnings({"unchecked", "rawtypes"})
        <S extends ViewState> org.occurrent.dsl.projection.Projection<S, TestEvent, String> projection() {
            return (org.occurrent.dsl.projection.Projection) org.occurrent.dsl.projection.Projection.<ViewState, TestEvent, String>builder(new ViewState())
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    abstract static class GenericPushModelProjectionFactory<S> {
        private final S initial;

        GenericPushModelProjectionFactory(S initial) {
            this.initial = initial;
        }

        @Projection(id = "generic-push-model-projection", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<S, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<S, TestEvent, String>builder(initial)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class BoundPushModelProjectionFactory extends GenericPushModelProjectionFactory<ViewState> {
        BoundPushModelProjectionFactory() {
            super(new ViewState());
        }
    }

    abstract static class GenericDcbProjectionFactory<S> {
        private final S initial;

        GenericDcbProjectionFactory(S initial) {
            this.initial = initial;
        }

        @Projection(id = "generic-dcb-projection")
        DcbProjection<S, TestEvent, String> projection() {
            return new DcbProjection<>(org.occurrent.dsl.projection.Projection.<S, TestEvent, String>builder(initial)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build(), DcbCriteria.all());
        }
    }

    static class BoundDcbProjectionFactory extends GenericDcbProjectionFactory<ViewState> {
        BoundDcbProjectionFactory() {
            super(new ViewState());
        }
    }

    abstract static class GenericPolledProjectionFactory<S> {
        private final S initial;

        GenericPolledProjectionFactory(S initial) {
            this.initial = initial;
        }

        @Projection(id = "generic-polled-projection")
        org.occurrent.dsl.projection.Projection<S, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<S, TestEvent, String>builder(initial)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class BoundPolledProjectionFactory extends GenericPolledProjectionFactory<ViewState> {
        BoundPolledProjectionFactory() {
            super(new ViewState());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PushModelCollaboratorsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        PushSubscriptionModel pushSubscriptionModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PolledCollaboratorsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        Subscriptions<TestEvent> subscriptions(CloudEventConverter<TestEvent> converter) {
            Subscribable model = mock(Subscribable.class);
            when(model.subscribe(anyString(), any(), any(org.occurrent.subscription.StartAt.class), any())).thenReturn(mock(Subscription.class));
            return new Subscriptions<>(model, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DcbCollaboratorsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        DcbSubscriptions<TestEvent> dcbSubscriptions(CloudEventConverter<TestEvent> converter) {
            SubscriptionModel model = mock(SubscriptionModel.class);
            when(model.subscribe(anyString(), any(), any(org.occurrent.subscription.StartAt.class), any())).thenReturn(mock(Subscription.class));
            return new DcbSubscriptions<>(model, converter);
        }
    }

    interface UnwrappedFactoryMarker {
    }

    static class BoundSnapshotFactoryBehindProxy extends GenericSnapshotFactory<ViewState> implements UnwrappedFactoryMarker {
        BoundSnapshotFactoryBehindProxy() {
            super(new ViewState());
        }
    }

    private static <I> I jdkProxyOverFixedTarget(Object target, Class<I> proxiedInterface) {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetSource(new SingletonTargetSource(target));
        proxyFactory.setInterfaces(proxiedInterface);
        return proxiedInterface.cast(proxyFactory.getProxy());
    }

    abstract static class GenericSnapshotFactory<S> {
        private final S initial;

        GenericSnapshotFactory(S initial) {
            this.initial = initial;
        }

        @Snapshot(id = "generic-snapshot")
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

    abstract static class GenericSagaFactory<S> {
        @Saga(id = "generic-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, S, OrderCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, S, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .build();
        }
    }

    static class BoundSagaFactory extends GenericSagaFactory<OrderState> {
    }

    @SuppressWarnings("rawtypes")
    static class RawSagaFactory extends GenericSagaFactory {
    }

    // No store bean of any kind, so the factory's state type is what the zero-config default store is created for
    // and the provider records it.
    @Configuration(proxyBeanMethods = false)
    static class RecordingProviderConfiguration {
        @Bean
        RecordingProjectionStoreProvider projectionStoreProvider() {
            return new RecordingProjectionStoreProvider(new CopyOnWriteArrayList<>());
        }

        @Bean
        RecordingSnapshotStoreProvider snapshotStoreProvider() {
            return new RecordingSnapshotStoreProvider(new CopyOnWriteArrayList<>());
        }

        @Bean
        RecordingSagaStateStoreProvider sagaStateStoreProvider() {
            return new RecordingSagaStateStoreProvider(new CopyOnWriteArrayList<>());
        }
    }

    record RecordingProjectionStoreProvider(List<Class<?>> stateTypes) implements DefaultProjectionStoreProvider {
        @Override
        public <S, ID> ViewStateRepository<S, ID> createDefaultProjectionStore(String projectionId, Class<S> stateType) {
            stateTypes.add(stateType);
            Map<ID, S> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }
    }

    record RecordingSnapshotStoreProvider(List<Class<?>> stateTypes) implements DefaultSnapshotStoreProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <S> SnapshotStore<S> createDefaultSnapshotStore(String snapshotId, Class<S> stateType) {
            stateTypes.add(stateType);
            return mock(SnapshotStore.class);
        }
    }

    record RecordingSagaStateStoreProvider(List<Class<?>> stateTypes) implements DefaultSagaStateStoreProvider {
        @Override
        public <S> SagaStateStore<S> createDefaultSagaStateStore(String sagaId, Class<S> stateType) {
            stateTypes.add(stateType);
            return SagaStateStore.inMemory();
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
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withBean(NonFixedTargetJdkProxyingPostProcessor.class, NonFixedTargetJdkProxyingPostProcessor::new)
                .withUserConfiguration(SnapshotInfrastructureConfiguration.class)
                .withBean("snapshotFactory", InterfaceSnapshotFactoryImpl.class, InterfaceSnapshotFactoryImpl::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertSoftly(softly -> {
                        softly.assertThat(Proxy.isProxyClass(context.getBean("snapshotFactory").getClass())).describedAs("the bean is a JDK proxy").isTrue();
                        softly.assertThat(INTERFACE_SNAPSHOT_FACTORY_INVOCATIONS).hasValue(1);
                        softly.assertThat(INTERFACE_SNAPSHOT_FACTORY_ADVICE_CALLS).describedAs("the factory ran through the proxy").hasValue(1);
                        softly.assertThat(mockingDetails(context.getBean(Subscriptions.class)).getInvocations())
                                .anyMatch(invocation -> invocation.getMethod().getName().equals("subscribe") && "interface-declared-snapshot".equals(invocation.getRawArguments()[0]));
                    });
                });
    }

    interface InterfaceSnapshotFactory {
        @Snapshot(id = "interface-declared-snapshot")
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
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, JdkProxyProductConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(INTERFACE_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(INTERFACE_FACTORY_ADVICE_CALLS).describedAs("the factory ran through the proxy").hasValue(1);
                });
    }

    @Test
    void a_class_declared_projection_factory_behind_a_jdk_proxy_that_does_not_implement_the_declaring_class_is_not_found_and_never_runs() {
        UNIMPLEMENTED_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, UnimplementedFactoryProductConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(UNIMPLEMENTED_FACTORY_INVOCATIONS).describedAs("the factory is not found, so it never ran").hasValue(0);
                });
    }

    @Test
    void an_interface_declared_projection_factory_on_a_prototype_that_switches_to_an_overriding_implementation_is_refused() {
        INTERFACE_FACTORY_INVOCATIONS.set(0);
        OVERRIDING_INTERFACE_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
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

    private static final AtomicInteger UNIMPLEMENTED_FACTORY_INVOCATIONS = new AtomicInteger();

    interface UnrelatedInterface {
        void other();
    }

    // Declares the factory on the class only, so the proxy over UnrelatedInterface has no method that runs it
    static class UnimplementedFactoryImpl implements UnrelatedInterface {
        @Override
        public void other() {
        }

        @Projection(id = "class-declared-projection", source = Source.PUSH)
        public org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            UNIMPLEMENTED_FACTORY_INVOCATIONS.incrementAndGet();
            return countingProjection();
        }
    }

    // Eager, since the interface declares nothing for the startup scan to decide to build the product from
    static class UnimplementedFactoryProductFactoryBean implements SmartFactoryBean<UnrelatedInterface> {
        @Override
        public boolean isEagerInit() {
            return true;
        }

        @Override
        public UnrelatedInterface getObject() {
            UnimplementedFactoryImpl target = new UnimplementedFactoryImpl();
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.addInterface(UnrelatedInterface.class);
            proxyFactory.setTargetSource(new TargetSource() {
                @Override
                public Class<?> getTargetClass() {
                    return UnimplementedFactoryImpl.class;
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
            return (UnrelatedInterface) proxyFactory.getProxy();
        }

        @Override
        public Class<?> getObjectType() {
            return UnrelatedInterface.class;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnimplementedFactoryProductConfiguration {
        @Bean
        UnimplementedFactoryProductFactoryBean unimplementedFactory() {
            return new UnimplementedFactoryProductFactoryBean();
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
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
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
        @Projection(id = "inherited-projection", source = Source.PUSH)
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
        @Snapshot(id = "inherited-snapshot")
        SnapshotView<TestState, TestEvent> snapshot() {
            SNAPSHOT_FACTORY_INVOCATIONS.incrementAndGet();
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class InheritingSnapshotFactory extends BaseSnapshotFactory {
    }

    abstract static class BaseSagaFactory {
        @Saga(id = "inherited-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            SAGA_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                    .build();
        }
    }

    static class InheritingSagaFactory extends BaseSagaFactory {
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
    static class InheritedSagaConfiguration {
        @Bean
        InheritingSagaFactory sagaFactory() {
            return new InheritingSagaFactory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(ProjectionFeedConfiguration.class)
    static class ProjectionInfrastructureConfiguration {
        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionFeedConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(emptyReader, converter, event -> "k");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(SnapshotCollaboratorsConfiguration.class)
    static class SnapshotInfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotCollaboratorsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
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
            return mock(Subscriptions.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(SagaCollaboratorsConfiguration.class)
    static class SagaInfrastructureConfiguration {
        @Bean
        SagaStateStore<OrderState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SagaCollaboratorsConfiguration {
        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                    return CloudEventBuilder.v1()
                            .withId("id")
                            .withSource(URI.create("urn:test"))
                            .withType(domainEvent.getClass().getSimpleName())
                            .withSubject(domainEvent.orderId())
                            .build();
                }

                @Override
                public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new OrderPlaced(cloudEvent.getSubject());
                }

                @Override
                public String getCloudEventType(Class<? extends OrderEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
        }

        @Bean
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        CommandDispatcher<OrderCommand> commandDispatcher() {
            List<OrderCommand> issued = new CopyOnWriteArrayList<>();
            return issued::add;
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

    sealed interface OrderEvent {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record OrderState(String orderId) {
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
