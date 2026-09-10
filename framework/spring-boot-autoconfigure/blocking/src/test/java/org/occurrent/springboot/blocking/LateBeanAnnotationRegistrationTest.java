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
import kotlin.jvm.functions.Function2;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * A bean the container has not built when the startup scan runs is read through a prediction rather than through its
 * class, because building it there would defeat {@code @Lazy} and {@code spring.main.lazy-initialization} alike, and
 * that prediction is the factory method's declared return type. An annotation only the concrete class declares is
 * invisible to it. These tests cover what happens instead. The container builds the bean later, building it is what
 * makes its class knowable, and the annotation registers then.
 * <p>
 * Container-free, like {@link SubscriptionAnnotationGuardTest} next to it. A mocked {@link Subscriptions} is all a
 * {@code @Subscription} needs to register, so nothing here needs Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class LateBeanAnnotationRegistrationTest {

    private static final AtomicInteger INSTANTIATIONS = new AtomicInteger();
    private static final AtomicInteger PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger INTERFACE_PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new);

    @BeforeEach
    void resetInstantiations() {
        INSTANTIATIONS.set(0);
    }

    // The case #981 describes, @Bean declared to return an interface, @Lazy so the bean does not exist when the
    // scan runs, and the handler on the concrete class the interface does not declare. The scan sees Marker and
    // nothing else, so nothing registers at startup, and the bean is left uncreated, which is what @Lazy asked for.
    // Asking for the bean is what builds it, and that is when the subscription registers.
    @Test
    void a_subscription_only_the_concrete_class_declares_registers_when_the_lazy_bean_is_created() {
        runner.withUserConfiguration(LazyInterfaceReturningConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("@Lazy is still honored by the scan").hasValue(0);
            verify(subscriptions, never()).subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));

            context.getBean("hiddenSubscriber");

            verify(subscriptions).subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Building the bean a second time cannot register the subscription a second time. The id is the durable
    // checkpoint key, so a duplicate registration is never a harmless repeat.
    @Test
    void a_lazy_beans_subscription_registers_once_however_often_the_bean_is_asked_for() {
        runner.withUserConfiguration(LazyInterfaceReturningConfiguration.class).run(context -> {
            context.getBean("hiddenSubscriber");
            context.getBean("hiddenSubscriber");

            verify(context.getBean(Subscriptions.class), times(1))
                    .subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // A SmartFactoryBean whose product is not eager exists at scan time while its product does not, and
    // getObjectType() answers with the interface for the same reason a factory method's return type does. The
    // product's own creation is what reveals the handler.
    @Test
    void a_subscription_only_a_factory_bean_product_declares_registers_when_the_product_is_created() {
        runner.withUserConfiguration(LazyFactoryBeanConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("the scan does not force the product") .hasValue(0);
            verify(subscriptions, never()).subscribe(eq("factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));

            context.getBean("hiddenProductFactory");

            verify(subscriptions).subscribe(eq("factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Registering a handler the prediction did see builds the bean, and building it records its class, so the scan
    // runs a second time over what it can now read. Without that second pass the handler the interface declares
    // would register and the one only the class declares would not, which is the worse half of the same defect. An
    // application would see one of its two subscriptions running and have no reason to suspect the other.
    @Test
    void a_handler_the_predicted_interface_hides_registers_alongside_one_it_declares() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, PartiallyVisibleConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            verify(subscriptions).subscribe(eq("declared-on-the-interface"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            verify(subscriptions).subscribe(eq("declared-on-the-class"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // A prototype passes through the same creation callback once per instance, and a subscription id is the durable
    // checkpoint key, so the second instance must not register it again. The one instance that did register stays
    // the handler's target too, because resolving a prototype by name per delivery would build a fresh bean for
    // every event, which is neither what the startup scan does for a prototype it can see nor anything a handler
    // could rely on.
    @Test
    void a_prototypes_hidden_subscription_registers_once_and_keeps_the_instance_that_registered_it() {
        runner.withUserConfiguration(PrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("hiddenPrototypeSubscriber");
            context.getBean("hiddenPrototypeSubscriber");

            verify(context.getBean(Subscriptions.class), times(1))
                    .subscribe(eq("prototype-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());
            assertThat(INSTANTIATIONS).hasValue(2);

            handler.getValue().invoke(null, new TestEvent());

            assertThat(INSTANTIATIONS).describedAs("a delivery builds no further instance").hasValue(2);
        });
    }

    // The bean a late handler is invoked on is resolved by name, per delivery, rather than held on to while the bean
    // is still being created. A BeanPostProcessor registered after the Occurrent one wraps the instance the creation
    // callback hands over, so holding on to that instance would invoke the handler on an inner layer and lose the
    // outer layer's advice, which is exactly the loss that moved registration to afterSingletonsInstantiated.
    @Test
    void a_late_handler_is_invoked_through_the_bean_the_context_publishes() {
        runner.withUserConfiguration(LateWrappingProxyConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("wrappedSubscriber");
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("late-wrapped-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());

            LateWrappingProxyConfiguration.ADVICE_CALLS.clear();
            handler.getValue().invoke(null, new TestEvent());

            assertThat(LateWrappingProxyConfiguration.ADVICE_CALLS).containsExactly("on");
        });
    }

    interface Marker {
    }

    static class HiddenSubscriber implements Marker {
        HiddenSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "lazy-hidden-handler")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LazyInterfaceReturningConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // Declared to return Marker, so applicationContext.getType predicts Marker however the body is written.
        @Bean
        @Lazy
        Marker hiddenSubscriber() {
            return new HiddenSubscriber();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LazyFactoryBeanConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        HiddenProductFactory hiddenProductFactory() {
            return new HiddenProductFactory();
        }
    }

    // isEagerInit() is false by SmartFactoryBean's default, so the container does not build the product during
    // startup and neither may the scan.
    static class HiddenProductFactory implements SmartFactoryBean<Marker> {
        @Override
        public Marker getObject() {
            return new FactoryProductSubscriber();
        }

        @Override
        public Class<?> getObjectType() {
            return Marker.class;
        }
    }

    static class FactoryProductSubscriber implements Marker {
        FactoryProductSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "factory-product-handler")
        void on(TestEvent event) {
        }
    }

    interface DeclaringMarker {
        @Subscription(id = "declared-on-the-interface")
        void onTheInterface(TestEvent event);
    }

    static class PartiallyVisibleSubscriber implements DeclaringMarker {
        @Override
        public void onTheInterface(TestEvent event) {
        }

        @Subscription(id = "declared-on-the-class")
        void onTheClass(TestEvent event) {
        }

        @Projection(id = "declared-on-the-class-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> hiddenProjection() {
            PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    // The collaborators a source = PUSH projection needs, shared by the two configurations below. Each of them runs
    // in a context of its own, so each gets its own DomainEventFeed, which matters because a feed feeds exactly one
    // projection.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionCollaboratorsConfiguration {
        // Returns a CloudEvent rather than null, unlike the stub the subscription-only fixtures use, because the
        // @Projection registration path converts events rather than only deriving a type filter from them.
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
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
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // An empty domain-feed reader is all a source = PUSH projection needs to register without Docker.
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

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

    }

    @Configuration(proxyBeanMethods = false)
    static class PartiallyVisibleConfiguration {
        @Bean
        @Lazy
        DeclaringMarker partiallyVisibleSubscriber() {
            return new PartiallyVisibleSubscriber();
        }
    }

    interface OverridingMarker {
        @Projection(id = "overridden-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> interfaceProjection();
    }

    static class OverridingSubscriber implements OverridingMarker {
        // Overrides a @Projection the interface declares, so the startup scan sees the interface's Method for it and
        // the rescan sees the overriding one. Registering it twice would refuse the id as a duplicate.
        @Override
        public org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> interfaceProjection() {
            INTERFACE_PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverriddenProjectionConfiguration {
        @Bean
        @Lazy
        OverridingMarker overridingSubscriber() {
            return new OverridingSubscriber();
        }
    }

    static class PrototypeSubscriber implements Marker {
        PrototypeSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "prototype-handler")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PrototypeConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // Declared to return Marker for the same reason the @Lazy fixtures are, so the handler is one only the
        // concrete class declares and the startup scan cannot see it.
        @Bean
        @Scope("prototype")
        Marker hiddenPrototypeSubscriber() {
            return new PrototypeSubscriber();
        }
    }

    public static class WrappedSubscriber implements Marker {
        @Subscription(id = "late-wrapped-handler")
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateWrappingProxyConfiguration {
        static final List<String> ADVICE_CALLS = new ArrayList<>();

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker wrappedSubscriber() {
            return new WrappedSubscriber();
        }

        // Declared in a user configuration, so it is registered after the post processor the runner contributes and
        // therefore wraps the bean after the post processor's own creation callback has already seen it.
        @Bean
        static BeanPostProcessor lateWrappingPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof WrappedSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> {
                        ADVICE_CALLS.add(invocation.getMethod().getName());
                        return invocation.proceed();
                    });
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    // The second scan pass, and the case only it reaches. A @Projection is collected as a (bean, method, annotation)
    // triple during the pass that reads the bean's type and registered afterwards, so unlike a subscription, whose
    // registration re-reads the type after the bean exists, it never revisits the type. Registering the handler the
    // interface does declare is what builds the bean, and only a second pass reads the class that build revealed.
    // Without it an application would see one of its two annotations working and have no reason to suspect the other.
    @Test
    void a_projection_only_the_concrete_class_declares_registers_alongside_a_subscription_the_interface_declares() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, PartiallyVisibleConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("declared-on-the-interface"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            assertThat(PROJECTION_FACTORY_INVOCATIONS).describedAs("the @Projection only the class declares").hasValue(1);
        });
    }

    // A @Projection the interface declares and the class overrides is a different Method on each pass, so a handler
    // key that included the declaring class would let the rescan register the same id a second time.
    @Test
    void a_projection_the_class_overrides_from_its_interface_registers_once() {
        INTERFACE_PROJECTION_FACTORY_INVOCATIONS.set(0);
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, OverriddenProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(INTERFACE_PROJECTION_FACTORY_INVOCATIONS).hasValue(1);
        });
    }

    record TestEvent() {
    }

    static class NoopCloudEventConverter implements CloudEventConverter<TestEvent> {
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
    }
}
