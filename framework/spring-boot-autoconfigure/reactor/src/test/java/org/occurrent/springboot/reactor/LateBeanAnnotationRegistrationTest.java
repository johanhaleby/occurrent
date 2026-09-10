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
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Scope;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Reactor twin of the blocking {@code LateBeanAnnotationRegistrationTest}: see that class for the mechanism. A bean
 * the container has not built when the startup scan runs is read through the factory method's declared return type,
 * an annotation only the concrete class declares is invisible to it, and building the bean later is what makes the
 * class knowable and the annotation registrable.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class LateBeanAnnotationRegistrationTest {

    private static final AtomicInteger INSTANTIATIONS = new AtomicInteger();
    private static final AtomicInteger PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger INTERFACE_PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger FAILING_PROJECTION_FACTORY_CALLS = new AtomicInteger();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new);

    @BeforeEach
    void resetCounters() {
        INSTANTIATIONS.set(0);
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        INTERFACE_PROJECTION_FACTORY_INVOCATIONS.set(0);
    }

    // The case #981 describes, @Bean declared to return an interface, @Lazy so the bean does not exist when the
    // scan runs, and the handler on the concrete class the interface does not declare.
    @Test
    void a_subscription_only_the_concrete_class_declares_registers_when_the_lazy_bean_is_created() {
        runner.withUserConfiguration(LazyInterfaceReturningConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("@Lazy is still honored by the scan").hasValue(0);
            verify(subscriptions, never()).subscribe(eq("reactive-lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));

            context.getBean("hiddenSubscriber");

            verify(subscriptions).subscribe(eq("reactive-lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
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
                    .subscribe(eq("reactive-lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        });
    }

    // A SmartFactoryBean whose product is not eager exists at scan time while its product does not, and
    // getObjectType() answers with the interface for the same reason a factory method's return type does.
    @Test
    void a_subscription_only_a_factory_bean_product_declares_registers_when_the_product_is_created() {
        runner.withUserConfiguration(LazyFactoryBeanConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("the scan does not force the product").hasValue(0);
            verify(subscriptions, never()).subscribe(eq("reactive-factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));

            context.getBean("hiddenProductFactory");

            verify(subscriptions).subscribe(eq("reactive-factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        });
    }

    // A prototype passes through the same creation callback once per instance, and a subscription id is the durable
    // checkpoint key, so the second instance must not register it again. The one instance that did register stays
    // the handler's target too, because resolving a prototype by name per delivery would build a fresh bean for
    // every event.
    @Test
    void a_prototypes_hidden_subscription_registers_once_and_keeps_the_instance_that_registered_it() {
        runner.withUserConfiguration(PrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, Mono<Void>>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("hiddenPrototypeSubscriber");
            context.getBean("hiddenPrototypeSubscriber");

            verify(context.getBean(Subscriptions.class), times(1))
                    .subscribe(eq("reactive-prototype-handler"), any(AgnosticSubscriptionFilter.class), any(), handler.capture());
            assertThat(INSTANTIATIONS).hasValue(2);

            handler.getValue().invoke(null, new TestEvent()).block();

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
            ArgumentCaptor<Function2<EventMetadata, TestEvent, Mono<Void>>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("wrappedSubscriber");
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("reactive-late-wrapped-handler"), any(AgnosticSubscriptionFilter.class), any(), handler.capture());

            LateWrappingProxyConfiguration.ADVICE_CALLS.clear();
            handler.getValue().invoke(null, new TestEvent()).block();

            assertThat(LateWrappingProxyConfiguration.ADVICE_CALLS).containsExactly("on");
        });
    }

    // The second scan pass, and the case only it reaches. A @Projection is collected as a (bean, method, annotation)
    // triple during the pass that reads the bean's type and registered afterwards, so unlike a subscription, whose
    // registration re-reads the type after the bean exists, it never revisits the type. Registering the handler the
    // interface does declare is what builds the bean, and only a second pass reads the class that build revealed.
    @Test
    void a_projection_only_the_concrete_class_declares_registers_alongside_a_subscription_the_interface_declares() {
        runner.withUserConfiguration(DomainFeedConfiguration.class, PartiallyVisibleConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("reactive-declared-on-the-interface"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
            assertThat(PROJECTION_FACTORY_INVOCATIONS).describedAs("the @Projection only the class declares").hasValue(1);
        });
    }

    // A @Projection the interface declares and the class overrides is a different Method on each pass, so a handler
    // key that included the declaring class would let the rescan register the same id a second time.
    @Test
    void a_projection_the_class_overrides_from_its_interface_registers_once() {
        runner.withUserConfiguration(DomainFeedConfiguration.class, OverriddenProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(INTERFACE_PROJECTION_FACTORY_INVOCATIONS).hasValue(1);
        });
    }

    // A handler registered from the creation callback can be delivered to before that callback returns, because
    // startupMode = WAIT_UNTIL_STARTED replays history inside subscribe. The singleton is not published yet at that
    // point, so a handler target that always asks the context by name would throw BeanCurrentlyInCreationException
    // and the bean could never finish being built.
    @Test
    void a_late_handler_replaying_history_inside_its_own_registration_is_delivered_to() {
        runner.withUserConfiguration(ReplayDuringRegistrationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            context.getBean("replayingSubscriber");

            assertThat(ReplayingSubscriber.DELIVERED).containsExactly("replayed");
        });
    }

    // The startup ordering used to make this impossible, since every subscription id was collected before the first
    // projection checked one out. A subscription registering after startup arrives after all of them, so it has to
    // refuse an id one of them already holds rather than write to the same durable checkpoint key.
    @Test
    void a_late_subscription_reusing_a_projections_id_is_refused() {
        runner.withUserConfiguration(DomainFeedConfiguration.class, LateIdClashConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("clashingSubscriber"))
                    .rootCause()
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("clashing-id");
        });
    }

    // The startup path binds a handler to the instance it resolved, so a prototype whose declared type already
    // exposes the annotation keeps the single instance it registered with.
    @Test
    void a_prototype_registered_at_startup_keeps_the_instance_it_registered_with() {
        runner.withUserConfiguration(VisiblePrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, Mono<Void>>> handler = ArgumentCaptor.forClass(Function2.class);
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("reactive-visible-prototype-handler"), any(AgnosticSubscriptionFilter.class), any(), handler.capture());
            int afterStartup = INSTANTIATIONS.get();

            handler.getValue().invoke(null, new TestEvent()).block();

            assertThat(INSTANTIATIONS).describedAs("a delivery builds no further instance").hasValue(afterStartup);
        });
    }

    // A registration that throws fails the bean's creation, and Spring caches nothing for a creation that failed,
    // so the next request builds the bean again. The handler has to be registered on that second attempt.
    @Test
    void a_late_handler_whose_first_registration_threw_registers_on_the_retry() {
        runner.withUserConfiguration(FailingFirstRegistrationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThatThrownBy(() -> context.getBean("retriedSubscriber")).isNotNull();

            context.getBean("retriedSubscriber");

            verify(context.getBean(Subscriptions.class), times(2))
                    .subscribe(eq("reactive-retried-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        });
    }

    // The register step reads the bean's class again, and by then the bean exists, so that class can declare a
    // handler the collecting pass never saw. Registering such a handler straight away would take its id without
    // ever checking it, which is how a second method could quietly share a durable checkpoint key with the first.
    @Test
    void a_second_handler_on_the_concrete_class_reusing_the_interfaces_id_is_refused() {
        runner.withUserConfiguration(SameIdAcrossMethodsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("reactive-shared-across-methods");
        });
    }

    // The id that gets claimed is the one the registrar read from the method it registered, not one resolved
    // earlier against a predicted type. An overriding method may declare a different id than the method it
    // overrides, and claiming the wrong one lets something else take the real id.
    @Test
    void an_overriding_methods_own_id_is_the_one_that_gets_claimed() {
        runner.withUserConfiguration(DomainFeedConfiguration.class, OverriddenIdConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("reactive-class-declared-id");
        });
    }

    // A descriptor registrar claims its id before the rest of its work, so a failure part way through would leave
    // the id held by a registration that never happened and the bean's next creation attempt would be refused as a
    // duplicate of itself.
    @Test
    void a_late_projection_whose_first_registration_threw_registers_on_the_retry() {
        FAILING_PROJECTION_FACTORY_CALLS.set(0);
        runner.withUserConfiguration(DomainFeedConfiguration.class, FailingFirstProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThatThrownBy(() -> context.getBean("failingProjectionHolder")).isNotNull();

            context.getBean("failingProjectionHolder");

            assertThat(FAILING_PROJECTION_FACTORY_CALLS).hasValue(2);
        });
    }

    // A CGLIB proxy added by a later BeanPostProcessor cannot override a final handler, so selecting the
    // method succeeds while invoking it reaches the inherited method directly and every layer's advice is skipped.
    // Registration cannot see that proxy, because it does not exist yet, so the guard runs again on the object the
    // handler is actually invoked on.
    @Test
    void a_final_late_handler_wrapped_in_a_cglib_proxy_afterwards_is_refused_on_delivery() {
        runner.withUserConfiguration(LateFinalHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, Mono<Void>>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("finalHandlerSubscriber");
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("reactive-late-final-handler"), any(AgnosticSubscriptionFilter.class), any(), handler.capture());

            assertThatThrownBy(() -> handler.getValue().invoke(null, new TestEvent()).block())
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("is final");
        });
    }

    // A bean can declare several handlers, and the second one failing must not leave the first subscribed. The
    // bean's creation fails either way, so a handler registered before the failure would be delivering to an
    // instance nobody can reach, and the context stays up because a lazily built bean failing does not close it.
    @Test
    void a_bean_whose_second_handler_cannot_register_leaves_the_first_one_unsubscribed() {
        runner.withUserConfiguration(SecondHandlerFailsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("twoHandlerSubscriber")).isNotNull();

            verify(context.getBean(Subscriptions.class), never())
                    .subscribe(eq("reactive-first-of-two"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        });
    }

    // The scan repeats until a pass registers nothing, and a pass that cannot register a @Projection because no
    // Subscribable bean exists still has to reach the step that builds the beans it scheduled. Skipping that step
    // left the same bean scheduled on every pass and the loop never ended. The timeout is what makes a return of
    // that fail here rather than hang the build.
    @Test
    @Timeout(30)
    void a_projection_that_cannot_register_without_a_subscribable_does_not_loop_the_scan() {
        runner.withUserConfiguration(NoSubscribableProjectionConfiguration.class).run(context ->
                assertThat(context).hasNotFailed());
    }

    // The two families share the key a handler registers under, so without this the subscription registers first
    // and the projection is dropped in silence. It used to be the second registrar refusing the return type.
    @Test
    void a_method_with_both_a_subscription_and_a_descriptor_annotation_is_refused_rather_than_half_registered() {
        runner.withUserConfiguration(MixedFamilyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("@Subscription and @Projection");
        });
    }

    // A valid subscription handler, so the subscription registers and nothing rejects the method. That is the only
    // shape where the projection goes missing in silence, since any other one fails a signature check first.
    static class MixedFamilyHolder {
        @Subscription(id = "reactive-mixed-family-subscription")
        @Projection(id = "reactive-mixed-family-projection", source = Source.PUSH)
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class MixedFamilyConfiguration {
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
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        MixedFamilyHolder mixedFamilyHolder() {
            return new MixedFamilyHolder();
        }
    }

    interface Marker {
    }

    static class HiddenSubscriber implements Marker {
        HiddenSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "reactive-lazy-hidden-handler", startupMode = StartupMode.BACKGROUND)
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

        @Subscription(id = "reactive-factory-product-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
        }
    }

    static class PrototypeSubscriber implements Marker {
        PrototypeSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "reactive-prototype-handler", startupMode = StartupMode.BACKGROUND)
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
        @Subscription(id = "reactive-late-wrapped-handler", startupMode = StartupMode.BACKGROUND)
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

    interface DeclaringMarker {
        @Subscription(id = "reactive-declared-on-the-interface", startupMode = StartupMode.BACKGROUND)
        void onTheInterface(TestEvent event);
    }

    static class PartiallyVisibleSubscriber implements DeclaringMarker {
        @Override
        public void onTheInterface(TestEvent event) {
        }

        @Projection(id = "reactive-declared-on-the-class-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> hiddenProjection() {
            PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PartiallyVisibleConfiguration {
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        DeclaringMarker partiallyVisibleSubscriber() {
            return new PartiallyVisibleSubscriber();
        }
    }

    interface OverridingMarker {
        @Projection(id = "reactive-overridden-projection", source = Source.PUSH)
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

    // Its own configuration rather than another bean in PartiallyVisibleConfiguration, because a DomainEventFeed
    // feeds exactly one projection and both would share the one DomainFeedConfiguration declares.
    @Configuration(proxyBeanMethods = false)
    static class OverriddenProjectionConfiguration {
        @Bean
        @Lazy
        OverridingMarker overridingSubscriber() {
            return new OverridingSubscriber();
        }
    }

    // Copied from ProjectionAnnotationJdkProxyTest next door. An empty domain-feed reader is all a source = PUSH
    // projection needs to register without Docker, and the mocked Subscribable is what the reactive post processor
    // needs present before it scans for @Projection at all.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

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

    public static class ReplayingSubscriber implements Marker {
        static final List<String> DELIVERED = new ArrayList<>();

        @Subscription(id = "reactive-replay-during-registration", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            DELIVERED.add("replayed");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ReplayDuringRegistrationConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        // Delivers to the handler from inside subscribe, the way a WAIT_UNTIL_STARTED history replay does, so the
        // delivery arrives while the bean this handler belongs to is still being built.
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            doAnswer(invocation -> {
                Function2<EventMetadata, TestEvent, Mono<Void>> handler = invocation.getArgument(3);
                handler.invoke(null, new TestEvent()).block();
                return null;
            }).when(subscriptions).subscribe(eq("reactive-replay-during-registration"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
            return subscriptions;
        }

        @Bean
        @Lazy
        Marker replayingSubscriber() {
            ReplayingSubscriber.DELIVERED.clear();
            return new ReplayingSubscriber();
        }
    }

    static class ClashingSubscriber implements Marker {
        @Subscription(id = "clashing-id", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
        }
    }

    static class ClashingProjectionHolder {
        @Projection(id = "clashing-id", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LateIdClashConfiguration {
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        ClashingProjectionHolder clashingProjectionHolder() {
            return new ClashingProjectionHolder();
        }

        @Bean
        @Lazy
        Marker clashingSubscriber() {
            return new ClashingSubscriber();
        }
    }

    static class VisiblePrototypeSubscriber {
        VisiblePrototypeSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "reactive-visible-prototype-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
        }
    }

    // Declared as the concrete class, so the startup scan sees the handler and registers it there rather than from
    // the creation callback.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class VisiblePrototypeConfiguration {
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
        @Scope("prototype")
        VisiblePrototypeSubscriber visiblePrototypeSubscriber() {
            return new VisiblePrototypeSubscriber();
        }
    }

    static class RetriedSubscriber implements Marker {
        @Subscription(id = "reactive-retried-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingFirstRegistrationConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        // Refuses the first registration and accepts the second, so the retry Spring performs after a failed bean
        // creation is what this fixture exercises.
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            doThrow(new IllegalStateException("refused once"))
                    .doAnswer(invocation -> null)
                    .when(subscriptions).subscribe(eq("reactive-retried-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
            return subscriptions;
        }

        @Bean
        @Lazy
        Marker retriedSubscriber() {
            return new RetriedSubscriber();
        }
    }

    interface SameIdMarker {
        @Subscription(id = "reactive-shared-across-methods", startupMode = StartupMode.BACKGROUND)
        void onTheInterface(TestEvent event);
    }

    static class SameIdSubscriber implements SameIdMarker {
        @Override
        public void onTheInterface(TestEvent event) {
        }

        // Only the concrete class declares the second handler, so the collecting pass sees it a pass later than
        // the first, and has to refuse the id it reuses.
        @Subscription(id = "reactive-shared-across-methods", startupMode = StartupMode.BACKGROUND)
        void onTheClass(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SameIdAcrossMethodsConfiguration {
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
        SameIdMarker sameIdSubscriber() {
            return new SameIdSubscriber();
        }
    }

    interface DifferentIdMarker {
        @Subscription(id = "reactive-interface-declared-id", startupMode = StartupMode.BACKGROUND)
        void handler(TestEvent event);
    }

    static class DifferentIdSubscriber implements DifferentIdMarker {
        @Override
        @Subscription(id = "reactive-class-declared-id", startupMode = StartupMode.BACKGROUND)
        public void handler(TestEvent event) {
        }
    }

    static class ClassIdProjectionHolder {
        @Projection(id = "reactive-class-declared-id", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverriddenIdConfiguration {
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        DifferentIdMarker differentIdSubscriber() {
            return new DifferentIdSubscriber();
        }

        @Bean
        ClassIdProjectionHolder classIdProjectionHolder() {
            return new ClassIdProjectionHolder();
        }
    }

    static class FailingFirstProjectionHolder implements Marker {
        // Throws the first time and succeeds the second, so the retry Spring performs after a failed bean creation
        // is what this fixture exercises.
        @Projection(id = "reactive-failing-first-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            if (FAILING_PROJECTION_FACTORY_CALLS.incrementAndGet() == 1) {
                throw new IllegalStateException("refused once");
            }
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FailingFirstProjectionConfiguration {
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker failingProjectionHolder() {
            return new FailingFirstProjectionHolder();
        }
    }

    public static class LateFinalHandlerSubscriber implements Marker {
        @Subscription(id = "reactive-late-final-handler", startupMode = StartupMode.BACKGROUND)
        public final void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateFinalHandlerConfiguration {
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
        Marker finalHandlerSubscriber() {
            return new LateFinalHandlerSubscriber();
        }

        // Wraps the bean only after this post processor's own callback has registered the handler, so the CGLIB
        // proxy does not exist when the registration guards run.
        @Bean
        static BeanPostProcessor lateCglibPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof LateFinalHandlerSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) MethodInvocation::proceed);
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    static class TwoHandlerSubscriber implements Marker {
        @Subscription(id = "reactive-first-of-two", startupMode = StartupMode.BACKGROUND)
        void first(TestEvent event) {
        }

        // Static, so resolveHandlerInvocation refuses it. The refusal has to happen before the handler above
        // subscribes, not after.
        @Subscription(id = "reactive-second-of-two", startupMode = StartupMode.BACKGROUND)
        static void second(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SecondHandlerFailsConfiguration {
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
        Marker twoHandlerSubscriber() {
            return new TwoHandlerSubscriber();
        }
    }

    interface NoSubscribableMarker {
    }

    static class NoSubscribableProjectionHolder implements NoSubscribableMarker {
        @Projection(id = "reactive-no-subscribable-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    // No Subscribable bean, so the coordinator cannot register a @Projection at all, and the bean is declared
    // behind an interface so its class is only predicted until something builds it.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class NoSubscribableProjectionConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @Lazy
        NoSubscribableMarker noSubscribableProjectionHolder() {
            return new NoSubscribableProjectionHolder();
        }
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
