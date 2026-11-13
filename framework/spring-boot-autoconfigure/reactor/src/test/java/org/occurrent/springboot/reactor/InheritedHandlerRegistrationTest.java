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
import kotlin.jvm.functions.Function2;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.reactor.other.CrossPackageEvent;
import org.occurrent.springboot.reactor.other.CrossPackagePackagePrivateHandler;
import org.occurrent.springboot.reactor.other.CrossPackagePublicHandler;
import org.occurrent.springboot.common.OccurrentProperties;
import org.mockito.Answers;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * A handler method counts whether the bean's class declares it, inherits it from a superclass or gets it as an
 * interface default method (issue #983). The scan registers the most specific method the bean runs, so an override is
 * one handler under its own annotation if it repeats one and under the overridden method's if it does not, and a
 * generic handler is one handler although the compiler adds a bridge method. Reactive counterpart of the blocking
 * {@code InheritedHandlerRegistrationTest}, reproduced without a running store (no
 * Docker), reading the ids and handlers off a mocked {@link Subscriptions}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InheritedHandlerRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class);

    @Test
    void a_handler_declared_on_an_abstract_superclass_registers_for_a_subclass_bean_that_does_not_override_it() {
        runner.withUserConfiguration(InheritingFromSuperclassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("base-handler");
            handlerFor(subscriptions, "base-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("base-body");
        });
    }

    @Test
    void an_interface_default_method_registers_for_a_bean_that_does_not_override_it() {
        runner.withUserConfiguration(DefaultMethodConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("default-method-handler");
            handlerFor(subscriptions, "default-method-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("default-body");
        });
    }

    @Test
    void an_override_that_drops_the_annotation_registers_once_with_the_overridden_methods_id_and_runs_the_override() {
        runner.withUserConfiguration(OverrideWithoutAnnotationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("base-handler");
            handlerFor(subscriptions, "base-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("override-body");
        });
    }

    @Test
    void an_override_that_repeats_the_annotation_with_another_id_registers_once_under_the_overrides_id() {
        runner.withUserConfiguration(OverrideWithOtherIdConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("override-handler");
            handlerFor(subscriptions, "override-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("override-body");
        });
    }

    @Test
    void a_generic_superclass_handler_overridden_with_a_concrete_type_registers_once_despite_the_bridge_method() {
        runner.withUserConfiguration(GenericHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("generic-handler");
            handlerFor(subscriptions, "generic-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("generic-body");
        });
    }

    @Test
    void a_generic_superclass_handler_re_declared_on_the_concrete_override_registers_once_despite_the_bridge_method() {
        runner.withUserConfiguration(GenericRedeclaredHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("generic-redeclared-handler");
        });
    }

    @Test
    void private_handlers_of_the_same_signature_on_a_superclass_and_its_subclass_are_two_handlers() {
        runner.withUserConfiguration(PrivateSameSignatureConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactlyInAnyOrder("base-private-handler", "subclass-private-handler");
            handlerFor(subscriptions, "base-private-handler").invoke(null, new TestEvent()).block();
            handlerFor(subscriptions, "subclass-private-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactlyInAnyOrder("base-private-body", "subclass-private-body");
        });
    }

    @Test
    void package_private_handlers_of_the_same_signature_in_a_superclass_and_a_subclass_in_other_packages_are_two_handlers() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", CrossPackageSameSignatureSubscriber.class, () -> new CrossPackageSameSignatureSubscriber(bodies)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactlyInAnyOrder("cross-package-package-private-handler", "cross-package-subclass-handler");
            anyHandlerFor(subscriptions, "cross-package-package-private-handler").invoke(null, new CrossPackageEvent()).block();
            anyHandlerFor(subscriptions, "cross-package-subclass-handler").invoke(null, new CrossPackageEvent()).block();
            assertThat(bodies).containsExactlyInAnyOrder("package-private-body", "subclass-body");
        });
    }

    @Test
    void a_lazy_bean_that_only_inherits_its_handler_registers_at_startup_without_being_asked_for() {
        runner.withUserConfiguration(LazyInheritingConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("base-handler");
        });
    }

    @Test
    void two_beans_extending_the_same_annotated_superclass_fail_startup_because_they_share_one_id() {
        runner.withUserConfiguration(TwoBeansOfOneSuperclassConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-handler");
        });
    }

    // ------------------------------------------------------------------------------------------------------
    // A generic handler is read against the bean's class
    // ------------------------------------------------------------------------------------------------------

    @Test
    void an_unbounded_generic_subscription_handler_subscribes_to_the_type_the_subclass_binds() {
        assertSubscribesTo(UnboundedSubscriptionForShipped.class, "subscriptions", "subscribe", "unbounded-subscription", AgnosticSubscriptionFilter.filter(Filter.type("Shipped")));
    }

    @Test
    void a_generic_subscription_handler_bounded_by_a_sealed_type_subscribes_to_the_type_the_subclass_binds_only() {
        assertSubscribesTo(BoundedSubscriptionForPlaced.class, "subscriptions", "subscribe", "bounded-subscription", AgnosticSubscriptionFilter.filter(Filter.type("Placed")));
    }

    @Test
    void an_unbounded_generic_stream_subscription_handler_subscribes_to_the_type_the_subclass_binds() {
        assertSubscribesTo(UnboundedStreamForShipped.class, "streamSubscriptions", "subscribe", "unbounded-stream", StreamSubscriptionFilter.filter(Filter.type("Shipped")));
    }

    @Test
    void a_generic_stream_subscription_handler_bounded_by_a_sealed_type_subscribes_to_the_type_the_subclass_binds_only() {
        assertSubscribesTo(BoundedStreamForPlaced.class, "streamSubscriptions", "subscribe", "bounded-stream", StreamSubscriptionFilter.filter(Filter.type("Placed")));
    }

    @Test
    void an_unbounded_generic_synchronous_subscription_handler_subscribes_to_the_type_the_subclass_binds() {
        assertSubscribesTo(UnboundedSynchronousForShipped.class, OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, "subscribe", "unbounded-synchronous", AgnosticSubscriptionFilter.filter(Filter.type("Shipped")));
    }

    @Test
    void a_generic_synchronous_subscription_handler_bounded_by_a_sealed_type_subscribes_to_the_type_the_subclass_binds_only() {
        assertSubscribesTo(BoundedSynchronousForPlaced.class, OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, "subscribe", "bounded-synchronous", AgnosticSubscriptionFilter.filter(Filter.type("Placed")));
    }

    @Test
    void an_unbounded_generic_dcb_subscription_handler_builds_its_criteria_from_the_type_the_subclass_binds() {
        assertSubscribesTo(UnboundedDcbForShipped.class, "dcbSubscriptions", "subscribeWithMetadata", "unbounded-dcb", DcbCriteria.types("Shipped"));
    }

    @Test
    void a_generic_dcb_subscription_handler_bounded_by_a_sealed_type_builds_its_criteria_from_the_type_the_subclass_binds_only() {
        assertSubscribesTo(BoundedDcbForPlaced.class, "dcbSubscriptions", "subscribeWithMetadata", "bounded-dcb", DcbCriteria.types("Placed"));
    }

    @Test
    void a_raw_subclass_of_a_generic_handler_bounded_by_a_sealed_type_keeps_subscribing_to_the_erased_bound_and_its_permitted_subtypes() {
        runner.withBean(RawBoundedSubscription.class, RawBoundedSubscription::new).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(argumentOf(context.getBean("subscriptions"), "subscribe", "bounded-subscription"))
                    .isEqualTo(AgnosticSubscriptionFilter.filter(new Filter.CompositionFilter(Filter.CompositionOperator.OR, List.of(Filter.type("OrderEvent"), Filter.type("Placed"), Filter.type("Paid")))));
        });
    }

    @Test
    void a_raw_dcb_subclass_of_a_generic_handler_bounded_by_a_sealed_type_builds_its_criteria_from_the_bound() {
        assertSubscribesTo(RawBoundedDcb.class, "dcbSubscriptions", "subscribeWithMetadata", "bounded-dcb", DcbCriteria.types("OrderEvent", "Placed", "Paid"));
    }

    @Test
    void a_raw_subclass_of_an_unbounded_generic_handler_keeps_failing_startup() {
        runner.withBean(RawUnboundedSubscription.class, RawUnboundedSubscription::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure())).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("the concrete event types dispatch would accept for java.lang.Object cannot all be enumerated")
                    .hasMessageContaining("unbounded-subscription");
        });
    }

    private void assertSubscribesTo(Class<?> bean, String subscriptionsBean, String method, String id, Object expectedFilterOrCriteria) {
        runner.withBean(bean).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(argumentOf(context.getBean(subscriptionsBean), method, id)).isEqualTo(expectedFilterOrCriteria);
        });
    }

    // The filter, or for a DCB subscription the criteria, a handler was subscribed with. It is the second argument of
    // every subscribe call the registrar makes.
    private static Object argumentOf(Object subscriptions, String method, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals(method) && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> invocation.getRawArguments()[1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No subscription was made under id " + id));
    }

    sealed interface OrderEvent permits Placed, Paid {
    }

    record Placed() implements OrderEvent {
    }

    record Paid() implements OrderEvent {
    }

    record Shipped() {
    }

    abstract static class UnboundedSubscription<E> {
        @Subscription(id = "unbounded-subscription", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class UnboundedSubscriptionForShipped extends UnboundedSubscription<Shipped> {
    }

    @SuppressWarnings("rawtypes")
    static class RawUnboundedSubscription extends UnboundedSubscription {
    }

    abstract static class BoundedSubscription<E extends OrderEvent> {
        @Subscription(id = "bounded-subscription", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class BoundedSubscriptionForPlaced extends BoundedSubscription<Placed> {
    }

    @SuppressWarnings("rawtypes")
    static class RawBoundedSubscription extends BoundedSubscription {
    }

    abstract static class UnboundedStream<E> {
        @StreamSubscription(id = "unbounded-stream", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class UnboundedStreamForShipped extends UnboundedStream<Shipped> {
    }

    abstract static class BoundedStream<E extends OrderEvent> {
        @StreamSubscription(id = "bounded-stream", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class BoundedStreamForPlaced extends BoundedStream<Placed> {
    }

    abstract static class UnboundedSynchronous<E> {
        @SynchronousSubscription(id = "unbounded-synchronous")
        public void on(E event) {
        }
    }

    static class UnboundedSynchronousForShipped extends UnboundedSynchronous<Shipped> {
    }

    abstract static class BoundedSynchronous<E extends OrderEvent> {
        @SynchronousSubscription(id = "bounded-synchronous")
        public void on(E event) {
        }
    }

    static class BoundedSynchronousForPlaced extends BoundedSynchronous<Placed> {
    }

    abstract static class UnboundedDcb<E> {
        @DcbSubscription(id = "unbounded-dcb", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class UnboundedDcbForShipped extends UnboundedDcb<Shipped> {
    }

    abstract static class BoundedDcb<E extends OrderEvent> {
        @DcbSubscription(id = "bounded-dcb", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
        }
    }

    static class BoundedDcbForPlaced extends BoundedDcb<Placed> {
    }

    @SuppressWarnings("rawtypes")
    static class RawBoundedDcb extends BoundedDcb {
    }

    // ------------------------------------------------------------------------------------------------------
    // A handler a CGLIB proxy cannot override
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_package_private_handler_inherited_from_another_package_is_refused_on_a_cglib_proxied_bean() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        cglibProxying(CrossPackagePackagePrivateSubscriber.class, new AtomicInteger())
                .withBean("subscriber", CrossPackagePackagePrivateSubscriber.class, () -> new CrossPackagePackagePrivateSubscriber(bodies))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                            .hasMessageContaining(CrossPackagePackagePrivateHandler.class.getName() + ".on")
                            .hasMessageContaining("The method is package-private and declared in package org.occurrent.springboot.reactor.other")
                            .hasMessageContaining("which subclasses " + CrossPackagePackagePrivateSubscriber.class.getName() + " in package org.occurrent.springboot.reactor");
                });
    }

    @Test
    void a_package_private_handler_inherited_from_another_package_registers_and_runs_on_a_bean_nothing_proxies() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", CrossPackagePackagePrivateSubscriber.class, () -> new CrossPackagePackagePrivateSubscriber(bodies)).run(context -> {
            assertThat(context).hasNotFailed();

            anyHandlerFor(context.getBean(Subscriptions.class), "cross-package-package-private-handler").invoke(null, new CrossPackageEvent());

            assertThat(bodies).containsExactly("package-private-body");
        });
    }

    @Test
    void a_package_private_handler_inherited_from_the_same_package_runs_through_the_advice_of_a_cglib_proxy() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger adviceCalls = new AtomicInteger();
        cglibProxying(SamePackageSubscriber.class, adviceCalls)
                .withBean("subscriber", SamePackageSubscriber.class, () -> new SamePackageSubscriber(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    anyHandlerFor(context.getBean(Subscriptions.class), "same-package-handler").invoke(null, new TestEvent());

                    assertThat(bodies).containsExactly("same-package-body");
                    assertThat(adviceCalls).hasValue(1);
                });
    }

    @Test
    void a_public_handler_inherited_from_another_package_runs_through_the_advice_of_a_cglib_proxy() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger adviceCalls = new AtomicInteger();
        cglibProxying(CrossPackagePublicSubscriber.class, adviceCalls)
                .withBean("subscriber", CrossPackagePublicSubscriber.class, () -> new CrossPackagePublicSubscriber(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    anyHandlerFor(context.getBean(Subscriptions.class), "cross-package-public-handler").invoke(null, new CrossPackageEvent());

                    assertThat(bodies).containsExactly("public-body");
                    assertThat(adviceCalls).hasValue(1);
                });
    }

    // The proxy does not exist when the handler registers, so registration succeeds, and the guard that runs again on
    // the object a delivery reaches is the only place left to refuse it.
    @Test
    void a_package_private_handler_from_another_package_wrapped_in_a_cglib_proxy_after_registration_is_refused_on_delivery() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        cglibProxying(LateCrossPackageSubscriber.class, new AtomicInteger())
                .withBean(LateCrossPackageConfiguration.class, () -> new LateCrossPackageConfiguration(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean("lateSubscriber");
                    Function2<EventMetadata, Object, Mono<Void>> handler = anyHandlerFor(context.getBean(Subscriptions.class), "cross-package-package-private-handler");

                    assertThatThrownBy(() -> handler.invoke(null, new CrossPackageEvent()))
                            .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                            .hasMessageContaining("The method is package-private and declared in package org.occurrent.springboot.reactor.other");
                    assertThat(bodies).isEmpty();
                });
    }

    @Test
    void a_final_handler_inherited_from_a_superclass_is_refused_on_a_cglib_proxied_bean() {
        cglibProxying(FinalInheritingSubscriber.class, new AtomicInteger())
                .withBean("subscriber", FinalInheritingSubscriber.class, FinalInheritingSubscriber::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                            .hasMessageContaining(FinalHandlerBase.class.getName() + ".on")
                            .hasMessageContaining("The method is final");
                });
    }

    private ApplicationContextRunner cglibProxying(Class<?> proxied, AtomicInteger adviceCalls) {
        return runner.withBean(CglibProxyingPostProcessor.class, () -> new CglibProxyingPostProcessor(proxied, adviceCalls));
    }

    @SuppressWarnings("unchecked")
    private static Function2<EventMetadata, Object, Mono<Void>> anyHandlerFor(Subscriptions<?> subscriptions, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe") && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> (Function2<EventMetadata, Object, Mono<Void>>) invocation.getRawArguments()[invocation.getRawArguments().length - 1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No handler was subscribed under id " + id));
    }

    static class CglibProxyingPostProcessor implements BeanPostProcessor {
        private final Class<?> proxied;
        private final AtomicInteger adviceCalls;

        CglibProxyingPostProcessor(Class<?> proxied, AtomicInteger adviceCalls) {
            this.proxied = proxied;
            this.adviceCalls = adviceCalls;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!proxied.isInstance(bean)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setTarget(bean);
            proxyFactory.setProxyTargetClass(true);
            proxyFactory.addAdvice((MethodInterceptor) invocation -> {
                adviceCalls.incrementAndGet();
                return invocation.proceed();
            });
            return proxyFactory.getProxy();
        }
    }

    static class CrossPackagePackagePrivateSubscriber extends CrossPackagePackagePrivateHandler {
        CrossPackagePackagePrivateSubscriber(List<String> bodies) {
            super(bodies);
        }
    }

    static class CrossPackagePublicSubscriber extends CrossPackagePublicHandler {
        CrossPackagePublicSubscriber(List<String> bodies) {
            super(bodies);
        }
    }

    static class CrossPackageSameSignatureSubscriber extends CrossPackagePackagePrivateHandler {
        CrossPackageSameSignatureSubscriber(List<String> bodies) {
            super(bodies);
        }

        @Subscription(id = "cross-package-subclass-handler", startupMode = StartupMode.BACKGROUND)
        void on(CrossPackageEvent event) {
            bodies.add("subclass-body");
        }
    }

    abstract static class SamePackageHandlerBase {
        final List<String> bodies;

        SamePackageHandlerBase(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "same-package-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
            bodies.add("same-package-body");
        }
    }

    static class SamePackageSubscriber extends SamePackageHandlerBase {
        SamePackageSubscriber(List<String> bodies) {
            super(bodies);
        }
    }

    interface LateMarker {
    }

    static class LateCrossPackageSubscriber extends CrossPackagePackagePrivateHandler implements LateMarker {
        LateCrossPackageSubscriber(List<String> bodies) {
            super(bodies);
        }
    }

    // Declared as the interface, so the scan cannot see the handler until the bean is built, and the bean is built
    // after startup, once the lazy bean is asked for.
    @Configuration(proxyBeanMethods = false)
    static class LateCrossPackageConfiguration {
        private final List<String> bodies;

        LateCrossPackageConfiguration(List<String> bodies) {
            this.bodies = bodies;
        }

        @Bean
        @Lazy
        LateMarker lateSubscriber() {
            return new LateCrossPackageSubscriber(bodies);
        }
    }

    abstract static class FinalHandlerBase {
        @Subscription(id = "final-inherited-handler", startupMode = StartupMode.BACKGROUND)
        public final void on(TestEvent event) {
        }
    }

    static class FinalInheritingSubscriber extends FinalHandlerBase {
    }

    // ------------------------------------------------------------------------------------------------------
    // Static handlers
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_static_handler_declared_on_a_superclass_fails_startup() {
        runner.withBean(StaticInheritingSubscriber.class, StaticInheritingSubscriber::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining(StaticHandlerBase.class.getName() + ".on")
                    .hasMessageContaining("The method is static");
        });
    }

    @Test
    void a_static_method_an_interface_declares_is_not_a_handler_of_the_class_implementing_it() {
        runner.withBean(StaticInterfaceImplementingSubscriber.class, StaticInterfaceImplementingSubscriber::new).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
        });
    }

    abstract static class StaticHandlerBase {
        @Subscription(id = "static-inherited-handler", startupMode = StartupMode.BACKGROUND)
        static void on(TestEvent event) {
        }
    }

    static class StaticInheritingSubscriber extends StaticHandlerBase {
    }

    interface DeclaresStaticHandler {
        @Subscription(id = "static-interface-handler", startupMode = StartupMode.BACKGROUND)
        static void on(TestEvent event) {
        }
    }

    static class StaticInterfaceImplementingSubscriber implements DeclaresStaticHandler {
    }

    // ------------------------------------------------------------------------------------------------------
    // One handler reached through two Method objects
    // ------------------------------------------------------------------------------------------------------

    // The startup scan reads the handler off the bean's class. A prototype built after startup is wrapped by a JDK
    // proxy that cannot be unwrapped to a target, and the scan of that proxy finds the interface's method instead. It
    // is the same handler, so the second scan must not register it again.
    @Test
    void a_handler_read_from_the_class_and_later_from_a_jdk_proxys_interface_method_registers_once() {
        AtomicBoolean proxying = new AtomicBoolean();
        runner.withBean(JdkProxyingPostProcessor.class, () -> new JdkProxyingPostProcessor(proxying))
                .withUserConfiguration(PrototypeInterfaceHandlerConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    proxying.set(true);

                    Object[] second = new Object[1];
                    assertThatCode(() -> second[0] = context.getBean("prototypeHandler")).doesNotThrowAnyException();

                    assertThat(Proxy.isProxyClass(second[0].getClass())).describedAs("the second instance reached the scan as a JDK proxy").isTrue();
                    verify(context.getBean(Subscriptions.class), times(1))
                            .subscribe(eq("proxied-interface-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
                });
    }

    interface InterfaceHandler {
        @Subscription(id = "proxied-interface-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event);
    }

    static class InterfaceHandlerBean implements InterfaceHandler {
        @Override
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeInterfaceHandlerConfiguration {
        @Bean
        @Scope("prototype")
        InterfaceHandlerBean prototypeHandler() {
            return new InterfaceHandlerBean();
        }
    }

    // Runs before the Occurrent post processor, so the instance it hands that one is already the proxy.
    static class JdkProxyingPostProcessor implements BeanPostProcessor, PriorityOrdered {
        private final AtomicBoolean proxying;

        JdkProxyingPostProcessor(AtomicBoolean proxying) {
            this.proxying = proxying;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!proxying.get() || !(bean instanceof InterfaceHandlerBean)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setInterfaces(InterfaceHandler.class);
            proxyFactory.setTargetSource(new TargetSource() {
                @Override
                public Class<?> getTargetClass() {
                    return InterfaceHandlerBean.class;
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
            return proxyFactory.getProxy();
        }

        @Override
        public int getOrder() {
            return HIGHEST_PRECEDENCE;
        }
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Function2<EventMetadata, TestEvent, Mono<Void>> handlerFor(Subscriptions<?> subscriptions, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe") && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> (Function2<EventMetadata, TestEvent, Mono<Void>>) invocation.getRawArguments()[invocation.getRawArguments().length - 1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No handler was subscribed under id " + id));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @Primary
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        StreamSubscriptions<TestEvent> streamSubscriptions() {
            return mock(StreamSubscriptions.class);
        }

        @Bean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> synchronousSubscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        DcbSubscriptions<TestEvent> dcbSubscriptions() {
            return mock(DcbSubscriptions.class, Answers.RETURNS_MOCKS);
        }

        @Bean
        Invocations invocations() {
            return new Invocations(new CopyOnWriteArrayList<>());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritingFromSuperclassConfiguration {
        @Bean
        InheritingBean inheritingBean(Invocations invocations) {
            return new InheritingBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DefaultMethodConfiguration {
        @Bean
        DefaultMethodBean defaultMethodBean(Invocations invocations) {
            return new DefaultMethodBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverrideWithoutAnnotationConfiguration {
        @Bean
        OverridingWithoutAnnotationBean overridingBean(Invocations invocations) {
            return new OverridingWithoutAnnotationBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverrideWithOtherIdConfiguration {
        @Bean
        OverridingWithOtherIdBean overridingBean(Invocations invocations) {
            return new OverridingWithOtherIdBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class GenericHandlerConfiguration {
        @Bean
        GenericImplementationBean genericBean(Invocations invocations) {
            return new GenericImplementationBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class GenericRedeclaredHandlerConfiguration {
        @Bean
        GenericRedeclaringBean genericBean(Invocations invocations) {
            return new GenericRedeclaringBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrivateSameSignatureConfiguration {
        @Bean
        PrivateSubclassBean privateBean(Invocations invocations) {
            return new PrivateSubclassBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyInheritingConfiguration {
        @Bean
        @Lazy
        InheritingBean lazyInheritingBean(Invocations invocations) {
            return new InheritingBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoBeansOfOneSuperclassConfiguration {
        @Bean
        InheritingBean first(Invocations invocations) {
            return new InheritingBean(invocations);
        }

        @Bean
        InheritingBean second(Invocations invocations) {
            return new InheritingBean(invocations);
        }
    }

    record Invocations(List<String> bodies) {
    }

    abstract static class BaseHandler {
        final Invocations invocations;

        BaseHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "base-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("base-body");
        }
    }

    static class InheritingBean extends BaseHandler {
        InheritingBean(Invocations invocations) {
            super(invocations);
        }
    }

    interface DefaultMethodHandler {
        Invocations invocations();

        @Subscription(id = "default-method-handler", startupMode = StartupMode.BACKGROUND)
        default void on(TestEvent event) {
            invocations().bodies().add("default-body");
        }
    }

    static class DefaultMethodBean implements DefaultMethodHandler {
        private final Invocations invocations;

        DefaultMethodBean(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public Invocations invocations() {
            return invocations;
        }
    }

    static class OverridingWithoutAnnotationBean extends BaseHandler {
        OverridingWithoutAnnotationBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("override-body");
        }
    }

    static class OverridingWithOtherIdBean extends BaseHandler {
        OverridingWithOtherIdBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        @Subscription(id = "override-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("override-body");
        }
    }

    abstract static class GenericBaseHandler<E> {
        final Invocations invocations;

        GenericBaseHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "generic-handler", startupMode = StartupMode.BACKGROUND)
        public void on(E event) {
            invocations.bodies().add("generic-body");
        }
    }

    // The override has a different erasure from the superclass method, so the compiler adds a bridge on(Object).
    static class GenericImplementationBean extends GenericBaseHandler<TestEvent> {
        GenericImplementationBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        public void on(TestEvent event) {
            super.on(event);
        }
    }

    abstract static class GenericRedeclaredBaseHandler<E> {
        public void on(E event) {
        }
    }

    // The compiler copies the annotation onto the bridge on(Object) too, so a scan of declared methods sees two.
    static class GenericRedeclaringBean extends GenericRedeclaredBaseHandler<TestEvent> {
        GenericRedeclaringBean(Invocations invocations) {
        }

        @Override
        @Subscription(id = "generic-redeclared-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
        }
    }

    static class PrivateBaseBean {
        final Invocations invocations;

        PrivateBaseBean(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "base-private-handler", startupMode = StartupMode.BACKGROUND)
        private void on(TestEvent event) {
            invocations.bodies().add("base-private-body");
        }
    }

    static class PrivateSubclassBean extends PrivateBaseBean {
        PrivateSubclassBean(Invocations invocations) {
            super(invocations);
        }

        @Subscription(id = "subclass-private-handler", startupMode = StartupMode.BACKGROUND)
        private void on(TestEvent event) {
            invocations.bodies().add("subclass-private-body");
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
