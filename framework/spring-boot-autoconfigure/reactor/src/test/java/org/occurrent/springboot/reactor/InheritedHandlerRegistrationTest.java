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
import org.occurrent.springboot.reactor.bridged.PublicBridgedSubscriber;
import org.occurrent.springboot.reactor.other.CrossPackageEvent;
import org.occurrent.springboot.reactor.other.CrossPackageFixedShadowing;
import org.occurrent.springboot.reactor.other.CrossPackageOverridingMid;
import org.occurrent.springboot.reactor.other.CrossPackageWideningMid;
import org.occurrent.springboot.reactor.shadowing.CrossPackageRepeatingShadowing;
import org.occurrent.springboot.reactor.shadowing.CrossPackageShadowing;
import org.occurrent.springboot.reactor.other.CrossPackagePackagePrivateHandler;
import org.occurrent.springboot.reactor.other.CrossPackagePublicHandler;
import org.occurrent.springboot.common.OccurrentProperties;
import org.mockito.Answers;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
    void a_package_private_handler_overridden_in_its_own_package_registers_once_and_runs_the_override_for_a_bean_in_another_package_that_declares_nothing() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", InheritsOverrideFromHandlersPackageSubscriber.class, () -> new InheritsOverrideFromHandlersPackageSubscriber(bodies)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("cross-package-package-private-handler");
            anyHandlerFor(subscriptions, "cross-package-package-private-handler").invoke(null, new CrossPackageEvent()).block();
            assertThat(bodies).containsExactly("mid-body");
        });
    }

    @Test
    void a_package_private_handler_widened_to_public_in_its_own_package_registers_once_and_runs_the_override_of_a_bean_in_another_package() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", OverridesWidenedHandlerSubscriber.class, () -> new OverridesWidenedHandlerSubscriber(bodies)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("cross-package-package-private-handler");
            anyHandlerFor(subscriptions, "cross-package-package-private-handler").invoke(null, new CrossPackageEvent()).block();
            assertThat(bodies).containsExactly("sub-body");
        });
    }

    @Test
    void a_lazy_bean_whose_declared_type_the_scan_refuses_but_whose_instance_overrides_correctly_starts_and_registers_once() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", CrossPackageShadowing.class, () -> new CrossPackageFixedShadowing(bodies), definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("cross-package-package-private-handler");
            anyHandlerFor(subscriptions, "cross-package-package-private-handler").invoke(null, new CrossPackageEvent()).block();
            assertThat(bodies).containsExactly("fixed-body");
        });
    }

    @Test
    void a_lazy_bean_whose_declared_type_the_scan_refuses_but_whose_instance_repeats_the_annotation_starts_and_registers_both_handlers() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", CrossPackageShadowing.class, () -> new CrossPackageRepeatingShadowing(bodies), definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactlyInAnyOrder("cross-package-package-private-handler", "repeating-handler");
            anyHandlerFor(subscriptions, "repeating-handler").invoke(null, new CrossPackageEvent()).block();
            assertThat(bodies).containsExactly("repeating-body");
        });
    }

    @Test
    void a_lazy_bean_whose_declared_type_and_instance_are_both_refused_still_fails_startup_with_the_refusal() {
        runner.withBean("subscriber", CrossPackageShadowing.class, () -> new RefusedShadowingSubscriber(new CopyOnWriteArrayList<>()), definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not override " + CrossPackagePackagePrivateHandler.class.getName() + "#on");
        });
    }

    @Test
    void a_scoped_proxy_of_interfaces_registers_its_handler_once_under_the_target_and_runs_the_beans_body() {
        runner.withUserConfiguration(ScopedInterfacesConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("scoped-interfaces-handler");
            handlerFor(subscriptions, "scoped-interfaces-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("scoped-interfaces-body");
        });
    }

    @Test
    void a_scoped_proxy_of_the_target_class_registers_its_handler_once_under_the_target_and_runs_the_beans_body() {
        runner.withUserConfiguration(ScopedTargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("scoped-target-class-handler");
            handlerFor(subscriptions, "scoped-target-class-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("scoped-target-class-body");
        });
    }

    @Test
    void a_bean_registers_its_handler_although_another_bean_is_named_scoped_target_plus_its_own_name() {
        runner.withBean("x", NameCollisionHandler.class, () -> new NameCollisionHandler(new Invocations(new CopyOnWriteArrayList<>())))
                .withBean("scopedTarget.x", UnrelatedBean.class, UnrelatedBean::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

                    assertThat(subscribedIds(subscriptions)).containsExactly("name-collision-handler");
                    handlerFor(subscriptions, "name-collision-handler").invoke(null, new TestEvent()).block();
                    assertThat(context.getBean("x", NameCollisionHandler.class).invocations.bodies()).containsExactly("name-collision-body");
                });
    }

    @Test
    void a_scoped_proxy_bean_in_a_scope_that_is_inactive_at_startup_still_fails_startup_because_the_scan_builds_its_target() {
        runner.withUserConfiguration(RequestLikeScopeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No thread-bound request");
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

    @Test
    void a_public_handler_inherited_from_a_package_private_class_through_a_public_subclass_registers_and_runs() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", PublicBridgedSubscriber.class, () -> new PublicBridgedSubscriber(bodies)).run(context -> {
            assertThat(context).hasNotFailed();

            anyHandlerFor(context.getBean(Subscriptions.class), "bridged-handler").invoke(null, new CrossPackageEvent());

            assertThat(bodies).containsExactly("bridged-body");
        });
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

    interface ScopedMarker {
    }

    record ScopedMarkerConsumer(ScopedMarker marker) {
    }

    static class ScopedInterfacesHandler implements ScopedMarker {
        private final Invocations invocations;

        ScopedInterfacesHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "scoped-interfaces-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("scoped-interfaces-body");
        }
    }

    public static class ScopedTargetClassHandler {
        private final Invocations invocations;

        ScopedTargetClassHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "scoped-target-class-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("scoped-target-class-body");
        }
    }

    static class NameCollisionHandler {
        final Invocations invocations;

        NameCollisionHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "name-collision-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("name-collision-body");
        }
    }

    static class UnrelatedBean {
    }

    public static class RequestLikeHandler {
        @Subscription(id = "request-like-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RequestLikeScopeConfiguration {
        @Bean
        static BeanFactoryPostProcessor requestLikeScope() {
            return beanFactory -> beanFactory.registerScope("request-like", new org.springframework.beans.factory.config.Scope() {
                @Override
                public Object get(String name, ObjectFactory<?> objectFactory) {
                    throw new IllegalStateException("No thread-bound request");
                }

                @Override
                public Object remove(String name) {
                    return null;
                }

                @Override
                public void registerDestructionCallback(String name, Runnable callback) {
                }

                @Override
                public Object resolveContextualObject(String key) {
                    return null;
                }

                @Override
                public String getConversationId() {
                    return null;
                }
            });
        }

        @Bean
        @Scope(value = "request-like", proxyMode = ScopedProxyMode.TARGET_CLASS)
        RequestLikeHandler requestLikeHandler() {
            return new RequestLikeHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ScopedInterfacesConfiguration {
        @Bean
        @Scope(value = "prototype", proxyMode = ScopedProxyMode.INTERFACES)
        ScopedInterfacesHandler scopedInterfacesHandler(Invocations invocations) {
            return new ScopedInterfacesHandler(invocations);
        }

        @Bean
        ScopedMarkerConsumer scopedMarkerConsumer(ScopedMarker marker) {
            return new ScopedMarkerConsumer(marker);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ScopedTargetClassConfiguration {
        @Bean
        @Scope(value = "prototype", proxyMode = ScopedProxyMode.TARGET_CLASS)
        ScopedTargetClassHandler scopedTargetClassHandler(Invocations invocations) {
            return new ScopedTargetClassHandler(invocations);
        }
    }

    static class InheritsOverrideFromHandlersPackageSubscriber extends CrossPackageOverridingMid {
        InheritsOverrideFromHandlersPackageSubscriber(List<String> bodies) {
            super(bodies);
        }
    }

    static class OverridesWidenedHandlerSubscriber extends CrossPackageWideningMid {
        OverridesWidenedHandlerSubscriber(List<String> bodies) {
            super(bodies);
        }

        @Override
        public void on(CrossPackageEvent event) {
            bodies.add("sub-body");
        }
    }

    static class RefusedShadowingSubscriber extends CrossPackageShadowing {
        RefusedShadowingSubscriber(List<String> bodies) {
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

    // A JDK proxy whose target source names no target class is scanned as its own class, whose only handlers are the
    // interface's methods. Comparing those to the proxy class's generated methods dropped the handler without a trace.
    @Test
    void an_interface_handler_registers_for_a_jdk_proxy_bean_whose_target_source_names_no_target_class() {
        runner.withUserConfiguration(NoTargetClassJdkProxyConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(Proxy.isProxyClass(context.getBean("noTargetClassHandler").getClass())).describedAs("the bean is a JDK proxy").isTrue();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("no-target-class-handler");
            handlerFor(subscriptions, "no-target-class-handler").invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("no-target-class-body");
        });
    }

    interface NoTargetClassHandler {
        @Subscription(id = "no-target-class-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event);
    }

    static class NoTargetClassHandlerBean implements NoTargetClassHandler {
        private final Invocations invocations;

        NoTargetClassHandlerBean(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("no-target-class-body");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoTargetClassJdkProxyConfiguration {
        @Bean
        NoTargetClassHandler noTargetClassHandler(Invocations invocations) {
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setInterfaces(NoTargetClassHandler.class);
            proxyFactory.setTargetSource(new TargetSource() {
                @Override
                public Class<?> getTargetClass() {
                    return null;
                }

                @Override
                public boolean isStatic() {
                    return false;
                }

                @Override
                public Object getTarget() {
                    return new NoTargetClassHandlerBean(invocations);
                }

                @Override
                public void releaseTarget(Object target) {
                }
            });
            return (NoTargetClassHandler) proxyFactory.getProxy();
        }
    }

    @Test
    void an_interface_handler_registers_for_a_jdk_proxy_built_over_a_sub_interface_that_redeclares_it() {
        assertRegistersForNoTargetClassProxy(NoTargetClassRedeclaringProxyConfiguration.class, "proxy-redeclared", "proxy-redeclared-body");
    }

    @Test
    void an_interface_handler_registers_for_a_jdk_proxy_built_over_a_sub_interface_that_overrides_it_with_a_default_method() {
        assertRegistersForNoTargetClassProxy(NoTargetClassDefaultProxyConfiguration.class, "proxy-default", "proxy-default-body");
    }

    @Test
    void a_generic_interface_handler_registers_for_a_jdk_proxy_built_over_a_sub_interface_that_binds_the_type_argument() {
        assertRegistersForNoTargetClassProxy(NoTargetClassGenericProxyConfiguration.class, "proxy-generic", "proxy-generic-body");
    }

    private void assertRegistersForNoTargetClassProxy(Class<?> configuration, String id, String body) {
        runner.withUserConfiguration(configuration).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly(id);
            handlerFor(subscriptions, id).invoke(null, new TestEvent()).block();
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly(body);
        });
    }

    interface ProxyRedeclaredBase {
        @Subscription(id = "proxy-redeclared", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event);
    }

    interface ProxyRedeclaringInterface extends ProxyRedeclaredBase {
        @Override
        void on(TestEvent event);
    }

    interface ProxyDefaultBase {
        @Subscription(id = "proxy-default", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event);
    }

    interface ProxyDefaultingInterface extends ProxyDefaultBase {
        @Override
        default void on(TestEvent event) {
        }
    }

    interface ProxyGenericBase<E> {
        @Subscription(id = "proxy-generic", startupMode = StartupMode.BACKGROUND)
        void on(E event);
    }

    interface ProxyGenericBinding extends ProxyGenericBase<TestEvent> {
        @Override
        void on(TestEvent event);
    }

    static class ProxyRedeclaredTarget implements ProxyRedeclaringInterface {
        private final Invocations invocations;

        ProxyRedeclaredTarget(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("proxy-redeclared-body");
        }
    }

    static class ProxyDefaultTarget implements ProxyDefaultingInterface {
        private final Invocations invocations;

        ProxyDefaultTarget(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("proxy-default-body");
        }
    }

    static class ProxyGenericTarget implements ProxyGenericBinding {
        private final Invocations invocations;

        ProxyGenericTarget(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("proxy-generic-body");
        }
    }

    private static Object noTargetClassProxy(Class<?> proxiedInterface, Object target) {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setInterfaces(proxiedInterface);
        proxyFactory.setTargetSource(new TargetSource() {
            @Override
            public Class<?> getTargetClass() {
                return null;
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
            public void releaseTarget(Object target) {
            }
        });
        return proxyFactory.getProxy();
    }

    @Configuration(proxyBeanMethods = false)
    static class NoTargetClassRedeclaringProxyConfiguration {
        @Bean
        ProxyRedeclaringInterface redeclaringProxy(Invocations invocations) {
            return (ProxyRedeclaringInterface) noTargetClassProxy(ProxyRedeclaringInterface.class, new ProxyRedeclaredTarget(invocations));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoTargetClassDefaultProxyConfiguration {
        @Bean
        ProxyDefaultingInterface defaultingProxy(Invocations invocations) {
            return (ProxyDefaultingInterface) noTargetClassProxy(ProxyDefaultingInterface.class, new ProxyDefaultTarget(invocations));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoTargetClassGenericProxyConfiguration {
        @Bean
        ProxyGenericBinding genericProxy(Invocations invocations) {
            return (ProxyGenericBinding) noTargetClassProxy(ProxyGenericBinding.class, new ProxyGenericTarget(invocations));
        }
    }

    @Test
    void a_generic_interface_handler_read_from_the_class_and_later_from_a_jdk_proxys_interface_method_registers_once() {
        AtomicBoolean proxying = new AtomicBoolean();
        runner.withBean(JdkProxyingPostProcessor.class, () -> new JdkProxyingPostProcessor(proxying))
                .withUserConfiguration(PrototypeGenericHandlerConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    proxying.set(true);

                    Object[] second = new Object[1];
                    assertThatCode(() -> second[0] = context.getBean("prototypeGenericHandler")).doesNotThrowAnyException();

                    assertThat(Proxy.isProxyClass(second[0].getClass())).describedAs("the second instance reached the scan as a JDK proxy").isTrue();
                    verify(context.getBean(Subscriptions.class), times(1))
                            .subscribe(eq("proxied-generic-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
                });
    }

    @Test
    void a_handler_annotated_on_both_the_class_and_the_interface_method_registers_once_across_a_jdk_proxy() {
        AtomicBoolean proxying = new AtomicBoolean();
        runner.withBean(JdkProxyingPostProcessor.class, () -> new JdkProxyingPostProcessor(proxying))
                .withUserConfiguration(PrototypeDoublyAnnotatedHandlerConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    proxying.set(true);

                    Object[] second = new Object[1];
                    assertThatCode(() -> second[0] = context.getBean("prototypeDoublyAnnotatedHandler")).doesNotThrowAnyException();

                    assertThat(Proxy.isProxyClass(second[0].getClass())).describedAs("the second instance reached the scan as a JDK proxy").isTrue();
                    verify(context.getBean(Subscriptions.class), times(1))
                            .subscribe(eq("proxied-doubly-annotated-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
                });
    }

    interface GenericInterfaceHandler<E> {
        @Subscription(id = "proxied-generic-handler", startupMode = StartupMode.BACKGROUND)
        void on(E event);
    }

    static class GenericInterfaceHandlerBean implements GenericInterfaceHandler<TestEvent> {
        @Override
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeGenericHandlerConfiguration {
        @Bean
        @Scope("prototype")
        GenericInterfaceHandlerBean prototypeGenericHandler() {
            return new GenericInterfaceHandlerBean();
        }
    }

    interface DoublyAnnotatedHandler {
        @Subscription(id = "proxied-doubly-annotated-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event);
    }

    static class DoublyAnnotatedHandlerBean implements DoublyAnnotatedHandler {
        @Override
        @Subscription(id = "proxied-doubly-annotated-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeDoublyAnnotatedHandlerConfiguration {
        @Bean
        @Scope("prototype")
        DoublyAnnotatedHandlerBean prototypeDoublyAnnotatedHandler() {
            return new DoublyAnnotatedHandlerBean();
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
            if (!proxying.get() || !(bean instanceof InterfaceHandlerBean || bean instanceof GenericInterfaceHandlerBean || bean instanceof DoublyAnnotatedHandlerBean)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setInterfaces(bean.getClass().getInterfaces());
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
            return proxyFactory.getProxy();
        }

        @Override
        public int getOrder() {
            return HIGHEST_PRECEDENCE;
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // A package-private handler that an unannotated method of a subclass in another package shadows
    // ------------------------------------------------------------------------------------------------------

    @Test
    void an_unannotated_method_shadowing_a_package_private_handler_in_another_package_fails_startup_naming_both_methods_the_id_and_both_fixes() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        runner.withBean("subscriber", CrossPackageShadowingSubscriber.class, () -> new CrossPackageShadowingSubscriber(bodies)).run(context -> {
            String shadowing = CrossPackageShadowingSubscriber.class.getName() + "#on(CrossPackageEvent)";
            String shadowed = CrossPackagePackagePrivateHandler.class.getName() + "#on(CrossPackageEvent)";

            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Method " + shadowing + " does not override " + shadowed)
                    .hasMessageContaining("with id \"cross-package-package-private-handler\"")
                    .hasMessageContaining("Make both " + shadowed + " and " + shadowing + " protected or public")
                    .hasMessageContaining("or rename " + shadowing);
            assertThat(bodies).isEmpty();
        });
    }

    static class CrossPackageShadowingSubscriber extends CrossPackagePackagePrivateHandler {
        CrossPackageShadowingSubscriber(List<String> bodies) {
            super(bodies);
        }

        void on(CrossPackageEvent event) {
            bodies.add("shadowing-body");
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // A generic interface handler a bean overrides, behind a JDK proxy that is not a fixed singleton
    // ------------------------------------------------------------------------------------------------------

    // The scan reads the class behind the proxy, so it finds the override and its id, and not the interface method
    // under the interface's id. Reading the proxy's class would register the interface method as a second handler.
    @Test
    void a_generic_interface_handler_overridden_with_its_own_annotation_registers_once_behind_a_jdk_proxy_and_receives_the_event() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        jdkProxyingNonSingleton(GenericInterfaceOverrider.class, new AtomicInteger())
                .withBean("subscriber", GenericInterfaceOverrider.class, () -> new GenericInterfaceOverrider(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Proxy.isProxyClass(context.getBean("subscriber").getClass())).describedAs("the bean is a JDK proxy").isTrue();
                    Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

                    assertThat(subscribedIds(subscriptions)).containsExactly("overriding-generic-handler");
                    anyHandlerFor(subscriptions, "overriding-generic-handler").invoke(null, new Placed()).block();
                    assertThat(bodies).containsExactly("overrider-body");
                });
    }

    // ------------------------------------------------------------------------------------------------------
    // A handler runs through the advice of the proxy around its bean
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_handler_declared_on_an_interface_runs_through_the_advice_of_a_jdk_proxy() {
        AtomicInteger adviceCalls = new AtomicInteger();
        jdkProxyingNonSingleton(InterfaceHandlerBean.class, adviceCalls)
                .withBean("subscriber", InterfaceHandlerBean.class, InterfaceHandlerBean::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Proxy.isProxyClass(context.getBean("subscriber").getClass())).describedAs("the bean is a JDK proxy").isTrue();
                    adviceCalls.set(0);

                    anyHandlerFor(context.getBean(Subscriptions.class), "proxied-interface-handler").invoke(null, new TestEvent()).block();

                    assertThat(adviceCalls).hasValue(1);
                });
    }

    @Test
    void a_generic_interface_handler_overridden_by_the_bean_runs_through_the_advice_of_a_jdk_proxy_by_the_interface_method() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger adviceCalls = new AtomicInteger();
        jdkProxyingNonSingleton(GenericInterfaceOverrider.class, adviceCalls)
                .withBean("subscriber", GenericInterfaceOverrider.class, () -> new GenericInterfaceOverrider(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    adviceCalls.set(0);

                    anyHandlerFor(context.getBean(Subscriptions.class), "overriding-generic-handler").invoke(null, new Placed()).block();

                    assertThat(bodies).containsExactly("overrider-body");
                    assertThat(adviceCalls).hasValue(1);
                });
    }

    @Test
    void a_generic_interface_handler_overridden_by_the_bean_runs_through_the_advice_of_a_cglib_proxy() {
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger adviceCalls = new AtomicInteger();
        cglibProxying(GenericInterfaceOverrider.class, adviceCalls)
                .withBean("subscriber", GenericInterfaceOverrider.class, () -> new GenericInterfaceOverrider(bodies))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Proxy.isProxyClass(context.getBean("subscriber").getClass())).describedAs("the bean is a CGLIB proxy").isFalse();
                    adviceCalls.set(0);

                    anyHandlerFor(context.getBean(Subscriptions.class), "overriding-generic-handler").invoke(null, new Placed()).block();

                    assertThat(bodies).containsExactly("overrider-body");
                    assertThat(adviceCalls).hasValue(1);
                });
    }

    private ApplicationContextRunner jdkProxyingNonSingleton(Class<?> proxied, AtomicInteger adviceCalls) {
        return runner.withBean(NonSingletonJdkProxyingPostProcessor.class, () -> new NonSingletonJdkProxyingPostProcessor(proxied, adviceCalls));
    }

    interface GenericPlacedHandler<E> {
        @Subscription(id = "interface-generic-handler", startupMode = StartupMode.BACKGROUND)
        void on(E event);
    }

    static class GenericInterfaceOverrider implements GenericPlacedHandler<Placed> {
        private final List<String> bodies;

        GenericInterfaceOverrider(List<String> bodies) {
            this.bodies = bodies;
        }

        @Override
        @Subscription(id = "overriding-generic-handler", startupMode = StartupMode.BACKGROUND)
        public void on(Placed event) {
            bodies.add("overrider-body");
        }
    }

    // Wraps the bean in a JDK proxy of its interfaces whose target source is not static, so nothing can unwrap it to a
    // target. Runs before the Occurrent post processor, so that is the instance it is handed.
    static class NonSingletonJdkProxyingPostProcessor implements BeanPostProcessor, PriorityOrdered {
        private final Class<?> proxied;
        private final AtomicInteger adviceCalls;

        NonSingletonJdkProxyingPostProcessor(Class<?> proxied, AtomicInteger adviceCalls) {
            this.proxied = proxied;
            this.adviceCalls = adviceCalls;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!proxied.isInstance(bean)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setInterfaces(bean.getClass().getInterfaces());
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
                adviceCalls.incrementAndGet();
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
    // A prototype whose factory returns an instance of another class than before
    // ------------------------------------------------------------------------------------------------------

    @Test
    void a_prototype_returning_another_class_that_declares_the_same_public_handler_is_skipped() {
        assertSecondInstanceIsSkipped(PublicVaryingA::new, PublicVaryingB::new);
    }

    @Test
    void a_prototype_returning_another_class_that_declares_the_same_private_handler_is_skipped() {
        assertSecondInstanceIsSkipped(PrivateVaryingA::new, PrivateVaryingB::new);
    }

    @Test
    void a_prototype_returning_another_class_that_inherits_the_same_public_handler_from_a_superclass_is_skipped() {
        assertSecondInstanceIsSkipped(PublicVaryingA::new, InheritingVaryingB::new);
    }

    // The first instance registers the handler, the second is built by the same factory after startup, and its class
    // has the handler under another declaring class. It is the same handler, so it must not take the id again, which
    // would fail the creation of the bean that asked for it.
    private void assertSecondInstanceIsSkipped(Function<List<String>, VaryingMarker> first, Function<List<String>, VaryingMarker> second) {
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger created = new AtomicInteger();
        runner.withBean("varying", VaryingMarker.class, () -> created.getAndIncrement() == 0 ? first.apply(bodies) : second.apply(bodies),
                beanDefinition -> beanDefinition.setScope("prototype")).run(context -> {
            assertThat(context).hasNotFailed();
            context.getBean("varying");

            assertThatCode(() -> context.getBean("varying")).doesNotThrowAnyException();

            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(created).describedAs("both instances were built").hasValue(2);
            assertThat(subscribedIds(subscriptions)).containsExactly("varying-handler");
            anyHandlerFor(subscriptions, "varying-handler").invoke(null, new TestEvent()).block();
            assertThat(bodies).describedAs("the handler stays on the instance that registered it").containsExactly("first-body");
        });
    }

    // Declared as the return type, so the startup scan cannot see the handler of the class behind it.
    interface VaryingMarker {
    }

    static class PublicVaryingA implements VaryingMarker {
        private final List<String> bodies;

        PublicVaryingA(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "varying-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            bodies.add("first-body");
        }
    }

    static class PublicVaryingB implements VaryingMarker {
        private final List<String> bodies;

        PublicVaryingB(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "varying-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            bodies.add("second-body");
        }
    }

    static class PrivateVaryingA implements VaryingMarker {
        private final List<String> bodies;

        PrivateVaryingA(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "varying-handler", startupMode = StartupMode.BACKGROUND)
        private void on(TestEvent event) {
            bodies.add("first-body");
        }
    }

    static class PrivateVaryingB implements VaryingMarker {
        private final List<String> bodies;

        PrivateVaryingB(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "varying-handler", startupMode = StartupMode.BACKGROUND)
        private void on(TestEvent event) {
            bodies.add("second-body");
        }
    }

    abstract static class PublicVaryingBase {
        final List<String> bodies;

        PublicVaryingBase(List<String> bodies) {
            this.bodies = bodies;
        }

        @Subscription(id = "varying-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            bodies.add("second-body");
        }
    }

    static class InheritingVaryingB extends PublicVaryingBase implements VaryingMarker {
        InheritingVaryingB(List<String> bodies) {
            super(bodies);
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
