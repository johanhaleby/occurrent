/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.common;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.scaffold.MethodGraph;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.StubMethod;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.springboot.common.internal.HandlerMethod;
import org.occurrent.springboot.common.internal.HandlerScan;
import org.occurrent.springboot.common.internal.RegisteredHandlers;
import org.occurrent.springboot.common.between.PublicFactoryThatDoesNotOverride;
import org.occurrent.springboot.common.between.PublicMethodThatDoesNotOverride;
import org.occurrent.springboot.common.bridged.PublicLeafWithBridges;
import org.occurrent.springboot.common.declaring.OverridesHandlerBelowAMethodThatDoesNot;
import org.occurrent.springboot.common.declaring.PackagePrivateFactoryBase;
import org.occurrent.springboot.common.declaring.PackagePrivateHandlerBase;
import org.occurrent.springboot.common.generic.LeafInTheGenericHandlersPackage;
import org.occurrent.springboot.common.generic.OverridesGenericPackagePrivateHandler;
import org.occurrent.springboot.common.declaring.PackagePrivateOverridingFactoryMid;
import org.occurrent.springboot.common.declaring.PackagePrivateOverridingMid;
import org.occurrent.springboot.common.declaring.PublicOverridingMid;
import org.occurrent.springboot.common.declaring.PublicOverridingOfPackagePrivateMid;
import org.occurrent.springboot.common.declaring.SubclassInDeclaringPackage;
import org.occurrent.springboot.common.declaring.SubclassInDeclaringPackageOfPublicFactory;
import org.occurrent.springboot.common.shadowed.a.A;
import org.occurrent.springboot.common.shadowed.a.LeafOfAnnotated;
import org.occurrent.springboot.common.shadowed.a.LeafOfPrivate;
import org.occurrent.springboot.common.shadowed.b.MidAnnotated;
import org.occurrent.springboot.common.other.CrossPackageMethods;
import org.occurrent.springboot.common.other.CrossPackageSubscriptionBase;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.context.support.GenericApplicationContext;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class SubscriptionAnnotationsTest {

    // ------------------------------------------------------------------------------------------------------
    // invokeDescriptorFactory (ADR 127 section 4: unwrapping a descriptor factory's bean before invoking it)
    // ------------------------------------------------------------------------------------------------------

    static class CglibFactoryHolder {
        String factory() {
            return "built";
        }
    }

    @Test
    void invokeDescriptorFactory_on_a_cglib_proxied_singleton_bean_runs_the_factory_directly_without_the_proxys_advice() throws Exception {
        AtomicInteger adviceInvocations = new AtomicInteger();
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(new CglibFactoryHolder());
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> {
            adviceInvocations.incrementAndGet();
            return invocation.proceed();
        });
        CglibFactoryHolder proxy = (CglibFactoryHolder) proxyFactory.getProxy();

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, CglibFactoryHolder.class.getDeclaredMethod("factory"));

        assertThat(result).isEqualTo("built");
        assertThat(adviceInvocations).hasValue(0);
    }

    // A TargetSource that is not a SingletonTargetSource, the one case ultimateTarget leaves proxied rather than
    // unwrapping. isStatic() = false is what AopProxyUtils.getSingletonTarget checks to skip it.
    static final class AlwaysFreshTargetSource implements TargetSource {
        private final Class<?> targetClass;

        AlwaysFreshTargetSource(Class<?> targetClass) {
            this.targetClass = targetClass;
        }

        @Override
        public Class<?> getTargetClass() {
            return targetClass;
        }

        @Override
        public boolean isStatic() {
            return false;
        }

        @Override
        public Object getTarget() throws Exception {
            return targetClass.getDeclaredConstructor().newInstance();
        }

        @Override
        public void releaseTarget(Object target) {
        }
    }

    interface NonSingletonMarker {
    }

    static class NonSingletonFactoryHolder implements NonSingletonMarker {
        String factory() {
            return "built";
        }
    }

    @Test
    void invokeDescriptorFactory_on_a_jdk_proxy_backed_by_a_non_singleton_target_source_fails_naming_the_annotation_and_both_remedies() throws Exception {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetSource(new AlwaysFreshTargetSource(NonSingletonFactoryHolder.class));
        proxyFactory.setInterfaces(NonSingletonMarker.class);
        proxyFactory.setProxyTargetClass(false);
        NonSingletonMarker proxy = (NonSingletonMarker) proxyFactory.getProxy();

        Method method = NonSingletonFactoryHolder.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Snapshot", proxy, method))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("factory")
                .hasMessageContaining("spring.aop.proxy-target-class=true")
                .hasMessageContaining("move the factory method off the advised bean");
    }

    static class InheritedFactoryBase {
        String factory() {
            return "base-built";
        }
    }

    static class InheritingFactoryHolder extends InheritedFactoryBase {
    }

    static class OverridingFactoryHolder extends InheritedFactoryBase {
        @Override
        String factory() {
            return "override-built";
        }
    }

    @Test
    void invokeDescriptorFactory_runs_a_factory_the_beans_class_inherits_unchanged() throws Exception {
        Method inherited = InheritedFactoryBase.class.getDeclaredMethod("factory");

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new InheritingFactoryHolder(), inherited);

        assertThat(result).isEqualTo("base-built");
    }

    @Test
    void invokeDescriptorFactory_runs_a_factory_the_beans_class_inherits_unchanged_through_a_cglib_proxy() throws Exception {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(new InheritingFactoryHolder());
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Snapshot", proxyFactory.getProxy(), InheritedFactoryBase.class.getDeclaredMethod("factory"));

        assertThat(result).isEqualTo("base-built");
    }

    @Test
    void invokeDescriptorFactory_refuses_a_bean_whose_class_overrides_the_inherited_factory() throws Exception {
        Method inherited = InheritedFactoryBase.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new OverridingFactoryHolder(), inherited))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection")
                .hasMessageContaining(OverridingFactoryHolder.class.getName())
                .hasMessageContaining(InheritedFactoryBase.class.getName());
    }

    @Test
    void invokeDescriptorFactory_refuses_a_bean_whose_class_does_not_inherit_the_factory() throws Exception {
        Method factory = InheritedFactoryBase.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Saga", new CglibFactoryHolder(), factory))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Saga")
                .hasMessageContaining(CglibFactoryHolder.class.getName());
    }

    @Test
    void invokeDescriptorFactory_runs_a_factory_the_target_of_a_jdk_proxy_inherits_unchanged() throws Exception {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(new InheritingFactoryHolderWithMarker());
        proxyFactory.setInterfaces(NonSingletonMarker.class);
        proxyFactory.setProxyTargetClass(false);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
        Object jdkProxy = proxyFactory.getProxy();
        Method inherited = InheritedFactoryBase.class.getDeclaredMethod("factory");

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Snapshot", jdkProxy, inherited);

        assertThat(result).isEqualTo("base-built");
    }

    static class InheritingFactoryHolderWithMarker extends InheritedFactoryBase implements NonSingletonMarker {
    }

    // ------------------------------------------------------------------------------------------------------
    // handlerMethods (issue #983: inherited handlers, interface default methods, overrides and bridge methods)
    // ------------------------------------------------------------------------------------------------------

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Marked {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface OtherMarked {
    }

    private static final List<Class<? extends Annotation>> MARKERS = List.of(Marked.class, OtherMarked.class);

    abstract static class AnnotatedBase {
        @Marked
        public void handle(String event) {
        }

        public void notAHandler(String event) {
        }
    }

    static class InheritsHandler extends AnnotatedBase {
    }

    interface DefaultHandler {
        @Marked
        default void handle(String event) {
        }
    }

    static class GetsDefaultHandler implements DefaultHandler {
    }

    static class OverridesWithoutAnnotation extends AnnotatedBase {
        @Override
        public void handle(String event) {
        }
    }

    static class OverridesDefaultWithoutAnnotation implements DefaultHandler {
        @Override
        public void handle(String event) {
        }
    }

    static class GenericBase<E> {
        @Marked
        public void handle(E event) {
        }
    }

    static class ConcreteOfGeneric extends GenericBase<String> {
        @Override
        public void handle(String event) {
        }
    }

    static class ReDeclaresOnGeneric extends GenericBase<String> {
        @Override
        @OtherMarked
        public void handle(String event) {
        }
    }

    static class PrivateBase {
        @Marked
        private void handle(String event) {
        }
    }

    static class PrivateSub extends PrivateBase {
        @Marked
        private void handle(String event) {
        }
    }

    @Test
    void handlerMethods_finds_a_method_the_class_inherits_from_a_superclass() throws Exception {
        assertThat(HandlerScan.handlerMethods(InheritsHandler.class, MARKERS))
                .containsExactly(AnnotatedBase.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_finds_an_interface_default_method() throws Exception {
        assertThat(HandlerScan.handlerMethods(GetsDefaultHandler.class, MARKERS))
                .containsExactly(DefaultHandler.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_leaves_out_a_method_without_any_of_the_annotations() {
        assertThat(HandlerScan.handlerMethods(InheritsHandler.class, List.of(OtherMarked.class))).isEmpty();
    }

    @Test
    void handlerMethods_gives_an_override_that_drops_the_annotation_once_as_the_override() throws Exception {
        assertThat(HandlerScan.handlerMethods(OverridesWithoutAnnotation.class, MARKERS))
                .containsExactly(OverridesWithoutAnnotation.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_gives_an_override_of_an_interface_default_method_once_as_the_override() throws Exception {
        assertThat(HandlerScan.handlerMethods(OverridesDefaultWithoutAnnotation.class, MARKERS))
                .containsExactly(OverridesDefaultWithoutAnnotation.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_leaves_out_the_bridge_method_of_a_generic_override() {
        assertThat(HandlerScan.handlerMethods(ConcreteOfGeneric.class, MARKERS))
                .singleElement()
                .satisfies(method -> {
                    assertThat(method.isBridge()).isFalse();
                    assertThat(method.getDeclaringClass()).isEqualTo(ConcreteOfGeneric.class);
                    assertThat(method.getParameterTypes()).containsExactly(String.class);
                });
    }

    @Test
    void handlerMethods_leaves_out_the_bridge_method_when_the_generic_override_re_declares_an_annotation() {
        assertThat(HandlerScan.handlerMethods(ReDeclaresOnGeneric.class, MARKERS))
                .singleElement()
                .satisfies(method -> assertThat(method.isBridge()).isFalse());
    }

    @Test
    void handlerMethods_keeps_two_private_methods_of_one_signature_on_a_class_and_its_superclass_apart() throws Exception {
        assertThat(HandlerScan.handlerMethods(PrivateSub.class, MARKERS))
                .containsExactlyInAnyOrder(PrivateBase.class.getDeclaredMethod("handle", String.class), PrivateSub.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void synchronous_with_startAt_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", true, true, false, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Projection")
                .hasMessageContaining("orders")
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void synchronous_with_startAtGlobalPosition_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", true, false, true, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("ledger")
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void synchronous_with_resumeBehavior_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", true, false, false, true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void synchronous_with_startupMode_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", true, false, false, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("ledger")
                .hasMessageContaining("mode = SYNCHRONOUS")
                .hasMessageContaining("startupMode");
    }

    @Test
    void startAt_together_with_startAtGlobalPosition_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", false, true, true, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("ledger")
                .hasMessageContaining("both startAt and startAtGlobalPosition");
    }

    @Test
    void asynchronous_with_all_catch_up_knobs_except_the_startAt_pair_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", false, true, false, true, true))
                .doesNotThrowAnyException();
    }

    @Test
    void synchronous_without_any_start_knob_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", true, false, false, false, false))
                .doesNotThrowAnyException();
    }

    @Test
    void asynchronous_with_only_startAtGlobalPosition_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", false, false, true, true, true))
                .doesNotThrowAnyException();
    }

    // ------------------------------------------------------------------------------------------------------
    // analyzeParameters + bindArguments (the @StreamId / @StreamVersion handler-parameter support)
    // ------------------------------------------------------------------------------------------------------

    record TestEvent(String value) {
    }

    @SuppressWarnings("unused")
    static class Handlers {
        void eventOnly(TestEvent event) {
        }

        void eventAndMetadata(TestEvent event, EventMetadata metadata) {
        }

        void eventAndStreamId(TestEvent event, @StreamId String streamId) {
        }

        void eventAndStreamVersion(TestEvent event, @StreamVersion long streamVersion) {
        }

        void everything(@StreamVersion long streamVersion, EventMetadata metadata, @StreamId String streamId, TestEvent event) {
        }

        void streamVersionBoxed(TestEvent event, @StreamVersion Long streamVersion) {
        }

        void streamIdWrongType(TestEvent event, @StreamId long streamId) {
        }

        void streamVersionWrongType(TestEvent event, @StreamVersion String streamVersion) {
        }

        void duplicateStreamId(TestEvent event, @StreamId String a, @StreamId String b) {
        }

        void bothOnOneParameter(TestEvent event, @StreamId @StreamVersion String streamId) {
        }

        void onOrderEvent(OrderEvent event) {
        }

        void onOpenEvent(OpenEvent event) {
        }

        void onReopenedEvent(ReopenedEvent event) {
        }
    }

    private static Method handler(String name) {
        for (Method method : Handlers.class.getDeclaredMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        throw new IllegalStateException("No handler named " + name);
    }

    private static List<HandlerParameter> analyzeStream(String handlerName) {
        return SubscriptionAnnotations.analyzeParameters(handler(handlerName), SubscriptionAnnotations::isStreamMetadataParameter, true);
    }

    // streamId "s-1" and streamVersion 42, keyed by the CloudEvent extension names EventMetadata reads.
    private static final EventMetadata METADATA = new EventMetadata(Map.of("streamid", "s-1", "streamversion", 42L));

    @Test
    void classifies_an_event_only_handler() {
        List<HandlerParameter> parameters = analyzeStream("eventOnly");
        assertThat(parameters).extracting(HandlerParameter::kind).containsExactly(HandlerParameterKind.EVENT);
    }

    @Test
    void classifies_event_and_metadata() {
        List<HandlerParameter> parameters = analyzeStream("eventAndMetadata");
        assertThat(parameters).extracting(HandlerParameter::kind)
                .containsExactly(HandlerParameterKind.EVENT, HandlerParameterKind.METADATA);
    }

    @Test
    void classifies_stream_id_and_version_in_any_order() {
        List<HandlerParameter> parameters = analyzeStream("everything");
        assertThat(parameters).extracting(HandlerParameter::kind).containsExactly(
                HandlerParameterKind.STREAM_VERSION, HandlerParameterKind.METADATA, HandlerParameterKind.STREAM_ID, HandlerParameterKind.EVENT);
    }

    @Test
    void binds_event_stream_id_and_version_by_kind() {
        List<HandlerParameter> parameters = analyzeStream("everything");
        TestEvent event = new TestEvent("x");
        Object[] arguments = SubscriptionAnnotations.bindArguments(parameters, event, METADATA, METADATA);
        assertThat(arguments).containsExactly(42L, METADATA, "s-1", event);
    }

    @Test
    void binds_a_boxed_long_stream_version() {
        List<HandlerParameter> parameters = analyzeStream("streamVersionBoxed");
        TestEvent event = new TestEvent("x");
        Object[] arguments = SubscriptionAnnotations.bindArguments(parameters, event, METADATA, METADATA);
        assertThat(arguments).containsExactly(event, 42L);
    }

    @Test
    void binds_only_the_event_when_that_is_the_only_parameter() {
        List<HandlerParameter> parameters = analyzeStream("eventOnly");
        TestEvent event = new TestEvent("x");
        Object[] arguments = SubscriptionAnnotations.bindArguments(parameters, event, METADATA, METADATA);
        assertThat(arguments).containsExactly(event);
    }

    @Test
    void rejects_a_stream_id_that_is_not_a_string() {
        assertThatThrownBy(() -> analyzeStream("streamIdWrongType"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@StreamId")
                .hasMessageContaining("String");
    }

    @Test
    void rejects_a_stream_version_that_is_not_a_long() {
        assertThatThrownBy(() -> analyzeStream("streamVersionWrongType"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@StreamVersion")
                .hasMessageContaining("long or Long");
    }

    @Test
    void rejects_more_than_one_stream_id() {
        assertThatThrownBy(() -> analyzeStream("duplicateStreamId"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most one @StreamId");
    }

    @Test
    void rejects_a_parameter_annotated_with_both_stream_id_and_stream_version() {
        assertThatThrownBy(() -> analyzeStream("bothOnOneParameter"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both @StreamId and @StreamVersion");
    }

    @Test
    void rejects_stream_accessors_when_not_supported() {
        assertThatThrownBy(() -> SubscriptionAnnotations.analyzeParameters(handler("eventAndStreamId"), SubscriptionAnnotations::isDcbMetadataParameter, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@StreamId")
                .hasMessageContaining("only supported on");
    }

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    interface OpenEvent {
    }

    sealed interface ReopenedEvent permits ReopenedBase {
    }

    static non-sealed class ReopenedBase implements ReopenedEvent {
    }

    private static List<Class<?>> resolveOrderEventTypes(Class<?>... eventTypesInAnnotation) {
        List<Class<OrderEvent>> resolved = SubscriptionAnnotations.resolveDomainEventTypes("order-subscription",
                new Handlers(), handler("onOrderEvent"), OrderEvent.class, eventTypesInAnnotation, "@Subscription");
        return List.copyOf(resolved);
    }

    @Test
    void a_sealed_event_type_resolves_to_the_declared_type_and_the_concrete_types_it_permits() {
        // The declared type used to be dropped, so an event stored under its own CloudEvent type never matched. That
        // happens with a CloudEventTypeMapper that maps a hierarchy onto the type string it was declared with.
        assertThat(resolveOrderEventTypes())
                .containsExactly(OrderEvent.class, OrderPlaced.class, PaymentReserved.class);
    }

    @Test
    void a_sealed_event_type_listed_in_the_annotation_keeps_the_declared_type_too() {
        assertThat(resolveOrderEventTypes(OrderEvent.class))
                .containsExactly(OrderEvent.class, OrderPlaced.class, PaymentReserved.class);
    }

    @Test
    void a_concrete_event_type_resolves_to_itself() {
        assertThat(resolveOrderEventTypes(OrderPlaced.class)).containsExactly(OrderPlaced.class);
    }

    @Test
    void refuses_an_interface_that_is_not_sealed() {
        assertThatThrownBy(() -> SubscriptionAnnotations.resolveDomainEventTypes("open-subscription", new Handlers(),
                handler("onOpenEvent"), OpenEvent.class, new Class<?>[0], "@Subscription"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OpenEvent.class.getName())
                .hasMessageContaining("open-subscription")
                .hasMessageContaining("cannot all be enumerated");
    }

    @Test
    void refuses_an_array_listed_in_eventTypes_with_a_message_covering_both_remedies() {
        // An array can never be sealed or final in a way that fixes this, so this shape gets its own message rather
        // than the "cannot all be enumerated" one, which would tell a reader to do something impossible. The message
        // cannot tell whether the array came from eventTypes or from the handler's own parameter, so it names both.
        assertThatThrownBy(() -> resolveOrderEventTypes(OrderPlaced[].class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("order-subscription")
                .hasMessageContaining("does not support an array")
                .hasMessageContaining("handler method's own event parameter")
                .hasMessageContaining("eventTypes attribute")
                .hasMessageNotContaining("cannot all be enumerated")
                .hasMessageNotContaining("final or sealed");
    }

    @Test
    void refuses_an_array_handler_parameter_with_the_same_dual_remedy_message() {
        // Unlike the case above, no eventTypes is specified at all here, the array is the handler's own declared
        // parameter type. Listing a concrete type in eventTypes would not fix this: resolveDomainEventTypes checks
        // eventTypes elements for assignability to the handler's parameter type, and no element type is assignable to
        // an array of it, so that path throws a different exception (IllegalStateException) rather than helping.
        assertThatThrownBy(() -> SubscriptionAnnotations.<OrderPlaced[]>resolveDomainEventTypes("array-param-subscription",
                new Handlers(), handler("onOrderEvent"), OrderPlaced[].class, new Class<?>[0], "@Subscription"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("array-param-subscription")
                .hasMessageContaining("does not support an array")
                .hasMessageContaining("handler method's own event parameter")
                .hasMessageContaining("eventTypes attribute")
                .hasMessageNotContaining("cannot all be enumerated")
                .hasMessageNotContaining("final or sealed");
    }

    @Test
    void refuses_a_sealed_hierarchy_reopened_below_the_declared_type() {
        // Unlike the two shapes above, this one is new in 0.33.0. 0.32.0 accepted it and matched only ReopenedBase's own
        // CloudEvent type, silently missing every concrete subtype of ReopenedBase.
        assertThatThrownBy(() -> SubscriptionAnnotations.resolveDomainEventTypes("reopened-subscription", new Handlers(),
                handler("onReopenedEvent"), ReopenedEvent.class, new Class<?>[0], "@Subscription"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReopenedEvent.class.getName())
                .hasMessageContaining("reopened-subscription")
                .hasMessageContaining("cannot all be enumerated");
    }

    // ------------------------------------------------------------------------------------------------------
    // resolveFeedBeanType (the read-only mirror of resolveFeedBean's own resolution rules)
    // ------------------------------------------------------------------------------------------------------

    interface CandidateA {
    }

    interface CandidateB {
    }

    static class CandidateAImpl implements CandidateA {
    }

    static class CandidateBImpl implements CandidateB {
    }

    @Test
    void resolveFeedBeanType_reads_an_explicit_subscriptionModel_type_straight_off_the_call_with_no_bean_lookup_at_all() {
        // No bean of any candidate type is registered, and the context is never even refreshed, so this only passes
        // if the explicit type short-circuits before touching the context.
        GenericApplicationContext context = new GenericApplicationContext();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, CandidateA.class, "", CandidateA.class, CandidateB.class);
        assertThat(type).isEqualTo(CandidateA.class);
    }

    @Test
    void resolveFeedBeanType_resolves_a_named_bean_by_its_actual_type() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("feed", CandidateBImpl.class);
        context.refresh();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, Void.class, "feed", CandidateA.class, CandidateB.class);
        assertThat(type).isEqualTo(CandidateBImpl.class);
    }

    @Test
    void resolveFeedBeanType_answers_null_for_a_name_with_no_such_bean() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, Void.class, "missing", CandidateA.class, CandidateB.class);
        assertThat(type).isNull();
    }

    @Test
    void resolveFeedBeanType_resolves_the_unique_bean_of_one_candidate_type() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("feed", CandidateAImpl.class);
        context.refresh();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, Void.class, "", CandidateA.class, CandidateB.class);
        assertThat(type).isEqualTo(CandidateAImpl.class);
    }

    @Test
    void resolveFeedBeanType_answers_null_when_no_candidate_bean_exists() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, Void.class, "", CandidateA.class, CandidateB.class);
        assertThat(type).isNull();
    }

    @Test
    void resolveFeedBeanType_answers_null_when_several_candidate_beans_exist() {
        // The same ambiguity resolveFeedBean itself would refuse to pick between. A caller sees null here rather
        // than a guess, and resolveFeedBean is still what raises the real error, at registration.
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("feedA", CandidateAImpl.class);
        context.registerBean("feedB", CandidateBImpl.class);
        context.refresh();
        Class<?> type = SubscriptionAnnotations.resolveFeedBeanType(context, Void.class, "", CandidateA.class, CandidateB.class);
        assertThat(type).isNull();
    }

    // ------------------------------------------------------------------------------------------------------
    // invokeDescriptorFactory on a proxy left proxied (a TargetSource that is not a fixed singleton)
    // ------------------------------------------------------------------------------------------------------

    interface DeclaresFactory {
        String factory();
    }

    static class ImplementsFactory implements DeclaresFactory {
        @Override
        public String factory() {
            return "interface-factory-built";
        }
    }

    static class OverridesInterfaceFactory extends ImplementsFactory {
        @Override
        public String factory() {
            return "overridden-factory-built";
        }
    }

    static class ClassOnlyFactory implements NonSingletonMarker {
        public String factory() {
            return "class-factory-built";
        }
    }

    private static Object jdkProxyOverAFreshTarget(Class<?> targetClass, Class<?> proxiedInterface, AtomicInteger adviceInvocations) {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetSource(new AlwaysFreshTargetSource(targetClass));
        proxyFactory.setInterfaces(proxiedInterface);
        proxyFactory.setProxyTargetClass(false);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> {
            adviceInvocations.incrementAndGet();
            return invocation.proceed();
        });
        return proxyFactory.getProxy();
    }

    @Test
    void invokeDescriptorFactory_runs_an_interface_declared_factory_through_a_jdk_proxy_whose_target_source_is_not_fixed() throws Exception {
        AtomicInteger adviceInvocations = new AtomicInteger();
        Object proxy = jdkProxyOverAFreshTarget(ImplementsFactory.class, DeclaresFactory.class, adviceInvocations);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresFactory.class.getDeclaredMethod("factory"));

        assertThat(result).isEqualTo("interface-factory-built");
        assertThat(adviceInvocations).describedAs("the factory ran through the proxy").hasValue(1);
    }

    @Test
    void invokeDescriptorFactory_refuses_a_factory_the_interfaces_of_a_jdk_proxy_do_not_declare() throws Exception {
        Object proxy = jdkProxyOverAFreshTarget(ClassOnlyFactory.class, NonSingletonMarker.class, new AtomicInteger());

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Snapshot", proxy, ClassOnlyFactory.class.getDeclaredMethod("factory")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Snapshot factory")
                .hasMessageContaining("the proxy does not implement " + ClassOnlyFactory.class.getName())
                .hasMessageContaining("spring.aop.proxy-target-class=true");
    }

    @Test
    void invokeDescriptorFactory_refuses_an_implementation_that_overrides_the_factory_the_scan_read() throws Exception {
        Method scanned = ImplementsFactory.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Saga", new OverridesInterfaceFactory(), scanned))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Saga factory")
                .hasMessageContaining(OverridesInterfaceFactory.class.getName() + " does not run the factory the scan read from " + ImplementsFactory.class.getName())
                .hasMessageContaining("it either overrides it or does not inherit it");
    }

    static class InheritsCrossPackageMethods extends CrossPackageMethods {
    }

    private static Method crossPackagePackagePrivate() throws Exception {
        return CrossPackageMethods.class.getDeclaredMethod("packagePrivate");
    }

    private static InheritsCrossPackageMethods cglibProxyOfInheritingBean(boolean fixedTarget) {
        ProxyFactory proxyFactory = new ProxyFactory();
        if (fixedTarget) {
            proxyFactory.setTarget(new InheritsCrossPackageMethods());
        } else {
            proxyFactory.setTargetSource(new AlwaysFreshTargetSource(InheritsCrossPackageMethods.class));
        }
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
        return (InheritsCrossPackageMethods) proxyFactory.getProxy();
    }

    @Test
    void invokeDescriptorFactory_refuses_a_package_private_factory_from_another_package_on_a_cglib_proxy_left_proxied() throws Exception {
        InheritsCrossPackageMethods proxy = cglibProxyOfInheritingBean(false);

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, crossPackagePackagePrivate()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection factory")
                .hasMessageContaining("it is package-private and declared in " + CrossPackageMethods.class.getName())
                .hasMessageContaining("outside the package of " + InheritsCrossPackageMethods.class.getName());
    }

    @Test
    void invokeDescriptorFactory_runs_a_package_private_factory_from_another_package_on_a_cglib_proxy_that_is_unwrapped() throws Exception {
        InheritsCrossPackageMethods proxy = cglibProxyOfInheritingBean(true);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, crossPackagePackagePrivate());

        assertThat(result).isEqualTo("package-private-ran");
    }

    @Test
    void invokeDescriptorFactory_runs_a_static_package_private_factory_from_another_package_on_a_cglib_proxy_left_proxied() throws Exception {
        InheritsCrossPackageMethods proxy = cglibProxyOfInheritingBean(false);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, CrossPackageMethods.class.getDeclaredMethod("packagePrivateStatic"));

        assertThat(result).isEqualTo("package-private-static-ran");
    }

    @Test
    void invokeDescriptorFactory_runs_a_package_private_factory_from_another_package_on_a_bean_nothing_proxies() throws Exception {
        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new InheritsCrossPackageMethods(), crossPackagePackagePrivate());

        assertThat(result).isEqualTo("package-private-ran");
    }

    // ------------------------------------------------------------------------------------------------------
    // invokeDescriptorFactory: a private or final factory on a cglib proxy left proxied
    // ------------------------------------------------------------------------------------------------------

    static class DeclaresPrivateAndFinalFactories {
        @SuppressWarnings("unused")
        private String privateFactory() {
            return "private-ran";
        }

        public final String finalFactory() {
            return "final-ran";
        }

        public static String staticFactory() {
            return "static-ran";
        }
    }

    static class InheritsPrivateAndFinalFactories extends DeclaresPrivateAndFinalFactories {
    }

    private static <T> T cglibProxyLeftProxied(Class<T> beanClass) {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetSource(new AlwaysFreshTargetSource(beanClass));
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
        return beanClass.cast(proxyFactory.getProxy());
    }

    @Test
    void invokeDescriptorFactory_refuses_an_inherited_private_factory_on_a_cglib_proxy_left_proxied() throws Exception {
        InheritsPrivateAndFinalFactories proxy = cglibProxyLeftProxied(InheritsPrivateAndFinalFactories.class);

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresPrivateAndFinalFactories.class.getDeclaredMethod("privateFactory")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection factory")
                .hasMessageContaining("it is inherited from " + DeclaresPrivateAndFinalFactories.class.getName() + " and private");
    }

    @Test
    void invokeDescriptorFactory_refuses_an_inherited_final_factory_on_a_cglib_proxy_left_proxied() throws Exception {
        InheritsPrivateAndFinalFactories proxy = cglibProxyLeftProxied(InheritsPrivateAndFinalFactories.class);

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresPrivateAndFinalFactories.class.getDeclaredMethod("finalFactory")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection factory")
                .hasMessageContaining("it is inherited from " + DeclaresPrivateAndFinalFactories.class.getName() + " and final");
    }

    @Test
    void invokeDescriptorFactory_runs_a_private_factory_the_beans_own_class_declares_on_a_cglib_proxy_left_proxied() throws Exception {
        DeclaresPrivateAndFinalFactories proxy = cglibProxyLeftProxied(DeclaresPrivateAndFinalFactories.class);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresPrivateAndFinalFactories.class.getDeclaredMethod("privateFactory"));

        assertThat(result).isEqualTo("private-ran");
    }

    @Test
    void invokeDescriptorFactory_runs_a_final_factory_the_beans_own_class_declares_on_a_cglib_proxy_left_proxied() throws Exception {
        DeclaresPrivateAndFinalFactories proxy = cglibProxyLeftProxied(DeclaresPrivateAndFinalFactories.class);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresPrivateAndFinalFactories.class.getDeclaredMethod("finalFactory"));

        assertThat(result).isEqualTo("final-ran");
    }

    @Test
    void invokeDescriptorFactory_runs_an_inherited_static_factory_on_a_cglib_proxy_left_proxied() throws Exception {
        InheritsPrivateAndFinalFactories proxy = cglibProxyLeftProxied(InheritsPrivateAndFinalFactories.class);

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, DeclaresPrivateAndFinalFactories.class.getDeclaredMethod("staticFactory"));

        assertThat(result).isEqualTo("static-ran");
    }

    // ------------------------------------------------------------------------------------------------------
    // HandlerMethod
    // ------------------------------------------------------------------------------------------------------

    interface GenericHandler<E> {
        @Marked
        void on(E event);
    }

    static class GenericHandlerBean implements GenericHandler<String> {
        @Override
        public void on(String event) {
        }
    }

    static class PrivateHandlerBase {
        @Marked
        @SuppressWarnings("unused")
        private void handle(String event) {
        }
    }

    static class PrivateHandlerSubclass extends PrivateHandlerBase {
        @Marked
        @SuppressWarnings("unused")
        private void handle(String event) {
        }
    }

    static class PackagePrivateSubclassOfOtherPackage extends CrossPackageMethods {
        @SuppressWarnings("unused")
        String packagePrivate() {
            return "subclass";
        }
    }

    @Test
    void handlerMethod_scans_the_proxy_class_of_a_jdk_proxy_over_a_target_source_that_is_not_fixed_so_a_generic_handler_is_the_interface_method() throws Exception {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTargetSource(new AlwaysFreshTargetSource(GenericHandlerBean.class));
        proxyFactory.setInterfaces(GenericHandler.class);
        proxyFactory.setProxyTargetClass(false);
        Object proxy = proxyFactory.getProxy();
        Class<?> scanned = HandlerScan.implementationClassOf(proxy);
        assertThat(scanned).describedAs("the proxy's own class, not the class the target source names").isEqualTo(proxy.getClass());

        Set<Method> handlers = HandlerScan.handlerMethods(scanned, MARKERS);
        Method interfaceMethod = GenericHandler.class.getDeclaredMethod("on", Object.class);

        assertThat(handlers).describedAs("the interface method is the one handler").containsExactly(interfaceMethod);
    }

    @Test
    void handlerMethod_leaves_the_declaring_class_out_of_the_signature_of_an_inherited_public_or_protected_method() throws Exception {
        Method publicMethodDeclaration = CrossPackageMethods.class.getDeclaredMethod("publicMethod");
        Method protectedMethodDeclaration = CrossPackageMethods.class.getDeclaredMethod("protectedMethod");
        HandlerMethod publicMethod = new HandlerMethod(publicMethodDeclaration, CrossPackageMethods.class);
        HandlerMethod protectedMethod = new HandlerMethod(protectedMethodDeclaration, CrossPackageMethods.class);

        assertThat(publicMethod.signature()).isEqualTo("publicMethod[]").doesNotContain(CrossPackageMethods.class.getName());
        assertThat(protectedMethod.signature()).isEqualTo("protectedMethod[]").doesNotContain(CrossPackageMethods.class.getName());
    }

    @Test
    void handlerMethod_keys_a_package_private_handler_the_same_when_a_subclass_in_another_package_declares_an_unannotated_private_method_of_its_signature() throws Exception {
        Method handler = A.class.getDeclaredMethod("on", String.class);
        Set<Method> scannedForA = HandlerScan.handlerMethods(A.class, List.of(Subscription.class));
        Set<Method> scannedForLeaf = HandlerScan.handlerMethods(LeafOfPrivate.class, List.of(Subscription.class));

        assertThat(scannedForLeaf).containsExactly(handler);
        assertThat(scannedForA).containsExactly(handler);
        assertThat(new HandlerMethod(scannedForLeaf.iterator().next(), LeafOfPrivate.class).signature())
                .describedAs("an unannotated method of the same signature below the handler is not a handler, so the scan of the subclass keys it as the scan of A does")
                .isEqualTo(new HandlerMethod(scannedForA.iterator().next(), A.class).signature())
                .doesNotContain(A.class.getName());
    }

    @Test
    void handlerMethod_keys_a_package_private_handler_the_same_when_a_subclass_in_another_package_declares_an_annotated_method_of_its_signature_and_lists_both() throws Exception {
        Method handler = A.class.getDeclaredMethod("on", String.class);
        Set<Method> scannedForA = HandlerScan.handlerMethods(A.class, List.of(Subscription.class));
        Set<Method> scannedForLeaf = HandlerScan.handlerMethods(LeafOfAnnotated.class, List.of(Subscription.class));

        assertThat(scannedForLeaf).hasSize(2);
        assertThat(new HandlerMethod(handler, A.class).signature()).isEqualTo(new HandlerMethod(scannedForA.iterator().next(), A.class).signature());
        assertThat(scannedForLeaf)
                .describedAs("both handlers of the signature are listed, and share the signature a registry keys them by")
                .extracting(method -> new HandlerMethod(method, LeafOfAnnotated.class).signature())
                .containsOnly(new HandlerMethod(handler, A.class).signature());
    }

    static class UnrelatedPrivateHandler {
        @Marked
        @SuppressWarnings("unused")
        private void handle(String event) {
        }
    }

    private static HandlerMethod handlerOf(Class<?> type, Class<?> declaring) {
        Set<Method> scanned = HandlerScan.handlerMethods(type, MARKERS);
        Method method = scanned.stream().filter(candidate -> candidate.getDeclaringClass().equals(declaring)).findFirst().orElseThrow();
        return new HandlerMethod(method, type);
    }

    @Test
    void registeredHandlers_skips_a_method_that_is_recorded_and_reports_a_second_mark_as_not_recorded() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod handler = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);

        assertThat(registered.isRegistered("bean", handler)).isFalse();
        assertThat(registered.markRegistered("bean", handler)).isTrue();
        assertThat(registered.isRegistered("bean", handler)).isTrue();
        assertThat(registered.markRegistered("bean", handler)).describedAs("already recorded").isFalse();
    }

    @Test
    void registeredHandlers_skips_the_method_of_an_unrelated_class_once_another_method_of_its_signature_is_recorded() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod first = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);
        HandlerMethod unrelated = handlerOf(UnrelatedPrivateHandler.class, UnrelatedPrivateHandler.class);
        registered.markRegistered("bean", first);

        assertThat(registered.isRegistered("bean", unrelated)).isTrue();
        assertThat(registered.markRegistered("bean", unrelated)).describedAs("a bean built from either class has one handler of the signature").isFalse();
    }

    @Test
    void registeredHandlers_registers_a_subclass_private_method_that_does_not_override_the_recorded_private_method_of_its_signature() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod base = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerBase.class);
        HandlerMethod subclass = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerSubclass.class);
        registered.markRegistered("bean", base);

        assertThat(registered.isRegistered("bean", subclass)).isFalse();
        assertThat(registered.markRegistered("bean", subclass)).isTrue();
        assertThat(registered.isRegistered("bean", base)).describedAs("the recorded method stays recorded").isTrue();
        assertThat(registered.markRegistered("bean", subclass)).describedAs("now it is recorded too").isFalse();
    }

    @Test
    void registeredHandlers_registers_a_forgotten_method_again() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod handler = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);
        registered.markRegistered("bean", handler);

        registered.forgetRegistered("bean", handler);

        assertThat(registered.isRegistered("bean", handler)).isFalse();
        assertThat(registered.markRegistered("bean", handler)).isTrue();
    }

    @Test
    void registeredHandlers_keeps_the_other_recorded_method_of_a_signature_when_one_is_forgotten() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod base = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerBase.class);
        HandlerMethod subclass = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerSubclass.class);
        registered.markRegistered("bean", base);
        registered.markRegistered("bean", subclass);

        registered.forgetRegistered("bean", subclass);

        assertThat(registered.isRegistered("bean", base)).isTrue();
        assertThat(registered.isRegistered("bean", subclass)).isFalse();
    }

    abstract static class PrivateSuperclassOnString {
        @Marked
        @SuppressWarnings("unused")
        private void on(String event) {
        }
    }

    interface DefaultOnString {
        @Marked
        default void on(String event) {
        }
    }

    static class PrivateAndDefaultBean extends PrivateSuperclassOnString implements DefaultOnString {
    }

    @Test
    void registeredHandlers_registers_a_private_method_of_a_superclass_and_an_interface_default_method_of_its_signature_that_one_class_inherits() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod privateOnSuperclass = handlerOf(PrivateAndDefaultBean.class, PrivateSuperclassOnString.class);
        HandlerMethod defaultMethod = handlerOf(PrivateAndDefaultBean.class, DefaultOnString.class);

        assertThat(registered.markRegistered("bean", privateOnSuperclass)).isTrue();
        assertThat(registered.isRegistered("bean", defaultMethod)).describedAs("the instance runs the private method for one and the default method for the other").isFalse();
        assertThat(registered.markRegistered("bean", defaultMethod)).isTrue();
    }

    @Test
    void registeredHandlers_keeps_the_handlers_of_a_class_and_its_subclass_and_skips_an_unrelated_class_that_registers_between_them() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod superclass = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);
        HandlerMethod unrelated = handlerOf(UnrelatedPrivateHandler.class, UnrelatedPrivateHandler.class);
        HandlerMethod subclass = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerSubclass.class);

        List<Boolean> registeredInOrder = List.of(
                registered.markRegistered("bean", superclass),
                registered.markRegistered("bean", unrelated),
                registered.markRegistered("bean", subclass));

        assertThat(registeredInOrder).describedAs("the unrelated class comes second and has the signature of the first hierarchy").containsExactly(true, false, true);
    }

    @Test
    void registeredHandlers_keeps_only_the_handler_of_an_unrelated_class_that_registers_before_a_class_and_its_subclass() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod unrelated = handlerOf(UnrelatedPrivateHandler.class, UnrelatedPrivateHandler.class);
        HandlerMethod superclass = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);
        HandlerMethod subclass = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerSubclass.class);

        List<Boolean> registeredInOrder = List.of(
                registered.markRegistered("bean", unrelated),
                registered.markRegistered("bean", superclass),
                registered.markRegistered("bean", subclass));

        assertThat(registeredInOrder).describedAs("the first hierarchy to register keeps the signature").containsExactly(true, false, false);
    }

    static class PrivateOnA {
        @Marked
        @SuppressWarnings("unused")
        private void on(String event) {
        }
    }

    static class PublicOnB extends PrivateOnA {
        @Marked
        public void on(String event) {
        }
    }

    static class OverridesOnC extends PublicOnB {
        @Override
        public void on(String event) {
        }
    }

    @Test
    void registeredHandlers_skips_the_override_of_a_recorded_method_when_its_scan_also_found_a_private_method_of_the_signature_above_it() {
        RegisteredHandlers registered = new RegisteredHandlers();
        registered.markRegistered("bean", handlerOf(PublicOnB.class, PrivateOnA.class));
        registered.markRegistered("bean", handlerOf(PublicOnB.class, PublicOnB.class));
        HandlerMethod privateOnA = handlerOf(OverridesOnC.class, PrivateOnA.class);
        HandlerMethod overrideOnC = handlerOf(OverridesOnC.class, OverridesOnC.class);

        assertThat(registered.isRegistered("bean", privateOnA)).isTrue();
        assertThat(registered.isRegistered("bean", overrideOnC)).describedAs("it is the handler of PublicOnB, which it overrides").isTrue();
        assertThat(registered.markRegistered("bean", overrideOnC)).isFalse();
    }

    @Test
    void registeredHandlers_registers_a_forgotten_private_method_of_a_superclass_when_the_private_method_of_its_subclass_is_still_recorded() {
        RegisteredHandlers registered = new RegisteredHandlers();
        HandlerMethod base = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerBase.class);
        HandlerMethod subclass = handlerOf(PrivateHandlerSubclass.class, PrivateHandlerSubclass.class);
        registered.markRegistered("bean", subclass);
        registered.markRegistered("bean", base);
        registered.forgetRegistered("bean", base);
        HandlerMethod baseOfLaterInstance = handlerOf(PrivateHandlerBase.class, PrivateHandlerBase.class);

        assertThat(registered.isRegistered("bean", baseOfLaterInstance)).describedAs("the private methods don't override each other").isFalse();
        assertThat(registered.markRegistered("bean", baseOfLaterInstance)).isTrue();
    }

    // ------------------------------------------------------------------------------------------------------
    // a bridge javac adds for a public method inherited from a package-private generic class has erased types
    // ------------------------------------------------------------------------------------------------------

    static class BridgedEvent {
    }

    static class BridgedSubEvent extends BridgedEvent {
    }

    abstract static class BridgedHandlerBase<E extends BridgedEvent> {
        @Marked
        public void on(E event) {
        }
    }

    public static class BridgedHandlerLeaf extends BridgedHandlerBase<BridgedSubEvent> {
    }

    abstract static class BridgedFactoryBase<E> {
        @Marked
        public Pair<E, String> factory() {
            return null;
        }
    }

    public static class BridgedFactoryLeaf extends BridgedFactoryBase<MyView> {
    }

    @Test
    void analyzeParameters_reads_the_event_type_of_a_visibility_bridge_from_the_method_it_bridges() {
        Method scanned = HandlerScan.handlerMethods(BridgedHandlerLeaf.class, MARKERS).iterator().next();

        assertThat(scanned.isBridge()).describedAs("the scan returned the bridge, which declares the erased BridgedEvent").isTrue();
        assertThat(HandlerScan.analyzeParameters(scanned, BridgedHandlerLeaf.class, SubscriptionAnnotations::isStreamMetadataParameter, true))
                .extracting(HandlerParameter::type)
                .containsExactly(BridgedSubEvent.class);
    }

    @Test
    void the_public_analyzeParameters_keeps_the_erased_event_type_of_a_visibility_bridge_as_in_0_34_0() {
        Method scanned = HandlerScan.handlerMethods(BridgedHandlerLeaf.class, MARKERS).iterator().next();

        assertThat(scanned.isBridge()).describedAs("the scan returned the bridge, which declares the erased BridgedEvent").isTrue();
        assertThat(SubscriptionAnnotations.analyzeParameters(scanned, SubscriptionAnnotations::isStreamMetadataParameter, true))
                .extracting(HandlerParameter::type)
                .containsExactly(BridgedEvent.class);
    }

    @Test
    void returnTypeArgument_reads_the_type_argument_of_a_visibility_bridge_from_the_method_it_bridges() {
        Method scanned = HandlerScan.handlerMethods(BridgedFactoryLeaf.class, MARKERS).iterator().next();

        assertThat(scanned.isBridge()).describedAs("the scan returned the bridge, whose return type is raw").isTrue();
        assertThat(HandlerScan.returnTypeArgument(scanned, BridgedFactoryLeaf.class, 0)).isEqualTo(MyView.class);
    }

    // ------------------------------------------------------------------------------------------------------
    // a static method hides a static method of its superclass instead of overriding it
    // ------------------------------------------------------------------------------------------------------

    static class StaticProjectionBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        static void on(String event) {
        }
    }

    static class HidesStaticProjectionWithoutAnnotation extends StaticProjectionBase {
        @SuppressWarnings("unused")
        static void on(String event) {
        }
    }

    static class HidesStaticProjectionRepeatingAnnotation extends StaticProjectionBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        static void on(String event) {
        }
    }

    @Test
    void handlerMethods_refuses_an_unannotated_static_method_hiding_an_annotated_static_method_of_its_superclass_naming_both_methods() {
        String hiding = HidesStaticProjectionWithoutAnnotation.class.getName() + "#on(String)";
        String hidden = StaticProjectionBase.class.getName() + "#on(String)";

        assertThatThrownBy(() -> HandlerScan.handlerMethods(HidesStaticProjectionWithoutAnnotation.class, List.of(Projection.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Method " + hiding + " is static, so it does not override " + hidden)
                .hasMessageContaining("@Projection with id \"p\"");
    }

    @Test
    void handlerMethods_returns_both_static_methods_when_the_one_hiding_the_other_repeats_the_annotation() throws Exception {
        Method hiding = HidesStaticProjectionRepeatingAnnotation.class.getDeclaredMethod("on", String.class);
        Method hidden = StaticProjectionBase.class.getDeclaredMethod("on", String.class);

        assertThat(HandlerScan.handlerMethods(HidesStaticProjectionRepeatingAnnotation.class, List.of(Projection.class)))
                .containsExactlyInAnyOrder(hiding, hidden);
    }
    // ------------------------------------------------------------------------------------------------------
    // handlerMethods refuses a method whose annotation Spring reads from a method it does not override
    // ------------------------------------------------------------------------------------------------------

    static class ShadowsPackagePrivateHandler extends CrossPackageSubscriptionBase {
        @SuppressWarnings("unused")
        void on(String event) {
        }
    }

    static class RepeatsAnnotationOnShadowingMethod extends CrossPackageSubscriptionBase {
        @Subscription(id = "own")
        @SuppressWarnings("unused")
        void on(String event) {
        }
    }

    @Test
    void handlerMethods_refuses_an_unannotated_method_shadowing_a_package_private_handler_in_another_package_naming_both_methods_the_id_and_both_fixes() {
        String shadowing = ShadowsPackagePrivateHandler.class.getName() + "#on(String)";
        String shadowed = CrossPackageSubscriptionBase.class.getName() + "#on(String)";

        assertThatThrownBy(() -> HandlerScan.handlerMethods(ShadowsPackagePrivateHandler.class, List.of(Subscription.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Method " + shadowing + " does not override " + shadowed)
                .hasMessageContaining("@Subscription with id \"h\"")
                .hasMessageContaining("Make both " + shadowed + " and " + shadowing + " protected or public")
                .hasMessageContaining("or rename " + shadowing);
    }

    @Test
    void handlerMethods_refusal_advises_making_both_methods_protected_or_public_since_a_public_override_of_a_package_private_method_is_not_enough() {
        String shadowing = ShadowsPackagePrivateHandler.class.getName() + "#on(String)";
        String shadowed = CrossPackageSubscriptionBase.class.getName() + "#on(String)";

        assertThatThrownBy(() -> HandlerScan.handlerMethods(ShadowsPackagePrivateHandler.class, List.of(Subscription.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Make both ")
                .hasMessageContaining(shadowed + " and " + shadowing + " protected or public");
    }

    static class StaticMethodShadowsPackagePrivateHandler extends CrossPackageSubscriptionBase {
        @SuppressWarnings("unused")
        static void on(String event) {
        }
    }

    @Test
    void handlerMethods_refuses_a_static_method_shadowing_a_package_private_handler_in_another_package_by_advising_a_rename_not_a_wider_modifier() {
        String shadowing = StaticMethodShadowsPackagePrivateHandler.class.getName() + "#on(String)";
        String shadowed = CrossPackageSubscriptionBase.class.getName() + "#on(String)";

        assertThatThrownBy(() -> HandlerScan.handlerMethods(StaticMethodShadowsPackagePrivateHandler.class, List.of(Subscription.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Method " + shadowing + " is static, so it does not override " + shadowed)
                .hasMessageContaining("@Subscription with id \"h\"")
                .hasMessageContaining("Rename " + shadowing + " to keep " + shadowed + " as the handler")
                .hasMessageNotContaining("protected or public");
    }

    // ------------------------------------------------------------------------------------------------------
    // handlerMethods follows the JVM's rules for overriding a package-private method
    // ------------------------------------------------------------------------------------------------------

    static class SubOfPackagePrivateOverridingMid extends PackagePrivateOverridingMid {
    }

    static class SubOfPackagePrivateOverridingFactoryMid extends PackagePrivateOverridingFactoryMid {
    }

    // The scan read the handler off the base, and the class below the overriding mid sits in another package. Spring
    // looks the method up from that package alone, so it finds the base's method and not the mid's override the JVM runs.
    @Test
    void runsImplementationOf_is_false_for_a_class_in_another_package_whose_superclass_overrides_the_package_private_handler_the_scan_read() throws Exception {
        Method read = PackagePrivateHandlerBase.class.getDeclaredMethod("on", String.class);

        assertThat(SubscriptionAnnotations.runsImplementationOf(new SubOfPackagePrivateOverridingMid(), read)).isFalse();
    }

    @Test
    void runsImplementationOf_is_true_for_the_override_the_scan_read_on_a_class_in_another_package_that_declares_nothing() throws Exception {
        Method read = PackagePrivateOverridingMid.class.getDeclaredMethod("on", String.class);

        assertThat(SubscriptionAnnotations.runsImplementationOf(new SubOfPackagePrivateOverridingMid(), read)).isTrue();
    }

    @Test
    void invokeDescriptorFactory_refuses_a_class_in_another_package_whose_superclass_overrides_the_package_private_factory_the_scan_read() throws Exception {
        Method read = PackagePrivateFactoryBase.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new SubOfPackagePrivateOverridingFactoryMid(), read))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection factory")
                .hasMessageContaining(SubOfPackagePrivateOverridingFactoryMid.class.getName() + " does not run the factory the scan read from " + PackagePrivateFactoryBase.class.getName())
                .hasMessageContaining("it either overrides it or does not inherit it");
    }

    // ------------------------------------------------------------------------------------------------------
    // a public class inheriting from a package-private class has bridge methods the scan reads
    // ------------------------------------------------------------------------------------------------------

    static class SubOfPublicLeafWithBridges extends PublicLeafWithBridges {
    }

    static class OverridesHandlerAndFactoryOfPublicLeafWithBridges extends PublicLeafWithBridges {
        @Override
        public void on(String event) {
        }

        @Override
        public String factory() {
            return "overridden-factory-built";
        }
    }

    private static Method bridgeTheScanReadsFrom(Class<?> type) {
        Set<Method> handlers = HandlerScan.handlerMethods(type, List.of(Subscription.class));
        assertThat(handlers).singleElement().satisfies(handler -> {
            assertThat(handler.isBridge()).isTrue();
            assertThat(handler.getDeclaringClass()).isEqualTo(PublicLeafWithBridges.class);
        });
        return handlers.iterator().next();
    }

    @Test
    void runsImplementationOf_is_true_for_a_public_class_and_the_bridge_the_scan_read_from_it() {
        Method read = bridgeTheScanReadsFrom(PublicLeafWithBridges.class);

        assertThat(SubscriptionAnnotations.runsImplementationOf(new PublicLeafWithBridges(), read)).isTrue();
    }

    @Test
    void runsImplementationOf_is_true_for_a_public_class_in_another_package_that_extends_the_one_with_the_bridge() {
        Method read = bridgeTheScanReadsFrom(PublicLeafWithBridges.class);

        assertThat(SubscriptionAnnotations.runsImplementationOf(new SubOfPublicLeafWithBridges(), read)).isTrue();
    }

    @Test
    void runsImplementationOf_is_false_for_a_class_that_overrides_the_handler_behind_the_bridge_the_scan_read() throws Exception {
        Method read = PublicLeafWithBridges.class.getDeclaredMethod("on", String.class);
        assertThat(read.isBridge()).isTrue();

        assertThat(SubscriptionAnnotations.runsImplementationOf(new OverridesHandlerAndFactoryOfPublicLeafWithBridges(), read)).isFalse();
    }

    @Test
    void invokeDescriptorFactory_runs_the_factory_read_from_the_bridge_of_a_public_class_instead_of_refusing_it() throws Exception {
        Method read = PublicLeafWithBridges.class.getDeclaredMethod("factory");
        assertThat(read.isBridge()).isTrue();

        Object result = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new SubOfPublicLeafWithBridges(), read);

        assertThat(result).isEqualTo("bridged-factory-built");
    }

    @Test
    void invokeDescriptorFactory_refuses_a_class_that_overrides_the_factory_behind_the_bridge_the_scan_read() throws Exception {
        Method read = PublicLeafWithBridges.class.getDeclaredMethod("factory");

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", new OverridesHandlerAndFactoryOfPublicLeafWithBridges(), read))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("it either overrides it or does not inherit it");
    }

    // ------------------------------------------------------------------------------------------------------
    // handlerMethods on a JDK proxy class
    // ------------------------------------------------------------------------------------------------------

    interface ProxiedHandler {
        @Subscription(id = "proxied")
        void on(String event);
    }

    // A JDK proxy hands every interface method to its invocation handler, so the interface method is the handler and
    // the proxy class's generated method of the same signature is not.
    @Test
    void handlerMethods_returns_the_interface_method_for_a_jdk_proxy_class() throws Exception {
        Class<?> proxyClass = Proxy.newProxyInstance(ProxiedHandler.class.getClassLoader(), new Class<?>[]{ProxiedHandler.class}, (proxy, method, args) -> null).getClass();

        assertThat(HandlerScan.handlerMethods(proxyClass, List.of(Subscription.class)))
                .containsExactly(ProxiedHandler.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_the_concrete_package_private_override_of_a_generic_handler_and_not_its_bridge_for_a_class_in_the_package_that_declares_it() throws Exception {
        Method override = OverridesGenericPackagePrivateHandler.class.getDeclaredMethod("on", String.class);

        assertThat(HandlerScan.handlerMethods(LeafInTheGenericHandlersPackage.class, List.of(Subscription.class)))
                .describedAs("the compiler's bridge method on(Object) of the overriding class is not a handler")
                .containsExactly(override);
    }

    interface ProxiedOverloadedHandler {
        @Subscription(id = "string")
        void on(String event);

        @Subscription(id = "integer")
        void on(Integer event);
    }

    @Test
    void handlerMethods_returns_both_overloads_of_an_interface_for_a_jdk_proxy_class() throws Exception {
        Class<?> proxyClass = jdkProxyClassOver(ProxiedOverloadedHandler.class);

        assertThat(HandlerScan.handlerMethods(proxyClass, List.of(Subscription.class)))
                .describedAs("two methods of one name and different parameter types are two handlers")
                .containsExactlyInAnyOrder(ProxiedOverloadedHandler.class.getDeclaredMethod("on", String.class), ProxiedOverloadedHandler.class.getDeclaredMethod("on", Integer.class));
    }

    static class OverridesWidenedMid extends PublicOverridingMid {
        @Override
        public void on(String event) {
        }
    }

    static class OverridesWidenedMidOfPackagePrivateMid extends PublicOverridingOfPackagePrivateMid {
        @Override
        public void on(String event) {
        }
    }

    static class InheritsWidenedMidOfPackagePrivateMid extends PublicOverridingOfPackagePrivateMid {
    }

    @Test
    void handlerMethods_returns_the_package_private_override_once_for_a_class_in_another_package_that_declares_nothing() throws Exception {
        assertThat(HandlerScan.handlerMethods(SubOfPackagePrivateOverridingMid.class, List.of(Subscription.class)))
                .containsExactly(PackagePrivateOverridingMid.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_the_public_override_of_a_package_private_handler_once_for_a_class_in_another_package_that_overrides_it_again() throws Exception {
        assertThat(HandlerScan.handlerMethods(OverridesWidenedMid.class, List.of(Subscription.class)))
                .containsExactly(OverridesWidenedMid.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_the_last_override_of_a_chain_that_starts_with_a_package_private_handler_and_widens_to_public() throws Exception {
        assertThat(HandlerScan.handlerMethods(OverridesWidenedMidOfPackagePrivateMid.class, List.of(Subscription.class)))
                .containsExactly(OverridesWidenedMidOfPackagePrivateMid.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_the_widened_override_of_a_chain_for_a_class_in_another_package_that_declares_nothing() throws Exception {
        assertThat(HandlerScan.handlerMethods(InheritsWidenedMidOfPackagePrivateMid.class, List.of(Subscription.class)))
                .containsExactly(PublicOverridingOfPackagePrivateMid.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_only_the_override_in_the_handlers_package_of_a_class_that_extends_a_method_that_does_not_override_the_handler() throws Exception {
        assertThat(HandlerScan.handlerMethods(OverridesHandlerBelowAMethodThatDoesNot.class, List.of(Subscription.class)))
                .containsExactly(OverridesHandlerBelowAMethodThatDoesNot.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_refuses_a_public_method_in_another_package_that_does_not_override_a_package_private_handler_when_the_scanned_class_is_in_the_handlers_package() throws Exception {
        String shadowing = PublicMethodThatDoesNotOverride.class.getName() + "#on(String)";
        String shadowed = PackagePrivateHandlerBase.class.getName() + "#on(String)";

        assertThatThrownBy(() -> HandlerScan.handlerMethods(SubclassInDeclaringPackage.class, List.of(Subscription.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(shadowing)
                .hasMessageContaining(shadowed)
                .hasMessageContaining("@Subscription with id \"h\"");
    }

    @Test
    void handlerMethods_returns_a_package_private_handler_below_a_private_method_of_the_same_signature_in_another_package() throws Exception {
        assertThat(HandlerScan.handlerMethods(LeafOfPrivate.class, List.of(Subscription.class)))
                .containsExactly(A.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_returns_both_a_package_private_handler_and_the_annotated_method_of_the_same_signature_above_it_in_another_package() throws Exception {
        assertThat(HandlerScan.handlerMethods(LeafOfAnnotated.class, List.of(Subscription.class)))
                .containsExactlyInAnyOrder(
                        A.class.getDeclaredMethod("on", String.class),
                        MidAnnotated.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void methodACglibProxyRunsInsteadOf_gives_the_public_method_of_another_package_that_sits_below_the_package_private_handler() throws Exception {
        Method handler = A.class.getDeclaredMethod("on", String.class);

        assertThat(HandlerScan.methodACglibProxyRunsInsteadOf(handler, LeafOfAnnotated.class))
                .isEqualTo(MidAnnotated.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void methodACglibProxyRunsInsteadOf_gives_null_when_the_method_below_the_package_private_handler_is_private() throws Exception {
        Method handler = A.class.getDeclaredMethod("on", String.class);

        assertThat(HandlerScan.methodACglibProxyRunsInsteadOf(handler, LeafOfPrivate.class)).isNull();
    }

    @Test
    void methodACglibProxyRunsInsteadOf_gives_null_when_the_method_below_the_package_private_handler_is_private_and_in_the_handlers_package() throws Exception {
        Method handler = A.class.getDeclaredMethod("on", String.class);

        assertThat(HandlerScan.methodACglibProxyRunsInsteadOf(handler, leafBelowAPrivateOnInThePackageOf(A.class))).isNull();
    }

    // javac refuses a private method with the name and parameters of a package-private method in a superclass
    // of its own package, so the classes are generated
    private static Class<?> leafBelowAPrivateOnInThePackageOf(Class<?> handlerClass) throws IllegalAccessException {
        ClassLoadingStrategy<ClassLoader> intoTheHandlersPackage = ClassLoadingStrategy.UsingLookup.of(MethodHandles.privateLookupIn(handlerClass, MethodHandles.lookup()));
        Class<?> mid = new ByteBuddy()
                .with(MethodGraph.Compiler.ForDeclaredMethods.INSTANCE)
                .subclass(handlerClass)
                .defineMethod("on", void.class, Visibility.PRIVATE).withParameters(String.class).intercept(StubMethod.INSTANCE)
                .make()
                .load(handlerClass.getClassLoader(), intoTheHandlersPackage)
                .getLoaded();
        return new ByteBuddy()
                .subclass(mid)
                .make()
                .load(handlerClass.getClassLoader(), intoTheHandlersPackage)
                .getLoaded();
    }

    @Test
    void invokeDescriptorFactory_refuses_a_package_private_factory_that_a_public_method_of_another_package_sits_below_on_a_cglib_proxy_left_proxied() throws Exception {
        SubclassInDeclaringPackageOfPublicFactory proxy = cglibProxyLeftProxied(SubclassInDeclaringPackageOfPublicFactory.class);

        assertThatThrownBy(() -> SubscriptionAnnotations.invokeDescriptorFactory("@Projection", proxy, PackagePrivateFactoryBase.class.getDeclaredMethod("factory")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("@Projection factory " + proxy.getClass().getName() + "#factory")
                .hasMessageContaining("cannot run because it is package-private and declared in " + PackagePrivateFactoryBase.class.getName() + ", and " + PublicFactoryThatDoesNotOverride.class.getName())
                .hasMessageContaining(PublicFactoryThatDoesNotOverride.class.getName() + " declares a method of the same name and parameter types without overriding it");
    }

    @Test
    void handlerMethods_accepts_a_method_that_repeats_the_annotation_of_a_package_private_handler_in_another_package() throws Exception {
        assertThat(HandlerScan.handlerMethods(RepeatsAnnotationOnShadowingMethod.class, List.of(Subscription.class)))
                .containsExactlyInAnyOrder(
                        RepeatsAnnotationOnShadowingMethod.class.getDeclaredMethod("on", String.class),
                        CrossPackageSubscriptionBase.class.getDeclaredMethod("on", String.class));
    }

    // ------------------------------------------------------------------------------------------------------
    // isPackagePrivateOutsidePackageOf
    // ------------------------------------------------------------------------------------------------------

    static class SamePackageMethods {
        void packagePrivate() {
        }
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_true_for_a_package_private_method_declared_in_another_package() throws Exception {
        assertThat(HandlerScan.isPackagePrivateOutsidePackageOf(crossPackagePackagePrivate(), InheritsCrossPackageMethods.class)).isTrue();
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_false_for_a_package_private_method_declared_in_the_same_package() throws Exception {
        assertThat(HandlerScan.isPackagePrivateOutsidePackageOf(SamePackageMethods.class.getDeclaredMethod("packagePrivate"), SamePackageMethods.class)).isFalse();
    }

    static class PackagePrivateInALoader {
        void packagePrivate() {
        }
    }

    static class DefinesPackagePrivateInALoaderItself extends ClassLoader {
        DefinesPackagePrivateInALoaderItself() {
            super(PackagePrivateInALoader.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(PackagePrivateInALoader.class.getName())) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    try (InputStream bytes = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        byte[] definition = bytes.readAllBytes();
                        loaded = defineClass(name, definition, 0, definition.length);
                    } catch (IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                return loaded;
            }
        }
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_true_for_a_package_private_method_declared_in_the_same_package_name_in_another_class_loader() throws Exception {
        Class<?> inAnotherLoader = new DefinesPackagePrivateInALoaderItself().loadClass(PackagePrivateInALoader.class.getName());

        assertThat(inAnotherLoader.getPackageName()).isEqualTo(SamePackageMethods.class.getPackageName());
        assertThat(HandlerScan.isPackagePrivateOutsidePackageOf(PackagePrivateInALoader.class.getDeclaredMethod("packagePrivate"), inAnotherLoader)).isTrue();
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_false_for_a_package_private_method_declared_in_the_same_package_name_in_the_same_class_loader() throws Exception {
        assertThat(HandlerScan.isPackagePrivateOutsidePackageOf(PackagePrivateInALoader.class.getDeclaredMethod("packagePrivate"), SamePackageMethods.class)).isFalse();
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_false_for_a_public_protected_or_private_method_declared_in_another_package() throws Exception {
        for (String name : List.of("publicMethod", "protectedMethod", "privateMethod")) {
            assertThat(HandlerScan.isPackagePrivateOutsidePackageOf(CrossPackageMethods.class.getDeclaredMethod(name), InheritsCrossPackageMethods.class))
                    .describedAs(name).isFalse();
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // returnTypeArgument (a factory's state type, read against the class that inherits the factory)
    // ------------------------------------------------------------------------------------------------------

    record Pair<A, B>(A first, B second) {
    }

    static class MyView {
    }

    static class OtherView {
    }

    static class FactoryBaseOfS<S> {
        Pair<S, String> stateFirst() {
            return null;
        }

        Pair<String, S> stateSecond() {
            return null;
        }
    }

    static class BindsMyView extends FactoryBaseOfS<MyView> {
    }

    @SuppressWarnings("rawtypes")
    static class RawSubclass extends FactoryBaseOfS {
    }

    static class StillGeneric<T> extends FactoryBaseOfS<T> {
    }

    static class MidLevel<T> extends FactoryBaseOfS<T> {
    }

    static class LeafOfMidLevel extends MidLevel<OtherView> {
    }

    interface FactoryInterface<S> {
        Pair<S, String> stateFirst();
    }

    static class ImplementsFactoryInterface implements FactoryInterface<MyView> {
        @Override
        public Pair<MyView, String> stateFirst() {
            return null;
        }
    }

    static class InheritsInterfaceFactory extends ImplementsFactoryInterface {
    }

    static class ShapesOfReturnTypes {
        Pair<MyView, String> concrete() {
            return null;
        }

        Pair<List<String>, String> parameterized() {
            return null;
        }

        Pair<?, String> wildcard() {
            return null;
        }

        <S extends Number> Pair<S, String> methodLevelVariable() {
            return null;
        }

        @SuppressWarnings("rawtypes")
        Pair raw() {
            return null;
        }

        String notParameterized() {
            return null;
        }
    }

    private static Method method(Class<?> type, String name) throws Exception {
        return type.getDeclaredMethod(name);
    }

    @Test
    void returnTypeArgument_resolves_a_type_variable_the_implementation_binds() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), BindsMyView.class, 0)).isEqualTo(MyView.class);
    }

    @Test
    void returnTypeArgument_reads_the_requested_index() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateSecond"), BindsMyView.class, 1)).isEqualTo(MyView.class);
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateSecond"), BindsMyView.class, 0)).isEqualTo(String.class);
    }

    @Test
    void returnTypeArgument_follows_a_binding_through_an_intermediate_generic_class() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), LeafOfMidLevel.class, 0)).isEqualTo(OtherView.class);
    }

    @Test
    void returnTypeArgument_resolves_a_type_variable_of_a_generic_interface_the_implementation_binds() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryInterface.class, "stateFirst"), InheritsInterfaceFactory.class, 0)).isEqualTo(MyView.class);
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_extends_the_generic_class_raw() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), RawSubclass.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_leaves_the_type_variable_unbound() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), StillGeneric.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_is_the_generic_class_itself() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), FactoryBaseOfS.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_for_a_type_variable_the_method_itself_declares() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "methodLevelVariable"), ShapesOfReturnTypes.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_for_a_wildcard_a_raw_return_type_a_non_generic_return_type_and_an_index_past_the_arguments() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "wildcard"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "raw"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "notParameterized"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "concrete"), ShapesOfReturnTypes.class, 2)).isNull();
    }

    @Test
    void returnTypeArgument_returns_a_concrete_class_and_the_raw_class_of_a_parameterized_argument() throws Exception {
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "concrete"), ShapesOfReturnTypes.class, 0)).isEqualTo(MyView.class);
        assertThat(HandlerScan.returnTypeArgument(method(ShapesOfReturnTypes.class, "parameterized"), ShapesOfReturnTypes.class, 0)).isEqualTo(List.class);
    }

    // ------------------------------------------------------------------------------------------------------
    // analyzeParameters reading a handler's generic parameter against the implementation
    // ------------------------------------------------------------------------------------------------------

    sealed interface Event permits Placed, Paid {
    }

    record Placed() implements Event {
    }

    record Paid() implements Event {
    }

    static class UnboundedHandlerBase<E> {
        void on(E event, EventMetadata metadata) {
        }
    }

    static class BoundedHandlerBase<E extends Event> {
        void on(E event, EventMetadata metadata) {
        }
    }

    static class UnboundedForPlaced extends UnboundedHandlerBase<Placed> {
    }

    static class BoundedForPlaced extends BoundedHandlerBase<Placed> {
    }

    @SuppressWarnings("rawtypes")
    static class RawUnbounded extends UnboundedHandlerBase {
    }

    @SuppressWarnings("rawtypes")
    static class RawBounded extends BoundedHandlerBase {
    }

    private static Class<?> eventTypeRead(Class<?> declaring, Class<?> implementation) {
        Method on = java.util.Arrays.stream(declaring.getDeclaredMethods()).filter(candidate -> candidate.getName().equals("on")).findFirst().orElseThrow();
        return SubscriptionAnnotations.eventTypeOf(HandlerScan.analyzeParameters(on, implementation, EventMetadata.class::equals, true));
    }

    @Test
    void analyzeParameters_reads_an_inherited_generic_event_parameter_as_the_type_the_implementation_binds() {
        assertThat(eventTypeRead(UnboundedHandlerBase.class, UnboundedForPlaced.class)).isEqualTo(Placed.class);
        assertThat(eventTypeRead(BoundedHandlerBase.class, BoundedForPlaced.class)).isEqualTo(Placed.class);
    }

    @Test
    void analyzeParameters_keeps_the_declared_type_for_a_generic_event_parameter_the_implementation_does_not_bind() {
        assertThat(eventTypeRead(UnboundedHandlerBase.class, RawUnbounded.class)).isEqualTo(Object.class);
        assertThat(eventTypeRead(BoundedHandlerBase.class, RawBounded.class)).isEqualTo(Event.class);
    }

    @Test
    void analyzeParameters_without_an_implementation_reads_the_type_the_method_declares() {
        Method on = java.util.Arrays.stream(BoundedHandlerBase.class.getDeclaredMethods()).filter(candidate -> candidate.getName().equals("on")).findFirst().orElseThrow();

        assertThat(SubscriptionAnnotations.eventTypeOf(SubscriptionAnnotations.analyzeParameters(on, EventMetadata.class::equals, true))).isEqualTo(Event.class);
    }

    // ------------------------------------------------------------------------------------------------------
    // handlerMethods and static methods
    // ------------------------------------------------------------------------------------------------------

    interface DeclaresStaticHandler {
        @Marked
        static void handle(String event) {
        }
    }

    static class ImplementsStaticHandlerInterface implements DeclaresStaticHandler {
    }

    abstract static class StaticHandlerBase {
        @Marked
        public static void handle(String event) {
        }
    }

    static class InheritsStaticHandler extends StaticHandlerBase {
    }

    @Test
    void handlerMethods_leaves_out_a_static_method_an_interface_declares() {
        assertThat(HandlerScan.handlerMethods(ImplementsStaticHandlerInterface.class, MARKERS)).isEmpty();
    }

    @Test
    void handlerMethods_returns_a_static_method_a_superclass_declares_so_the_registrar_can_refuse_it() throws Exception {
        assertThat(HandlerScan.handlerMethods(InheritsStaticHandler.class, MARKERS))
                .containsExactly(StaticHandlerBase.class.getDeclaredMethod("handle", String.class));
    }

    interface ProxiedBaseHandler {
        @Subscription(id = "proxied-base")
        void on(String event);
    }

    interface ProxiedRedeclaringHandler extends ProxiedBaseHandler {
        @Override
        void on(String event);
    }

    private static Class<?> jdkProxyClassOver(Class<?>... interfaces) {
        return Proxy.newProxyInstance(SubscriptionAnnotationsTest.class.getClassLoader(), interfaces, (proxy, method, args) -> null).getClass();
    }

    @Test
    void handlerMethods_gives_the_sub_interface_method_for_a_jdk_proxy_built_over_a_sub_interface_that_redeclares_the_handler() throws Exception {
        assertThat(HandlerScan.handlerMethods(jdkProxyClassOver(ProxiedRedeclaringHandler.class), List.of(Subscription.class)))
                .containsExactly(ProxiedRedeclaringHandler.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_gives_one_method_for_a_jdk_proxy_listing_both_an_interface_and_its_redeclaring_sub_interface() throws Exception {
        assertThat(HandlerScan.handlerMethods(jdkProxyClassOver(ProxiedBaseHandler.class, ProxiedRedeclaringHandler.class), List.of(Subscription.class)))
                .containsExactly(ProxiedBaseHandler.class.getDeclaredMethod("on", String.class));
    }

    interface UnrelatedProxiedHandler {
        @Subscription(id = "proxied-unrelated")
        void on(String event);
    }

    @Test
    void handlerMethods_gives_one_method_for_a_jdk_proxy_listing_an_unrelated_interface_before_a_sub_interface_redeclaring_the_handler_of_another() throws Exception {
        assertThat(HandlerScan.handlerMethods(jdkProxyClassOver(UnrelatedProxiedHandler.class, ProxiedRedeclaringHandler.class), List.of(Subscription.class)))
                .containsExactly(UnrelatedProxiedHandler.class.getDeclaredMethod("on", String.class));
    }

    interface ProxiedRedeclaringHandlerWithOwnId extends ProxiedBaseHandler {
        @Override
        @Subscription(id = "proxied-redeclared-b")
        void on(String event);
    }

    interface ProxiedSiblingRedeclaringHandlerWithOwnId extends ProxiedBaseHandler {
        @Override
        @Subscription(id = "proxied-redeclared-c")
        void on(String event);
    }

    interface ProxiedPlainOn {
        void on(String event);
    }

    @Test
    void handlerMethods_gives_the_annotated_method_for_a_jdk_proxy_listing_an_unannotated_interface_before_an_annotated_one_declaring_the_same_signature() throws Exception {
        assertThat(HandlerScan.handlerMethods(jdkProxyClassOver(ProxiedPlainOn.class, UnrelatedProxiedHandler.class), List.of(Subscription.class)))
                .containsExactly(UnrelatedProxiedHandler.class.getDeclaredMethod("on", String.class));
    }

    @Test
    void handlerMethods_gives_one_method_for_a_jdk_proxy_listing_two_sub_interfaces_that_each_redeclare_the_same_handler() throws Exception {
        assertThat(HandlerScan.handlerMethods(jdkProxyClassOver(ProxiedRedeclaringHandlerWithOwnId.class, ProxiedSiblingRedeclaringHandlerWithOwnId.class), List.of(Subscription.class)))
                .containsExactly(ProxiedRedeclaringHandlerWithOwnId.class.getDeclaredMethod("on", String.class));
    }

    interface ProxiedAsynchronousOn {
        @Subscription(id = "proxied-differing-a")
        void on(String event);
    }

    interface ProxiedSynchronousOn {
        @SynchronousSubscription(id = "proxied-differing-b")
        void on(String event);
    }

    @Test
    void handlerMethods_refuses_a_jdk_proxy_listing_a_subscription_interface_before_a_synchronous_subscription_interface_declaring_the_same_signature() {
        Class<?> proxyClass = jdkProxyClassOver(ProxiedAsynchronousOn.class, ProxiedSynchronousOn.class);

        assertThatThrownBy(() -> HandlerScan.handlerMethods(proxyClass, List.of(Subscription.class, SynchronousSubscription.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same name and parameter types")
                .hasMessageContaining("@Subscription")
                .hasMessageContaining("@SynchronousSubscription");
    }

    @Test
    void handlerMethods_refuses_a_jdk_proxy_listing_a_synchronous_subscription_interface_before_a_subscription_interface_declaring_the_same_signature() {
        Class<?> proxyClass = jdkProxyClassOver(ProxiedSynchronousOn.class, ProxiedAsynchronousOn.class);

        assertThatThrownBy(() -> HandlerScan.handlerMethods(proxyClass, List.of(Subscription.class, SynchronousSubscription.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same name and parameter types")
                .hasMessageContaining("@Subscription")
                .hasMessageContaining("@SynchronousSubscription");
    }
}
