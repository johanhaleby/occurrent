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

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.common.other.CrossPackageMethods;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
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
        assertThat(SubscriptionAnnotations.handlerMethods(InheritsHandler.class, MARKERS))
                .containsExactly(AnnotatedBase.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_finds_an_interface_default_method() throws Exception {
        assertThat(SubscriptionAnnotations.handlerMethods(GetsDefaultHandler.class, MARKERS))
                .containsExactly(DefaultHandler.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_leaves_out_a_method_without_any_of_the_annotations() {
        assertThat(SubscriptionAnnotations.handlerMethods(InheritsHandler.class, List.of(OtherMarked.class))).isEmpty();
    }

    @Test
    void handlerMethods_gives_an_override_that_drops_the_annotation_once_as_the_override() throws Exception {
        assertThat(SubscriptionAnnotations.handlerMethods(OverridesWithoutAnnotation.class, MARKERS))
                .containsExactly(OverridesWithoutAnnotation.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_gives_an_override_of_an_interface_default_method_once_as_the_override() throws Exception {
        assertThat(SubscriptionAnnotations.handlerMethods(OverridesDefaultWithoutAnnotation.class, MARKERS))
                .containsExactly(OverridesDefaultWithoutAnnotation.class.getDeclaredMethod("handle", String.class));
    }

    @Test
    void handlerMethods_leaves_out_the_bridge_method_of_a_generic_override() {
        assertThat(SubscriptionAnnotations.handlerMethods(ConcreteOfGeneric.class, MARKERS))
                .singleElement()
                .satisfies(method -> {
                    assertThat(method.isBridge()).isFalse();
                    assertThat(method.getDeclaringClass()).isEqualTo(ConcreteOfGeneric.class);
                    assertThat(method.getParameterTypes()).containsExactly(String.class);
                });
    }

    @Test
    void handlerMethods_leaves_out_the_bridge_method_when_the_generic_override_re_declares_an_annotation() {
        assertThat(SubscriptionAnnotations.handlerMethods(ReDeclaresOnGeneric.class, MARKERS))
                .singleElement()
                .satisfies(method -> assertThat(method.isBridge()).isFalse());
    }

    @Test
    void handlerMethods_keeps_two_private_methods_of_one_signature_on_a_class_and_its_superclass_apart() throws Exception {
        assertThat(SubscriptionAnnotations.handlerMethods(PrivateSub.class, MARKERS))
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
    // handlerKeys
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
    void handlerKeys_gives_the_class_and_interface_methods_of_a_generic_handler_the_same_second_key() throws Exception {
        List<String> fromClass = SubscriptionAnnotations.handlerKeys("bean", GenericHandlerBean.class.getDeclaredMethod("on", String.class), MARKERS);
        List<String> fromInterface = SubscriptionAnnotations.handlerKeys("bean", GenericHandler.class.getDeclaredMethod("on", Object.class), MARKERS);

        assertThat(fromClass.get(1)).isEqualTo(fromInterface.get(1));
        assertThat(fromClass.get(0)).describedAs("the signature keys differ, which is the bug the second key covers").isNotEqualTo(fromInterface.get(0));
    }

    @Test
    void handlerKeys_gives_a_private_method_and_the_private_method_of_its_subclass_distinct_keys() throws Exception {
        List<String> base = SubscriptionAnnotations.handlerKeys("bean", PrivateHandlerBase.class.getDeclaredMethod("handle", String.class), MARKERS);
        List<String> subclass = SubscriptionAnnotations.handlerKeys("bean", PrivateHandlerSubclass.class.getDeclaredMethod("handle", String.class), MARKERS);

        assertThat(base.get(0)).isNotEqualTo(subclass.get(0));
        assertThat(base.get(1)).isNotEqualTo(subclass.get(1));
    }

    @Test
    void handlerKeys_gives_a_cross_package_package_private_method_and_its_subclass_method_distinct_keys() throws Exception {
        List<String> base = SubscriptionAnnotations.handlerKeys("bean", CrossPackageMethods.class.getDeclaredMethod("packagePrivate"), MARKERS);
        List<String> subclass = SubscriptionAnnotations.handlerKeys("bean", PackagePrivateSubclassOfOtherPackage.class.getDeclaredMethod("packagePrivate"), MARKERS);

        assertThat(base.get(0)).isNotEqualTo(subclass.get(0));
        assertThat(base.get(1)).isNotEqualTo(subclass.get(1));
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
        assertThat(SubscriptionAnnotations.isPackagePrivateOutsidePackageOf(crossPackagePackagePrivate(), InheritsCrossPackageMethods.class)).isTrue();
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_false_for_a_package_private_method_declared_in_the_same_package() throws Exception {
        assertThat(SubscriptionAnnotations.isPackagePrivateOutsidePackageOf(SamePackageMethods.class.getDeclaredMethod("packagePrivate"), SamePackageMethods.class)).isFalse();
    }

    @Test
    void isPackagePrivateOutsidePackageOf_is_false_for_a_public_protected_or_private_method_declared_in_another_package() throws Exception {
        for (String name : List.of("publicMethod", "protectedMethod", "privateMethod")) {
            assertThat(SubscriptionAnnotations.isPackagePrivateOutsidePackageOf(CrossPackageMethods.class.getDeclaredMethod(name), InheritsCrossPackageMethods.class))
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
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), BindsMyView.class, 0)).isEqualTo(MyView.class);
    }

    @Test
    void returnTypeArgument_reads_the_requested_index() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateSecond"), BindsMyView.class, 1)).isEqualTo(MyView.class);
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateSecond"), BindsMyView.class, 0)).isEqualTo(String.class);
    }

    @Test
    void returnTypeArgument_follows_a_binding_through_an_intermediate_generic_class() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), LeafOfMidLevel.class, 0)).isEqualTo(OtherView.class);
    }

    @Test
    void returnTypeArgument_resolves_a_type_variable_of_a_generic_interface_the_implementation_binds() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryInterface.class, "stateFirst"), InheritsInterfaceFactory.class, 0)).isEqualTo(MyView.class);
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_extends_the_generic_class_raw() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), RawSubclass.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_leaves_the_type_variable_unbound() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), StillGeneric.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_when_the_implementation_is_the_generic_class_itself() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(FactoryBaseOfS.class, "stateFirst"), FactoryBaseOfS.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_for_a_type_variable_the_method_itself_declares() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "methodLevelVariable"), ShapesOfReturnTypes.class, 0)).isNull();
    }

    @Test
    void returnTypeArgument_is_null_for_a_wildcard_a_raw_return_type_a_non_generic_return_type_and_an_index_past_the_arguments() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "wildcard"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "raw"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "notParameterized"), ShapesOfReturnTypes.class, 0)).isNull();
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "concrete"), ShapesOfReturnTypes.class, 2)).isNull();
    }

    @Test
    void returnTypeArgument_returns_a_concrete_class_and_the_raw_class_of_a_parameterized_argument() throws Exception {
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "concrete"), ShapesOfReturnTypes.class, 0)).isEqualTo(MyView.class);
        assertThat(SubscriptionAnnotations.returnTypeArgument(method(ShapesOfReturnTypes.class, "parameterized"), ShapesOfReturnTypes.class, 0)).isEqualTo(List.class);
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
        return SubscriptionAnnotations.eventTypeOf(SubscriptionAnnotations.analyzeParameters(on, implementation, EventMetadata.class::equals, true));
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
        assertThat(SubscriptionAnnotations.handlerMethods(ImplementsStaticHandlerInterface.class, MARKERS)).isEmpty();
    }

    @Test
    void handlerMethods_returns_a_static_method_a_superclass_declares_so_the_registrar_can_refuse_it() throws Exception {
        assertThat(SubscriptionAnnotations.handlerMethods(InheritsStaticHandler.class, MARKERS))
                .containsExactly(StaticHandlerBase.class.getDeclaredMethod("handle", String.class));
    }
}
