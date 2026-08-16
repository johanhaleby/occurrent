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

import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class SubscriptionAnnotationsTest {

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
}
