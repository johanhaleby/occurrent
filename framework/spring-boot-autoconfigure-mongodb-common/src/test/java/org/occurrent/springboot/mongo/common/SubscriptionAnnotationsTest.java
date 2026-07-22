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

package org.occurrent.springboot.mongo.common;

import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations.HandlerParameter;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations.HandlerParameterKind;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
}
