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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.inmemory.filtermatching.FilterMatcher.matcherIgnoringPayloadConditions;

/**
 * Covers which events {@link FilterMatcher#matcherIgnoringPayloadConditions(Filter)} accepts, which is what callers
 * depend on, rather than the shape of the widened filter behind it.
 * <p>
 * The case that earns this its own test is a payload condition under {@code OR}. Removing the condition instead of
 * treating it as satisfied passes every other case here and silently rejects an event that matched only on the payload.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PayloadConditionsTest {

    private static final CloudEvent EVENT = event("SomethingHappened");
    private static final CloudEvent OTHER_TYPE = event("SomethingElseHappened");

    @Test
    void a_payload_condition_on_its_own_accepts_anything() {
        Predicate<CloudEvent> matcher = matcherIgnoringPayloadConditions(Filter.data("amount", eq(42)));

        assertThat(matcher.test(EVENT)).isTrue();
        assertThat(matcher.test(OTHER_TYPE)).isTrue();
    }

    @Test
    void a_sibling_condition_under_and_is_still_enforced() {
        Predicate<CloudEvent> matcher = matcherIgnoringPayloadConditions(Filter.type("SomethingHappened").and(Filter.data("amount", eq(42))));

        assertThat(matcher.test(EVENT)).isTrue();
        assertThat(matcher.test(OTHER_TYPE)).isFalse();
    }

    @Test
    void a_payload_condition_under_or_makes_the_whole_or_pass() {
        // The event does not match the type, and under the original filter the payload condition is what would have let
        // it through. Removing the condition rather than treating it as satisfied would reject the event here.
        Predicate<CloudEvent> matcher = matcherIgnoringPayloadConditions(Filter.type("SomethingHappened").or(Filter.data("amount", eq(42))));

        assertThat(matcher.test(OTHER_TYPE)).isTrue();
    }

    @Test
    void a_filter_without_a_payload_condition_still_decides_exactly_as_before() {
        Predicate<CloudEvent> matcher = matcherIgnoringPayloadConditions(Filter.type("SomethingHappened"));

        assertThat(matcher.test(EVENT)).isTrue();
        assertThat(matcher.test(OTHER_TYPE)).isFalse();
    }

    @Test
    void a_nested_payload_condition_is_handled_at_any_depth() {
        Filter nested = Filter.type("SomethingHappened").and(Filter.id("id").or(Filter.data("amount", eq(42))));

        assertThat(matcherIgnoringPayloadConditions(nested).test(EVENT)).isTrue();
    }

    @Test
    void the_matcher_never_needs_a_data_field_reader() {
        // A payload condition is answered without reading a payload, so the refusing reader is never reached.
        assertThat(matcherIgnoringPayloadConditions(Filter.data("amount", eq(42))).test(EVENT)).isTrue();
    }

    @Test
    void a_field_merely_starting_with_data_is_not_a_payload_condition() {
        // datacontenttype is an attribute, not a path into the payload, so it must still be checked.
        Predicate<CloudEvent> matcher = matcherIgnoringPayloadConditions(Filter.dataContentType("application/json"));

        assertThat(matcher.test(EVENT)).isFalse();
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId("id")
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
