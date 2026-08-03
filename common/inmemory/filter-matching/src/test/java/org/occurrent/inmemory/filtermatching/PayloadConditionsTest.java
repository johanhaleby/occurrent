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

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.inmemory.filtermatching.PayloadConditions.assumingPayloadConditionsMatch;
import static org.occurrent.inmemory.filtermatching.PayloadConditions.containsPayloadCondition;

/**
 * The rewrite is asserted through the matcher rather than by comparing filter trees, because what matters is which
 * events a rewritten filter accepts, not what it is made of.
 * <p>
 * The case that earns this its own test is a payload condition under {@code OR}. Removing the condition instead of
 * replacing it passes every other case here and silently discards an event that matched only on the payload.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PayloadConditionsTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:occurrent:test"))
            .withType("SomethingHappened")
            .build();

    private static final CloudEvent OTHER_TYPE = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:occurrent:test"))
            .withType("SomethingElseHappened")
            .build();

    @Test
    void a_payload_condition_on_its_own_matches_anything() {
        Filter rewritten = assumingPayloadConditionsMatch(Filter.data("amount", eq(42)));

        assertThat(matches(rewritten, EVENT)).isTrue();
        assertThat(matches(rewritten, OTHER_TYPE)).isTrue();
    }

    @Test
    void a_sibling_condition_under_and_is_still_enforced() {
        Filter rewritten = assumingPayloadConditionsMatch(Filter.type("SomethingHappened").and(Filter.data("amount", eq(42))));

        assertThat(matches(rewritten, EVENT)).isTrue();
        assertThat(matches(rewritten, OTHER_TYPE)).isFalse();
    }

    @Test
    void a_payload_condition_under_or_makes_the_whole_or_pass() {
        // The event does not match the type, and under the original filter the payload condition is what would have
        // let it through. Removing the condition rather than replacing it would reject the event here.
        Filter rewritten = assumingPayloadConditionsMatch(Filter.type("SomethingHappened").or(Filter.data("amount", eq(42))));

        assertThat(matches(rewritten, OTHER_TYPE)).isTrue();
    }

    @Test
    void a_filter_without_a_payload_condition_is_left_deciding_exactly_as_before() {
        Filter original = Filter.type("SomethingHappened");
        Filter rewritten = assumingPayloadConditionsMatch(original);

        assertThat(matches(rewritten, EVENT)).isTrue();
        assertThat(matches(rewritten, OTHER_TYPE)).isFalse();
    }

    @Test
    void a_nested_payload_condition_is_rewritten_at_any_depth() {
        Filter nested = Filter.type("SomethingHappened").and(Filter.id("id").or(Filter.data("amount", eq(42))));

        assertThat(matches(assumingPayloadConditionsMatch(nested), EVENT)).isTrue();
    }

    @Test
    void the_rewrite_never_needs_a_data_field_reader() {
        // The point of the rewrite: the resulting filter is answerable by a matcher that refuses to read a payload.
        Filter rewritten = assumingPayloadConditionsMatch(Filter.data("amount", eq(42)));

        assertThat(FilterMatcher.matchesFilter(EVENT, rewritten)).isTrue();
    }

    @Test
    void containing_a_payload_condition_is_reported_at_any_depth() {
        assertThat(containsPayloadCondition(Filter.data("amount", eq(42)))).isTrue();
        assertThat(containsPayloadCondition(Filter.type("SomethingHappened").and(Filter.data("amount", eq(42))))).isTrue();
        assertThat(containsPayloadCondition(Filter.type("SomethingHappened"))).isFalse();
        assertThat(containsPayloadCondition(Filter.all())).isFalse();
    }

    @Test
    void a_field_merely_starting_with_data_is_not_a_payload_condition() {
        // datacontenttype is an attribute, not a path into the payload, so it must still be checked.
        Filter rewritten = assumingPayloadConditionsMatch(Filter.dataContentType("application/json"));

        assertThat(matches(rewritten, EVENT)).isFalse();
    }

    private static boolean matches(Filter filter, CloudEvent cloudEvent) {
        return FilterMatcher.matchesFilter(cloudEvent, filter);
    }
}
