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

package org.occurrent.subscription;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionRefusedExceptionTest {

    @Nested
    class TheFamily {

        @Test
        void is_catchable_as_IllegalArgumentException_so_code_written_before_it_existed_still_works() {
            List<SubscriptionRefusedException> everyRefusal = List.of(
                    new DuplicateSubscriptionIdException("id"),
                    new UnknownSubscriptionException("id"),
                    new SubscriptionNotRunningException("id"),
                    new SubscriptionAlreadyRunningException("id"),
                    new UnsupportedSubscriptionFilterException(StreamSubscriptionFilter.class),
                    new UnsupportedStartAtException(StartAt.now()));

            assertThat(everyRefusal).allSatisfy(refusal -> assertThat(refusal).isInstanceOf(IllegalArgumentException.class));
        }

        @Test
        void permits_exactly_the_six_conditions_the_contract_names() {
            assertThat(SubscriptionRefusedException.class.getPermittedSubclasses())
                    .containsExactlyInAnyOrder(DuplicateSubscriptionIdException.class, UnknownSubscriptionException.class,
                            SubscriptionNotRunningException.class, SubscriptionAlreadyRunningException.class,
                            UnsupportedSubscriptionFilterException.class, UnsupportedStartAtException.class);
        }
    }

    @Nested
    class TheStandardMessage {

        @Test
        void names_the_id_that_is_already_defined() {
            assertThat(new DuplicateSubscriptionIdException("orders"))
                    .hasMessage("Subscription orders is already defined.");
        }

        @Test
        void says_an_unknown_id_is_not_known_to_this_model_rather_than_that_it_is_in_the_wrong_state() {
            assertThat(new UnknownSubscriptionException("orders"))
                    .hasMessage("Subscription orders is not known to this subscription model.");
        }

        @Test
        void names_the_id_that_is_not_running() {
            assertThat(new SubscriptionNotRunningException("orders"))
                    .hasMessage("Subscription orders is not running.");
        }

        @Test
        void names_the_id_that_is_already_running() {
            assertThat(new SubscriptionAlreadyRunningException("orders"))
                    .hasMessage("Subscription orders is already running.");
        }

        @Test
        void names_the_filter_type_in_full_since_a_simple_name_does_not_identify_a_caller_supplied_filter() {
            assertThat(new UnsupportedSubscriptionFilterException(StreamSubscriptionFilter.class))
                    .hasMessage("Unsupported SubscriptionFilter type: " + StreamSubscriptionFilter.class.getName() + ".");
        }

        @Test
        void names_the_start_position_that_was_refused() {
            assertThat(new UnsupportedStartAtException(StartAt.now()))
                    .hasMessage("Unsupported StartAt: " + StartAt.now() + ".");
        }

        @Test
        void is_replaced_when_a_model_supplies_one_of_its_own_and_the_accessor_still_answers() {
            SubscriptionNotRunningException refusal =
                    new SubscriptionNotRunningException("orders", "Subscription orders is registered but has not been started.");

            assertThat(refusal).hasMessage("Subscription orders is registered but has not been started.");
            assertThat(refusal.subscriptionId()).isEqualTo("orders");
        }
    }

    @Nested
    class WhatARefusalCarries {

        @Test
        void the_subscription_id_the_call_named() {
            assertThat(new DuplicateSubscriptionIdException("orders").subscriptionId()).isEqualTo("orders");
            assertThat(new UnknownSubscriptionException("orders").subscriptionId()).isEqualTo("orders");
            assertThat(new SubscriptionNotRunningException("orders").subscriptionId()).isEqualTo("orders");
            assertThat(new SubscriptionAlreadyRunningException("orders").subscriptionId()).isEqualTo("orders");
        }

        @Test
        void the_filter_type_the_model_could_not_apply() {
            assertThat(new UnsupportedSubscriptionFilterException(DcbSubscriptionFilter.class).filterType())
                    .isEqualTo(DcbSubscriptionFilter.class);
        }

        @Test
        void the_start_position_the_model_would_not_accept() {
            StartAt refused = StartAt.now();

            assertThat(new UnsupportedStartAtException(refused).startAt()).isSameAs(refused);
        }
    }
}
