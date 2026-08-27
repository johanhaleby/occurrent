/*
 *
 *  Copyright 2026 Johan Haleby
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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.springboot.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plain unit tests for the hand-written validation on {@link OccurrentProperties}. No Spring context: the setters are
 * where the rejection happens, and Spring only turns the resulting exception into a startup failure, which
 * {@code OccurrentMongoAutoConfigurationCharacterizationTest} covers separately.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentPropertiesTest {

    @Test
    void the_catch_up_then_live_tunables_are_unset_by_default_so_the_built_in_defaults_apply() {
        CatchupThenLiveProperties properties = new OccurrentProperties().getSubscription().getCatchupThenLive();

        // Null rather than a copy of the real default, so the numbers live in one place and cannot drift.
        assertThat(properties.getDedupCacheSize()).isNull();
        assertThat(properties.getMaxBufferedEvents()).isNull();
    }

    @Test
    void a_non_positive_dedup_cache_size_is_rejected_with_the_property_key_in_the_message() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();

        assertThatThrownBy(() -> properties.setDedupCacheSize(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("occurrent.subscription.catchup-then-live.dedup-cache-size must be greater than zero");
        assertThatThrownBy(() -> properties.setDedupCacheSize(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_non_positive_max_buffered_events_is_rejected_with_the_property_key_in_the_message() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();

        assertThatThrownBy(() -> properties.setMaxBufferedEvents(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("occurrent.subscription.catchup-then-live.max-buffered-events must be greater than zero");
        assertThatThrownBy(() -> properties.setMaxBufferedEvents(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void clearing_a_tunable_back_to_unset_is_allowed() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();
        properties.setDedupCacheSize(50_000);

        properties.setDedupCacheSize(null);

        assertThat(properties.getDedupCacheSize()).isNull();
    }

    /**
     * Covers how {@code occurrent.event-store.mongodb.collection} and the deprecated
     * {@code occurrent.event-store.collection} combine. The pair is allowed while they agree, because a recipe
     * rewrites configuration files but cannot reach an environment variable, so an application mid-migration can
     * legitimately have both set. Mirrors {@link SubscriptionModeTest}.
     */
    @Nested
    class Resolving_the_event_store_collection {

        @Test
        void defaults_to_events_when_neither_is_set() {
            assertThat(resolveEventStoreCollection(null, null)).isEqualTo("events");
        }

        @Test
        void uses_the_new_key_when_only_it_is_set() {
            assertThat(resolveEventStoreCollection(null, "events-v2")).isEqualTo("events-v2");
        }

        @Test
        void uses_the_deprecated_key_when_only_it_is_set() {
            assertThat(resolveEventStoreCollection("events-v2", null)).isEqualTo("events-v2");
        }

        @Test
        void accepts_both_when_they_agree() {
            assertThat(resolveEventStoreCollection("events-v2", "events-v2")).isEqualTo("events-v2");
        }

        @Test
        void fails_when_both_are_set_and_contradict_each_other() {
            assertThatThrownBy(() -> resolveEventStoreCollection("events-v1", "events-v2"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("occurrent.event-store.mongodb.collection is \"events-v2\"")
                    .hasMessageContaining("occurrent.event-store.collection is \"events-v1\"")
                    .hasMessageContaining("environment variables");
        }

        private static String resolveEventStoreCollection(@Nullable String deprecated, @Nullable String mongodb) {
            EventStoreProperties properties = new EventStoreProperties();
            properties.setCollection(deprecated);
            properties.getMongodb().setCollection(mongodb);
            return properties.resolveCollection();
        }
    }

    /**
     * Covers how {@code occurrent.event-store.mongodb.time-representation} and the deprecated
     * {@code occurrent.event-store.time-representation} combine. Mirrors {@link Resolving_the_event_store_collection}.
     */
    @Nested
    class Resolving_the_event_store_time_representation {

        @Test
        void defaults_to_date_when_neither_is_set() {
            assertThat(resolveTimeRepresentation(null, null)).isEqualTo(TimeRepresentation.DATE);
        }

        @Test
        void uses_the_new_key_when_only_it_is_set() {
            assertThat(resolveTimeRepresentation(null, TimeRepresentation.RFC_3339_STRING)).isEqualTo(TimeRepresentation.RFC_3339_STRING);
        }

        @Test
        void uses_the_deprecated_key_when_only_it_is_set() {
            assertThat(resolveTimeRepresentation(TimeRepresentation.RFC_3339_STRING, null)).isEqualTo(TimeRepresentation.RFC_3339_STRING);
        }

        @Test
        void accepts_both_when_they_agree() {
            assertThat(resolveTimeRepresentation(TimeRepresentation.RFC_3339_STRING, TimeRepresentation.RFC_3339_STRING)).isEqualTo(TimeRepresentation.RFC_3339_STRING);
        }

        @Test
        void fails_when_both_are_set_and_contradict_each_other() {
            assertThatThrownBy(() -> resolveTimeRepresentation(TimeRepresentation.DATE, TimeRepresentation.RFC_3339_STRING))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("occurrent.event-store.mongodb.time-representation is RFC_3339_STRING")
                    .hasMessageContaining("occurrent.event-store.time-representation is DATE")
                    .hasMessageContaining("environment variables");
        }

        private static TimeRepresentation resolveTimeRepresentation(@Nullable TimeRepresentation deprecated, @Nullable TimeRepresentation mongodb) {
            EventStoreProperties properties = new EventStoreProperties();
            properties.setTimeRepresentation(deprecated);
            properties.getMongodb().setTimeRepresentation(mongodb);
            return properties.resolveTimeRepresentation();
        }
    }

    /**
     * Covers how {@code occurrent.subscription.mongodb.collection} and the deprecated
     * {@code occurrent.subscription.collection} combine. Mirrors {@link Resolving_the_event_store_collection}.
     */
    @Nested
    class Resolving_the_subscription_collection {

        @Test
        void defaults_to_subscriptions_when_neither_is_set() {
            assertThat(resolveSubscriptionCollection(null, null)).isEqualTo("subscriptions");
        }

        @Test
        void uses_the_new_key_when_only_it_is_set() {
            assertThat(resolveSubscriptionCollection(null, "subscriptions-v2")).isEqualTo("subscriptions-v2");
        }

        @Test
        void uses_the_deprecated_key_when_only_it_is_set() {
            assertThat(resolveSubscriptionCollection("subscriptions-v2", null)).isEqualTo("subscriptions-v2");
        }

        @Test
        void accepts_both_when_they_agree() {
            assertThat(resolveSubscriptionCollection("subscriptions-v2", "subscriptions-v2")).isEqualTo("subscriptions-v2");
        }

        @Test
        void fails_when_both_are_set_and_contradict_each_other() {
            assertThatThrownBy(() -> resolveSubscriptionCollection("subscriptions-v1", "subscriptions-v2"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("occurrent.subscription.mongodb.collection is \"subscriptions-v2\"")
                    .hasMessageContaining("occurrent.subscription.collection is \"subscriptions-v1\"")
                    .hasMessageContaining("environment variables");
        }

        private static String resolveSubscriptionCollection(@Nullable String deprecated, @Nullable String mongodb) {
            SubscriptionProperties properties = new SubscriptionProperties();
            properties.setCollection(deprecated);
            properties.getMongodb().setCollection(mongodb);
            return properties.resolveCollection();
        }
    }

    /**
     * Covers how {@code occurrent.subscription.mongodb.restart-on-change-stream-history-lost} and the deprecated
     * {@code occurrent.subscription.restart-on-change-stream-history-lost} combine. Mirrors
     * {@link Resolving_the_event_store_collection}.
     */
    @Nested
    class Resolving_restart_on_change_stream_history_lost {

        @Test
        void defaults_to_true_when_neither_is_set() {
            assertThat(resolveRestartOnChangeStreamHistoryLost(null, null)).isTrue();
        }

        @Test
        void uses_the_new_key_when_only_it_is_set() {
            assertThat(resolveRestartOnChangeStreamHistoryLost(null, false)).isFalse();
        }

        @Test
        void uses_the_deprecated_key_when_only_it_is_set() {
            assertThat(resolveRestartOnChangeStreamHistoryLost(false, null)).isFalse();
        }

        @Test
        void accepts_both_when_they_agree() {
            assertThat(resolveRestartOnChangeStreamHistoryLost(false, false)).isFalse();
        }

        @Test
        void fails_when_both_are_set_and_contradict_each_other() {
            assertThatThrownBy(() -> resolveRestartOnChangeStreamHistoryLost(true, false))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("occurrent.subscription.mongodb.restart-on-change-stream-history-lost is false")
                    .hasMessageContaining("occurrent.subscription.restart-on-change-stream-history-lost is true")
                    .hasMessageContaining("environment variables");
        }

        private static boolean resolveRestartOnChangeStreamHistoryLost(@Nullable Boolean deprecated, @Nullable Boolean mongodb) {
            SubscriptionProperties properties = new SubscriptionProperties();
            // setRestartOnChangeStreamHistoryLost(boolean) is primitive, matching the released signature, so "unset"
            // is represented by not calling it at all rather than by passing it null.
            if (deprecated != null) {
                properties.setRestartOnChangeStreamHistoryLost(deprecated);
            }
            properties.getMongodb().setRestartOnChangeStreamHistoryLost(mongodb);
            return properties.resolveRestartOnChangeStreamHistoryLost();
        }
    }

    /**
     * The four deprecated getters ({@code EventStoreProperties.getCollection()}, {@code getTimeRepresentation()},
     * {@code SubscriptionProperties.getCollection()}, {@code isRestartOnChangeStreamHistoryLost()}) returned a
     * non-null, defaulted value before {@code occurrent.event-store.mongodb.*}/{@code occurrent.subscription.mongodb.*}
     * existed. Each now delegates to its {@code resolve*()} counterpart instead of returning the raw, possibly-unset
     * field, so a caller compiled against the old non-null contract keeps seeing a resolved value, including one
     * supplied only through the new key, rather than null.
     */
    @Nested
    class Deprecated_getters_delegate_to_the_resolved_value {

        @Test
        void event_store_get_collection_reflects_the_new_key_when_the_deprecated_key_is_unset() {
            EventStoreProperties properties = new EventStoreProperties();
            properties.getMongodb().setCollection("events-v3");

            assertThat(properties.getCollection()).isEqualTo("events-v3");
        }

        @Test
        void event_store_get_time_representation_reflects_the_new_key_when_the_deprecated_key_is_unset() {
            EventStoreProperties properties = new EventStoreProperties();
            properties.getMongodb().setTimeRepresentation(TimeRepresentation.RFC_3339_STRING);

            assertThat(properties.getTimeRepresentation()).isEqualTo(TimeRepresentation.RFC_3339_STRING);
        }

        @Test
        void subscription_get_collection_reflects_the_new_key_when_the_deprecated_key_is_unset() {
            SubscriptionProperties properties = new SubscriptionProperties();
            properties.getMongodb().setCollection("subscriptions-v3");

            assertThat(properties.getCollection()).isEqualTo("subscriptions-v3");
        }

        @Test
        void is_restart_on_change_stream_history_lost_reflects_the_new_key_set_to_false_when_the_deprecated_key_is_unset() {
            SubscriptionProperties properties = new SubscriptionProperties();
            properties.getMongodb().setRestartOnChangeStreamHistoryLost(false);

            assertThat(properties.isRestartOnChangeStreamHistoryLost()).isFalse();
        }

        // A getter that ignores the new key entirely and just returns "the deprecated field, defaulting to false
        // when unset" would still pass the case above, since that default coincides with what was explicitly set
        // here. Setting the new key to true, which the same broken shortcut would return as false, closes that gap.
        @Test
        void is_restart_on_change_stream_history_lost_reflects_the_new_key_set_to_true_when_the_deprecated_key_is_unset() {
            SubscriptionProperties properties = new SubscriptionProperties();
            properties.getMongodb().setRestartOnChangeStreamHistoryLost(true);

            assertThat(properties.isRestartOnChangeStreamHistoryLost()).isTrue();
        }

        @Test
        void none_of_the_four_deprecated_getters_return_null_when_nothing_is_set() {
            EventStoreProperties eventStore = new EventStoreProperties();
            SubscriptionProperties subscription = new SubscriptionProperties();

            assertThat(eventStore.getCollection()).isEqualTo("events");
            assertThat(eventStore.getTimeRepresentation()).isEqualTo(TimeRepresentation.DATE);
            assertThat(subscription.getCollection()).isEqualTo("subscriptions");
            assertThat(subscription.isRestartOnChangeStreamHistoryLost()).isTrue();
        }
    }

    @Nested
    class The_applied_append_attempt_limit {

        @Test
        void defaults_to_ten_attempts() {
            OccurrentProperties properties = new OccurrentProperties();

            assertThat(properties.getProjection().getAppliedAppend().getMaxAttempts()).isEqualTo(10);
        }

        @Test
        void rejects_zero_because_a_store_that_is_never_called_records_nothing() {
            OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                    new OccurrentProperties().getProjection().getAppliedAppend();

            assertThatThrownBy(() -> appliedAppend.setMaxAttempts(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("occurrent.projection.applied-append.max-attempts");
        }

        @Test
        void rejects_a_negative_number_of_attempts() {
            OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                    new OccurrentProperties().getProjection().getAppliedAppend();

            assertThatThrownBy(() -> appliedAppend.setMaxAttempts(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least 1");
        }

        @Test
        void rejects_more_attempts_than_the_store_would_ever_make() {
            OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                    new OccurrentProperties().getProjection().getAppliedAppend();

            assertThatThrownBy(() -> appliedAppend.setMaxAttempts(
                    OccurrentProperties.ProjectionProperties.AppliedAppendProperties.MAX_ATTEMPTS_CEILING + 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot exceed 1000")
                    .hasMessageContaining("1001");
        }

        @Test
        void allows_exactly_the_ceiling_the_store_stops_an_unstopping_policy_at() {
            OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                    new OccurrentProperties().getProjection().getAppliedAppend();
            int ceiling = OccurrentProperties.ProjectionProperties.AppliedAppendProperties.MAX_ATTEMPTS_CEILING;

            appliedAppend.setMaxAttempts(ceiling);

            assertThat(appliedAppend.getMaxAttempts()).isEqualTo(ceiling);
        }

        @Test
        void allows_a_single_attempt_meaning_no_retry_at_all() {
            OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                    new OccurrentProperties().getProjection().getAppliedAppend();

            appliedAppend.setMaxAttempts(1);

            assertThat(appliedAppend.getMaxAttempts()).isEqualTo(1);
        }
    }
}
