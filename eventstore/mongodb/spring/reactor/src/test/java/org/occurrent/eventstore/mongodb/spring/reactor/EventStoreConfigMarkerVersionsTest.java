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

package org.occurrent.eventstore.mongodb.spring.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class EventStoreConfigMarkerVersionsTest {

    @Test
    void resumes_from_marker_versions_by_default() {
        assertThat(builder().build().resumeFromMarkerVersions).isTrue();
    }

    @Test
    void bounds_the_commit_of_the_snapshot_that_reports_marker_versions_by_five_seconds_by_default() {
        assertThat(builder().build().markerVersionsCommitTimeout).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void can_turn_off_resuming_from_marker_versions() {
        assertThat(builder().resumeFromMarkerVersions(false).build().resumeFromMarkerVersions).isFalse();
    }

    @Test
    void keeps_the_commit_timeout_it_was_given() {
        assertThat(builder().markerVersionsCommitTimeout(Duration.ofMillis(250)).build().markerVersionsCommitTimeout).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void accepts_a_commit_timeout_of_one_millisecond() {
        assertThat(builder().markerVersionsCommitTimeout(Duration.ofMillis(1)).build().markerVersionsCommitTimeout).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void rejects_a_commit_timeout_of_zero() {
        assertThatThrownBy(() -> builder().markerVersionsCommitTimeout(Duration.ZERO))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 millisecond");
    }

    @Test
    void rejects_a_negative_commit_timeout() {
        assertThatThrownBy(() -> builder().markerVersionsCommitTimeout(Duration.ofSeconds(-1)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 millisecond");
    }

    @Test
    void rejects_a_commit_timeout_below_one_millisecond() {
        assertThatThrownBy(() -> builder().markerVersionsCommitTimeout(Duration.ofNanos(999_999)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 millisecond");
    }

    @Test
    void rejects_a_commit_timeout_too_large_to_express_in_milliseconds() {
        assertThatThrownBy(() -> builder().markerVersionsCommitTimeout(Duration.ofSeconds(Long.MAX_VALUE)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
    }

    @Test
    void rejects_a_null_commit_timeout() {
        assertThatThrownBy(() -> builder().markerVersionsCommitTimeout(null))
                .isExactlyInstanceOf(NullPointerException.class);
    }

    private static EventStoreConfig.Builder builder() {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(TransactionalOperator.create(neverStartedTransactions()))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING);
    }

    private static ReactiveTransactionManager neverStartedTransactions() {
        return (ReactiveTransactionManager) Proxy.newProxyInstance(ReactiveTransactionManager.class.getClassLoader(), new Class<?>[]{ReactiveTransactionManager.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("The config tests never start a transaction");
                });
    }
}
