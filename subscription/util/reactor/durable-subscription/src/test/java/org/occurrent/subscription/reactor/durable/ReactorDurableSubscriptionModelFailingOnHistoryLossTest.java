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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.HistoryLostException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The durable model reads without a position of its own, so what the wrapped model says about a lost history is what
 * a catch-up on top of it gets.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactorDurableSubscriptionModelFailingOnHistoryLossTest {

    private static final SubscriptionFilter FILTER = StreamSubscriptionFilter.filter(Filter.all());
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint LOST_FROM = new StringBasedCheckpoint("lost from");

    private final List<String> storageCalls = new CopyOnWriteArrayList<>();

    @Test
    void a_flux_that_fails_on_history_loss_is_the_one_of_the_wrapped_model_for_the_same_filter_and_start_and_stores_and_reads_nothing() {
        ReportingModel wrapped = new ReportingModel();
        ReactorDurableSubscriptionModel durable = new ReactorDurableSubscriptionModel(wrapped, untouchedStorage());
        HistoryLostException lost = new HistoryLostException(LOST_FROM, new IllegalStateException("oplog no longer reaches back"));
        wrapped.reads = Flux.concat(Flux.just(event("e1")), Flux.error(lost));

        StepVerifier.create(durable.subscribeFailingOnHistoryLoss(FILTER, StartAt.checkpoint(START)))
                .assertNext(delivered -> assertThat(delivered.getId()).isEqualTo("e1"))
                .expectErrorSatisfies(failure -> assertThat(failure).isSameAs(lost))
                .verify();

        assertAll(
                () -> assertThat(wrapped.askedWith).as("filter and start the wrapped model was asked with").containsExactly(FILTER + " " + StartAt.checkpoint(START)),
                () -> assertThat(wrapped.plainSubscribes).as("plain subscribes of the wrapped model").isEmpty(),
                () -> assertThat(storageCalls).as("calls to the checkpoint storage").isEmpty());
    }

    @Test
    void a_flux_that_fails_on_history_loss_over_a_wrapped_model_that_does_not_report_history_loss_is_refused() {
        ReactorDurableSubscriptionModel durable = new ReactorDurableSubscriptionModel(new SilentModel(), untouchedStorage());

        assertThatThrownBy(() -> durable.subscribeFailingOnHistoryLoss(FILTER, StartAt.checkpoint(START)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(SilentModel.class.getName())
                .hasMessageContaining(HistoryLossReportingSubscriptions.class.getSimpleName());
    }

    // Records the calls it gets and fails each of them
    private CheckpointStorage untouchedStorage() {
        return new CheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return called("read");
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return called("save");
            }

            @Override
            public Mono<Long> writeVersion(String subscriptionId) {
                return called("writeVersion");
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return called("delete");
            }

            private <T> Mono<T> called(String call) {
                storageCalls.add(call);
                return Mono.error(new AssertionError("The checkpoint storage must not be used, but " + call + " was called"));
            }
        };
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build();
    }

    // Reports a loss only through subscribeFailingOnHistoryLoss and fails any other read
    private static final class ReportingModel implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions {
        final List<String> askedWith = new CopyOnWriteArrayList<>();
        final List<String> plainSubscribes = new CopyOnWriteArrayList<>();
        volatile Flux<CloudEvent> reads = Flux.empty();

        @Override
        public Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt) {
            askedWith.add(filter + " " + startAt);
            return reads;
        }

        @Override
        public Flux<CloudEvent> subscribeFailingIfStartLost(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("Only subscribeFailingOnHistoryLoss is delegated"));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            plainSubscribes.add(filter + " " + startAt);
            return Flux.error(new AssertionError("Only subscribeFailingOnHistoryLoss is delegated"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(START);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
        }
    }

    // A wrapped model without the capability
    private static final class SilentModel implements CheckpointAwareSubscriptionModel {
        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(START);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }
    }
}
