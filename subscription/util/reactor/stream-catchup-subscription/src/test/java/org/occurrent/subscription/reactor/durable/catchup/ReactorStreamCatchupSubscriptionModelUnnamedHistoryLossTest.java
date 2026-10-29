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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.HistoryLostException;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.slf4j.helpers.MessageFormatter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An unnamed catch-up has no subscription to pause, so a history loss the live model cannot answer by replaying again
 * fails its Flux with {@link CatchupCannotResumeException} instead of restarting it from the present. The live model
 * here only reports a loss through {@code subscribeFailingOnHistoryLoss}, and fails the test if it is read any other way.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactorStreamCatchupSubscriptionModelUnnamedHistoryLossTest {

    private static final Checkpoint LOST_TOKEN = new StringBasedCheckpoint("a token the oplog lost");
    private static final Checkpoint VALID_TOKEN = new StringBasedCheckpoint("a token the oplog has");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("a token after reading on");

    private final ScriptedLiveModel live = new ScriptedLiveModel();
    private final ErrorsLoggedByTheCatchup logged = new ErrorsLoggedByTheCatchup();
    private final ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(live, new ReactorStreamCatchupSubscriptionModelHistoryLossTest.ScriptedReader());

    @AfterEach
    void close() {
        logged.close();
    }

    @Test
    void a_checkpoint_start_from_a_token_the_live_model_lost_fails_the_flux_before_anything_is_read_and_delivers_nothing() {
        live.loses(LOST_TOKEN);
        live.opens(Flux::empty);

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(LOST_TOKEN)))
                .expectErrorSatisfies(failure -> assertThat(failure).as("failure")
                        .isInstanceOf(CatchupCannotResumeException.class)
                        .hasMessageContaining("Cannot start catch-up subscription")
                        .hasMessageContaining(LOST_TOKEN.asString()))
                .verify();

        assertAll(
                () -> assertThat(live.opens).as("opens of the live model").isEmpty(),
                () -> assertThat(live.resumeProbes).as("tokens asked about").containsExactly(LOST_TOKEN));
    }

    @Test
    void a_checkpoint_start_from_a_token_the_live_model_has_that_reads_on_and_loses_the_history_fails_the_flux_with_what_it_delivered_first() {
        live.opens(() -> Flux.just(liveEvent("e1", 1)).concatWith(Flux.error(live.lostFrom(LATER_TOKEN))));

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(VALID_TOKEN)))
                .assertNext(delivered -> assertThat(delivered.getId()).isEqualTo("e1"))
                .expectErrorSatisfies(failure -> assertThat(failure).as("failure")
                        .isInstanceOf(CatchupCannotResumeException.class)
                        .hasMessageContaining(LATER_TOKEN.asString())
                        .hasCauseInstanceOf(HistoryLostException.class))
                .verify();

        assertAll(
                () -> assertThat(live.opens).as("opens of the live model").containsExactly(StartAt.checkpoint(VALID_TOKEN).toString()),
                () -> assertThat(logged.errors()).as("errors logged").containsExactly(unnamedTerminalFailureLog(LATER_TOKEN)));
    }

    @Test
    void a_start_from_now_that_reads_on_and_loses_the_history_fails_the_flux_instead_of_starting_from_the_present_again() {
        live.opens(() -> Flux.just(liveEvent("e1", 1)).concatWith(Flux.error(live.lostFrom(LATER_TOKEN))));

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.now()))
                .assertNext(delivered -> assertThat(delivered.getId()).isEqualTo("e1"))
                .expectErrorSatisfies(failure -> assertThat(failure).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(LATER_TOKEN.asString()))
                .verify();

        assertAll(
                () -> assertThat(live.opens).as("opens of the live model").hasSize(1),
                () -> assertThat(logged.errors()).as("errors logged").containsExactly(unnamedTerminalFailureLog(LATER_TOKEN)));
    }

    @Test
    void a_start_from_now_that_loses_the_history_before_it_reads_anything_fails_the_flux_with_the_position_it_opened_at() {
        live.opens(() -> Flux.error(live.lostFrom(LATER_TOKEN)));

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.now()))
                .expectErrorSatisfies(failure -> assertThat(failure).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(LATER_TOKEN.asString()))
                .verify();

        assertThat(live.opens).as("opens of the live model").hasSize(1);
    }

    @Test
    void a_catch_up_that_replayed_handed_over_read_on_and_lost_the_history_from_a_later_token_fails_the_flux_and_logs_one_error() {
        live.opens(() -> Flux.just(liveEvent("e4", 4)).concatWith(Flux.error(live.lostFrom(LATER_TOKEN))));

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectNextMatches(event -> event.getId().equals("e1"))
                .expectNextMatches(event -> event.getId().equals("e2"))
                .expectNextMatches(event -> event.getId().equals("e3"))
                .expectNextMatches(event -> event.getId().equals("e4"))
                .expectErrorSatisfies(failure -> assertThat(failure).as("failure")
                        .isInstanceOf(CatchupCannotResumeException.class)
                        .hasMessageContaining(LATER_TOKEN.asString()))
                .verify();

        assertAll(
                () -> assertThat(live.opens).as("opens of the live model, so no replay and no restart from the present").hasSize(1),
                () -> assertThat(logged.errors()).as("errors logged").containsExactly(unnamedTerminalFailureLog(LATER_TOKEN)));
    }

    @Test
    void a_catch_up_whose_live_half_loses_exactly_the_live_start_it_opened_at_replays_again_and_logs_no_error() {
        live.opens(() -> Flux.error(live.lostFrom(ScriptedLiveModel.token(1))));
        live.opens(() -> Flux.just(liveEvent("e4", 4)).concatWith(Flux.never()));

        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(GlobalCheckpoint.of(0))).map(CloudEvent::getId).take(7))
                .expectNext("e1", "e2", "e3", "e1", "e2", "e3", "e4")
                .verifyComplete();

        assertAll(
                () -> assertThat(live.opens).as("opens of the live model").hasSize(2),
                () -> assertThat(logged.errors()).as("errors logged").isEmpty());
    }

    private static String unnamedTerminalFailureLog(Checkpoint lostFrom) {
        return MessageFormatter.arrayFormat(HistoryLossMessages.UNNAMED_TERMINAL_FAILURE_LOG, new Object[]{lostFrom.asString(), lostFrom.asString()}).getMessage();
    }

    private static CloudEvent liveEvent(String id, long position) {
        return OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build(), position);
    }

    // Reports a loss only through subscribeFailingOnHistoryLoss, and fails the test when it is read in any other way.
    private static final class ScriptedLiveModel implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions {
        private final Set<Checkpoint> lost = ConcurrentHashMap.newKeySet();
        private final AtomicInteger tokens = new AtomicInteger();
        private final Queue<Supplier<Flux<CloudEvent>>> script = new ArrayDeque<>();
        final List<Checkpoint> resumeProbes = new CopyOnWriteArrayList<>();
        final List<String> opens = new CopyOnWriteArrayList<>();

        static Checkpoint token(int number) {
            return new StringBasedCheckpoint("live start " + number);
        }

        void loses(Checkpoint checkpoint) {
            lost.add(checkpoint);
        }

        // What the next open reads, the last one is repeated
        synchronized void opens(Supplier<Flux<CloudEvent>> read) {
            script.add(read);
        }

        HistoryLostException lostFrom(Checkpoint checkpoint) {
            return new HistoryLostException(checkpoint, new IllegalStateException("oplog no longer reaches back"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.fromSupplier(() -> token(tokens.incrementAndGet()));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
            return Mono.fromSupplier(() -> {
                resumeProbes.add(checkpoint);
                return !lost.contains(checkpoint);
            });
        }

        @Override
        public Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.defer(() -> {
                opens.add(startAt.toString());
                return next().get();
            });
        }

        private synchronized Supplier<Flux<CloudEvent>> next() {
            return script.size() > 1 ? script.remove() : script.isEmpty() ? Flux::never : script.peek();
        }

        @Override
        public Flux<CloudEvent> subscribeFailingIfStartLost(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The unnamed catch-up must read through subscribeFailingOnHistoryLoss"));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The unnamed catch-up must read through subscribeFailingOnHistoryLoss"));
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
        }
    }
}
