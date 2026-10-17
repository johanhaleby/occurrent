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

package org.occurrent.dsl.projection.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupProjectionFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    @Test
    void catches_up_history_then_folds_live_domain_events() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        feed.acceptAndWait(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void catch_up_threads_event_metadata_into_the_fold() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position: both come from the replayed
        // CloudEvent, so if the catch-up did not thread the metadata, keying on getStreamId() would fail on empty metadata.
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, metadataReader("1", "2"), converter, Counted::eventId, null);

        feed.catchUp().block();

        // Keyed under the stream id "s" from the metadata, folded to the last replayed event's position (2), not to the
        // 0 that an empty-metadata fold would leave behind.
        assertThat(repo).containsOnlyKeys("s");
        assertThat(repo.get("s")).isEqualTo(2L);
    }

    @Test
    void a_live_domain_event_fed_with_metadata_lands_under_the_metadata_derived_key() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position, same shape as issue 389's
        // headline bug: catches up correctly, then a live event supplies its own metadata via acceptAndWait(metadata, event).
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, metadataReader("1", "2"), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.acceptAndWait(metadata("live-stream", 99L), new Counted("3")).block();

        assertThat(repo).containsKey("live-stream");
        assertThat(repo.get("live-stream")).isEqualTo(99L);
    }

    @Test
    void a_live_domain_event_fed_without_metadata_for_a_position_keyed_projection_throws_IllegalStateException() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<Long, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, Long> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed on getPosition(), the accessor that returns null on empty metadata rather than throwing. Before the
        // fix this silently dropped the live event and completed normally instead of surfacing the missing metadata.
        Projection<Integer, Counted, Long> projection = Projection.<Integer, Counted, Long>builder(0)
                .id((metadata, event) -> metadata.getPosition())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions-key", projection, repository, metadataReader("1"), converter, Counted::eventId, null);
        feed.catchUp().block();

        StepVerifier.create(feed.acceptAndWait(new Counted("2")))
                .verifyErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("would have been skipped silently"));
    }

    @Test
    void a_projection_declared_metadata_keyed_but_ignoring_the_metadata_does_not_error_on_a_plain_live_event() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Declared through id(BiFunction), so metadataKeyed() is true, but the key itself never reads the metadata,
        // so it always returns a real id and the metadata guard must never trip for it.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id((metadata, event) -> event.eventId())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "ignoring-metadata", projection, repository, reader(), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.acceptAndWait(new Counted("live-1")).block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("live-1")).isEqualTo(1));
    }

    @Test
    void an_event_keyed_projection_whose_id_returns_null_skips_that_event_and_still_folds_the_rest() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // id(Function), not id(BiFunction): metadataKeyed() stays false, so a null id here is the documented "skip",
        // not the metadata guard's failure mode.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id(event -> event.eventId().equals("skip-me") ? null : "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "skip-null-id", projection, repository, reader(), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.acceptAndWait(new Counted("skip-me")).block();
        feed.acceptAndWait(new Counted("keep-me")).block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(1));
    }

    @Test
    void accept_and_wait_during_the_catch_up_completes_only_once_the_event_is_folded() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);

            CompletableFuture<List<String>> foldedWhenAcceptCompleted = feed.acceptAndWait(new Counted("live"))
                    .then(Mono.fromCallable(() -> List.copyOf(folded)))
                    .toFuture();
            releaseReplay.countDown();

            catchUp.get(5, TimeUnit.SECONDS);
            assertThat(foldedWhenAcceptCompleted.get(5, TimeUnit.SECONDS)).as("what was folded when acceptAndWait(..) completed")
                    .containsExactly("1", "live");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void stopping_the_catch_up_while_accept_and_wait_waits_errors_accept_and_wait_and_folds_nothing_live() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);

            CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();
            feed.stopCatchUp();
            releaseReplay.countDown();

            catchUp.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) errored with once the catch-up stopped")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
            assertThat(folded).containsExactly("1");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void a_catch_up_failing_while_accept_and_wait_waits_errors_accept_and_wait_with_the_failure() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold failed");
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, foldFailure),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);

            CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();
            releaseReplay.countDown();

            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).hasCause(foldFailure);
            assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) errored with once the catch-up failed")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.catchUpFailed("projection feed"))
                    .hasCauseReference(foldFailure);
            assertThat(folded).isEmpty();
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void stopping_a_feed_whose_catch_up_never_started_errors_a_waiting_accept_and_wait_and_every_accept_and_wait_after_it() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();

        feed.stopCatchUp();

        assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) errored with once the feed stopped")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
        CompletableFuture<Void> acceptedAfterTheStop = feed.acceptAndWait(new Counted("live-after-stop")).toFuture();
        assertThatThrownBy(() -> acceptedAfterTheStop.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) errored with after the stop")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
        assertThat(folded).isEmpty();
    }

    @Test
    void a_catch_up_after_a_stop_before_any_catch_up_takes_the_feed_live() {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feed.stopCatchUp();

        feed.catchUp().block(ofSeconds(5));
        feed.acceptAndWait(new Counted("live")).block(ofSeconds(5));

        assertThat(folded).containsExactly("1", "live");
    }

    // The stop comes while the first catch-up reads the marker, and the second catch-up starts after the stop. Both
    // reads throw, the first one first.
    @Test
    void a_stop_between_two_catch_ups_whose_marker_reads_throw_errors_a_waiting_accept_and_wait() throws Exception {
        assertThat(acceptAfterTwoCatchUpsThrewAroundAStop(true)).as("what acceptAndWait(..) ended with after stopCatchUp() and two catch-ups that threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
    }

    @Test
    void a_stop_between_two_catch_ups_whose_marker_reads_throw_errors_a_waiting_accept_and_wait_when_the_later_one_throws_first() throws Exception {
        assertThat(acceptAfterTwoCatchUpsThrewAroundAStop(false)).as("what acceptAndWait(..) ended with after stopCatchUp() and two catch-ups that threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
    }

    @Test
    void a_stop_during_a_catch_up_whose_marker_read_throws_errors_a_waiting_accept_and_wait() throws Exception {
        CountDownLatch asking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, markerWhoseReadsThrow(List.of(asking), List.of(release)));
        CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<?> catchingUp = executor.submit(() -> feed.catchUp());
            awaitLatch(asking);
            feed.stopCatchUp();
            release.countDown();

            assertThatThrownBy(() -> catchingUp.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) ended with")
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
            assertThat(folded).isEmpty();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_stop_during_a_catch_up_whose_marker_read_throws_errors_a_waiting_accept_and_wait_on_the_thread_whose_catch_up_threw() throws Exception {
        CountDownLatch asking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", (Counted e) -> Mono.<Void>empty(),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, markerWhoseReadsThrow(List.of(asking), List.of(release)));
        CompletableFuture<String> erroredOn = new CompletableFuture<>();
        feed.acceptAndWait(new Counted("live")).subscribe(null, error -> erroredOn.complete(Thread.currentThread().getName()));
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor(task -> new Thread(task, "catch-up-caller"));
        try {
            java.util.concurrent.Future<?> catchingUp = executor.submit(() -> feed.catchUp());
            awaitLatch(asking);
            feed.stopCatchUp();
            release.countDown();

            assertThatThrownBy(() -> catchingUp.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            assertThat(erroredOn.get(5, TimeUnit.SECONDS)).as("the thread the error handling of acceptAndWait(..) ran on").isEqualTo("catch-up-caller");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    // A goLive() called before the stop counts as a catch-up and waits for the replay. It has nothing to replay, so it
    // does not notice the stop, and takes the feed live once the stopped replay has ended.
    @Test
    void a_goLive_called_before_a_stop_takes_the_feed_live_once_the_stopped_replay_ends() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);
            CompletableFuture<Void> wentLive = goLiveWaitingForTheReplay(feed);
            CompletableFuture<Void> fedBeforeTheStop = feed.acceptAndWait(new Counted("before-stop")).toFuture();
            feed.stopCatchUp();
            releaseReplay.countDown();

            catchUp.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> fedBeforeTheStop.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) of an event fed before the stop errored with")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
            wentLive.get(5, TimeUnit.SECONDS);
            feed.acceptAndWait(new Counted("after-stop")).block(ofSeconds(5));
            assertThat(folded).containsExactly("1", "after-stop");
        } finally {
            releaseReplay.countDown();
        }
    }

    // The stopped replay tells the view it was abandoned before it gives its count back. A catchUp() called in that
    // window clears the stop and throws while the stopped one still counts.
    @Test
    void a_catch_up_whose_marker_read_throws_while_a_stopped_replay_is_ending_leaves_the_feed_stopped() throws Exception {
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch stopAsked = new CountDownLatch(1);
        CountDownLatch abandoning = new CountDownLatch(1);
        CountDownLatch otherThrew = new CountDownLatch(1);
        final class ViewHeldWhenAbandoned implements java.util.function.BiFunction<EventMetadata, Counted, Mono<Void>>, ReactiveReplayAware {
            @Override
            public Mono<Void> apply(EventMetadata metadata, Counted event) {
                return Mono.fromRunnable(() -> {
                    if (event.eventId().equals("1")) {
                        replaying.countDown();
                        awaitLatch(stopAsked);
                    }
                });
            }

            @Override
            public void replayStarted() {
            }

            @Override
            public Mono<Void> replayCompleted() {
                return Mono.empty();
            }

            @Override
            public void replayAbandoned() {
                abandoning.countDown();
                awaitLatch(otherThrew);
            }
        }
        AtomicInteger reads = new AtomicInteger();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", new ViewHeldWhenAbandoned(),
                Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId,
                markerReadAs(() -> {
                    if (reads.getAndIncrement() == 0) {
                        return Mono.empty();
                    }
                    throw new IllegalStateException("marker unreadable");
                }));
        CompletableFuture<Void> stoppedCatchUp = feed.catchUp().toFuture();
        awaitLatch(replaying);
        feed.stopCatchUp();
        stopAsked.countDown();
        awaitLatch(abandoning);

        try {
            assertThatThrownBy(feed::catchUp).hasMessage("marker unreadable");
        } finally {
            otherThrew.countDown();
        }
        stoppedCatchUp.get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();
        assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what acceptAndWait(..) ended with once the stopped replay ended and the other catch-up threw")
                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
    }

    // Returns what the Mono of an event fed before both catch-ups ended with, a TimeoutException when it never ended.
    private Throwable acceptAfterTwoCatchUpsThrewAroundAStop(boolean firstThrowsFirst) throws Exception {
        CountDownLatch askingFirst = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch askingSecond = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId,
                markerWhoseReadsThrow(List.of(askingFirst, askingSecond), List.of(releaseFirst, releaseSecond)));
        CompletableFuture<Void> accepted = feed.acceptAndWait(new Counted("live")).toFuture();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<?> first = executor.submit(() -> feed.catchUp());
            awaitLatch(askingFirst);
            feed.stopCatchUp();
            java.util.concurrent.Future<?> second = executor.submit(() -> feed.catchUp());
            awaitLatch(askingSecond);

            if (firstThrowsFirst) {
                releaseFirst.countDown();
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
                releaseSecond.countDown();
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            } else {
                releaseSecond.countDown();
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
                releaseFirst.countDown();
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            }

            assertThat(folded).isEmpty();
            try {
                accepted.get(5, TimeUnit.SECONDS);
                return null;
            } catch (java.util.concurrent.ExecutionException e) {
                return e.getCause();
            } catch (java.util.concurrent.TimeoutException e) {
                return e;
            }
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            executor.shutdownNow();
        }
    }

    // Read number n counts down asking n, waits for release n, then throws instead of returning a Mono.
    private static CheckpointStorage markerWhoseReadsThrow(List<CountDownLatch> asking, List<CountDownLatch> release) {
        AtomicInteger reads = new AtomicInteger();
        return markerReadAs(() -> {
            int read = reads.getAndIncrement();
            asking.get(read).countDown();
            awaitLatch(release.get(read));
            throw new IllegalStateException("marker unreadable");
        });
    }

    private static CheckpointStorage markerReadAs(java.util.function.Supplier<Mono<org.occurrent.subscription.Checkpoint>> read) {
        return new CheckpointStorage() {
            @Override
            public Mono<org.occurrent.subscription.Checkpoint> read(String subscriptionId) {
                return read.get();
            }

            @Override
            public Mono<org.occurrent.subscription.Checkpoint> save(String subscriptionId, org.occurrent.subscription.Checkpoint checkpoint,
                                                                   org.occurrent.subscription.CheckpointWriteCondition condition) {
                return Mono.error(new UnsupportedOperationException("save"));
            }

            @Override
            public Mono<Long> writeVersion(String subscriptionId) {
                return Mono.error(new UnsupportedOperationException("writeVersion"));
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.error(new UnsupportedOperationException("delete"));
            }
        };
    }

    // goLive() completing while catchUp() still replayed left the feed refusing live events after the caller was told
    // it is live.
    @Test
    void goLive_while_the_catch_up_replays_completes_only_once_that_replay_has_ended() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);

            CompletableFuture<Void> wentLive = goLiveWaitingForTheReplay(feed);

            assertThat(wentLive).as("goLive() while the replay is held").isNotDone();
            releaseReplay.countDown();
            catchUp.get(5, TimeUnit.SECONDS);
            wentLive.get(5, TimeUnit.SECONDS);
            assertThat(feed.acceptOrDefer(EventMetadata.empty(), new Counted("live")).block(ofSeconds(5))).isEqualTo(RoutingOutcome.DELIVERED);
            assertThat(folded).containsExactly("1", "live");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void goLive_while_the_catch_up_replays_errors_when_that_replay_fails() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold failed");
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, foldFailure),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        try {
            CompletableFuture<Void> catchUp = Mono.defer(feed::catchUp).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(replaying);

            CompletableFuture<Void> wentLive = goLiveWaitingForTheReplay(feed);

            assertThat(wentLive).as("goLive() while the replay is held").isNotDone();
            releaseReplay.countDown();
            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).hasCause(foldFailure);
            assertThatThrownBy(() -> wentLive.get(5, TimeUnit.SECONDS)).as("what goLive() errored with once the replay failed")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.catchUpFailed("projection feed"))
                    .hasCauseReference(foldFailure);
        } finally {
            releaseReplay.countDown();
        }
    }

    // Folds every event, and on the one named first runs what feeds this feed, before the event itself is recorded
    private static Function<Counted, Mono<Void>> feedingBackOn(String eventId, AtomicReference<CatchupProjectionFeed<Counted>> feed,
                                                               List<String> folded, Function<CatchupProjectionFeed<Counted>, Mono<Void>> feedBack) {
        return event -> (event.eventId().equals(eventId) ? Mono.defer(() -> feedBack.apply(feed.get())) : Mono.<Void>empty())
                .then(Mono.fromRunnable(() -> folded.add(event.eventId())));
    }

    // The fold carries on when its acceptOrDefer(..) for "n" errors. "3" is accepted from outside afterwards, and
    // being folded with no "n" before it shows "n" was never queued.
    private void assertThatAcceptOrDeferFromTheFoldIsRefused(boolean live, Function<CatchupProjectionFeed<Counted>, Mono<RoutingOutcome>> acceptOrDefer) {
        List<String> folded = new CopyOnWriteArrayList<>();
        List<RoutingOutcome> outcomes = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn(live ? "2" : "1", feedRef, folded, self -> acceptOrDefer.apply(self)
                        .doOnNext(outcomes::add).doOnError(errors::add).onErrorComplete().then()),
                Filter.all(), live ? reader("1") : reader("1", "2"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));
        if (live) {
            StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));
        }

        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));

        assertThat(outcomes).isEmpty();
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.acceptedOrDeferredFromOwnDelivery("projection feed"));
        assertThat(folded).containsExactly("1", "2", "3");
    }

    // goLive() subscribes its wait from the one task it schedules, and the marker read ahead of that wait is
    // synchronous, so once that task has run the call is waiting, or has completed, which the caller then catches
    private static CompletableFuture<Void> goLiveWaitingForTheReplay(CatchupProjectionFeed<Counted> feed) {
        CountDownLatch subscribed = new CountDownLatch(1);
        String hook = "goLiveWaitingForTheReplay";
        Schedulers.onScheduleHook(hook, task -> () -> {
            try {
                task.run();
            } finally {
                subscribed.countDown();
            }
        });
        try {
            CompletableFuture<Void> wentLive = feed.goLive().toFuture();
            awaitLatch(subscribed);
            return wentLive;
        } finally {
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    // Records every event it folds, and holds the replay at the given event until released
    private static Function<Counted, Mono<Void>> holdingTheReplayAt(String held, List<String> folded, CountDownLatch reached,
                                                                    CountDownLatch release, RuntimeException failure) {
        return event -> Mono.fromRunnable(() -> {
            if (event.eventId().equals(held)) {
                reached.countDown();
                awaitLatch(release);
                if (failure != null) {
                    throw failure;
                }
            }
            folded.add(event.eventId());
        });
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_folded_once() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.acceptAndWait(new Counted("2")).subscribe(); // buffered before catch-up, overlaps the replay
        feed.catchUp().block();

        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    // The replay applies the overlapping event and the live copy is suppressed, which is right. What used to go
    // missing with it was the recording. An applied append is written down on a live delivery, so a projection wired
    // to record appends applied the event and never recorded it, and waitUntilApplied kept answering false.
    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_recorded_as_applied() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AppendId appendId = new AppendId(UUID.randomUUID());
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter",
                Projections.recordingAppliedAppends(
                        Projections.reactiveUpdateWithMetadata(projection(), repository, "counter"), "counter", appliedAppends),
                Filter.all(), stampedReader(appendId, "1", "2"), converter, Counted::eventId, null);

        // "2" also arrives live before the catch-up completes, carrying the metadata a broker source reads off the
        // message rather than the no-metadata overload the test above uses.
        feed.acceptAndWait(EventMetadata.from(stamped(appendId, "2")), new Counted("2")).subscribe();
        feed.catchUp().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo.get("counter")).isEqualTo(2);
            assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
        });
    }

    // Projection.id returning null skips an event, so the replay can deliver an event without applying it. Its live
    // copy is still suppressed as a duplicate of that delivery, and must not record the append, because nothing in
    // the read model came from it.
    @Test
    void an_event_the_replay_skipped_is_not_recorded_as_applied_when_its_live_copy_is_suppressed() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AppendId appendId = new AppendId(UUID.randomUUID());
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter",
                Projections.recordingAppliedAppends(
                        Projections.reactiveUpdateWithMetadata(skippingEveryEvent(), repository, "counter"), "counter", appliedAppends),
                Filter.all(), stampedReader(appendId, "1"), converter, Counted::eventId, null);
        EventMetadata liveCopy = EventMetadata.from(stamped(appendId, "1"));

        // One copy buffers during the catch-up and one arrives after it, the two moments a copy can be suppressed.
        // The live sink delivers in order, so once the second has been handled the first has too.
        feed.acceptAndWait(liveCopy, new Counted("1")).subscribe();
        feed.catchUp().block();
        feed.acceptAndWait(liveCopy, new Counted("1")).block(ofSeconds(5));

        assertThat(repo).isEmpty();
        assertThat(appliedAppends.hasApplied("counter", appendId)).isFalse();
    }

    // A stopped replay discards what a coalescing view buffered. The live copy of an event that replay delivered has
    // to be applied after goLive(), not skipped as a duplicate of a delivery whose work was thrown away.
    @Test
    void an_event_a_stopped_replay_delivered_is_applied_when_its_live_copy_arrives_after_go_live() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AppendId appendId = new AppendId(UUID.randomUUID());
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        // A batch larger than the history, so the view writes nothing before the stop discards what it buffered.
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter",
                Projections.recordingAppliedAppends(
                        Projections.reactiveUpdateWithMetadata(stoppingTheReplayAtTheFirstEvent(feedRef), repository, new MaterializedViewOptions(100)),
                        "counter", appliedAppends),
                Filter.all(), stampedReader(appendId, "1", "2"), converter, Counted::eventId, null);
        feedRef.set(feed);

        feed.catchUp().block(ofSeconds(5));
        assertThat(repo).isEmpty();

        feed.goLive().block(ofSeconds(5));
        feed.acceptAndWait(EventMetadata.from(stamped(appendId, "1")), new Counted("1")).block(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo.get("counter")).isEqualTo(1);
            assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
        });
    }

    // goLive() after a finished catch-up replays nothing. The live copy of an event that replay applied is still
    // suppressed, and its append is still recorded, because the copy reaches the source whose replay applied it.
    @Test
    void go_live_after_a_finished_catch_up_still_records_an_append_whose_live_copy_is_suppressed() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AppendId appendId = new AppendId(UUID.randomUUID());
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter",
                Projections.recordingAppliedAppends(
                        Projections.reactiveUpdateWithMetadata(projection(), repository, "counter"), "counter", appliedAppends),
                Filter.all(), stampedReader(appendId, "1", "2"), converter, Counted::eventId, null);

        feed.catchUp().block(ofSeconds(5));
        feed.goLive().block(ofSeconds(5));
        feed.acceptAndWait(EventMetadata.from(stamped(appendId, "2")), new Counted("2")).block(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo.get("counter")).isEqualTo(2);
            assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
        });

        // goLive() found the feed already live and left its pipeline running, so a later event is still applied. A
        // refusal would arrive after goLive() completed, hence the pause.
        Mono.delay(ofMillis(500)).block();
        feed.acceptAndWait(EventMetadata.from(stamped(appendId, "3")), new Counted("3")).block(ofSeconds(5));
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    // catchUp() after goLive() runs a replay on a live feed. Events that arrive live while it runs, a copy of an event
    // the replay already delivered and one it never reads, must reach the read model when the replay is stopped,
    // though a coalescing view throws away everything it buffered during that replay.
    @Test
    void events_that_arrive_live_while_a_replay_runs_on_a_live_feed_are_applied_after_that_replay_is_stopped() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        // A batch larger than the history, so the view writes nothing before the stop discards what it buffered.
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter",
                Projections.reactiveUpdateWithMetadata(receivingLiveEventsThenStoppingTheReplayAtTheSecondEvent(feedRef), repository, new MaterializedViewOptions(100)),
                Filter.all(), reader("1", "2", "3"), converter, Counted::eventId, null);
        feedRef.set(feed);

        feed.goLive().block(ofSeconds(5));
        feed.catchUp().block(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    @Test
    void a_live_event_not_in_the_replay_is_folded_after_the_catch_up() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.acceptAndWait(new Counted("3")).subscribe(); // buffered, not in history
        feed.catchUp().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", reader("1", "2"), converter, repo, marker).catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        // Restart: the replay is skipped, so the persisted count is not re-folded (which would double it).
        CatchupProjectionFeed<Counted> restarted = feed("counter", reader("1", "2"), converter, repo, marker);
        restarted.catchUp().block();
        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        restarted.acceptAndWait(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void overflowing_the_live_buffer_during_catch_up_fails_loud() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, reader(), converter, Counted::eventId, null, new CatchupThenLiveOptions(10, 2));

        feed.acceptAndWait(new Counted("l1")).subscribe();
        feed.acceptAndWait(new Counted("l2")).subscribe();
        StepVerifier.create(feed.acceptAndWait(new Counted("l3")))
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed"));
    }

    @Test
    void accept_or_defer_completes_with_deferred_before_the_feed_is_live_and_never_folds_the_event_afterwards() {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);

        RoutingOutcome plain = feed.acceptOrDefer(new Counted("early")).block(ofSeconds(5));
        RoutingOutcome withMetadata = feed.acceptOrDefer(metadata("early-stream", 7L), new Counted("early-with-metadata")).block(ofSeconds(5));

        assertThat(plain).isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(withMetadata).isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(folded).isEmpty();

        feed.catchUp().block(ofSeconds(5));

        assertThat(folded).as("only the replayed event, the deferred ones were not kept").containsExactly("1");
    }

    @Test
    void accept_or_defer_completes_with_delivered_once_the_feed_is_live_and_the_event_is_folded() {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feed.catchUp().block(ofSeconds(5));

        RoutingOutcome outcome = feed.acceptOrDefer(new Counted("2")).block(ofSeconds(5));

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(folded).containsExactly("1", "2");
    }

    @Test
    void accept_or_defer_with_metadata_hands_the_metadata_to_the_fold_and_completes_with_delivered_once_live() {
        List<String> streamIds = new CopyOnWriteArrayList<>();
        BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) -> Mono.fromRunnable(() -> streamIds.add(metadata.getStreamId()));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), metadataReader("1"), countedConverter(), Counted::eventId, null);
        feed.catchUp().block(ofSeconds(5));

        RoutingOutcome outcome = feed.acceptOrDefer(metadata("live-stream", 99L), new Counted("2")).block(ofSeconds(5));

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(streamIds).containsExactly("s", "live-stream");
    }

    @Test
    void accept_or_defer_completes_with_deferred_after_the_catch_up_was_stopped_before_the_feed_went_live() {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);

        feed.stopCatchUp();
        RoutingOutcome outcome = feed.acceptOrDefer(new Counted("after-stop")).block(ofSeconds(5));

        assertThat(outcome).isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(folded).isEmpty();
    }

    @Test
    void accept_or_defer_errors_after_a_catch_up_failure_instead_of_completing_with_an_outcome() {
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", failingReader(), countedConverter(), repo, null);
        StepVerifier.create(feed.catchUp()).expectError(IllegalStateException.class).verify(ofSeconds(5));

        StepVerifier.create(feed.acceptOrDefer(new Counted("x")))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed"))
                .verify(ofSeconds(5));
        StepVerifier.create(feed.acceptOrDefer(EventMetadata.empty(), new Counted("y")))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed"))
                .verify(ofSeconds(5));
        assertThat(repo).isEmpty();
    }

    @Test
    void accept_or_defer_errors_with_an_IllegalStateException_instead_of_an_outcome_once_the_live_queue_is_full() throws Exception {
        CountDownLatch folding = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        List<String> folded = new CopyOnWriteArrayList<>();
        Function<Counted, Mono<Void>> fold = event -> "blocked".equals(event.eventId())
                ? Mono.defer(() -> {
                    folding.countDown();
                    return Mono.fromFuture(release);
                })
                : Mono.fromRunnable(() -> folded.add(event.eventId()));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader(), countedConverter(), Counted::eventId, null, new CatchupThenLiveOptions(10, 1));
        feed.catchUp().block(ofSeconds(5));
        // A listener that acknowledges its message only when it is handed an outcome.
        List<RoutingOutcome> acknowledged = new CopyOnWriteArrayList<>();
        try {
            CompletableFuture<RoutingOutcome> blocked = feed.acceptOrDefer(new Counted("blocked")).doOnNext(acknowledged::add).toFuture();
            awaitLatch(folding);

            StepVerifier.create(feed.acceptOrDefer(metadata("overflow-stream", 2L), new Counted("overflow")).doOnNext(acknowledged::add))
                    .expectErrorSatisfies(error -> assertThat(error)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("buffer overflowed")
                            .hasMessageContaining("(cap 1)"))
                    .verify(ofSeconds(5));

            assertThat(acknowledged).as("the overflowing event is not acknowledged, so the broker delivers it again").isEmpty();
            release.complete(null);
            assertThat(blocked.get(5, TimeUnit.SECONDS)).isEqualTo(RoutingOutcome.DELIVERED);
            assertThat(acknowledged).containsExactly(RoutingOutcome.DELIVERED);
            assertThat(folded).as("the refused event was never folded").isEmpty();

            assertThat(feed.acceptOrDefer(new Counted("overflow")).block(ofSeconds(5))).as("the same event once the queue has emptied")
                    .isEqualTo(RoutingOutcome.DELIVERED);
            assertThat(folded).containsExactly("overflow");
        } finally {
            release.complete(null);
        }
    }

    // "3" is queued behind the fold of "2" when the second catchUp() starts replaying. The fold of "2" has started, so
    // the replay waits for it, and "3" has not, so it is answered deferred at once and never folded.
    @Test
    void accept_or_defer_queued_behind_a_live_fold_completes_deferred_when_a_catch_up_starts_and_the_event_is_never_folded() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch foldingTwo = new CountDownLatch(1);
        CountDownLatch releaseTwo = new CountDownLatch(1);
        Function<Counted, Mono<Void>> fold = event -> Mono.fromRunnable(() -> {
            if (event.eventId().equals("2")) {
                foldingTwo.countDown();
                try {
                    releaseTwo.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            folded.add(event.eventId());
        });
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feed.catchUp().block(ofSeconds(5));
        try {
            CompletableFuture<RoutingOutcome> two = feed.acceptOrDefer(new Counted("2")).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatch(foldingTwo);
            CompletableFuture<RoutingOutcome> three = CompletableFuture.supplyAsync(() -> feed.acceptOrDefer(new Counted("3")).toFuture()).join();
            assertThat(three).as("3 queued behind the fold of 2").isNotDone();

            CompletableFuture<Void> replaying = feed.catchUp().toFuture();

            assertThat(three).as("3 before the fold of 2 is released").succeedsWithin(ofSeconds(5)).isEqualTo(RoutingOutcome.DEFERRED);
            assertThat(two).as("2 while its fold is held").isNotDone();
            releaseTwo.countDown();
            assertThat(two).succeedsWithin(ofSeconds(5)).isEqualTo(RoutingOutcome.DELIVERED);
            assertThat(replaying).succeedsWithin(ofSeconds(5));
            assertThat(feed.acceptOrDefer(new Counted("4")).block(ofSeconds(5))).as("the barrier behind 3").isEqualTo(RoutingOutcome.DELIVERED);
            assertThat(folded).doesNotContain("3").endsWith("4");
        } finally {
            releaseTwo.countDown();
        }
    }

    @Test
    @SuppressWarnings("removal")
    void the_deprecated_accept_forwards_to_accept_and_wait_and_completes_once_the_event_is_folded() {
        List<String> streamIds = new CopyOnWriteArrayList<>();
        BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) -> Mono.fromRunnable(() -> streamIds.add(
                event.eventId() + "@" + (metadata.getStreamId())));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), metadataReader("1"), countedConverter(), Counted::eventId, null);
        feed.catchUp().block(ofSeconds(5));

        feed.accept(metadata("live-stream", 99L), new Counted("2")).block(ofSeconds(5));

        assertThat(streamIds).as("the metadata overload forwards its metadata").containsExactly("1@s", "2@live-stream");
    }

    @Test
    @SuppressWarnings("removal")
    void the_deprecated_accept_without_metadata_folds_the_event_live_like_accept_and_wait() {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feed.catchUp().block(ofSeconds(5));

        feed.accept(new Counted("2")).block(ofSeconds(5));

        assertThat(folded).containsExactly("1", "2");
    }

    @Test
    @SuppressWarnings("removal")
    void the_deprecated_accept_errors_like_accept_and_wait_when_the_feed_is_stopped_before_the_event_is_folded() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        CompletableFuture<Void> accepted = feed.accept(new Counted("live")).toFuture();

        feed.stopCatchUp();

        assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what the deprecated accept(..) errored with once the feed stopped")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("projection feed"));
        assertThat(folded).isEmpty();
    }

    @Test
    void a_catch_up_failure_fails_live_acks_instead_of_hanging() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        PositionOrderedReader failingReader = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupProjectionFeed<Counted> feed = feed("counter", failingReader, converter, repo, null);

        feed.catchUp().subscribe(v -> {
        }, e -> {
        }); // replay fails, so the pipeline terminates
        // A live event fed after the failed catch-up must error rather than hang, and it says the catch-up is what
        // failed rather than only surfacing the raw cause, so the caller can tell a terminal refusal from a handler
        // that happened to throw. The blocking twin has always worded it this way.
        StepVerifier.create(feed.acceptAndWait(new Counted("live")))
                .expectErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed for this projection feed")
                        .rootCause().hasMessageContaining("replay boom"))
                .verify(ofSeconds(5));
    }

    @Test
    void catches_up_and_folds_live_events_through_a_caller_supplied_fold() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        // The parity overload: drive an existing reactive fold plus a replay filter, instead of a projection and
        // repository. This is what the reactor DomainEventFeed uses to back a MaterializedView store.
        Function<Counted, Mono<Void>> fold = event -> Mono.fromRunnable(() -> repo.merge("counter", 1, Integer::sum));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", fold, Filter.all(), reader("1", "2"), converter, Counted::eventId, null);

        feed.catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        feed.acceptAndWait(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    // The fold returns the call as part of its own Mono, so the call completes once "n" is queued, and "n" is folded
    // after that fold
    @Test
    void accept_and_wait_from_the_feeds_own_live_fold_completes_once_queued_and_the_event_is_folded_after_that_fold() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn("2", feedRef, folded, self -> self.acceptAndWait(new Counted("n"))),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).containsExactly("1", "2", "n"));
    }

    // The 0.33.0 shape of a fold feeding its own feed, subscribed and not waited for
    @Test
    void accept_and_wait_the_feeds_own_live_fold_subscribes_and_leaves_has_its_event_folded_after_that_fold() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn("2", feedRef, folded, self -> Mono.fromRunnable(() -> self.acceptAndWait(new Counted("n")).subscribe())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).containsExactly("1", "2", "n"));
    }

    // "n" folds on another thread after a delay, so "2" is only answered after it when the answer waits for "n"
    @Test
    void accept_and_wait_the_feeds_own_live_fold_composes_in_answers_the_event_once_the_fed_event_is_folded() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> foldedWhenAnswered = new AtomicReference<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> switch (event.eventId()) {
            case "2" -> Mono.defer(() -> feedRef.get().acceptAndWait(new Counted("n"))).then(Mono.fromRunnable(() -> folded.add("2")));
            case "n" -> Mono.delay(ofMillis(100)).then(Mono.fromRunnable(() -> folded.add("n")));
            default -> Mono.fromRunnable(() -> folded.add(event.eventId()));
        };
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2")).doOnSuccess(ignored -> foldedWhenAnswered.set(List.copyOf(folded))))
                .expectComplete().verify(ofSeconds(5));

        assertThat(foldedWhenAnswered.get()).containsExactly("1", "2", "n");
    }

    @Test
    void accept_and_wait_the_feeds_own_live_fold_subscribes_answers_the_event_once_the_fed_event_is_folded() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> foldedWhenAnswered = new AtomicReference<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> switch (event.eventId()) {
            case "2" -> Mono.fromRunnable(() -> feedRef.get().acceptAndWait(new Counted("n")).subscribe()).then(Mono.fromRunnable(() -> folded.add("2")));
            case "n" -> Mono.delay(ofMillis(100)).then(Mono.fromRunnable(() -> folded.add("n")));
            default -> Mono.fromRunnable(() -> folded.add(event.eventId()));
        };
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2")).doOnSuccess(ignored -> foldedWhenAnswered.set(List.copyOf(folded))))
                .expectComplete().verify(ofSeconds(5));

        assertThat(foldedWhenAnswered.get()).containsExactly("1", "2", "n");
    }

    // "2" is not recorded as delivered while "n" failed, so its redelivery folds it again, and the fixed "n" with it.
    @Test
    void a_fed_event_whose_fold_fails_fails_the_event_that_fed_it_and_that_event_is_folded_again_when_redelivered() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicBoolean nFails = new AtomicBoolean(true);
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> switch (event.eventId()) {
            case "2" -> Mono.defer(() -> feedRef.get().acceptAndWait(new Counted("n"))).then(Mono.fromRunnable(() -> folded.add("2")));
            case "n" -> Mono.delay(ofMillis(20)).then(Mono.defer(() -> nFails.get()
                    ? Mono.<Void>error(new IllegalStateException("fold failed for n"))
                    : Mono.fromRunnable(() -> folded.add("n"))));
            default -> Mono.fromRunnable(() -> folded.add(event.eventId()));
        };
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2")))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("fold failed for n"))
                .verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "2");

        nFails.set(false);
        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "2", "2", "n");

        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "2", "2", "n");
    }

    // The cap leaves no room for "n2" while "2" and "n1" are still counted. The fold swallows that refusal, so it is the
    // feed that fails the answer to "2", after "n1" is folded.
    @Test
    void a_fed_event_refused_for_a_full_buffer_fails_the_event_that_fed_it() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> foldedWhenAnswered = new AtomicReference<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> event.eventId().equals("2")
                ? Flux.just("n1", "n2").concatMap(next -> feedRef.get().acceptAndWait(new Counted(next)).onErrorComplete())
                .then(Mono.fromRunnable(() -> folded.add("2")))
                : Mono.fromRunnable(() -> folded.add(event.eventId()));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null, new CatchupThenLiveOptions(10, 2));
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2")).doOnError(ignored -> foldedWhenAnswered.set(List.copyOf(folded))))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.bufferOverflow(2)))
                .verify(ofSeconds(5));

        assertThat(foldedWhenAnswered.get()).containsExactly("1", "2", "n1");
        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
    }

    // Each event in the chain holds back the answer of the one before it, so a chain the cap does not stop would leave
    // the answer to "2" waiting for good. The chain ends at "n100" so that a feed without the cap fails the assertion
    // instead of running out of memory.
    @Test
    void a_fold_that_feeds_new_events_without_end_fails_the_event_the_chain_started_from() {
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> {
            String id = event.eventId();
            if (id.equals("2")) {
                return feedRef.get().acceptAndWait(new Counted("n1"));
            }
            int next = id.startsWith("n") ? Integer.parseInt(id.substring(1)) + 1 : 0;
            return next == 0 || next > 100 ? Mono.<Void>empty() : feedRef.get().acceptAndWait(new Counted("n" + next));
        };
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold,
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null, new CatchupThenLiveOptions(10, 3));
        feedRef.set(feed);
        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2")))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("projection feed", 3)))
                .verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
    }

    @Test
    void accept_and_wait_from_the_feeds_own_replayed_fold_completes_once_queued_and_the_event_is_folded_after_the_replay() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn("1", feedRef, folded, self -> self.acceptAndWait(new Counted("n"))),
                Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);

        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).containsExactly("1", "2", "n"));
    }

    @Test
    void accept_or_defer_from_the_feeds_own_live_fold_errors_and_the_event_is_never_folded() {
        assertThatAcceptOrDeferFromTheFoldIsRefused(true, self -> self.acceptOrDefer(new Counted("n")));
    }

    @Test
    void accept_or_defer_with_metadata_from_the_feeds_own_live_fold_errors_and_the_event_is_never_folded() {
        assertThatAcceptOrDeferFromTheFoldIsRefused(true, self -> self.acceptOrDefer(EventMetadata.empty(), new Counted("n")));
    }

    @Test
    void accept_or_defer_from_the_feeds_own_replayed_fold_errors_and_the_event_is_never_folded() {
        assertThatAcceptOrDeferFromTheFoldIsRefused(false, self -> self.acceptOrDefer(new Counted("n")));
    }

    @Test
    @SuppressWarnings("removal")
    void the_deprecated_accept_waits_for_the_feed_to_go_live_and_folds_the_event_instead_of_deferring_it() throws Exception {
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                (Counted e) -> Mono.fromRunnable(() -> folded.add(e.eventId())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        CompletableFuture<Void> plain = feed.accept(new Counted("early")).toFuture();
        CompletableFuture<Void> withMetadata = feed.accept(EventMetadata.empty(), new Counted("early-with-metadata")).toFuture();

        assertThat(plain).as("accept(..) before the feed is live").isNotDone();
        assertThat(withMetadata).as("accept(metadata, ..) before the feed is live").isNotDone();
        feed.catchUp().block(ofSeconds(5));

        plain.get(5, TimeUnit.SECONDS);
        withMetadata.get(5, TimeUnit.SECONDS);
        assertThat(folded).containsExactly("1", "early", "early-with-metadata");
    }

    // The fold of "2" returns catchUp() as part of its own Mono, and that catch-up replays "1", which cannot start
    // before the fold returns
    @Test
    void catch_up_from_the_feeds_own_live_fold_completes_without_waiting_and_replays_after_that_fold() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn("2", feedRef, folded, CatchupProjectionFeed::catchUp),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.goLive()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).containsExactly("2", "1"));
        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("2", "1", "3");
    }

    // The fold of "2" asks for two catch-ups, both replaying "1", before either can start
    @Test
    void two_catch_ups_from_the_feeds_own_live_fold_replay_once() {
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                feedingBackOn("2", feedRef, folded, self -> self.catchUp().then(self.catchUp())),
                Filter.all(), reader("1"), countedConverter(), Counted::eventId, null);
        feedRef.set(feed);
        StepVerifier.create(feed.goLive()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(feed.acceptAndWait(new Counted("2"))).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).contains("1"));
        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("2", "1", "3");
    }

    // The fold of "1" stops the running replay and then asks for a catch-up, before that replay reads "2"
    @Test
    void a_catch_up_the_view_asks_for_after_a_stop_does_not_undo_that_stop_for_the_running_replay() {
        List<String> folded = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        AtomicBoolean asked = new AtomicBoolean();
        Function<Counted, Mono<Void>> fold = event -> (event.eventId().equals("1") && asked.compareAndSet(false, true)
                ? Mono.defer(() -> {
                    feedRef.get().stopCatchUp();
                    return feedRef.get().catchUp();
                })
                : Mono.<Void>empty())
                .then(Mono.fromRunnable(() -> folded.add(event.eventId())));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold, Filter.all(),
                reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(feed);

        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> StepVerifier.create(marker.read("counter")).expectNextCount(1).verifyComplete());
        assertThat(folded).containsExactly("1", "1", "2");
        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "1", "2", "3");
    }

    // The stop comes when the replay reads its history, after catchUp() and before the catch-up ends, and there is no
    // event to notice it at
    @Test
    void a_stop_during_a_catch_up_with_an_empty_history_does_not_keep_the_feed_from_going_live() {
        List<String> folded = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        PositionOrderedReader stoppingOnRead = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(() -> {
                    feedRef.get().stopCatchUp();
                    return Flux.<CloudEvent>empty();
                });
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                event -> Mono.fromRunnable(() -> folded.add(event.eventId())), Filter.all(), stoppingOnRead,
                countedConverter(), Counted::eventId, marker);
        feedRef.set(feed);

        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        StepVerifier.create(marker.read("counter")).expectNextCount(1).verifyComplete();
        StepVerifier.create(feed.acceptAndWait(new Counted("1"))).expectComplete().verify(ofSeconds(5));
        assertThat(folded).containsExactly("1");
    }

    // The batch is larger than the history, so the first write is the flush in replayCompleted(), and the stop comes
    // after the replay has read its last event
    @Test
    void a_stop_while_a_coalescing_view_writes_its_replay_batch_does_not_keep_the_feed_from_going_live() {
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, (id, state) -> {
            feedRef.get().stopCatchUp();
            repo.put(id, state);
        });
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter",
                Projections.reactiveUpdateWithMetadata(projection(), repository, new MaterializedViewOptions(100)),
                Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(feed);

        StepVerifier.create(feed.catchUp()).expectComplete().verify(ofSeconds(5));

        assertThat(repo.get("counter")).isEqualTo(2);
        StepVerifier.create(marker.read("counter")).expectNextCount(1).verifyComplete();
        StepVerifier.create(feed.acceptAndWait(new Counted("3"))).expectComplete().verify(ofSeconds(5));
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    // "bad" is in no store. The fold of the replayed "2" feeds it, and it is folded before the marker is written. Its
    // fold fails once, which fails the catch-up, so no marker says the history was read and a new feed replays it.
    @Test
    void after_an_event_a_fold_fed_fails_a_new_feed_replays_the_history_and_the_fold_feeds_it_again() {
        CheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicBoolean badFails = new AtomicBoolean(true);
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        Function<Counted, Mono<Void>> fold = event -> switch (event.eventId()) {
            case "2" -> feedRef.get().acceptAndWait(new Counted("bad")).then(Mono.fromRunnable(() -> folded.add("2")));
            case "bad" -> badFails.getAndSet(false)
                    ? Mono.error(new IllegalStateException("fold failed for bad"))
                    : Mono.fromRunnable(() -> folded.add("bad"));
            default -> Mono.fromRunnable(() -> folded.add(event.eventId()));
        };
        CatchupProjectionFeed<Counted> failed = CatchupProjectionFeed.create(
                "counter", fold, Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(failed);
        StepVerifier.create(failed.catchUp())
                .expectErrorSatisfies(error -> assertThat(error).hasMessage("fold failed for bad"))
                .verify(ofSeconds(5));
        StepVerifier.create(failed.acceptAndWait(new Counted("3")))
                .expectErrorSatisfies(error -> assertThat(error).hasRootCauseMessage("fold failed for bad"))
                .verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "2");
        assertThat(marker.read("counter").blockOptional()).isEmpty();

        folded.clear();
        CatchupProjectionFeed<Counted> rebuilt = CatchupProjectionFeed.create(
                "counter", fold, Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(rebuilt);
        StepVerifier.create(rebuilt.catchUp()).expectComplete().verify(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(folded).containsExactly("1", "2", "bad"));
    }

    // The view feeds "bad" from replayCompleted(), which runs before the marker, so the marker waits for its fold. That
    // fold fails once, which fails the catch-up, so no marker says the history was read and a new feed replays it.
    @Test
    void after_an_event_a_view_fed_from_replay_completed_fails_a_new_feed_replays_the_history_and_the_view_feeds_it_again() {
        CheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicBoolean badFails = new AtomicBoolean(true);
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        final class FeedsFromReplayCompleted implements java.util.function.BiFunction<EventMetadata, Counted, Mono<Void>>, ReactiveReplayAware {
            @Override
            public Mono<Void> apply(EventMetadata metadata, Counted event) {
                if (event.eventId().equals("bad") && badFails.getAndSet(false)) {
                    return Mono.error(new IllegalStateException("fold failed for bad"));
                }
                return Mono.fromRunnable(() -> folded.add(event.eventId()));
            }

            @Override
            public void replayStarted() {
            }

            @Override
            public Mono<Void> replayCompleted() {
                return Mono.defer(() -> feedRef.get().acceptAndWait(new Counted("bad")));
            }

            @Override
            public void replayAbandoned() {
            }
        }
        CatchupProjectionFeed<Counted> failed = CatchupProjectionFeed.create(
                "counter", new FeedsFromReplayCompleted(), Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(failed);
        StepVerifier.create(failed.catchUp())
                .expectErrorSatisfies(error -> assertThat(error).hasMessage("fold failed for bad"))
                .verify(ofSeconds(5));
        assertThat(folded).containsExactly("1", "2");
        assertThat(marker.read("counter").blockOptional()).isEmpty();

        folded.clear();
        CatchupProjectionFeed<Counted> rebuilt = CatchupProjectionFeed.create(
                "counter", new FeedsFromReplayCompleted(), Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, marker);
        feedRef.set(rebuilt);
        StepVerifier.create(rebuilt.catchUp()).expectComplete().verify(ofSeconds(5));

        assertThat(folded).containsExactly("1", "2", "bad");
        assertThat(marker.read("counter").blockOptional()).isPresent();
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        assertThatThrownBy(() ->
                CatchupProjectionFeed.create("counter", projection(), repository, positionlessReader(), converter, Counted::eventId, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writesPosition");
    }

    @Test
    void go_live_delivers_buffered_events_without_ever_reading_history() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        // Errors if replay() is ever subscribed to, so a passing test proves goLive() never reads history.
        PositionOrderedReader failingReader = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupProjectionFeed<Counted> feed = feed("counter", failingReader, converter, repo, null);

        feed.acceptAndWait(new Counted("1")).subscribe();
        feed.goLive().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(1));

        feed.acceptAndWait(new Counted("2")).block();
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void go_live_writes_no_completion_marker_so_a_later_catch_up_still_replays_history() {
        CloudEventConverter<Counted> converter = countedConverter();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", reader("1", "2"), converter, new ConcurrentHashMap<>(), marker).goLive().block();

        assertThat(marker.read("counter").blockOptional()).isEmpty();

        Map<String, Integer> repo = new ConcurrentHashMap<>();
        feed("counter", reader("1", "2"), converter, repo, marker).catchUp().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    @Test
    void go_live_completes_normally_the_first_time() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader(), converter, repo, null);

        StepVerifier.create(feed.goLive()).verifyComplete();

        feed.acceptAndWait(new Counted("1")).block();
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void a_null_event_id_fails_fast_instead_of_silently_dropping() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, reader("1"), converter, event -> null, null);

        StepVerifier.create(feed.catchUp())
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(NullPointerException.class).hasMessageContaining("eventId function returned null"));
    }

    // --- helpers ---

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CatchupProjectionFeed<Counted> feed(String id, PositionOrderedReader reader, CloudEventConverter<Counted> converter,
                                                             Map<String, Integer> repo, CheckpointStorage marker) {
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        return CatchupProjectionFeed.create(id, projection(), repository, reader, converter, Counted::eventId, marker);
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // Stops the replay from inside the first event's id lookup, so that event is buffered and then discarded.
    private static Projection<Integer, Counted, String> stoppingTheReplayAtTheFirstEvent(AtomicReference<CatchupProjectionFeed<Counted>> feed) {
        AtomicBoolean stopped = new AtomicBoolean();
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> {
                    if (stopped.compareAndSet(false, true)) {
                        feed.get().stopCatchUp();
                    }
                    return "counter";
                })
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // While the replay reads its second event, the feed receives two live events, a copy of the first event and one
    // the replay never reads, and the replay is then stopped. The live pipeline delivers on its own thread, so the
    // pause gives it the time it would need to reach the view before the stop.
    private static Projection<Integer, Counted, String> receivingLiveEventsThenStoppingTheReplayAtTheSecondEvent(AtomicReference<CatchupProjectionFeed<Counted>> feed) {
        AtomicInteger lookups = new AtomicInteger();
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> {
                    if (lookups.incrementAndGet() == 2) {
                        feed.get().acceptAndWait(new Counted("1")).subscribe();
                        feed.get().acceptAndWait(new Counted("live")).subscribe();
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        feed.get().stopCatchUp();
                    }
                    return "counter";
                })
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static Projection<Integer, Counted, String> skippingEveryEvent() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> null)
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // A reader whose history is the given event ids, in position order.
    private PositionOrderedReader reader(String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(List.of(eventIds)).map(CatchupProjectionFeedTest::cloudEvent);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    // A reader whose history carries real stream metadata (stream id "s", a stream version, and a global position),
    // unlike reader(...) above which builds bare CloudEvents with no extensions. Needed to prove the catch-up threads
    // the decoded EventMetadata into the fold rather than EventMetadata.empty().
    private static PositionOrderedReader metadataReader(String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                List<CloudEvent> events = new ArrayList<>();
                for (int i = 0; i < eventIds.length; i++) {
                    long version = i + 1;
                    CloudEvent event = CloudEventBuilder.v1()
                            .withId(eventIds[i])
                            .withSource(SOURCE)
                            .withType("Counted")
                            .withExtension(OccurrentCloudEventExtension.occurrent("s", version))
                            .build();
                    events.add(OccurrentCloudEventExtension.withPosition(event, version));
                }
                return Flux.fromIterable(events);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static EventMetadata metadata(String streamId, long position) {
        Map<String, Object> data = new HashMap<>();
        data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        data.put(OccurrentCloudEventExtension.POSITION, position);
        return new EventMetadata(data);
    }

    // A reader whose writesPosition() returns false, so the fail-fast guard rejects it before any replay.
    private PositionOrderedReader positionlessReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    // A reader whose history carries the append id a store stamps on the write it created.
    private PositionOrderedReader stampedReader(AppendId appendId, String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(List.of(eventIds)).map(eventId -> stamped(appendId, eventId));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent stamped(AppendId appendId, String eventId) {
        return OccurrentCloudEventExtension.withAppendId(cloudEvent(eventId), appendId.value().toString());
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(SOURCE).withType("Counted").build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return cloudEvent(domainEvent.eventId());
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                return new Counted(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return "Counted";
            }
        };
    }

    record Counted(String eventId) {
    }

}
