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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import io.cloudevents.CloudEvent;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.Counted;
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.InMemoryLog;
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.RecordingReplayAwareView;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.dsl.projection.reactor.ResumePositionFixtures.converter;

/**
 * What the marker of a {@link CatchupProjectionFeed} means (ADR 179): the head read before a replay, which the next
 * catch-up resumes after. Every start is a fresh feed over the same log and marker storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupProjectionFeedResumePositionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final CloudEventConverter<Counted> converter = converter();
    private final InMemoryLog log = new InMemoryLog();
    private final InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

    @Test
    void a_catch_up_without_a_marker_replays_the_whole_history_and_stores_the_head() {
        log.append("1", "2");

        RecordingReplayAwareView view = catchUp();

        assertThat(view.updates()).containsExactly("1", "2");
        assertThat(storedPosition()).contains(2L);
    }

    @Test
    void a_catch_up_with_a_stored_marker_replays_only_the_events_after_it_and_stores_the_new_head() {
        log.append("1", "2");
        catchUp();
        log.append("3", "4");

        RecordingReplayAwareView view = catchUp();

        assertThat(view.updates()).as("replayed after the stored marker, nothing before it and nothing lost").containsExactly("3", "4");
        assertThat(storedPosition()).contains(4L);
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() {
        log.append("1", "2");
        long olderHead = log.head();
        catchUp();
        log.append("3");
        catchUp();
        assertThat(storedPosition().orElseThrow()).as("newer head stored by the second start").isGreaterThan(olderHead);
        marker.save("counter", GlobalCheckpoint.of(olderHead)).block(TIMEOUT);
        log.append("4");

        assertThat(catchUp().updates()).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        log.append("1", "2");
        long headBeforeTheReplay = log.head();

        // An event is appended while the replay reads, so the head has moved by the time the marker is written.
        feed(new RecordingReplayAwareView(event -> {
            if (event.eventId().equals("1")) {
                log.append("during");
            }
        })).catchUp().block(TIMEOUT);

        assertThat(log.head()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(storedPosition()).contains(headBeforeTheReplay);
        assertThat(catchUp().updates()).as("the event appended during the replay is replayed by the next catch-up").containsExactly("during");
    }

    @Test
    void a_catch_up_that_fails_does_not_store_its_head() {
        log.append("1", "2");
        catchUp();
        log.append("3", "4");

        Throwable failure = catchThrowable(() -> feed(new RecordingReplayAwareView(event -> {
            if (event.eventId().equals("4")) {
                throw new IllegalStateException("fold boom");
            }
        })).catchUp().block(TIMEOUT));

        assertThat(failure).hasStackTraceContaining("fold boom");
        assertThat(storedPosition()).as("marker after the failed catch-up, which deleted it").isEmpty();
        assertThat(catchUp().updates()).as("replayed again from the beginning, the events of the failed catch-up among them").containsExactly("1", "2", "3", "4");
    }

    @Test
    void a_restart_folds_an_event_that_is_both_replayed_and_live_once() {
        log.append("1", "2");
        catchUp();
        log.append("3", "4");

        RecordingReplayAwareView view = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> feed = feed(view);
        // Fed ahead of the catch-up, so it is buffered, and the replay reads the same event.
        CompletableFuture<Void> liveCopy = feed.acceptAndWait(new Counted("4")).toFuture();
        feed.catchUp().block(TIMEOUT);

        assertThat(liveCopy).succeedsWithin(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(view.updates()).containsExactly("3", "4");
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        log.append("1", "2", "3");
        marker.save("counter", GlobalCheckpoint.of(2)).block(TIMEOUT);

        assertThat(catchUp().updates()).containsExactly("3");
    }

    @Test
    void a_catch_up_with_a_stored_marker_and_nothing_after_it_replays_nothing_and_goes_live() {
        log.append("1", "2");
        catchUp();

        RecordingReplayAwareView view = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> feed = feed(view);
        feed.catchUp().block(TIMEOUT);
        feed.acceptAndWait(new Counted("3")).block(TIMEOUT);

        assertThat(view.updates()).containsExactly("3");
    }

    @Test
    void replay_callbacks_reach_the_view_for_a_replay_from_the_beginning_and_not_for_a_replay_after_a_stored_marker() {
        log.append("1", "2");
        RecordingReplayAwareView fromTheBeginning = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> first = feed(fromTheBeginning);
        CompletableFuture<Void> liveCopyOfReplayed = first.acceptAndWait(new Counted("2")).toFuture();
        first.catchUp().block(TIMEOUT);
        assertThat(liveCopyOfReplayed).succeedsWithin(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        log.append("3");
        RecordingReplayAwareView afterTheMarker = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> restarted = feed(afterTheMarker);
        CompletableFuture<Void> liveCopyOfReplayedAfterMarker = restarted.acceptAndWait(new Counted("3")).toFuture();
        restarted.catchUp().block(TIMEOUT);
        assertThat(liveCopyOfReplayedAfterMarker).succeedsWithin(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(fromTheBeginning.calls).containsExactly("replayStarted", "update:1", "update:2", "replayCompleted", "alreadyDeliveredByReplay");
        assertThat(afterTheMarker.calls).as("plain updates, no replay callbacks").containsExactly("update:3");
    }

    @Test
    void a_stopped_replay_tells_the_view_it_was_abandoned_when_it_replays_from_the_beginning_and_not_when_it_replays_after_a_stored_marker() {
        log.append("1", "2");
        RecordingReplayAwareView abandonedFromTheBeginning = stoppedAtTheFirstReplayedEvent();
        assertThat(abandonedFromTheBeginning.calls).containsExactly("replayStarted", "update:1", "replayAbandoned");

        catchUp();
        log.append("3", "4");
        RecordingReplayAwareView abandonedAfterTheMarker = stoppedAtTheFirstReplayedEvent();
        assertThat(abandonedAfterTheMarker.calls).as("replayed after the stored marker, so no replay callbacks").containsExactly("update:3");
    }

    @Test
    void going_live_without_a_catch_up_writes_no_marker() {
        log.append("1", "2");

        feed(new RecordingReplayAwareView()).goLive().block(TIMEOUT);

        assertThat(storedPosition()).as("marker after goLive()").isEmpty();
    }

    @Test
    void an_event_appended_while_the_marker_is_read_is_delivered_and_covered_by_the_stored_head() {
        log.append("1", "2");
        catchUp();
        InMemoryCheckpointStorage appendingOnRead = new InMemoryCheckpointStorage() {
            private boolean appended;

            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.defer(() -> {
                    if (!appended) {
                        appended = true;
                        log.append("3");
                    }
                    return super.read(subscriptionId);
                });
            }
        };
        appendingOnRead.save("counter", GlobalCheckpoint.of(2)).block(TIMEOUT);
        RecordingReplayAwareView view = new RecordingReplayAwareView();

        feed(view, appendingOnRead).catchUp().block(TIMEOUT);

        assertThat(view.updates()).as("the event appended while the marker was read").containsExactly("3");
        assertThat(GlobalCheckpoint.positionOf(appendingOnRead.read("counter").block(TIMEOUT)))
                .as("stored marker, at least the position of the event appended while the marker was read").isGreaterThanOrEqualTo(3L);
    }

    @Test
    void a_stored_marker_that_is_not_a_catch_up_marker_fails_the_catch_up_naming_the_id_and_suggesting_a_new_one() {
        log.append("1");
        marker.save("counter", new StringBasedCheckpoint("resume-token-xyz")).block(TIMEOUT);

        Throwable failure = catchThrowable(() -> feed(new RecordingReplayAwareView()).catchUp().block(TIMEOUT));

        assertThat(failure).hasMessageContaining("\"counter\"").hasMessageContaining("subscribe under a new id");
    }

    @Test
    void a_stored_checkpoint_that_is_not_a_catch_up_marker_is_still_stored_unchanged_after_the_catch_up_refused_it() {
        log.append("1");
        StringBasedCheckpoint foreign = new StringBasedCheckpoint("resume-token-xyz");
        marker.save("counter", foreign).block(TIMEOUT);

        // The failure is reported after the forget attempts have run, so nothing is left in flight to wait for.
        assertThat(catchThrowable(() -> feed(new RecordingReplayAwareView()).catchUp().block(TIMEOUT))).as("the refused catch-up").isNotNull();

        assertThat(marker.read("counter").block(TIMEOUT)).as("checkpoint of another subscription under the same id").isEqualTo(foreign);
    }

    @Test
    void a_failed_catch_up_forgets_the_marker_its_own_read_found_when_another_catch_up_on_the_feed_read_a_foreign_checkpoint_while_it_forgot() throws Exception {
        log.append("1", "2");
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Void> releaseThirdRead = new CompletableFuture<>();
        CountDownLatch firstDeleteApplied = new CountDownLatch(1);
        CompletableFuture<Void> releaseFirstDelete = new CompletableFuture<>();
        AtomicBoolean firstDelete = new AtomicBoolean(true);
        // Holds the third marker read until the test says so, and the answer of the first delete after it is applied, so
        // the order of the reads and of the forgets is the test's and not the scheduler's.
        InMemoryCheckpointStorage ordered = new InMemoryCheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return reads.incrementAndGet() == 3 ? Mono.fromFuture(releaseThirdRead).then(Mono.defer(() -> super.read(subscriptionId))) : super.read(subscriptionId);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.defer(() -> super.delete(subscriptionId).then(Mono.defer(() -> {
                    if (!firstDelete.compareAndSet(true, false)) {
                        return Mono.empty();
                    }
                    firstDeleteApplied.countDown();
                    return Mono.fromFuture(releaseFirstDelete);
                })));
            }
        };
        CountDownLatch foldingThree = new CountDownLatch(1);
        CompletableFuture<Void> failTheFold = new CompletableFuture<>();
        BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) -> event.eventId().equals("3")
                ? Mono.fromRunnable(foldingThree::countDown).then(Mono.fromFuture(failTheFold)).then(Mono.error(new IllegalStateException("fold boom")))
                : Mono.empty();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", fold, Filter.all(), log, converter, Counted::eventId, ordered);
        // Reads no marker, stores one and goes live, so it is the catch-up that delivers live when the next one fails.
        feed.catchUp().block(TIMEOUT);
        log.append("3");

        // Reads that marker and holds in the fold of 3.
        CompletableFuture<Void> failing = feed.catchUp().toFuture();
        assertThat(foldingThree.await(5, TimeUnit.SECONDS)).as("the second catch-up is folding 3").isTrue();
        // Asked before the first one fails, so it is let in, but its read has not answered yet.
        CompletableFuture<Void> reading = feed.catchUp().toFuture();

        failTheFold.complete(null);
        assertThat(firstDeleteApplied.await(5, TimeUnit.SECONDS)).as("the failing catch-up has deleted the marker it read").isTrue();
        // Something else is stored under the id now, which the third catch-up reads and refuses.
        ordered.save("counter", new StringBasedCheckpoint("resume-token-xyz")).block(TIMEOUT);
        releaseThirdRead.complete(null);
        assertThat(catchThrowable(() -> reading.get(5, TimeUnit.SECONDS))).as("the catch-up that read the foreign checkpoint")
                .hasStackTraceContaining("subscribe under a new id");
        // A catch-up marker is stored by the time the catch-up that delivers live forgets.
        ordered.save("counter", GlobalCheckpoint.of(2)).block(TIMEOUT);
        releaseFirstDelete.complete(null);

        assertThat(catchThrowable(() -> failing.get(5, TimeUnit.SECONDS))).as("the catch-up that failed in its fold").hasStackTraceContaining("fold boom");
        assertThat(ordered.read("counter").blockOptional(TIMEOUT))
                .as("checkpoint under the id once the catch-up that delivers live forgot, which read a marker itself")
                .isEmpty();
    }

    @Test
    void a_failed_catch_up_leaves_a_foreign_checkpoint_stored_when_the_catch_up_that_delivers_live_read_a_marker_before_it_was_stored() {
        log.append("1", "2");
        marker.save("counter", GlobalCheckpoint.of(1)).block(TIMEOUT);
        CatchupProjectionFeed<Counted> feed = feed(new RecordingReplayAwareView());
        // Reads the marker, so what it finds is a catch-up marker, and goes live, so it is the catch-up that delivers live.
        feed.catchUp().block(TIMEOUT);
        StringBasedCheckpoint foreign = new StringBasedCheckpoint("resume-token-xyz");
        // Something else is stored under the id after that.
        marker.save("counter", foreign).block(TIMEOUT);

        // Reads the foreign checkpoint and refuses it. The handover then forgets for it and for the catch-up that delivers live.
        assertThat(catchThrowable(() -> feed.catchUp().block(TIMEOUT))).as("the catch-up that read the foreign checkpoint")
                .hasStackTraceContaining("subscribe under a new id");

        assertThat(marker.read("counter").blockOptional(TIMEOUT))
                .as("checkpoint under the id once the catch-up that delivers live forgot, which read a marker itself")
                .contains(foreign);
    }

    @Test
    void an_event_a_live_fold_fed_back_that_the_replay_does_not_read_is_delivered_by_the_next_start_when_its_fold_fails_and_the_marker_cannot_be_deleted() throws Exception {
        log.append("1");
        InMemoryCheckpointStorage undeletable = new InMemoryCheckpointStorage() {
            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.defer(() -> {
                    return Mono.error(new IllegalStateException("marker delete failed"));
                });
            }
        };
        AtomicBoolean secondCatchUp = new AtomicBoolean();
        CountDownLatch aReadHead = new CountDownLatch(1);
        // Hides W and X from the replay of the second catch-up only, as if both committed after that catch-up read the head.
        PositionOrderedReader lateCommit = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(org.occurrent.filter.Filter filter, PositionRange range) {
                Flux<CloudEvent> read = log.readInPositionOrder(filter, range);
                return secondCatchUp.get() ? read.filter(e -> !e.getId().equals("X") && !e.getId().equals("W")) : read;
            }

            @Override
            public Mono<Long> currentPosition() {
                Mono<Long> head = log.currentPosition();
                return secondCatchUp.get() ? head.doAfterTerminate(aReadHead::countDown) : head;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CountDownLatch foldingW = new CountDownLatch(1);
        CountDownLatch xFailed = new CountDownLatch(1);
        AtomicBoolean wFed = new AtomicBoolean();
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feed = new AtomicReference<>();
        java.util.function.BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) -> Mono.defer(() -> {
            folded.add(event.eventId());
            if (event.eventId().equals("X")) {
                xFailed.countDown();
                return Mono.error(new IllegalStateException("fold failed for X"));
            }
            if (event.eventId().equals("W") && wFed.compareAndSet(false, true)) {
                log.append("X");
                return feed.get().acceptAndWait(new Counted("X")).then(Mono.fromRunnable(() -> {
                    foldingW.countDown();
                    try {
                        aReadHead.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            return Mono.empty();
        });
        feed.set(CatchupProjectionFeed.create("counter", fold, Filter.all(), lateCommit, converter, Counted::eventId, undeletable));
        feed.get().catchUp().block(TIMEOUT);
        log.append("W");
        feed.get().acceptOrDefer(new Counted("W")).subscribeOn(Schedulers.boundedElastic()).subscribe(outcome -> { }, error -> { });
        assertThat(foldingW.await(5, TimeUnit.SECONDS)).as("W is folding").isTrue();
        secondCatchUp.set(true);
        CompletableFuture<Void> a = feed.get().catchUp().toFuture();

        assertThat(xFailed.await(5, TimeUnit.SECONDS)).as("X was folded and failed").isTrue();
        assertThat(catchThrowable(() -> a.get(5, TimeUnit.SECONDS))).as("what the second catch-up ended with").isNull();
        secondCatchUp.set(false);

        RecordingReplayAwareView next = new RecordingReplayAwareView();
        CatchupProjectionFeed.create("counter", next, Filter.all(), log, converter, Counted::eventId, undeletable).catchUp().block(TIMEOUT);

        assertThat(next.updates()).as("delivered by the next start of the same id").contains("X");
    }

    @Test
    void a_catch_up_the_view_asks_for_while_folding_a_replayed_event_does_not_deliver_an_event_at_or_below_the_head_the_asking_catch_up_read_when_no_marker_is_stored() {
        log.append("1", "2", "3");

        assertThat(foldsWhenFoldingAskedForACatchUpOn("2")).containsExactly("1", "2", "3");
    }

    @Test
    void a_catch_up_the_view_asks_for_while_folding_a_replayed_event_does_not_deliver_an_event_at_or_below_the_head_the_asking_catch_up_read_when_a_marker_is_stored() {
        log.append("1", "2", "3");
        marker.save("counter", GlobalCheckpoint.of(1)).block(TIMEOUT);

        assertThat(foldsWhenFoldingAskedForACatchUpOn("2")).containsExactly("2", "3");
    }

    private List<String> foldsWhenFoldingAskedForACatchUpOn(String eventId) {
        List<String> folds = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feed = new AtomicReference<>();
        java.util.function.BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) -> Mono.defer(() -> {
            folds.add(event.eventId());
            return event.eventId().equals(eventId) ? feed.get().catchUp() : Mono.empty();
        });
        feed.set(CatchupProjectionFeed.create("counter", fold, Filter.all(), log, converter, Counted::eventId, marker));

        feed.get().catchUp().block(TIMEOUT);
        feed.get().catchUp().block(TIMEOUT);

        return folds;
    }

    // A stopped catch-up writes no marker, so a later one replays from where the stopped one started.
    private RecordingReplayAwareView stoppedAtTheFirstReplayedEvent() {
        AtomicReference<CatchupProjectionFeed<Counted>> feed = new AtomicReference<>();
        RecordingReplayAwareView view = new RecordingReplayAwareView(event -> feed.get().stopCatchUp());
        feed.set(feed(view));
        feed.get().catchUp().block(TIMEOUT);
        return view;
    }

    private RecordingReplayAwareView catchUp() {
        RecordingReplayAwareView view = new RecordingReplayAwareView();
        feed(view).catchUp().block(TIMEOUT);
        return view;
    }

    private Optional<Long> storedPosition() {
        return marker.read("counter").map(GlobalCheckpoint::positionOf).blockOptional(TIMEOUT);
    }

    private CatchupProjectionFeed<Counted> feed(RecordingReplayAwareView view) {
        return feed(view, marker);
    }

    private CatchupProjectionFeed<Counted> feed(RecordingReplayAwareView view, InMemoryCheckpointStorage markerStorage) {
        return CatchupProjectionFeed.create("counter", view, Filter.all(), log, converter, Counted::eventId, markerStorage);
    }
}
