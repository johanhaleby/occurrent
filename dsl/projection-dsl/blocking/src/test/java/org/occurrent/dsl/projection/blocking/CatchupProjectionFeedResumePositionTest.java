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
package org.occurrent.dsl.projection.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.blocking.ResumePositionFixtures.Counted;
import org.occurrent.dsl.projection.blocking.ResumePositionFixtures.RecordingReplayAwareView;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.projection.blocking.ResumePositionFixtures.bufferedLiveDelivery;
import static org.occurrent.dsl.projection.blocking.ResumePositionFixtures.converter;

/**
 * What the marker of a {@link CatchupProjectionFeed} means (ADR 179): the head read before a replay, which the next
 * catch-up resumes after. Every start is a fresh feed over the same store and marker storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupProjectionFeedResumePositionTest {

    private final CloudEventConverter<Counted> converter = converter();
    private final InMemoryEventStore store = new InMemoryEventStore();
    private final InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

    @Test
    void a_marker_that_was_deleted_and_then_written_late_makes_the_next_catch_up_replay_only_the_events_after_it() {
        write("1", "2");
        long head = store.currentPosition();
        catchUp(new CopyOnWriteArrayList<>());
        marker.delete("counter");
        marker.save("counter", GlobalCheckpoint.of(head));
        write("3");

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUp(folded);

        assertThat(folded).as("replayed after the surviving marker, nothing before it and nothing lost").containsExactly("3");
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() {
        write("1", "2");
        long olderHead = store.currentPosition();
        catchUp(new CopyOnWriteArrayList<>());
        write("3");
        catchUp(new CopyOnWriteArrayList<>());
        assertThat(GlobalCheckpoint.positionOf(marker.read("counter"))).as("newer head stored by the second start").isGreaterThan(olderHead);
        marker.save("counter", GlobalCheckpoint.of(olderHead));
        write("4");

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUp(folded);

        assertThat(folded).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void a_restart_replays_the_events_since_the_previous_start_and_folds_a_replayed_and_live_event_once() {
        write("1", "2");
        catchUp(new CopyOnWriteArrayList<>());
        write("3", "4");

        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = feed(event -> folded.add(event.eventId()));
        FutureTask<Void> liveCopy = bufferedLiveDelivery(() -> feed.acceptAndWait(new Counted("4")));
        feed.catchUp();

        assertThat(liveCopy).succeedsWithin(Duration.ofSeconds(5));
        assertThat(folded).containsExactly("3", "4");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        write("1", "2");
        long headBeforeTheReplay = store.currentPosition();

        // An event is written while the replay reads, so the head has moved by the time the marker is written.
        CatchupProjectionFeed<Counted> feed = feed(event -> {
            if (event.eventId().equals("1")) {
                write("during");
            }
        });
        feed.catchUp();

        assertThat(store.currentPosition()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(GlobalCheckpoint.positionOf(marker.read("counter"))).isEqualTo(headBeforeTheReplay);
        List<String> folded = new CopyOnWriteArrayList<>();
        catchUp(folded);
        assertThat(folded).as("the event written during the replay is replayed by the next catch-up").containsExactly("during");
    }

    @Test
    void an_event_that_commits_after_the_head_was_read_at_a_position_before_it_is_folded_once_when_the_broker_delivers_it_again() {
        write("1", "2");
        long head = store.currentPosition();
        List<String> folded = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feed = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        AtomicReference<RoutingOutcome> firstAnswer = new AtomicReference<>();
        // X holds a position at or before the head and commits after the head was read, so the replay does not return
        // it. Its broker message reaches the listener while the catch-up runs.
        PositionOrderedReader reservedBeforeTheHeadCommittedAfter = new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return store.readInPositionOrder(filter, range);
            }

            @Override
            public long currentPosition() {
                long read = store.currentPosition();
                if (offered.compareAndSet(false, true)) {
                    firstAnswer.set(feed.get().acceptOrDefer(new Counted("X")));
                }
                return read;
            }

            @Override
            public boolean writesPosition() {
                return store.writesPosition();
            }
        };
        feed.set(CatchupProjectionFeed.create("counter", (MaterializedView<Counted>) event -> folded.add(event.eventId()), Filter.all(),
                reservedBeforeTheHeadCommittedAfter, converter, Counted::eventId, marker));

        feed.get().catchUp();

        // A broker whose message was not acknowledged delivers it again, here after the feed went live.
        RoutingOutcome answer = firstAnswer.get();
        while (answer == RoutingOutcome.DEFERRED) {
            answer = feed.get().acceptOrDefer(new Counted("X"));
        }
        assertThat(folded).as("X folded once, through the listener path").containsExactlyInAnyOrder("1", "2", "X");
        assertThat(GlobalCheckpoint.positionOf(marker.read("counter"))).isEqualTo(head);
    }

    @Test
    void an_event_that_commits_after_the_head_was_read_at_a_position_before_it_is_folded_once_when_the_listener_waits_for_the_drain() {
        write("1", "2");
        long head = store.currentPosition();
        List<String> order = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupProjectionFeed<Counted>> feed = new AtomicReference<>();
        AtomicReference<FutureTask<Void>> listener = new AtomicReference<>();
        // X is not in the store, so only the drain after the replay can fold it. acceptAndWait(..) from the catch-up
        // thread would wait for its own drain, so the listener is a second thread, parked in the buffer before the
        // head is returned.
        PositionOrderedReader reservedBeforeTheHeadCommittedAfter = new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return store.readInPositionOrder(filter, range);
            }

            @Override
            public long currentPosition() {
                long read = store.currentPosition();
                if (listener.get() == null) {
                    listener.set(bufferedLiveDelivery(() -> {
                        feed.get().acceptAndWait(new Counted("X"));
                        order.add("returned:X");
                    }));
                }
                return read;
            }

            @Override
            public boolean writesPosition() {
                return store.writesPosition();
            }
        };
        feed.set(CatchupProjectionFeed.create("counter", (MaterializedView<Counted>) event -> order.add("fold:" + event.eventId()), Filter.all(),
                reservedBeforeTheHeadCommittedAfter, converter, Counted::eventId, marker));

        feed.get().catchUp();

        assertThat(listener.get()).succeedsWithin(Duration.ofSeconds(5));
        assertThat(order).as("X folded once by the drain, and acceptAndWait(..) returned normally only after that fold")
                .containsExactly("fold:1", "fold:2", "fold:X", "returned:X");
        assertThat(GlobalCheckpoint.positionOf(marker.read("counter"))).isEqualTo(head);
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        write("1", "2", "3");
        marker.save("counter", GlobalCheckpoint.of(store.currentPosition() - 1));

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUp(folded);

        assertThat(folded).containsExactly("3");
    }

    @Test
    void a_catch_up_with_a_stored_marker_and_nothing_after_it_runs_an_empty_replay_and_goes_live() {
        write("1", "2");
        catchUp(new CopyOnWriteArrayList<>());

        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupProjectionFeed<Counted> feed = feed(event -> folded.add(event.eventId()));
        feed.catchUp();
        feed.acceptAndWait(new Counted("3"));

        assertThat(folded).containsExactly("3");
    }

    @Test
    void replay_callbacks_reach_the_view_for_a_replay_from_the_beginning_and_not_for_a_replay_after_a_stored_marker() {
        write("1", "2");
        RecordingReplayAwareView fromTheBeginning = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> first = feed(fromTheBeginning);
        FutureTask<Void> liveCopyOfReplayed = bufferedLiveDelivery(() -> first.acceptAndWait(new Counted("2")));
        first.catchUp();
        assertThat(liveCopyOfReplayed).succeedsWithin(Duration.ofSeconds(5));

        write("3");
        RecordingReplayAwareView afterTheMarker = new RecordingReplayAwareView();
        CatchupProjectionFeed<Counted> restarted = feed(afterTheMarker);
        FutureTask<Void> liveCopyOfReplayedAfterMarker = bufferedLiveDelivery(() -> restarted.acceptAndWait(new Counted("3")));
        restarted.catchUp();
        assertThat(liveCopyOfReplayedAfterMarker).succeedsWithin(Duration.ofSeconds(5));

        assertThat(fromTheBeginning.calls).containsExactly("replayStarted", "update:1", "update:2", "replayCompleted", "alreadyDeliveredByReplay");
        assertThat(afterTheMarker.calls).as("plain updates, no replay callbacks").containsExactly("update:3");
    }

    private void write(String... ids) {
        store.write("s", converter.toCloudEvents(java.util.Arrays.stream(ids).map(Counted::new).toList()));
    }

    private void catchUp(List<String> folded) {
        feed(event -> folded.add(event.eventId())).catchUp();
    }

    private CatchupProjectionFeed<Counted> feed(MaterializedView<Counted> view) {
        return CatchupProjectionFeed.create("counter", view, Filter.all(), store, converter, Counted::eventId, marker);
    }
}
