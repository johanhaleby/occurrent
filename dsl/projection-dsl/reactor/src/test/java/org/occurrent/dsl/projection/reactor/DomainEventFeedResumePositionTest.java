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
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.Counted;
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.InMemoryLog;
import org.occurrent.dsl.projection.reactor.ResumePositionFixtures.RecordingReplayAwareView;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.projection.reactor.ResumePositionFixtures.converter;

/**
 * What the marker of a {@link DomainEventFeed} means (ADR 179): per projection id, the head read before that
 * projection's replay, which its next catch-up resumes after.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedResumePositionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final CloudEventConverter<Counted> converter = converter();
    private final InMemoryLog log = new InMemoryLog();
    private final InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

    @Test
    void a_catch_up_without_a_marker_replays_the_whole_history_and_stores_the_head() {
        log.append("1", "2");

        RecordingReplayAwareView view = catchUpAll();

        assertThat(view.updates()).containsExactly("1", "2");
        assertThat(storedPosition()).contains(2L);
    }

    @Test
    void a_restart_replays_only_the_events_after_the_stored_marker() {
        log.append("1", "2");
        catchUpAll();
        log.append("3");

        RecordingReplayAwareView view = catchUpAll();

        assertThat(view.updates()).containsExactly("3");
        assertThat(storedPosition()).contains(3L);
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() {
        log.append("1", "2");
        long olderHead = log.head();
        catchUpAll();
        log.append("3");
        catchUpAll();
        marker.save("counter", GlobalCheckpoint.of(olderHead)).block(TIMEOUT);
        log.append("4");

        assertThat(catchUpAll().updates()).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        log.append("1", "2");
        long headBeforeTheReplay = log.head();

        catchUpAll(new RecordingReplayAwareView(event -> {
            if (event.eventId().equals("1")) {
                log.append("during");
            }
        }));

        assertThat(log.head()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(storedPosition()).contains(headBeforeTheReplay);
        assertThat(catchUpAll().updates()).as("the event appended during the replay is replayed by the next catch-up").containsExactly("during");
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        log.append("1", "2", "3");
        marker.save("counter", GlobalCheckpoint.of(2)).block(TIMEOUT);

        assertThat(catchUpAll().updates()).containsExactly("3");
    }

    @Test
    void replay_callbacks_reach_the_view_for_a_replay_from_the_beginning_and_not_for_a_replay_after_a_stored_marker() {
        log.append("1", "2");
        RecordingReplayAwareView fromTheBeginning = catchUpAll();
        log.append("3");
        RecordingReplayAwareView afterTheMarker = catchUpAll();

        assertThat(fromTheBeginning.calls).containsExactly("replayStarted", "update:1", "update:2", "replayCompleted");
        assertThat(afterTheMarker.calls).as("plain updates, no replay callbacks").containsExactly("update:3");
    }

    @Test
    void a_stopped_replay_tells_the_view_it_was_abandoned_when_it_replays_from_the_beginning_and_not_when_it_replays_after_a_stored_marker() {
        log.append("1", "2");
        RecordingReplayAwareView abandonedFromTheBeginning = stoppedAtTheFirstReplayedEvent();
        assertThat(abandonedFromTheBeginning.calls).containsExactly("replayStarted", "update:1", "replayAbandoned");

        catchUpAll();
        log.append("3", "4");
        RecordingReplayAwareView abandonedAfterTheMarker = stoppedAtTheFirstReplayedEvent();
        assertThat(abandonedAfterTheMarker.calls).as("replayed after the stored marker, so no replay callbacks").containsExactly("update:3");
    }

    @Test
    void going_live_without_a_catch_up_writes_no_marker() {
        log.append("1", "2");
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(log, converter, Counted::eventId, marker);
        feed.register("counter", new RecordingReplayAwareView(), Filter.all());

        feed.goLive("counter").block(TIMEOUT);

        assertThat(storedPosition()).as("marker after goLive(..)").isEmpty();
    }

    // A stopped catch-up writes no marker, so a later one replays from where the stopped one started.
    private RecordingReplayAwareView stoppedAtTheFirstReplayedEvent() {
        AtomicReference<DomainEventFeed<Counted>> feed = new AtomicReference<>();
        RecordingReplayAwareView view = new RecordingReplayAwareView(event -> feed.get().stopCatchUp());
        feed.set(new DomainEventFeed<>(log, converter, Counted::eventId, marker));
        feed.get().register("counter", view, Filter.all());
        feed.get().catchUpAll().block(TIMEOUT);
        return view;
    }

    private RecordingReplayAwareView catchUpAll() {
        RecordingReplayAwareView view = new RecordingReplayAwareView();
        catchUpAll(view);
        return view;
    }

    private void catchUpAll(RecordingReplayAwareView view) {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(log, converter, Counted::eventId, marker);
        feed.register("counter", view, Filter.all());
        feed.catchUpAll().block(TIMEOUT);
    }

    private Optional<Long> storedPosition() {
        return marker.read("counter").map(GlobalCheckpoint::positionOf).blockOptional(TIMEOUT);
    }
}
