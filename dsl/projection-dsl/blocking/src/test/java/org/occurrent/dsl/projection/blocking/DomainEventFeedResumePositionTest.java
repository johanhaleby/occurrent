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
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.blocking.ResumePositionFixtures.Counted;
import org.occurrent.dsl.projection.blocking.ResumePositionFixtures.RecordingReplayAwareView;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.projection.blocking.ResumePositionFixtures.converter;

/**
 * What the marker of a {@link DomainEventFeed} means (ADR 179): per projection id, the head read before that
 * projection's replay, which its next catch-up resumes after.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedResumePositionTest {

    private final CloudEventConverter<Counted> converter = converter();
    private final InMemoryEventStore store = new InMemoryEventStore();
    private final InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

    @Test
    void a_restart_replays_only_the_events_after_the_stored_marker() {
        write("1", "2");
        catchUpAll(new CopyOnWriteArrayList<>());
        write("3");

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUpAll(folded);

        assertThat(folded).containsExactly("3");
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() {
        write("1", "2");
        long olderHead = store.currentPosition();
        catchUpAll(new CopyOnWriteArrayList<>());
        write("3");
        catchUpAll(new CopyOnWriteArrayList<>());
        marker.save("counter", GlobalCheckpoint.of(olderHead));
        write("4");

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUpAll(folded);

        assertThat(folded).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        write("1", "2");
        long headBeforeTheReplay = store.currentPosition();

        catchUpAll(event -> {
            if (event.eventId().equals("1")) {
                write("during");
            }
        });

        assertThat(store.currentPosition()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(GlobalCheckpoint.positionOf(marker.read("counter"))).isEqualTo(headBeforeTheReplay);
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        write("1", "2", "3");
        marker.save("counter", GlobalCheckpoint.of(store.currentPosition() - 1));

        List<String> folded = new CopyOnWriteArrayList<>();
        catchUpAll(folded);

        assertThat(folded).containsExactly("3");
    }

    @Test
    void replay_callbacks_reach_the_view_for_a_replay_from_the_beginning_and_not_for_a_replay_after_a_stored_marker() {
        write("1", "2");
        RecordingReplayAwareView fromTheBeginning = new RecordingReplayAwareView();
        catchUpAll(fromTheBeginning);
        write("3");
        RecordingReplayAwareView afterTheMarker = new RecordingReplayAwareView();
        catchUpAll(afterTheMarker);

        assertThat(fromTheBeginning.calls).containsExactly("replayStarted", "update:1", "update:2", "replayCompleted");
        assertThat(afterTheMarker.calls).as("plain updates, no replay callbacks").containsExactly("update:3");
    }

    private void write(String... ids) {
        store.write("s", converter.toCloudEvents(Arrays.stream(ids).map(Counted::new).toList()));
    }

    private void catchUpAll(List<String> folded) {
        catchUpAll(event -> folded.add(event.eventId()));
    }

    private void catchUpAll(MaterializedView<Counted> view) {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, marker);
        feed.register("counter", view, Filter.all());
        feed.catchUpAll();
    }
}
