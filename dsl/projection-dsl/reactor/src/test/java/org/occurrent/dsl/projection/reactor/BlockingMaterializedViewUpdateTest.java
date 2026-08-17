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
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Projections#reactiveUpdateWithMetadata(MaterializedView)} bridges a blocking {@link MaterializedView} onto a
 * reactive pipeline. Driven for real through {@link CatchupProjectionFeed}, so the assertions exercise the same
 * {@code instanceof} probe a production catch-up replay does, not a hand-called shortcut.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingMaterializedViewUpdateTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    @Test
    void a_catch_up_replay_forwards_its_lifecycle_to_a_replay_aware_blocking_view_wrapped_through_the_bridge() {
        FakeReplayAwareView view = new FakeReplayAwareView();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", Projections.reactiveUpdateWithMetadata(view), Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);

        feed.catchUp().block();

        assertThat(view.calls).containsExactly("replayStarted", "update:1:replaying", "update:2:replaying", "replayCompleted");
    }

    @Test
    void a_stopped_catch_up_forwards_replay_abandoned_to_the_wrapped_view_instead_of_replay_completed() {
        FakeReplayAwareView view = new FakeReplayAwareView();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", Projections.reactiveUpdateWithMetadata(view), Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);
        // Stops from inside the first fold, so the stop is in place before the replay considers delivering "2" and
        // the abandon runs on a replay genuinely still in flight.
        view.onUpdate = feed::stopCatchUp;

        feed.catchUp().block();

        assertThat(view.calls).containsExactly("replayStarted", "update:1:replaying", "replayAbandoned");
    }

    @Test
    void a_blocking_view_with_no_replay_awareness_is_driven_write_through_with_no_lifecycle_calls() {
        FakeView view = new FakeView();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", Projections.reactiveUpdateWithMetadata(view), Filter.all(), reader("1", "2"), countedConverter(), Counted::eventId, null);

        feed.catchUp().block();

        assertThat(view.calls).containsExactly("update:1", "update:2");
    }

    private static final class FakeReplayAwareView implements MaterializedView<Counted>, ReplayAware {
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private volatile Runnable onUpdate = () -> {
        };
        private boolean replaying = false;

        @Override
        public void update(Counted event) {
            update(EventMetadata.empty(), event);
        }

        @Override
        public void update(EventMetadata metadata, Counted event) {
            calls.add("update:" + event.eventId() + (replaying ? ":replaying" : ":live"));
            onUpdate.run();
        }

        @Override
        public void replayStarted() {
            calls.add("replayStarted");
            replaying = true;
        }

        @Override
        public void replayCompleted() {
            replaying = false;
            calls.add("replayCompleted");
        }

        @Override
        public void replayAbandoned() {
            replaying = false;
            calls.add("replayAbandoned");
        }
    }

    private static final class FakeView implements MaterializedView<Counted> {
        private final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public void update(Counted event) {
            update(EventMetadata.empty(), event);
        }

        @Override
        public void update(EventMetadata metadata, Counted event) {
            calls.add("update:" + event.eventId());
        }
    }

    private PositionOrderedReader reader(String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(List.of(eventIds)).map(BlockingMaterializedViewUpdateTest::cloudEvent);
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
