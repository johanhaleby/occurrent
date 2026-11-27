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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A recording projection learns of an append only from the live copy of its event. That copy is delivered live or by
 * the catch-up drain, or reported to the projection when the replay applied the event first (ADR 137). A live copy that
 * finds the live buffer full before the replay has started is in nobody's buffer, so dropping it would leave its append
 * unrecorded and {@code waitUntilApplied} would time out for it. The write throws and the catch-up fails instead.
 */
@Timeout(30)
@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingProjectionBufferOverflowBeforeReplayTest {

    private static final String SUBSCRIPTION_ID = "recording-projection";

    @Test
    void an_append_that_finds_the_live_buffer_full_before_the_replay_starts_fails_the_catch_up_without_a_marker_and_later_appends_are_refused() throws Exception {
        // Given
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        CountDownLatch markerLookupReached = new CountDownLatch(1);
        CountDownLatch releaseMarkerLookup = new CountDownLatch(1);
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage() {
            @Override
            public boolean exists(String subscriptionId) {
                markerLookupReached.countDown();
                await(releaseMarkerLookup);
                return super.exists(subscriptionId);
            }
        };
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker, new CatchupThenLiveOptions(10, 1));
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        List<String> applied = new CopyOnWriteArrayList<>();
        MaterializedView<String> view = new MaterializedView<>() {
            @Override
            public void update(String event) {
                applied.add(event);
            }
        };
        RecordingMaterializedView<String> recording = Projections.recordingAppliedAppends(view, SUBSCRIPTION_ID, appliedAppends);
        model.listenForCatchup(SUBSCRIPTION_ID, recording);
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> recording.update(EventMetadata.from(event), event.getId()));
        try {
            assertThat(markerLookupReached.await(5, TimeUnit.SECONDS)).as("the replay is held at the catch-up marker lookup, the live feed is registered").isTrue();

            // When
            Throwable first = catchThrowable(() -> store.write("stream", List.of(event("first"))));
            Throwable second = catchThrowable(() -> store.write("stream", List.of(event("second"))));
            releaseMarkerLookup.countDown();
            Throwable startFailure = catchThrowable(subscription::waitUntilStarted);
            Throwable later = catchThrowable(() -> store.write("stream", List.of(event("later"))));

            // Then
            assertThat(first).as("the append the buffer had room for").isNull();
            assertThat(second).as("the append that found the buffer full, which would otherwise return normally and never be recorded").isNotNull().hasMessageContaining("buffer overflowed");
            assertThat(store.read("stream").events().map(CloudEvent::getId).toList()).as("the store committed every append before the feed refused it").containsExactly("first", "second", "later");
            assertThat(startFailure).as("the catch-up that found the buffer full before it replayed").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Catch-up failed").cause().hasMessageContaining("buffer overflowed");
            assertThat(marker.exists(SUBSCRIPTION_ID)).as("catch-up marker").isFalse();
            assertThat(later).as("a later append, refused after the store committed it").isNotNull().hasMessageContaining("Catch-up failed");
            AppendId secondAppend = AppendId.from(EventMetadata.from(store.read("stream").events().skip(1).findFirst().orElseThrow())).orElseThrow();
            assertThat(appliedAppends.hasApplied(SUBSCRIPTION_ID, secondAppend)).as("the refused append is not claimed as applied").isFalse();
            assertThat(applied).as("what the failed catch-up applied of the appends the live feed took").doesNotContain("second", "later");
        } finally {
            releaseMarkerLookup.countDown();
            model.shutdown();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }
}
