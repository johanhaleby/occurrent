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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What each push model answers when asked whether it still holds an event, which is what a saga asks before it stops
 * retrying one. The catch-up model is the reason the question is asked per event rather than once for the model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PushModelHistoryRetentionTest {

    /**
     * A bare feed is handed events from outside and stores none of them, so it cannot answer at all and does not
     * implement the capability. This is the saga configured with {@code catchup = NONE}, and it is why such a saga
     * keeps the blocking behaviour it had before 0.34.0.
     */
    @Test
    void a_push_feed_on_its_own_cannot_say_and_declares_nothing() {
        assertThat(HistoryRetainingSubscriptions.findIn(new PushSubscriptionModel())).isEmpty();
    }

    /**
     * The two halves of one wiring. An event this application wrote is in the store the model replays from, so it can
     * be obtained again and quarantining it loses nothing. An event that arrived over the feed and was never written
     * here cannot, which is what a bridge consuming another service's events delivers. Both reach the same saga
     * through the same model, so the answer belongs to the event and not to the model or to how it was configured.
     */
    @Test
    void a_catch_up_feed_holds_what_this_application_wrote_and_not_what_only_arrived() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("orders", List.of(event("written-here")));

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        HistoryRetainingSubscriptions retention = HistoryRetainingSubscriptions.findIn(model).orElseThrow();

        assertAll(
                () -> assertThat(retention.retains(event("written-here"))).isTrue(),
                () -> assertThat(retention.retains(event("only-arrived"))).isFalse(),
                () -> assertThat(retention.retainsEveryEvent()).isFalse()
        );
    }

    /**
     * Why asking the store needs no guard against a store that cannot be asked. A reader with no position never
     * reaches the question, because this model refuses one at construction, so the check is free to assume it.
     */
    @Test
    void a_reader_without_a_position_is_refused_at_construction_rather_than_when_asked() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept).withoutStreamPosition();

        assertThatThrownBy(() -> new CatchupThenPushSubscriptionModel(store, feed, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writesPosition");
    }

    /**
     * An event with no id cannot be looked up, and an unanswerable question reads as a no rather than as an exception
     * reaching the saga, so an instance keeps blocking instead of losing its event.
     */
    @Test
    void an_event_that_cannot_be_looked_up_answers_no() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        assertThat(HistoryRetainingSubscriptions.findIn(model).orElseThrow().retains(event("never-written"))).isFalse();
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("OrderPlaced").build();
    }
}
