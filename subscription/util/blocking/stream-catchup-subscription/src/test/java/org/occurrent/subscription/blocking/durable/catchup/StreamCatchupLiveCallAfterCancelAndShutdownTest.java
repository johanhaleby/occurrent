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

package org.occurrent.subscription.blocking.durable.catchup;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A live call that reaches the catch-up model once it has begun to shut down gets the event to the action only for a
 * subscription the shutdown itself forgot. A subscription that a cancel forgot before the shutdown began is a
 * subscription that has ended, so the call throws {@link AttemptNotMade} and the action never sees the event. A live
 * delegate whose call was under way when the cancel returned does exactly this.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupLiveCallAfterCancelAndShutdownTest {

    private static final String ID = "live";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final InMemorySubscriptionModel inMemorySubscriptionModel = new InMemorySubscriptionModel();
    private final StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(inMemorySubscriptionModel), new CatchupSubscriptionModelConfig(100));

    @Test
    void a_live_event_the_live_delegate_calls_as_the_shutdown_begins_reaches_the_action_of_a_subscription_no_cancel_has_ended() {
        // Given
        catchup.subscribe(ID, null, StartAt.now(), event -> received.add(event.getId()));
        live.whenItShutsDown(() -> live.callTheKeptAction(ID));

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction()).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }

    @Test
    void a_live_event_the_live_delegate_calls_as_the_shutdown_begins_throws_AttemptNotMade_without_reaching_the_action_of_a_subscription_a_cancel_that_returned_has_ended() {
        // Given
        catchup.subscribe(ID, null, StartAt.now(), event -> received.add(event.getId()));
        catchup.cancelSubscription(ID);
        live.whenItShutsDown(() -> live.callTheKeptAction(ID));

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction()).as("what the live call threw").hasSize(1).allSatisfy(thrown -> assertThat(thrown).isInstanceOf(AttemptNotMade.class)),
                () -> assertThat(received).as("the events the action received").isEmpty());
    }
}
