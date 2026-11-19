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

package org.occurrent.subscription.blocking.subclassed;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.KeepsTheLiveActionSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subclass of a catch-up model written against 0.34.0, in a package of its own, overrides the protected
 * {@code subscribe} with the five arguments it has there, and hands the subscription to the wrapped model through the
 * protected {@code subscribeInTheWrappedModel} with the five arguments it has there. Both still work, and a live call
 * the wrapped model makes after a cancel throws {@link AttemptNotMade} for a subscription handed over that way.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupSubclassOutsideThePackageTest {

    private static final String ID = "live";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final InMemorySubscriptionModel inMemorySubscriptionModel = new InMemorySubscriptionModel();

    @Test
    void the_override_of_the_protected_subscribe_is_called_for_a_public_subscribe_and_the_live_events_reach_the_action_through_super() {
        // Given
        OverridingSubscribe catchup = new OverridingSubscribe(false);

        // When
        catchup.subscribe(ID, null, StartAt.now(), event -> received.add(event.getId()));
        live.callTheKeptAction(ID);

        // Then
        assertAll(
                () -> assertThat(catchup.subscribed).as("the subscriptions the override was called for").containsExactly(ID),
                () -> assertThat(live.thrownByTheAction()).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }

    @Test
    void a_live_call_after_a_cancel_throws_AttemptNotMade_without_reaching_the_action_when_the_subclass_handed_the_subscription_over_with_the_protected_subscribeInTheWrappedModel_on_the_subscribing_thread() {
        // Given
        OverridingSubscribe catchup = new OverridingSubscribe(true);
        catchup.subscribe(ID, null, StartAt.now(), event -> received.add(event.getId()));
        catchup.cancelSubscription(ID);

        // When
        live.callTheKeptAction(ID);

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction()).as("what the live call threw").hasSize(1).allSatisfy(thrown -> assertThat(thrown).isInstanceOf(AttemptNotMade.class)),
                () -> assertThat(received).as("the events the action received").isEmpty());
    }

    // As a subclass of 0.34.0 is written: the five argument signatures only
    private final class OverridingSubscribe extends StreamCatchupSubscriptionModel {
        private final List<String> subscribed = new CopyOnWriteArrayList<>();
        private final boolean handsTheSubscriptionOverItself;

        private OverridingSubscribe(boolean handsTheSubscriptionOverItself) {
            super(live, new InMemoryEventStore(inMemorySubscriptionModel), new CatchupSubscriptionModelConfig(100));
            this.handsTheSubscriptionOverItself = handsTheSubscriptionOverItself;
        }

        @Override
        protected Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
            subscribed.add(subscriptionId);
            return handsTheSubscriptionOverItself
                    ? subscribeInTheWrappedModel(subscriptionId, filter, startAt, action, holdPaused)
                    : super.subscribe(subscriptionId, filter, startAt, action, holdPaused);
        }
    }
}
