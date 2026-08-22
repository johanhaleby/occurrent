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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A lifecycle call and a subscribe on the same id are one step against each other.
 * <p>
 * Registering on the live feed, keeping the handover, keeping the launcher and starting the replay used to happen
 * one after another with nothing holding them together, so a cancel arriving part way through left some of them
 * installed for a subscription that no longer exists.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupThenPushSubscriptionModelLifecycleAtomicityTest {

    @Test
    void a_cancel_arriving_while_subscribe_is_registering_leaves_nothing_behind_for_the_cancelled_id() throws Exception {
        AtomicReference<CatchupThenPushSubscriptionModel> modelRef = new AtomicReference<>();
        CountDownLatch cancelStarted = new CountDownLatch(1);
        CountDownLatch cancelFinished = new CountDownLatch(1);

        // The cancel is fired from inside the live feed's own registration call, which is the first thing
        // subscribe(..) does, so it arrives while the rest of the tail is still to run.
        PushSubscriptionModel feed = new PushSubscriptionModel() {
            @Override
            Subscription subscribeCatchupThenPush(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                                  RegisteringSubscribable.RoutingAction action) {
                Subscription subscription = super.subscribeCatchupThenPush(subscriptionId, filter, startAt, action);
                Thread.ofVirtual().start(() -> {
                    cancelStarted.countDown();
                    modelRef.get().cancelSubscription(subscriptionId);
                    cancelFinished.countDown();
                });
                awaitLatch(cancelStarted);
                return subscription;
            }
        };

        List<String> handled = new CopyOnWriteArrayList<>();
        PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        modelRef.set(model);

        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> handled.add(ce.getId()));
        assertThat(cancelFinished.await(10, TimeUnit.SECONDS)).isTrue();

        // Long enough for a replay the cancel failed to stop to work through its three events and settle.
        Thread.sleep(1500);
        assertThat(model.isCatchingUp("sub")).as("no replay is left running for a cancelled id").isFalse();
        assertThat(model.isRunning("sub")).as("nothing is left registered for a cancelled id").isFalse();
        assertThat(model.isReadyForLiveDelivery("sub"))
                .as("no handover is left mapped for a cancelled id, which is what this answers off")
                .isFalse();
        assertThat(feed.subscriptionIds()).as("the live feed registration went with it").doesNotContain("sub");

        // start(true) has nothing to relaunch, since a cancel keeps no launcher. A launcher left behind by the
        // interrupted subscribe would replay a cancelled subscription's whole history here.
        int handledBeforeStart = handled.size();
        model.start(true);
        Thread.sleep(1500);
        assertThat(handled).as("no launcher survived the cancel, so start(true) replays nothing")
                .hasSize(handledBeforeStart);
    }

    /**
     * A pause asked for while the replay is finishing is either handed to the live feed or still waiting to be.
     * It used to be possible for the completion to read an empty pending-pause record, exit, and only then have the
     * pause written into it, which left the subscription delivering while {@code isPaused} answered true.
     */
    @Test
    void a_pause_asked_for_while_the_replay_is_completing_is_never_both_recorded_and_ignored() throws Exception {
        for (int attempt = 0; attempt < 40; attempt++) {
            PushSubscriptionModel feed = new PushSubscriptionModel();
            PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1")));
            CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

            CountDownLatch handling = new CountDownLatch(1);
            var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> handling.countDown());
            assertThat(handling.await(5, TimeUnit.SECONDS)).isTrue();

            Thread pauser = Thread.ofVirtual().start(() -> model.pauseSubscription("sub"));
            subscription.waitUntilStarted(Duration.ofSeconds(5));
            pauser.join();

            assertThat(model.isPaused("sub"))
                    .as("attempt %d: a pause this model reports must have reached the live feed, or still be "
                            + "waiting for a replay that is still running", attempt)
                    .isEqualTo(feed.isPaused("sub") || model.isCatchingUp("sub"));
            model.shutdown();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the latch", e);
        }
    }

    private static PositionOrderedReader reader(Supplier<Stream<CloudEvent>> history) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return history.get();
            }

            @Override
            public long currentPosition() {
                return 1L;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Created")
                .build();
    }
}
