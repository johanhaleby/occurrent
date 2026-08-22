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
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fresh-context verification test for reviewer CLAIM 5 (MAJOR): {@code stop()} immediately followed by
 * {@code start(true)} while a replay has already decided to stop but has not yet reached {@code forget(...)} is
 * claimed to lose that {@code start(true)} call entirely. {@code relaunchInterruptedReplay} sees
 * {@code replayingSubscriptions.containsKey(id)} still {@code true} (the deciding replay has not removed its own
 * entry yet) and returns {@code null} without relaunching; the replay then unwinds and clears the entry; nothing
 * is left to notice the abandoned {@code start(true)} call, so the subscription stays interrupted (launcher kept,
 * nothing replaying, model-level {@code stopped} cleared) until a caller happens to call {@code start(true)} or
 * {@code resumeSubscription(id)} again.
 * <p>
 * The race window is made deterministic with a reader whose replay {@link Stream} blocks in its {@code close()}
 * handler (run by the try-with-resources in {@code BlockingHandover.catchUp(..)} right after the loop exits, so
 * by the time it is parked there {@code stoppedMidReplay} is already latched {@code true}, and strictly before
 * {@code forget(...)} runs back in {@code launchReplay}'s continuation).
 * <p>
 * This test forces exactly that interleaving and asserts the CORRECT behavior the reviewer proposes: the
 * subscription reaches live (finishes replaying) off the back of the one {@code start(true)} call made during the
 * race. If CLAIM 5 is real, this assertion fails: the second historical event is never folded, and only a further,
 * un-raced {@code start(true)} call recovers it.
 */
class CatchupThenPushSubscriptionModelStopStartRaceVerificationTest {

    @Test
    void a_start_call_racing_a_replays_own_decision_to_stop_still_relaunches_it() throws Exception {
        CloudEvent event1 = cloudEvent("1", "Created");
        CloudEvent event2 = cloudEvent("2", "Updated");

        CountDownLatch foldEntered = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);

        PositionOrderedReader reader = reader(() -> Stream.of(event1, event2).onClose(() -> {
            closeEntered.countDown();
            awaitLatch(releaseClose);
        }), 2);

        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        List<String> folded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            if (ce.getId().equals("1")) {
                foldEntered.countDown();
                awaitLatch(releaseFold);
            }
        });
        assertThat(foldEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // stop() while parked inside the first fold: the per-item keepReplaying() check for event "2" has not run
        // yet, so nothing has decided anything.
        model.stop();
        releaseFold.countDown();

        // Releasing the fold lets the loop reach its per-item check for event "2", which now sees stopped == true
        // and breaks (stoppedMidReplay = true) before ever folding it, then exits the try block, which is what
        // runs the reader's Stream.close() handler, parking there next.
        assertThat(closeEntered.await(5, TimeUnit.SECONDS))
                .as("the replay must have already decided to stop (stoppedMidReplay latched) and be unwinding "
                        + "through the try-with-resources close, strictly before forget(..) runs")
                .isTrue();

        // The exact race: replayingSubscriptions still maps "sub" to this replay (forget(..) has not run), so
        // relaunchInterruptedReplay's containsKey(id) check is still true and this call is claimed to no-op.
        model.start(true);

        releaseClose.countDown();

        // Give the replay thread's own unwind (post-loop check, abandon, forget(), catchUp() and launchReplay's
        // continuation returning) time to finish, then check whether the one start(true) call above actually
        // relaunched the catch-up.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((model.isCatchingUp("sub") || !folded.contains("2")) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(folded)
                .as("the start(true) call made during the race must not be silently lost: the subscription must "
                        + "reach live and fold the second historical event off the back of it alone")
                .contains("2");
        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(model.isRunning("sub")).isTrue();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the latch", e);
        }
    }

    private static PositionOrderedReader reader(java.util.function.Supplier<Stream<CloudEvent>> stream, long head) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return stream.get();
            }

            @Override
            public long currentPosition() {
                return head;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
