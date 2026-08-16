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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Registering again under an id whose earlier attempt has not settled is the documented recovery from a refused
 * registration: {@link ReactorDurableSubscriptionModel#cancelSubscription(String)} to free the id, then
 * {@code subscribe(..)} again. Cancelling disposes the first attempt's actual subscription, including whatever
 * position read it was waiting on, so a signal that arrives for that read afterwards has nowhere left to go:
 * Reactor drops a signal delivered to an already-cancelled subscriber instead of routing it anywhere, rather than
 * resurrecting a subscription nothing is listening to any more. That is what keeps this recovery safe before any of
 * {@link ReactorDurableSubscriptionModel}'s own per-call bookkeeping enters into it, and it is worth pinning down on
 * its own rather than assuming it.
 * <p>
 * A narrower race lives one level down, inside a single registration's own bookkeeping: a late error could, in
 * principle, land between that call installing its map entry and finishing recording its identity, which would let
 * an already-terminated subscription be reported as running with nothing left able to remove it. That gap is
 * closed in {@link ReactorDurableSubscriptionModel#startInternalSubscription} by giving each call one map entry for
 * its whole lifetime rather than installing a placeholder and replacing it later, verifiable by reading the method:
 * there is exactly one {@code put}, before the call ever subscribes, so a concurrent reader is never in a position
 * to see one entry while the call's own bookkeeping still names another.
 * <p>
 * That gap was not reproducible here with a racing thread. A background thread armed to fail the position read the
 * instant it saw the read subscribed to, competing against the registering thread's own remaining statements,
 * landed safely outside the gap on every one of several hundred thousand contended attempts, run as many such pairs
 * at once to oversubscribe the available cores on purpose. The two sides are consistently a cache line or more
 * apart in timing rather than close enough for ordinary scheduling noise to land between them, so this file does
 * not carry a timing-based regression test for that specific gap; its closure rests on the single-{@code put}
 * argument above.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelReRegistrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_first_registrations_late_error_is_dropped_once_cancelled_and_does_not_affect_a_second_registration() {
        // The first registration's position read hangs here until the test completes it, standing in for an error
        // that is still on its way when the second registration below is made.
        Sinks.One<Checkpoint> firstAttemptRead = Sinks.one();
        StringBasedCheckpoint secondAttemptPosition = new StringBasedCheckpoint("second-attempt");
        DelayableSubscriptionModel delegate = new DelayableSubscriptionModel(firstAttemptRead.asMono(), secondAttemptPosition);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());

        Subscription first = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();

        // Cancelling and registering again under the same id is the documented recovery from a registration that
        // has not settled, and the model allows it: nothing here waits for the first attempt to finish.
        model.cancelSubscription(SUBSCRIPTION_ID);
        Subscription second = model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(secondAttemptPosition), __ -> Mono.empty());
        second.waitUntilStarted().block(TIMEOUT);
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();

        // The first attempt's read now fails, arriving after cancellation already disposed of it. Sinks.One accepts
        // an emission whether or not a subscriber is still attached (it is a replaying, not a multicast, sink), so
        // acceptance alone says nothing about delivery; what matters is observed below, on the model itself.
        firstAttemptRead.tryEmitError(new IllegalStateException("first attempt: cannot read the position"));

        // A cancelled subscription's own handle never settles either way: cancelling severed it from
        // firstAttemptRead before this error was emitted, so nothing carries the error to startedSink. Asserted as
        // a timeout on the handle itself, not by catching whichever exception type block(Duration) happens to throw
        // when it gives up.
        StepVerifier.create(first.waitUntilStarted())
                .expectTimeout(TIMEOUT)
                .verify();

        assertThat(model.isRunning(SUBSCRIPTION_ID))
                .as("the first attempt's cancelled, undelivered error must not affect the second registration")
                .isTrue();
        assertThat(model.subscriptionIds()).containsExactly(SUBSCRIPTION_ID);
    }

    /**
     * Answers the first call to {@link #globalCheckpoint()} from a {@link Mono} the test controls the completion of,
     * and every call after that with the given checkpoint, so a test can register once, register again under the
     * same id, and only then decide when the first registration's read fails.
     */
    private static final class DelayableSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final AtomicInteger globalCheckpointCalls = new AtomicInteger();
        private final Mono<Checkpoint> firstRead;
        private final Checkpoint subsequentCheckpoint;

        private DelayableSubscriptionModel(Mono<Checkpoint> firstRead, Checkpoint subsequentCheckpoint) {
            this.firstRead = firstRead;
            this.subsequentCheckpoint = subsequentCheckpoint;
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.defer(() -> globalCheckpointCalls.getAndIncrement() == 0 ? firstRead : Mono.just(subsequentCheckpoint));
        }
    }
}
