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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A registration's position read runs on the wrapped model's own signal, so nothing orders it against a later
 * registration for the same id. Registering again under an id whose first attempt has not yet finished failing is
 * exactly the recovery the upgrade guide documents, cancelling first to free the id, so this covers that a first
 * attempt's error, however late it arrives, only ever removes its own entry.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelReRegistrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_first_registrations_late_error_does_not_remove_a_second_registrations_entry() {
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

        // The first attempt's read now fails, arriving after the second registration has already replaced it.
        firstAttemptRead.tryEmitError(new IllegalStateException("first attempt: cannot read the position"));

        assertThatThrownBy(() -> first.waitUntilStarted().block(TIMEOUT))
                .as("the first attempt's own handle still reports its failure")
                .isInstanceOf(IllegalStateException.class);
        assertThat(model.isRunning(SUBSCRIPTION_ID))
                .as("the first attempt's late error must remove only the entry it put there, not the second registration's")
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
