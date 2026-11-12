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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A dynamic {@link StartAt} is evaluated on the calling thread while subscribing, so it can throw there, for example
 * when it reads a stored position and storage is unreachable. A call that failed that way must leave nothing behind
 * under the subscription id, so that trying again with the same id is not refused as a duplicate.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelThrowingStartAtTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String STORAGE_UNREACHABLE = "storage is unreachable";

    @Test
    void a_subscribe_whose_dynamic_start_throws_leaves_nothing_behind_and_the_same_id_can_subscribe_again() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("global");
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        StartAt failsOnce = failsOnceThenStartsAt(new IllegalStateException(STORAGE_UNREACHABLE), "stored");

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, failsOnce, __ -> Mono.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("storage is unreachable");
        assertThat(model.subscriptionIds()).isEmpty();

        model.subscribe(SUBSCRIPTION_ID, null, failsOnce, __ -> Mono.empty());

        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThat(delegate.startedAt).extracting(StartAt::toString).containsExactly("stored");
    }

    // A checked exception is one that Kotlin code throws without declaring it
    @ParameterizedTest
    @ValueSource(strings = {"runtime exception", "error", "checked exception"})
    void a_resume_whose_dynamic_start_throws_leaves_the_subscription_paused_and_it_can_be_resumed_again(String kind) {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("global");
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        Throwable threw = ThrownAtTheCall.of(kind, STORAGE_UNREACHABLE);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, failsOnceThenStartsAt(threw, "stored"), __ -> Mono.empty());

        assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID)).isSameAs(threw);
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThat(delegate.startedAt).extracting(StartAt::toString).containsExactly("stored");
    }

    /**
     * A start of a stopped model resumes two subscriptions, and the dynamic start position asked first throws. That
     * subscription stays paused, the other one starts, and the start then throws what was thrown. A checked exception is
     * one that Kotlin code throws without declaring it.
     */
    @ParameterizedTest
    @ValueSource(strings = {"runtime exception", "error", "checked exception"})
    void a_start_whose_dynamic_start_throws_for_one_subscription_leaves_it_paused_starts_the_other_and_throws_what_it_threw(String kind) {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("global");
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        Throwable threw = ThrownAtTheCall.of(kind, STORAGE_UNREACHABLE);
        AtomicReference<String> askedFirst = new AtomicReference<>();
        model.stop();
        for (String subscriptionId : List.of("first", "second")) {
            model.subscribe(subscriptionId, null, StartAt.dynamic(() -> {
                if (askedFirst.compareAndSet(null, subscriptionId)) {
                    throw ThrownAtTheCall.undeclared(threw);
                }
                return StartAt.checkpoint(new StringBasedCheckpoint("stored"));
            }), __ -> Mono.empty());
        }

        Throwable thrown = catchThrowable(() -> model.start());

        String threwFor = askedFirst.get();
        String startedAfterIt = "first".equals(threwFor) ? "second" : "first";
        assertThat(thrown).as("what the start threw").isSameAs(threw);
        assertThat(model.isPaused(threwFor)).as("whether the subscription whose start position threw is paused").isTrue();
        assertThat(model.isRunning(startedAfterIt)).as("whether the other subscription is running").isTrue();
        assertThat(delegate.startedAt).as("start positions the feed was opened from").extracting(StartAt::toString).containsExactly("stored");
    }

    private static StartAt failsOnceThenStartsAt(Throwable threw, String checkpoint) {
        AtomicInteger calls = new AtomicInteger();
        return StartAt.dynamic(() -> {
            if (calls.getAndIncrement() == 0) {
                throw ThrownAtTheCall.undeclared(threw);
            }
            return StartAt.checkpoint(new StringBasedCheckpoint(checkpoint));
        });
    }
}
