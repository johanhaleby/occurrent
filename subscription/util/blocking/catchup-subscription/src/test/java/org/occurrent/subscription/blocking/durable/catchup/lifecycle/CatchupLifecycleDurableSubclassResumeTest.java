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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A catch-up subscription model over a subclass of {@link DurableSubscriptionModel} that overrides
 * {@code resumeSubscription(String)} and not {@code resumeAsPrepared(..)} resumes through that override, as it did in
 * 0.34.0, since a resume as prepared would go around it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupLifecycleDurableSubclassResumeTest {

    private static final String X = "x";

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final ControllableLiveModel live = new ControllableLiveModel();
    private final List<String> resumedThroughTheOverride = new CopyOnWriteArrayList<>();
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(new DurableSubscriptionModel(live, storage) {
        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            resumedThroughTheOverride.add(subscriptionId);
            return super.resumeSubscription(subscriptionId);
        }
    }, new GatedHistory(cloudEvent("1")), new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

    @AfterEach
    void shut_down() {
        model.shutdown();
    }

    @Test
    void a_resume_of_a_paused_subscription_calls_the_resumeSubscription_override_of_the_durable_subclass() {
        // Given a paused subscription
        givenPaused();

        // When it is resumed
        Subscription resumed = model.resumeSubscription(X);

        // Then it runs, and the override was called for it
        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(10))).as("whether x started").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs").isTrue();
        assertThat(resumedThroughTheOverride).as("the subscriptions resumed through the override").contains(X);
    }

    private void givenPaused() {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
        resumedThroughTheOverride.clear();
    }
}
