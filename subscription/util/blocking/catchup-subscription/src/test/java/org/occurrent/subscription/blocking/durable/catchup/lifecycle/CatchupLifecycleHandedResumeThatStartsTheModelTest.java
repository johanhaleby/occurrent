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
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A resume an action handed over, which starts a stopped model before it resumes the subscription, is not withdrawn
 * once its turn has begun. The lifecycle thread gives up on a handed call that hasn't begun within ten seconds, so a
 * start that takes longer than that must not let it withdraw the call after the model was started, which would leave
 * the model started and the subscription paused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupLifecycleHandedResumeThatStartsTheModelTest {

    private static final String A = "a";
    private static final String X = "x";
    // More than the ten seconds the lifecycle thread waits for a handed call to begin
    private static final long A_START_THAT_TAKES_LONGER_THAN_THE_WAIT_MICROS = 12_500_000;

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final ControllableLiveModel live = new ControllableLiveModel();
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(new DurableSubscriptionModel(live, storage), new GatedHistory(cloudEvent("1")),
            new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

    @AfterEach
    void shut_down() throws Exception {
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_handed_over_by_an_action_that_starts_the_stopped_model_with_a_start_that_outlasts_the_wait_for_handed_calls_still_resumes_the_subscription() throws Exception {
        // Given a paused subscription, and an action that stops the model and resumes the subscription once it has returned
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        model.subscribe(A, null, StartAt.now(), event -> {
            model.stop();
            model.resumeSubscription(X);
        });
        live.startDelayMicros = A_START_THAT_TAKES_LONGER_THAN_THE_WAIT_MICROS;

        // When the action runs, so the resume starts the model with a start that takes longer than the wait for handed calls
        CompletableFuture.runAsync(() -> live.publish(new CheckpointAwareCloudEvent(cloudEvent("go"), new StringBasedCheckpoint("checkpoint-go"))), daemonExecutor("live-delivery"));

        // Then the resume goes on once that start has returned, and the subscription runs
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (!model.isRunning(X) && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(model.isRunning(X)).as("whether x runs once the resume the action handed over has started the model and resumed it").isTrue();
        assertThat(live.isRunning()).as("whether the live delegate runs").isTrue();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }
}
