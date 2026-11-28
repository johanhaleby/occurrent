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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code resumeSubscription(..)} waits for the claim of its subscription while another call holds it. A
 * {@code shutdown()} that begins during that wait shuts the live delegate down without the model lock, since the wait
 * counts as a wait for one subscription. The resume that gets the claim after that changes nothing, so it doesn't
 * resume the subscription in a live delegate that is shut down.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class StreamCatchupResumeWaitingForAClaimThatAShutdownOvertookTest {

    private static final String X = "x";
    private static final long BOUND_SECONDS = 25;

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private StreamCatchupSubscriptionModel model;
    private AbstractCatchupSubscriptionModel.Claim claimOfX;

    @BeforeEach
    void create_a_model_with_a_paused_x() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(), new CatchupSubscriptionModelConfig(100));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
    }

    @AfterEach
    void release_the_claim_and_shut_down() throws Exception {
        if (claimOfX != null) {
            claimOfX.close();
        }
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_that_gets_the_claim_of_its_subscription_after_a_shutdown_began_while_it_waited_for_it_changes_nothing() throws Exception {
        // Given another call holds the claim of x, and a resume of x waits for it
        claimOfX = model.claim(X);
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(Thread.getAllStackTraces().entrySet())
                .as("the threads waiting for the claim of x")
                .anyMatch(entry -> entry.getKey().getName().equals("resume-of-x") && entry.getKey().getState() == Thread.State.TIMED_WAITING));
        assertThat(resume.isDone()).as("whether the resume of x returned before it had the claim").isFalse();

        // When a shutdown begins and shuts the live delegate down, and then the claim is released
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-under-test"));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(wrapped.shutdowns.get()).as("the shutdowns the live delegate got while the resume waited for the claim").isEqualTo(1));
        claimOfX.close();
        claimOfX = null;

        // Then the resume returns without resuming x in the live delegate, and the shutdown completes
        assertThat(resume).as("the resume of x once it had the claim").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(shutdown).as("the shutdown that began while the resume waited for the claim").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        assertThat(model.isRunning(X)).as("whether x runs after the resume that got its claim once a shutdown had begun").isFalse();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }
}
