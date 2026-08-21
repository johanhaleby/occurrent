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

package org.occurrent.springboot.broker.kafka.blocking;

import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.context.support.GenericApplicationContext;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CatchupThenPushReadiness} is what {@link DefaultKafkaCloudEventBridgeFactory} pre-seeds every bridge's
 * {@code readinessSource} with, so a zero-config Spring application gets the quiet path during a replay without
 * naming {@code readinessSource} itself.
 */
class CatchupThenPushReadinessTest {

    @Test
    void answers_true_for_a_subscription_id_no_wrapper_bean_claims() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();

        boolean ready = CatchupThenPushReadiness.isReady(context, "proj");

        assertThat(ready).isTrue();
    }

    @Test
    void defers_to_the_wrapper_bean_that_owns_the_subscription_id() throws Exception {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel wrapper = new CatchupThenPushSubscriptionModel(new InMemoryEventStore(), liveFeed, null);
        wrapper.subscribe("proj", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(5));

        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        context.getBeanFactory().registerSingleton("catchupThenPushSubscriptionModel-proj", wrapper);

        assertThat(CatchupThenPushReadiness.isReady(context, "proj"))
                .as("the wrapper's own replay reached live with nothing to catch up on")
                .isTrue();
        assertThat(CatchupThenPushReadiness.isReady(context, "some-other-id"))
                .as("this wrapper does not own this id, so it defers to the zero-config default")
                .isTrue();
    }

    @Test
    void answers_false_while_the_owning_wrapper_is_still_replaying() throws Exception {
        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(CloudEventBuilder.v1()
                .withId("historical")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Historical")
                .build()));
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel wrapper = new CatchupThenPushSubscriptionModel(store, liveFeed, null);
        wrapper.subscribe("proj", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            replayEntered.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        context.getBeanFactory().registerSingleton("catchupThenPushSubscriptionModel-proj", wrapper);

        try {
            assertThat(CatchupThenPushReadiness.isReady(context, "proj")).isFalse();
        } finally {
            releaseReplay.countDown();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
