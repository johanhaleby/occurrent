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

package org.occurrent.springboot.broker.rabbitmq.blocking;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CatchupThenPushReadiness} is what {@link DefaultRabbitMqCloudEventBridgeFactory} pre-seeds every bridge's
 * {@code readinessSource} with, so a zero-config Spring application gets the quiet path during a replay without
 * naming {@code readinessSource} itself.
 */
class CatchupThenPushReadinessTest {

    @Test
    void answers_ready_when_no_registry_bean_is_published_even_though_an_unrelated_wrapper_bean_shares_the_subscription_id() throws Exception {
        // No occurrentCatchupThenPushSubscriptionModelsByLiveFeed bean at all, the shape a hand-built wrapper with
        // no framework registrar produces. An unrelated CatchupThenPushSubscriptionModel, wrapping a different
        // PushSubscriptionModel entirely, happens to share this subscription id and is still replaying.
        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(CloudEventBuilder.v1()
                .withId("historical")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Historical")
                .build()));
        CatchupThenPushSubscriptionModel unrelatedWrapper = new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), null);
        unrelatedWrapper.subscribe("proj", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            replayEntered.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        context.getBeanFactory().registerSingleton("unrelatedWrapper", unrelatedWrapper);

        try {
            Predicate<String> readinessSource = CatchupThenPushReadiness.memoized(context, new PushSubscriptionModel());

            assertThat(readinessSource.test("proj"))
                    .as("this bridge's own liveFeed is not unrelatedWrapper's, so its still-replaying answer must " +
                            "not pause a bridge that has nothing to do with it")
                    .isTrue();
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void a_poll_before_the_wrapper_is_published_is_re_resolved_on_a_later_poll_once_it_is() throws Exception {
        // A second, unrelated wrapper sharing the same subscription id, healthy, registered directly as a bean:
        // ADR 102 permits two independent CatchupThenPushSubscriptionModel instances to subscribe under the same
        // id, and this one stands in for a wrapper the id-only lookup this class refuses to do could otherwise
        // have confused for liveFeed's own, ready immediately since it has nothing to catch up on. If a "no
        // identity match yet" poll were ever memoized as this bean's own answer, ready would stay wrongly cached
        // even once the real wrapper's own entry lands in the registry.
        CatchupThenPushSubscriptionModel otherWrapper = new CatchupThenPushSubscriptionModel(new InMemoryEventStore(), new PushSubscriptionModel(), null);
        otherWrapper.subscribe("proj", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(5));
        assertThat(otherWrapper.isReadyForLiveDelivery("proj")).isTrue();

        // The registry bean itself: published lazily by the framework registrar, on its first registration, so a
        // bridge's own poll can run before it exists at all, and separately, before this specific liveFeed's own
        // entry lands in it once it does. Registered here as a plain mutable map, standing in for both windows:
        // liveFeed is absent from it at first, exactly as if this bridge's own poll ran before the framework
        // registrar's registration for it had completed.
        Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed = new ConcurrentHashMap<>();
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();
        context.getBeanFactory().registerSingleton("occurrentCatchupThenPushSubscriptionModelsByLiveFeed", wrappersByLiveFeed);
        context.getBeanFactory().registerSingleton("otherWrapper", otherWrapper);

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

        // The bridge's own predicate, built here before the registrar has published anything for liveFeed: this is
        // the exact "poll once before publishing" this test is about, one Predicate<String> instance carried across
        // both polls below, since memoized(...) is only called once per bridge, at build time.
        Predicate<String> readinessSource = CatchupThenPushReadiness.memoized(context, liveFeed);

        assertThat(readinessSource.test("proj"))
                .as("liveFeed has no entry in the registry yet, so this answers ready rather than guessing at "
                        + "otherWrapper's own answer by subscription id, and must not memoize that guess as a "
                        + "wrong wrapper forever")
                .isTrue();

        // The registrar's own registration, landing between these two polls, the same as a projection or saga bean
        // finishing initialization after this bridge's consumer has already started polling.
        wrappersByLiveFeed.put(liveFeed, wrapper);

        try {
            assertThat(readinessSource.test("proj"))
                    .as("the same Predicate instance, asked again: liveFeed's own wrapper is in the registry now, "
                            + "and its still-replaying answer is picked up rather than the first poll's answer, "
                            + "or otherWrapper's, having been cached forever")
                    .isFalse();
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
