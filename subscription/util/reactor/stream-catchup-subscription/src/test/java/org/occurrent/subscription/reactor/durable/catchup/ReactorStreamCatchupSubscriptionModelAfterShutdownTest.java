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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A pause or a resume on a reactor stream catch-up that was shut down. The wrapped model is a double that answers after
 * its shutdown as {@code ReactorMongoSubscriptionModel} does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelAfterShutdownTest {

    private static final SubscriptionFilter FILTER = StreamSubscriptionFilter.filter(Filter.all());

    private final ShutDownLikeMongo wrapped = new ShutDownLikeMongo();
    private final ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEvents());

    // "live" went straight to live, "replayed" replayed the history and handed over
    @BeforeEach
    void subscribeAndShutDown() {
        catchup.subscribe("live", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.subscribe("replayed", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5));
        catchup.shutdown();
    }

    @AfterEach
    void wrappedModelIsNotAskedAfterTheShutdown() {
        assertThat(wrapped.callsAfterShutdown).as("calls the wrapped model got after its shutdown").isEmpty();
    }

    @Test
    void a_pause_after_shutdown_throws_SubscriptionModelShutdownException() {
        assertAll(
                () -> assertThat(catchThrowable(() -> catchup.pauseSubscription("live"))).as("what a pause of the subscription that went straight to live throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class).hasMessage("Cannot pause subscription live because the subscription model is shutdown."),
                () -> assertThat(catchThrowable(() -> catchup.pauseSubscription("replayed"))).as("what a pause of the subscription that replayed throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(catchThrowable(() -> catchup.pauseSubscription("unknown"))).as("what a pause of an id never subscribed throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class)
        );
    }

    @Test
    void a_resume_after_shutdown_throws_SubscriptionModelShutdownException() {
        assertAll(
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription("live"))).as("what a resume of the subscription that went straight to live throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class).hasMessage("Cannot resume subscription live because the subscription model is shutdown."),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription("replayed"))).as("what a resume of the subscription that replayed throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription("unknown"))).as("what a resume of an id never subscribed throws")
                        .isInstanceOf(SubscriptionModelShutdownException.class)
        );
    }

    // Events e1 to e3 at positions 1 to 3
    private static final class ThreeEvents implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(3L), 3L);
            return Flux.fromStream(LongStream.rangeClosed(from, to).boxed()
                    .map(position -> CloudEventBuilder.v1().withId("e" + position).withSource(URI.create("urn:test")).withType("type").build()));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(3L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // After shutdown() it throws IllegalStateException from a pause or a resume, as ReactorMongoSubscriptionModel does,
    // and records every call it gets
    private static final class ShutDownLikeMongo implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        final List<String> callsAfterShutdown = new CopyOnWriteArrayList<>();
        private volatile boolean shutdown;

        private void called(String call) {
            if (shutdown) {
                callsAfterShutdown.add(call);
            }
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(TOKEN);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(TOKEN);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            called("subscribe:" + subscriptionId);
            if (shutdown) {
                throw new SubscriptionModelShutdownException();
            }
            if (!subscribed.add(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            return handle(subscriptionId);
        }

        @Override
        public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
            called("cancel:" + subscriptionId);
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public synchronized void stop() {
            called("stop");
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            called("start");
        }

        @Override
        public synchronized void shutdown() {
            called("shutdown");
            shutdown = true;
            subscribed.clear();
            paused.clear();
        }

        @Override
        public boolean isRunning() {
            return !shutdown;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            called("resume:" + subscriptionId);
            if (shutdown) {
                throw new IllegalStateException("ShutDownLikeMongo is shutdown");
            }
            if (!paused.remove(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            return handle(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            called("pause:" + subscriptionId);
            if (shutdown) {
                throw new IllegalStateException("ShutDownLikeMongo is shutdown");
            }
            if (!subscribed.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.add(subscriptionId);
        }

        private static Subscription handle(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }
    }
}
