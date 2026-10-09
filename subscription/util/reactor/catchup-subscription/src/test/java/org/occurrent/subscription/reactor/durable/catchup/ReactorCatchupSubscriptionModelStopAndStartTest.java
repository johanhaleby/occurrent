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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelStopAndStartTest {

    @Test
    void resuming_a_stream_subscription_while_the_model_is_stopped_starts_the_dcb_catch_up_too() {
        WrappedModel wrapped = new WrappedModel();
        DcbStoreThatNeverAnswers dcbStore = new DcbStoreThatNeverAnswers();
        ReactorCatchupSubscriptionModel catchup = new ReactorCatchupSubscriptionModel(wrapped, new OneEventReader(), dcbStore, DcbCriteria.all(), Filter.all(), 100, 100);
        List<String> delivered = new CopyOnWriteArrayList<>();
        catchup.stop();
        catchup.subscribe("s", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> delivered.add("s:" + cloudEvent.getId())));

        catchup.resumeSubscription("s");
        catchup.subscribe("d", DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());

        assertThat(dcbStore.reads.get()).as("DCB reads by the replay of d, subscribed after resumeSubscription(s)").isEqualTo(1);
        assertThat(catchup.isPaused("d")).isFalse();
        assertThat(catchup.isRunning("d")).isTrue();
        assertThat(delivered).containsExactly("s:e1");
        assertThat(wrapped.startCalls).containsExactly(false, false, false);
    }

    private static final class OneEventReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(1L), 1L);
            return from > to ? Flux.empty() : Flux.just(CloudEventBuilder.v1().withId("e1").withSource(URI.create("urn:test")).withType("type").build());
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(1L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Counts the reads and answers none, so a replay that started stays in its first read
    private static final class DcbStoreThatNeverAnswers implements DcbEventStore {
        final AtomicInteger reads = new AtomicInteger();

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            reads.incrementAndGet();
            return Mono.never();
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("nothing is appended"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("nothing is appended"));
        }
    }

    // stop() pauses everything it holds and start(true) resumes it, as the life-cycle contract says
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final List<Boolean> startCalls = new CopyOnWriteArrayList<>();
        volatile boolean running = true;

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribed.add(subscriptionId);
            if (!running) {
                paused.add(subscriptionId);
            }
            return handle(subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public void stop() {
            running = false;
            paused.addAll(subscribed);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            startCalls.add(resumeSubscriptionsAutomatically);
            running = true;
            if (resumeSubscriptionsAutomatically) {
                paused.clear();
            }
        }

        @Override
        public boolean isRunning() {
            return running;
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
        public Subscription resumeSubscription(String subscriptionId) {
            if (!subscribed.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.remove(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
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
