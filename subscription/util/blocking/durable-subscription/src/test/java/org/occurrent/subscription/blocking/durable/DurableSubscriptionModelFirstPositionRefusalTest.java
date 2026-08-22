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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A subscription registered with the model default and no stored checkpoint is either recorded from the wrapped
 * model's {@code globalCheckpoint()} before anything is delivered, or refused when that answer is {@code null}.
 * Issue #852 is what these tests close: without the refusal, a fresh subscription over a wrapped model that cannot
 * answer starts from wherever the feed is, and a crash after a failed first delivery starts over from wherever the
 * feed has reached by then, so the failed event is never seen again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRefusalTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_subscription_that_cannot_record_a_first_position_is_refused_rather_than_losing_its_first_event_to_a_restart() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        List<String> deliveredBeforeRestart = new ArrayList<>();

        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> {
            deliveredBeforeRestart.add(cloudEvent.getId());
            throw new IllegalStateException("first delivery fails");
        }));

        // Either arm keeps the same promise, no first event is silently lost. A model that refuses has nothing to
        // lose. One that starts anyway, which is what 0.33.0 did, must survive the sequence below, where the first
        // delivery fails and the process dies before any checkpoint is saved.
        if (refusal == null) {
            feed.publish(cloudEvent("event-1"));
            assertThat(deliveredBeforeRestart).containsExactly("event-1");
            durable.shutdown();

            DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage);
            List<String> deliveredAfterRestart = new ArrayList<>();
            restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));

            assertThat(deliveredAfterRestart).contains("event-1");
        } else {
            assertThat(refusal).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");
        }
    }

    @Test
    void subscribing_with_the_model_default_is_refused_when_nothing_is_stored_and_the_position_source_cannot_answer() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(InMemoryFeed.class.getName())
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing");

        assertThat(feed.subscriptions).isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_refused_subscription_id_is_left_free_so_subscribing_again_works_once_the_position_source_answers() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).isInstanceOf(IllegalStateException.class);

        feed.answersCurrentPosition = true;
        List<String> delivered = new ArrayList<>();
        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-1"));

        assertThat(delivered).containsExactly("event-1");
    }

    @Test
    void a_dynamic_start_position_resolving_to_the_model_default_is_refused_the_same_way() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");
    }

    @Test
    void the_first_position_is_recorded_before_anything_is_delivered_when_the_position_source_answers() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.read(SUBSCRIPTION_ID).asString()).isEqualTo("0");
    }

    @Test
    void a_restart_after_a_failed_first_delivery_resumes_from_the_recorded_position() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
            throw new IllegalStateException("first delivery fails");
        });
        feed.publish(cloudEvent("event-1"));
        durable.shutdown();

        DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage);
        List<String> deliveredAfterRestart = new ArrayList<>();
        restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));

        assertThat(deliveredAfterRestart).containsExactly("event-1");
    }

    @Test
    void a_stored_checkpoint_is_taken_without_asking_the_position_source() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("0"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        List<String> delivered = new ArrayList<>();

        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-1"));

        assertThat(delivered).containsExactly("event-1");
        assertThat(feed.globalCheckpointCalls).isZero();
    }

    @Test
    void a_start_position_of_your_own_is_never_refused_and_records_nothing() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(feed.subscriptions).containsKey(SUBSCRIPTION_ID);
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_dynamic_start_position_opting_out_hands_the_subscription_to_the_wrapped_model_unchanged() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        StartAt optOut = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? null : StartAt.now());

        durable.subscribe(SUBSCRIPTION_ID, null, optOut, __ -> {
        });

        assertThat(feed.subscriptions).containsKey(SUBSCRIPTION_ID);
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void the_override_starts_a_subscription_the_position_source_cannot_answer_for_and_records_nothing() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        List<String> delivered = new ArrayList<>();

        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));

        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        feed.publish(cloudEvent("event-1"));
        assertThat(delivered).containsExactly("event-1");
    }

    @Test
    void the_override_keeps_the_loss_window_it_accepts_so_a_restart_after_a_failed_first_delivery_starts_from_the_feed() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModelConfig config = new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, config);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
            throw new IllegalStateException("first delivery fails");
        });
        feed.publish(cloudEvent("event-1"));
        durable.shutdown();

        DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage, config);
        List<String> deliveredAfterRestart = new ArrayList<>();
        restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-2"));

        assertThat(deliveredAfterRestart).containsExactly("event-2");
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    /**
     * A feed with change-stream mechanics reduced to what these tests need. The model default and
     * {@link StartAt#now()} both mean the end of what has been published so far, a checkpoint means everything
     * after that position, and each subscription is delivered to synchronously. A delivery whose action throws is
     * not retried and the subscription's position advances past the event anyway, which is the state a crash right
     * after the failure leaves behind. The published events and nothing else survive {@link #shutdown()}, the way
     * a database outlives a process.
     */
    private static final class InMemoryFeed implements CheckpointAwareSubscriptionModel {
        final List<CloudEvent> published = new ArrayList<>();
        final Map<String, FeedSubscription> subscriptions = new LinkedHashMap<>();
        boolean answersCurrentPosition = false;
        int globalCheckpointCalls = 0;

        void publish(CloudEvent cloudEvent) {
            published.add(cloudEvent);
            List.copyOf(subscriptions.values()).forEach(this::deliverPending);
        }

        private void deliverPending(FeedSubscription subscription) {
            while (subscription.position < published.size()) {
                CloudEvent next = published.get(subscription.position);
                subscription.position++;
                try {
                    subscription.action.accept(new CheckpointAwareCloudEvent(next, new StringBasedCheckpoint(Integer.toString(subscription.position))));
                } catch (RuntimeException ignored) {
                }
            }
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.isDynamic() ? startAt.get(new SubscriptionModelContext(InMemoryFeed.class)) : startAt;
            int position = resolved instanceof StartAt.StartAtCheckpoint startAtCheckpoint
                    ? Integer.parseInt(startAtCheckpoint.checkpoint.asString())
                    : published.size();
            FeedSubscription subscription = new FeedSubscription(position, action);
            subscriptions.put(subscriptionId, subscription);
            deliverPending(subscription);
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointCalls++;
            return answersCurrentPosition ? new StringBasedCheckpoint(Integer.toString(published.size())) : null;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static SubscriptionHandle dummySubscription(String subscriptionId) {
            return new SubscriptionHandle() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        private static final class FeedSubscription {
            int position;
            final Consumer<CloudEvent> action;

            private FeedSubscription(int position, Consumer<CloudEvent> action) {
                this.position = position;
                this.action = action;
            }
        }
    }
}
