package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class CatchupThenPushSubscriptionModelMarkerDeleteFailureTest {

    @Test
    void a_cancel_whose_marker_delete_fails_keeps_the_subscription_and_a_cancel_tried_again_makes_the_next_subscribe_replay_the_history() {
        // Given
        InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        AtomicInteger deleteFailuresLeft = new AtomicInteger(1);
        CheckpointStorage marker = new CheckpointStorage() {
            @Override
            public Checkpoint read(String subscriptionId) {
                return backing.read(subscriptionId);
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
                return backing.save(subscriptionId, checkpoint, writeCondition);
            }

            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return backing.writeVersion(subscriptionId);
            }

            @Override
            public boolean exists(String subscriptionId) {
                return backing.exists(subscriptionId);
            }

            @Override
            public void delete(String subscriptionId) {
                if (deleteFailuresLeft.getAndDecrement() > 0) {
                    throw new IllegalStateException("The marker store is unavailable");
                }
                backing.delete(subscriptionId);
            }
        };
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("stream", List.of(event("1"), event("2")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);
        List<String> first = new CopyOnWriteArrayList<>();
        model.subscribe("projection", null, StartAt.subscriptionModelDefault(), e -> first.add(e.getId())).waitUntilStarted();
        assertThat(backing.exists("projection")).as("the marker once the first subscription has caught up").isTrue();

        // When
        Throwable cancelFailure = catchThrowable(() -> model.cancelSubscription("projection"));

        // Then
        assertThat(cancelFailure).as("the cancel whose marker delete failed").hasMessage("The marker store is unavailable");
        assertThat(model.isRunning("projection")).as("the subscription a cancel whose marker delete failed was asked to cancel runs").isTrue();
        store.write("stream", List.of(event("3")));
        assertThat(first).as("events delivered to the subscription whose cancel failed").containsExactly("1", "2", "3");

        model.cancelSubscription("projection");
        List<String> afterTheCancel = new CopyOnWriteArrayList<>();
        model.subscribe("projection", null, StartAt.subscriptionModelDefault(), e -> afterTheCancel.add(e.getId())).waitUntilStarted();
        store.write("stream", List.of(event("4")));
        assertThat(afterTheCancel).as("events delivered to the subscription made after the cancel that was tried again").containsExactly("1", "2", "3", "4");
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }
}
