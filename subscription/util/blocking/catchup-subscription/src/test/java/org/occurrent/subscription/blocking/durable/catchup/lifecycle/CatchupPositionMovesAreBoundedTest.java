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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * The record a catch-up model keeps of where each subscription's position last moved is kept only for a subscription the
 * model still has, so it stays as small as the subscriptions are, however many come and go.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupPositionMovesAreBoundedTest {

    private static final int SUBSCRIPTIONS = 50;
    private static final Duration BOUND = Duration.ofSeconds(20);

    private enum Kind {
        THE_DISPATCHER, STREAM_CATCHUP_MODEL
    }

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final Map<String, List<String>> delivered = new ConcurrentHashMap<>();
    private SubscriptionModel model;

    @AfterEach
    void shut_down() {
        if (model != null) {
            model.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void fifty_subscriptions_that_were_written_to_and_cancelled_leave_no_position_move_behind(Kind kind) {
        // Given fifty subscriptions that replayed, so their positions were written
        model = modelOf(kind, true);
        Map<String, Long> moves = positionMovesOf(model);
        subscribeAll();
        awaitAllDelivered();
        assertThat(moves).as("the position moves while the subscriptions still exist").isNotEmpty();

        // When they are all cancelled
        subscriptionIds().forEach(model::cancelSubscription);

        // Then the model keeps no position move for any of them
        await().atMost(BOUND).untilAsserted(() -> assertThat(moves).as("the position moves once every subscription was cancelled").isEmpty());
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void fifty_subscriptions_that_were_written_to_leave_no_position_move_behind_once_the_model_is_shut_down(Kind kind) {
        // Given fifty subscriptions that replayed, so their positions were written
        model = modelOf(kind, true);
        Map<String, Long> moves = positionMovesOf(model);
        subscribeAll();
        awaitAllDelivered();
        assertThat(moves).as("the position moves while the subscriptions still exist").isNotEmpty();

        // When the model is shut down
        model.shutdown();

        // Then the model keeps no position move for any of them
        await().atMost(BOUND).untilAsserted(() -> assertThat(moves).as("the position moves once the model was shut down").isEmpty());
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void a_cancel_of_a_subscription_the_model_never_knew_leaves_no_position_move_in_a_model_without_checkpoint_storage(Kind kind) {
        // Given a model that has no checkpoint storage of its own
        model = modelOf(kind, false);
        Map<String, Long> moves = positionMovesOf(model);

        // When a subscription it never knew is cancelled
        Throwable thrown = catchThrowable(() -> model.cancelSubscription("never-known"));

        // Then it keeps no position move for it, whether or not the cancel itself is refused
        assertThat(moves).as("the position moves after the cancel of an unknown subscription, which threw " + thrown).isEmpty();
    }

    @Test
    void fifty_unknown_subscriptions_cancelled_in_a_row_leave_no_position_move_behind_in_a_model_with_checkpoint_storage() {
        // Given a model with checkpoint storage and no subscriptions
        model = modelOf(Kind.THE_DISPATCHER, true);
        Map<String, Long> moves = positionMovesOf(model);

        // When subscriptions it never knew are cancelled
        subscriptionIds().forEach(id -> catchThrowable(() -> model.cancelSubscription(id)));

        // Then it keeps no position move for any of them
        assertThat(moves).as("the position moves after the cancels of unknown subscriptions").isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void a_cancelled_subscription_has_no_position_move_left_once_the_model_deletes_its_stored_position(Kind kind) {
        // Given a subscription that replayed, so its position was written, and a storage that notes whether the model still kept a position move for it at each delete
        DeleteWatchingStorage watching = new DeleteWatchingStorage();
        model = modelOf(kind, true, watching);
        Map<String, Long> moves = positionMovesOf(model);
        watching.moves = moves;
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> delivered.computeIfAbsent("subscription", key -> new CopyOnWriteArrayList<>()).add(event.getId()));
        await().atMost(BOUND).untilAsserted(() -> assertThat(delivered).as("the subscriptions that delivered the replayed event").containsKey("subscription"));
        assertThat(moves).as("the position moves while the subscription exists").containsKey("subscription");

        // When it is cancelled
        model.cancelSubscription("subscription");

        // Then the model had forgotten its position move by the time it deleted the stored position, the last delete of the cancel
        assertThat(watching.movesKeptAtDelete).as("whether the model kept a position move for the subscription at each delete of its stored position, in order").isNotEmpty();
        assertThat(watching.movesKeptAtDelete.getLast()).as("whether the model kept a position move for the cancelled subscription when it deleted the stored position").isFalse();
        assertThat(moves).as("the position moves once the subscription was cancelled").isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void a_subscribe_that_fails_after_the_model_noted_a_position_move_leaves_no_position_move_behind(Kind kind) {
        // Given a live model whose subscribe makes the model delete the stored position, which notes a position move while the subscription has a record, and then fails
        model = modelOf(kind, true);
        Map<String, Long> moves = positionMovesOf(model);
        StreamCatchupSubscriptionModel catchup = streamCatchupOf(model);
        AtomicBoolean notedWhileTheRecordExisted = new AtomicBoolean();
        live.beforeSubscribe = id -> {
            catchup.deletePositionFromStorage(id);
            notedWhileTheRecordExisted.set(moves.containsKey(id));
            throw new IllegalStateException("the live subscribe failed on purpose");
        };

        // When the subscribe fails
        Throwable thrown = catchThrowable(() -> model.subscribe("subscription", null, StartAt.now(), event -> {
        }));

        // Then the model keeps no position move for the subscription, which has no record to put back
        assertThat(thrown).as("what the subscribe threw").hasStackTraceContaining("the live subscribe failed on purpose");
        assertThat(notedWhileTheRecordExisted).as("whether a position move was kept while the subscription had a record").isTrue();
        assertThat(moves).as("the position moves once the subscribe failed").isEmpty();
    }

    private SubscriptionModel modelOf(Kind kind, boolean withCheckpointStorage) {
        return modelOf(kind, withCheckpointStorage, new InMemoryCheckpointStorage());
    }

    private SubscriptionModel modelOf(Kind kind, boolean withCheckpointStorage, InMemoryCheckpointStorage storage) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage);
        CatchupSubscriptionModelConfig config = withCheckpointStorage ? new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)) : new CatchupSubscriptionModelConfig(100);
        return kind == Kind.THE_DISPATCHER ? new CatchupSubscriptionModel(durable, history, config) : new StreamCatchupSubscriptionModel(durable, history, config);
    }

    private List<String> subscriptionIds() {
        return IntStream.range(0, SUBSCRIPTIONS).mapToObj(i -> "subscription-" + i).toList();
    }

    private void subscribeAll() {
        subscriptionIds().forEach(id -> model.subscribe(id, StartAtTime.beginningOfTime(), event -> delivered.computeIfAbsent(id, key -> new CopyOnWriteArrayList<>()).add(event.getId())));
    }

    private void awaitAllDelivered() {
        await().atMost(BOUND).untilAsserted(() -> assertThat(delivered).as("the subscriptions that delivered the replayed event").hasSize(SUBSCRIPTIONS));
    }

    // Notes, at each delete, whether the model still kept a position move for the subscription
    private static final class DeleteWatchingStorage extends InMemoryCheckpointStorage {
        volatile Map<String, Long> moves = Map.of();
        final List<Boolean> movesKeptAtDelete = new CopyOnWriteArrayList<>();

        @Override
        public void delete(String subscriptionId) {
            movesKeptAtDelete.add(moves.containsKey(subscriptionId));
            super.delete(subscriptionId);
        }
    }

    private static StreamCatchupSubscriptionModel streamCatchupOf(SubscriptionModel model) {
        try {
            if (model instanceof CatchupSubscriptionModel) {
                Field child = CatchupSubscriptionModel.class.getDeclaredField("streamCatchupSubscriptionModel");
                child.setAccessible(true);
                return (StreamCatchupSubscriptionModel) child.get(model);
            }
            return (StreamCatchupSubscriptionModel) model;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Could not read the stream catch-up model of " + model.getClass().getSimpleName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Long> positionMovesOf(SubscriptionModel model) {
        try {
            Object catchup = streamCatchupOf(model);
            Field field = Class.forName("org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel").getDeclaredField("positionMoves");
            field.setAccessible(true);
            return (Map<String, Long>) field.get(catchup);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Could not read the position moves of " + model.getClass().getSimpleName(), e);
        }
    }
}
