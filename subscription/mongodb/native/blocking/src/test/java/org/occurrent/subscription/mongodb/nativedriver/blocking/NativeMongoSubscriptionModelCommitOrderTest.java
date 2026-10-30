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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.CommitOrderedSubscriptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig.withConfig;

/**
 * What {@link NativeMongoSubscriptionModel} answers to {@link CommitOrderedSubscriptions#deliversEveryEventInCommitOrder()}.
 * The answer is fixed when the model is created, so these tests need no MongoDB: nothing here subscribes.
 */
@DisplayName("NativeMongoSubscriptionModelCommitOrder")
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelCommitOrderTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void delivers_every_event_in_commit_order_with_the_default_config() {
        // Given
        NativeMongoSubscriptionModel model = modelWith(withConfig());

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isTrue();
    }

    @Test
    void delivers_every_event_in_commit_order_when_restarting_after_lost_history_is_turned_off() {
        // Given
        NativeMongoSubscriptionModel model = modelWith(withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isTrue();
    }

    @Test
    void does_not_deliver_every_event_in_commit_order_when_restarting_after_lost_history_is_turned_on() {
        // Given
        NativeMongoSubscriptionModel model = modelWith(withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isFalse();
    }

    @Test
    void is_found_as_a_commit_ordered_model_by_the_lookup() {
        // Given
        NativeMongoSubscriptionModel model = modelWith(withConfig());

        // When
        var found = CommitOrderedSubscriptions.findIn(model);

        // Then
        assertThat(found).containsSame(model);
    }

    @SuppressWarnings("unchecked")
    private NativeMongoSubscriptionModel modelWith(NativeMongoSubscriptionModelConfig config) {
        return new NativeMongoSubscriptionModel(mock(MongoDatabase.class), mock(MongoCollection.class), TimeRepresentation.RFC_3339_STRING, executor, config);
    }
}
