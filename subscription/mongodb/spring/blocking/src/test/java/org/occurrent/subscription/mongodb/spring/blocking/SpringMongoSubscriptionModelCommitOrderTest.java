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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.CommitOrderedSubscriptions;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * What {@link SpringMongoSubscriptionModel} answers to {@link CommitOrderedSubscriptions#deliversEveryEventInCommitOrder()}.
 * The answer is fixed when the model is created, so these tests need no MongoDB: nothing here subscribes.
 */
@DisplayName("SpringMongoSubscriptionModelCommitOrder")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelCommitOrderTest {

    private final List<SpringMongoSubscriptionModel> created = new ArrayList<>();

    @AfterEach
    void shutdownModels() {
        created.forEach(SpringMongoSubscriptionModel::shutdown);
    }

    @Test
    void delivers_every_event_in_commit_order_with_the_default_config() {
        // Given
        SpringMongoSubscriptionModel model = modelWith(withConfig("events", TimeRepresentation.RFC_3339_STRING));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isTrue();
    }

    @Test
    void delivers_every_event_in_commit_order_when_restarting_after_lost_history_is_turned_off() {
        // Given
        SpringMongoSubscriptionModel model = modelWith(withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(false));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isTrue();
    }

    @Test
    void does_not_deliver_every_event_in_commit_order_when_restarting_after_lost_history_is_turned_on() {
        // Given
        SpringMongoSubscriptionModel model = modelWith(withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isFalse();
    }

    @Test
    void is_found_as_a_commit_ordered_model_by_the_lookup() {
        // Given
        SpringMongoSubscriptionModel model = modelWith(withConfig("events", TimeRepresentation.RFC_3339_STRING));

        // When
        var found = CommitOrderedSubscriptions.findIn(model);

        // Then
        assertThat(found).containsSame(model);
    }

    private SpringMongoSubscriptionModel modelWith(SpringMongoSubscriptionModelConfig config) {
        SpringMongoSubscriptionModel model = new SpringMongoSubscriptionModel(mock(MongoTemplate.class), config);
        created.add(model);
        return model;
    }
}
