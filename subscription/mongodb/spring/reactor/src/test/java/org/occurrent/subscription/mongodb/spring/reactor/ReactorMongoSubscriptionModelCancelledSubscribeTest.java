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

package org.occurrent.subscription.mongodb.spring.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

/**
 * A subscribe with whether the call was cancelled, see {@link org.occurrent.subscription.api.reactor.internal.CancellableRegistration},
 * on a model that is shut down. The model is over a mock, as a shut-down model never reaches MongoDB.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelCancelledSubscribeTest {

    @Test
    void throws_when_the_call_was_cancelled_and_the_model_is_shut_down() {
        // Given
        ReactorMongoSubscriptionModel model = new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING);
        model.shutdown();

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe("someSubscription", null, StartAt.now(), __ -> Mono.empty(), () -> true));

        // Then
        assertThat(thrown).as("what the call threw").isInstanceOf(SubscriptionModelShutdownException.class);
    }

    @Test
    void stands_in_for_the_named_subscribe_for_the_shipped_model() {
        assertThat(new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING).standsInForSubscribe())
                .as("whether the shipped model stands in for its named subscribe").isTrue();
    }

    @Test
    void does_not_stand_in_for_the_named_subscribe_for_a_subclass_that_overrides_nothing() {
        assertThat(new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING) {
        }.standsInForSubscribe()).as("whether a subclass overriding nothing stands in for the named subscribe").isFalse();
    }
}
