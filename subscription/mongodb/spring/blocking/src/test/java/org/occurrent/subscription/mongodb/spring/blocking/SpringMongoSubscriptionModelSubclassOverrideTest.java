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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A catch-up calls the two-argument canResumeFrom, so a subclass that overrides only the one-argument method is still
 * called. The override never calls super, so the model is built over a mock template that is never asked anything, and
 * no MongoDB is needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelSubclassOverrideTest {

    private SpringMongoSubscriptionModel model;

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
    }

    @Test
    void a_two_argument_canResumeFrom_calls_a_subclass_that_overrides_only_the_one_argument_method_and_returns_its_answer() {
        // Given
        List<Checkpoint> asked = new CopyOnWriteArrayList<>();
        model = new SpringMongoSubscriptionModel(mock(MongoTemplate.class), "events", TimeRepresentation.RFC_3339_STRING) {
            @Override
            public boolean canResumeFrom(Checkpoint checkpoint) {
                asked.add(checkpoint);
                return false;
            }
        };
        Checkpoint checkpoint = new StringBasedCheckpoint("asked");

        // When
        boolean canResume = model.canResumeFrom(checkpoint, () -> true);

        // Then
        assertThat(canResume).as("answer").isFalse();
        assertThat(asked).as("checkpoints the override was asked about").containsExactly(checkpoint);
    }
}
