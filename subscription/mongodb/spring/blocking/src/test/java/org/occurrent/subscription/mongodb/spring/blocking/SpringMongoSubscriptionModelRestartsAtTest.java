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

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;

/**
 * A subscription this model subscribes at a checkpoint, as a durable subscription model does with the checkpoint it
 * read back after a restart, starts reading there only for a resume token or an operation time. For anything else the
 * change stream opens at the present, so a model that stores positions on top of this model must not store them.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelRestartsAtTest {

    private final SpringMongoSubscriptionModel model = newModel();

    @AfterEach
    void shutdown() {
        model.shutdown();
    }

    @Test
    void restartsAt_is_true_for_a_resume_token_checkpoint() {
        Checkpoint checkpoint = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("8263A1B2C3000000012B022C0100296E5A10")));

        assertThat(model.restartsAt(checkpoint)).isTrue();
    }

    @Test
    void restartsAt_is_true_for_a_resume_token_checkpoint_read_back_as_a_string() {
        Checkpoint checkpoint = new StringBasedCheckpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("8263A1B2C3000000012B022C0100296E5A10"))).asString());

        assertThat(model.restartsAt(checkpoint)).isTrue();
    }

    @Test
    void restartsAt_is_true_for_an_operation_time_checkpoint() {
        Checkpoint checkpoint = new MongoOperationTimeCheckpoint(new BsonTimestamp(1_700_000_000, 1));

        assertThat(model.restartsAt(checkpoint)).isTrue();
    }

    @Test
    void restartsAt_is_true_for_an_operation_time_checkpoint_read_back_as_a_string() {
        Checkpoint checkpoint = new StringBasedCheckpoint(new MongoOperationTimeCheckpoint(new BsonTimestamp(1_700_000_000, 1)).asString());

        assertThat(model.restartsAt(checkpoint)).isTrue();
    }

    @Test
    void restartsAt_is_false_for_a_GlobalCheckpoint() {
        assertThat(model.restartsAt(GlobalCheckpoint.of(42))).isFalse();
    }

    @Test
    void restartsAt_is_false_for_a_CatchupTimeCheckpoint() {
        Checkpoint checkpoint = CatchupTimeCheckpoint.of("2026-01-01T00:00:05Z", new StringBasedCheckpoint("live"), "2026-01-01T00:00:00Z");

        assertThat(model.restartsAt(checkpoint)).isFalse();
    }

    @Test
    void restartsAt_is_false_for_a_plain_RFC_3339_time() {
        assertThat(model.restartsAt(new StringBasedCheckpoint("2026-01-01T00:00:00Z"))).isFalse();
    }

    @Test
    void restartsAt_is_false_for_a_checkpoint_the_change_stream_cannot_parse() {
        assertThat(model.restartsAt(new StringBasedCheckpoint("position:3"))).isFalse();
    }

    // restartsAt answers from the checkpoint alone, so the model needs no running MongoDB
    private static SpringMongoSubscriptionModel newModel() {
        return new SpringMongoSubscriptionModel(mock(MongoTemplate.class), "events", RFC_3339_STRING);
    }
}
