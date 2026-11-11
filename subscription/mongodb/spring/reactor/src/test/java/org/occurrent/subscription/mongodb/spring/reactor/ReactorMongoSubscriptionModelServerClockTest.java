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

import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What {@link ReactorMongoSubscriptionModel#globalCheckpointAsOfNow()} answers when MongoDB's reply to {@code hello}
 * gives no position, and when MongoDB can't be reached. Needs no MongoDB, since the reply is all the model reads.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelServerClockTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void global_checkpoint_as_of_now_completes_empty_when_the_reply_has_no_clock() {
        ReactorMongoSubscriptionModel model = modelAnswering(Mono.just(new Document("ok", 1.0)));

        StepVerifier.create(model.globalCheckpointAsOfNow()).expectComplete().verify(TIMEOUT);
    }

    @Test
    void global_checkpoint_as_of_now_fails_when_mongodb_cannot_be_reached() {
        DataAccessResourceFailureException unreachable = new DataAccessResourceFailureException("MongoDB can't be reached");
        ReactorMongoSubscriptionModel model = modelAnswering(Mono.error(unreachable));

        StepVerifier.create(model.globalCheckpointAsOfNow())
                .expectErrorSatisfies(throwable -> assertThat(throwable).isSameAs(unreachable))
                .verify(TIMEOUT);
    }

    private static ReactorMongoSubscriptionModel modelAnswering(Mono<Document> reply) {
        ReactiveMongoOperations mongo = mock(ReactiveMongoOperations.class);
        when(mongo.executeCommand(any(Document.class))).thenReturn(reply);
        return new ReactorMongoSubscriptionModel(mongo, "events", TimeRepresentation.RFC_3339_STRING);
    }
}
