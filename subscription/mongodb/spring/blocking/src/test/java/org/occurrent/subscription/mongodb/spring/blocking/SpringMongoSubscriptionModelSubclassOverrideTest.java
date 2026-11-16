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

import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A catch-up calls the two-argument canResumeFrom, so a subclass that overrides only the one-argument method is still
 * called. The override never calls super, so the model is built over a mock template that is never asked anything, and
 * no MongoDB is needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelSubclassOverrideTest {

    private static final int RETRY_ATTEMPTS = 3;
    private static final Checkpoint CHECKPOINT = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("00")));

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

    @Test
    void a_one_argument_call_after_a_two_argument_call_has_returned_does_not_see_its_stillWanted() {
        // Given a model whose first one-argument call answers without asking MongoDB, and then asks an unreachable one
        AtomicInteger probes = new AtomicInteger();
        MongoSocketReadException failure = transientFailure();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        model = modelOverUnreachableMongoDb(probes, failure, checkpoint -> firstCall.getAndSet(false) ? Boolean.TRUE : null);
        AtomicInteger askedByTheEarlierCall = new AtomicInteger();
        assertThat(model.canResumeFrom(CHECKPOINT, () -> {
            askedByTheEarlierCall.incrementAndGet();
            return false;
        })).as("answer of the two-argument call").isTrue();

        // When a one-argument call follows on the same thread
        Throwable thrown = catchThrowable(() -> model.canResumeFrom(CHECKPOINT));

        // Then it retries as it does with no stillWanted, and never reads the one of the call that has returned
        assertThat(thrown).as("thrown by the one-argument call").isSameAs(failure);
        assertThat(askedByTheEarlierCall).as("times the stillWanted of the earlier call was read").hasValue(0);
        assertThat(probes).as("times MongoDB was asked").hasValue(RETRY_ATTEMPTS);
    }

    @Test
    void a_one_argument_call_after_a_two_argument_call_has_thrown_does_not_see_its_stillWanted() {
        // Given a model whose first one-argument call throws, and then asks an unreachable MongoDB
        AtomicInteger probes = new AtomicInteger();
        MongoSocketReadException failure = transientFailure();
        IllegalStateException overrideFailure = new IllegalStateException("the override failed");
        AtomicBoolean firstCall = new AtomicBoolean(true);
        model = modelOverUnreachableMongoDb(probes, failure, checkpoint -> {
            if (firstCall.getAndSet(false)) {
                throw overrideFailure;
            }
            return null;
        });
        AtomicInteger askedByTheEarlierCall = new AtomicInteger();
        assertThat(catchThrowable(() -> model.canResumeFrom(CHECKPOINT, () -> {
            askedByTheEarlierCall.incrementAndGet();
            return false;
        }))).as("thrown by the two-argument call").isSameAs(overrideFailure);

        // When a one-argument call follows on the same thread
        Throwable thrown = catchThrowable(() -> model.canResumeFrom(CHECKPOINT));

        // Then it retries as it does with no stillWanted, and never reads the one of the call that has thrown
        assertThat(thrown).as("thrown by the one-argument call").isSameAs(failure);
        assertThat(askedByTheEarlierCall).as("times the stillWanted of the earlier call was read").hasValue(0);
        assertThat(probes).as("times MongoDB was asked").hasValue(RETRY_ATTEMPTS);
    }

    @Test
    void a_call_to_super_after_a_nested_two_argument_call_sees_the_stillWanted_of_the_outer_call_again() {
        // Given an override that makes a nested two-argument call, with another stillWanted, before it calls super
        AtomicInteger probes = new AtomicInteger();
        MongoSocketReadException failure = transientFailure();
        AtomicBoolean nested = new AtomicBoolean();
        AtomicInteger askedByTheOuterCall = new AtomicInteger();
        AtomicInteger askedByTheInnerCall = new AtomicInteger();
        model = modelOverUnreachableMongoDb(probes, failure, checkpoint -> {
            if (nested.getAndSet(true)) {
                return Boolean.TRUE;
            }
            model.canResumeFrom(checkpoint, () -> {
                askedByTheInnerCall.incrementAndGet();
                return true;
            });
            return null;
        });

        // When
        Throwable thrown = catchThrowable(() -> model.canResumeFrom(CHECKPOINT, () -> {
            askedByTheOuterCall.incrementAndGet();
            return true;
        }));

        // Then the call to super retries on the stillWanted of the outer call
        assertThat(thrown).as("thrown by the outer call").isSameAs(failure);
        assertThat(askedByTheOuterCall).as("times the stillWanted of the outer call was read").hasValueGreaterThan(0);
        assertThat(askedByTheInnerCall).as("times the stillWanted of the nested call was read").hasValue(0);
        assertThat(probes).as("times MongoDB was asked").hasValue(RETRY_ATTEMPTS);
    }

    // The override answers with what the function returns, or calls super when it returns null
    private SpringMongoSubscriptionModel modelOverUnreachableMongoDb(AtomicInteger probes, RuntimeException failure, Function<Checkpoint, @Nullable Boolean> override) {
        MongoTemplate unreachable = mock(MongoTemplate.class);
        when(unreachable.executeCommand(any(Document.class))).thenAnswer(invocation -> {
            probes.incrementAndGet();
            throw failure;
        });
        return new SpringMongoSubscriptionModel(unreachable, "events", TimeRepresentation.RFC_3339_STRING, RetryStrategy.fixed(Duration.ofMillis(1)).maxAttempts(RETRY_ATTEMPTS)) {
            @Override
            public boolean canResumeFrom(Checkpoint checkpoint) {
                Boolean answer = override.apply(checkpoint);
                return answer != null ? answer : super.canResumeFrom(checkpoint);
            }
        };
    }

    private static MongoSocketReadException transientFailure() {
        return new MongoSocketReadException("Prematurely reached end of stream", new ServerAddress(), new IOException("connection reset"));
    }
}
