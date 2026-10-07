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

import org.bson.BsonTimestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel.Run;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ReactorMongoSubscriptionModel Run")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelRunTest {

    private final StartAt original = StartAt.checkpoint(new MongoOperationTimeCheckpoint(new BsonTimestamp(1, 1)));
    private final StartAt next = StartAt.checkpoint(new MongoOperationTimeCheckpoint(new BsonTimestamp(2, 1)));
    private final AtomicReference<StartAt> position = new AtomicReference<>(original);
    private final Run run = new Run();

    @Test
    void move_moves_the_position_of_a_run_that_is_open() {
        // When
        run.move(position, next);

        // Then
        assertThat(position.get()).isSameAs(next);
    }

    @Test
    void move_does_not_move_the_position_after_the_run_is_closed_and_no_step_is_under_way() {
        // Given
        run.close();

        // When
        run.move(position, next);

        // Then
        assertThat(position.get()).isSameAs(original);
    }

    @Test
    void move_moves_the_position_after_the_run_is_closed_while_a_step_that_started_before_the_close_is_under_way() {
        // Given
        Sinks.Empty<Void> work = Sinks.empty();
        run.step(work::asMono).subscribe();
        run.close();

        // When
        run.move(position, next);

        // Then
        assertThat(position.get()).isSameAs(next);
    }

    @Test
    void move_does_not_move_the_position_once_the_step_that_was_under_way_when_the_run_was_closed_has_ended() {
        // Given
        Sinks.Empty<Void> work = Sinks.empty();
        run.step(work::asMono).subscribe();
        run.close();
        work.tryEmitEmpty();

        // When
        run.move(position, next);

        // Then
        assertThat(position.get()).isSameAs(original);
    }

    @Test
    void compareAndMove_answers_true_without_moving_the_position_after_the_run_is_closed_and_no_step_is_under_way() {
        // Given
        run.close();

        // When
        boolean answer = run.compareAndMove(position, original, next);

        // Then
        assertThat(answer).isTrue();
        assertThat(position.get()).isSameAs(original);
    }

    @Test
    void step_runs_the_work_of_a_run_that_is_open() {
        // Given
        AtomicInteger worked = new AtomicInteger();

        // When
        Mono<String> step = run.step(() -> {
            worked.incrementAndGet();
            return Mono.just("worked");
        });

        // Then
        StepVerifier.create(step).expectNext("worked").verifyComplete();
        assertThat(worked).hasValue(1);
    }

    @Test
    void step_never_runs_the_work_of_a_run_that_is_closed() {
        // Given
        run.close();
        AtomicInteger worked = new AtomicInteger();

        // When
        Mono<String> step = run.step(() -> {
            worked.incrementAndGet();
            return Mono.just("worked");
        });

        // Then
        StepVerifier.create(step).expectSubscription().expectNoEvent(Duration.ofMillis(200)).thenCancel().verify();
        assertThat(worked).hasValue(0);
    }

    @Test
    void the_run_has_not_ended_while_a_step_that_started_before_the_close_is_under_way() {
        // Given
        Sinks.Empty<Void> work = Sinks.empty();
        run.step(work::asMono).subscribe();

        // When
        run.close();

        // Then
        assertThat(run.ended().toFuture()).isNotDone();
        work.tryEmitEmpty();
        assertThat(run.ended().toFuture()).isDone();
    }
}
