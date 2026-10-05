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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointAwareSubscriptionModelTest {

    @Test
    void global_checkpoint_as_of_now_answers_what_global_checkpoint_answers_when_it_is_subscribed_to() {
        // Given
        GlobalCheckpointOnly model = new GlobalCheckpointOnly();
        model.answer.set(Mono.just(new StringBasedCheckpoint("at the call")));

        // When
        Mono<Checkpoint> asOfNow = model.globalCheckpointAsOfNow();
        model.answer.set(Mono.just(new StringBasedCheckpoint("at the subscription")));

        // Then
        assertThat(model.reads).hasValue(0);
        StepVerifier.create(asOfNow).expectNext(new StringBasedCheckpoint("at the subscription")).verifyComplete();
        assertThat(model.reads).hasValue(1);
    }

    @Test
    void global_checkpoint_as_of_now_completes_empty_when_global_checkpoint_does() {
        GlobalCheckpointOnly model = new GlobalCheckpointOnly();
        model.answer.set(Mono.empty());

        StepVerifier.create(model.globalCheckpointAsOfNow()).verifyComplete();
    }

    @Test
    void global_checkpoint_as_of_now_fails_when_global_checkpoint_does() {
        GlobalCheckpointOnly model = new GlobalCheckpointOnly();
        model.answer.set(Mono.error(new IllegalStateException("cannot read the position")));

        StepVerifier.create(model.globalCheckpointAsOfNow()).verifyErrorMessage("cannot read the position");
    }

    private static final class GlobalCheckpointOnly implements CheckpointAwareSubscriptionModel {
        private final AtomicReference<Mono<Checkpoint>> answer = new AtomicReference<>(Mono.empty());
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.defer(() -> {
                reads.incrementAndGet();
                return answer.get();
            });
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }
}
