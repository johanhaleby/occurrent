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

package org.occurrent.application.service.reactor;

import org.occurrent.application.service.ExecutorScope;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveTransactionExecutorTest {

    private static final String INSIDE = "inside";

    @Test
    void the_action_is_told_that_this_call_opened_a_transaction_when_one_is_in_force_only_inside() {
        ReactiveTransactionExecutor executor = new ReactiveTransactionExecutor() {
            @Override
            public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                return Mono.defer(action).contextWrite(Context.of(INSIDE, true));
            }

            @Override
            public Mono<Boolean> isTransactional() {
                return Mono.deferContextual(context -> Mono.just(context.hasKey(INSIDE)));
            }
        };

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(scope -> Mono.just(scope.openedTransaction())).block()).isTrue();
    }

    @Test
    void the_action_is_told_that_this_call_opened_no_transaction_when_one_was_already_in_force() {
        ReactiveTransactionExecutor executor = new ReactiveTransactionExecutor() {
            @Override
            public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                return Mono.defer(action);
            }

            @Override
            public Mono<Boolean> isTransactional() {
                return Mono.just(true);
            }
        };

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(scope -> Mono.just(scope.openedTransaction())).block()).isFalse();
    }

    @Test
    void the_action_is_told_that_this_call_opened_no_transaction_when_the_answer_is_empty() {
        ReactiveTransactionExecutor executor = new ReactiveTransactionExecutor() {
            @Override
            public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                return Mono.defer(action);
            }

            @Override
            public Mono<Boolean> isTransactional() {
                return Mono.empty();
            }
        };

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(scope -> Mono.just(scope.openedTransaction())).block()).isFalse();
    }

    @Test
    void the_default_tells_the_action_that_this_call_started_no_synchronization() {
        assertThat(ReactiveTransactionExecutor.noTransaction().<Boolean>inTransactionTellingIfOpened(scope -> Mono.just(scope.startedSynchronization())).block()).isFalse();
    }
}
