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

package org.occurrent.application.service.spring.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringReactiveTransactionExecutorTest {

    @Test
    void isTransactional_is_true_inside_inTransaction_with_default_propagation() {
        // Given
        TransactionalOperator operator = TransactionalOperator.create(new StubReactiveTransactionManager());
        SpringReactiveTransactionExecutor executor = new SpringReactiveTransactionExecutor(operator);

        // When
        Mono<Boolean> transactional = executor.inTransaction(executor::isTransactional);

        // Then
        StepVerifier.create(transactional)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void isTransactional_is_false_inside_inTransaction_with_propagation_not_supported() {
        // Given
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
        TransactionalOperator operator = TransactionalOperator.create(new StubReactiveTransactionManager(), definition);
        SpringReactiveTransactionExecutor executor = new SpringReactiveTransactionExecutor(operator);

        // When
        Mono<Boolean> transactional = executor.inTransaction(executor::isTransactional);

        // Then
        StepVerifier.create(transactional)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void isTransactional_is_false_inside_inTransaction_with_propagation_never() {
        // Given
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_NEVER);
        TransactionalOperator operator = TransactionalOperator.create(new StubReactiveTransactionManager(), definition);
        SpringReactiveTransactionExecutor executor = new SpringReactiveTransactionExecutor(operator);

        // When
        Mono<Boolean> transactional = executor.inTransaction(executor::isTransactional);

        // Then
        StepVerifier.create(transactional)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void isTransactional_is_false_outside_inTransaction() {
        // Given
        SpringReactiveTransactionExecutor executor = new SpringReactiveTransactionExecutor(new StubReactiveTransactionManager());

        // When
        Mono<Boolean> transactional = executor.isTransactional();

        // Then
        StepVerifier.create(transactional)
                .expectNext(false)
                .verifyComplete();
    }

    // A transaction manager with no real resource to manage: doBegin/doCommit/doRollback are no-ops, and
    // doGetTransaction hands out a fresh marker object per call so AbstractReactiveTransactionManager treats every
    // call as a request for a new transaction rather than joining an existing one.
    private static final class StubReactiveTransactionManager extends AbstractReactiveTransactionManager {

        @Override
        protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager) {
            return new Object();
        }

        @Override
        protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction, TransactionDefinition definition) {
            return Mono.empty();
        }

        @Override
        protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) {
            return Mono.empty();
        }

        @Override
        protected Mono<Void> doRollback(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) {
            return Mono.empty();
        }
    }
}
