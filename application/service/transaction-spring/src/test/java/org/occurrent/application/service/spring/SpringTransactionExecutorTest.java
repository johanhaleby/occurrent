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

package org.occurrent.application.service.spring;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringTransactionExecutorTest {

    @Test
    void isTransactional_is_true_inside_inTransaction_with_default_propagation() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new TransactionTemplate(new StubTransactionManager()));

        // When
        boolean transactional = executor.inTransaction(executor::isTransactional);

        // Then
        assertThat(transactional).isTrue();
    }

    @Test
    void isTransactional_is_false_inside_inTransaction_with_propagation_not_supported() {
        // Given
        TransactionTemplate transactionTemplate = new TransactionTemplate(new StubTransactionManager());
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionTemplate);

        // When
        boolean transactional = executor.inTransaction(executor::isTransactional);

        // Then
        assertThat(transactional).isFalse();
    }

    @Test
    void isTransactional_is_false_inside_inTransaction_with_propagation_never() {
        // Given
        TransactionTemplate transactionTemplate = new TransactionTemplate(new StubTransactionManager());
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NEVER);
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionTemplate);

        // When
        boolean transactional = executor.inTransaction(executor::isTransactional);

        // Then
        assertThat(transactional).isFalse();
    }

    @Test
    void isTransactional_is_false_outside_inTransaction() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());

        // When
        boolean transactional = executor.isTransactional();

        // Then
        assertThat(transactional).isFalse();
    }

    @Test
    void rollsBackAJoinedTransactionOnFailure_is_true_by_default() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());

        // When
        boolean rollsBack = executor.rollsBackAJoinedTransactionOnFailure();

        // Then
        assertThat(rollsBack).isTrue();
    }

    @Test
    void rollsBackAJoinedTransactionOnFailure_is_false_when_the_manager_does_not_mark_a_joined_transaction_rollback_only() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        transactionManager.setGlobalRollbackOnParticipationFailure(false);
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new TransactionTemplate(transactionManager));

        // When
        boolean rollsBack = executor.rollsBackAJoinedTransactionOnFailure();

        // Then
        assertThat(rollsBack).isFalse();
    }

    @Test
    void rollsBackAJoinedTransactionOnFailure_is_false_when_the_manager_is_not_an_AbstractPlatformTransactionManager() {
        // Given
        PlatformTransactionManager transactionManager = new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void commit(TransactionStatus status) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void rollback(TransactionStatus status) {
                throw new UnsupportedOperationException();
            }
        };
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new TransactionTemplate(transactionManager));

        // When
        boolean rollsBack = executor.rollsBackAJoinedTransactionOnFailure();

        // Then
        assertThat(rollsBack).isFalse();
    }

    // A transaction manager with no real resource to manage: doBegin/doCommit/doRollback are no-ops, and
    // doGetTransaction hands out a fresh marker object per call so AbstractPlatformTransactionManager treats every
    // call as a request for a new transaction rather than joining an existing one.
    private static final class StubTransactionManager extends AbstractPlatformTransactionManager {

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
