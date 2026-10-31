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

import com.mongodb.MongoException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void the_default_executor_runs_the_action_again_when_it_fails_with_a_transient_data_access_exception() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_runs_the_action_again_when_a_data_integrity_violation_carries_the_transient_transaction_error_label() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw new DataIntegrityViolationException("write conflict", labelled(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_runs_the_action_again_when_the_transient_transaction_error_label_is_deeper_in_the_cause_chain() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw new IllegalStateException("handler failed", new RuntimeException(labelled(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)));
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_runs_the_action_again_when_a_mongo_exception_that_spring_does_not_translate_carries_the_transient_transaction_error_label() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw labelled(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_runs_the_action_once_when_it_fails_with_a_duplicate_key() {
        // A duplicate key is a result of the work, not a conflict, so it carries no label and the action runs once.
        runsOnceAndFailsWith(new DuplicateKeyException("duplicate", new MongoException(11000, "E11000 duplicate key")));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_it_fails_with_a_document_validation_error() {
        runsOnceAndFailsWith(new DataIntegrityViolationException("validation", new MongoException(121, "Document failed validation")));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_it_fails_with_a_data_integrity_violation_that_carries_no_label() {
        runsOnceAndFailsWith(new DataIntegrityViolationException("write conflict"));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_the_commit_result_is_unknown() {
        // The commit may have gone through, so running the unit of work again could apply it twice.
        runsOnceAndFailsWith(new DataIntegrityViolationException("commit", labelled(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_an_exception_that_spring_does_not_translate_has_an_unknown_commit_result() {
        runsOnceAndFailsWith(labelled(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_a_commit_error_has_both_the_transient_and_the_unknown_commit_label() {
        // The driver's withTransaction looks at the unknown commit label first, and so does the executor.
        MongoException both = labelled(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
        both.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        runsOnceAndFailsWith(new DataIntegrityViolationException("commit", both));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_a_transient_data_access_exception_wraps_an_unknown_commit_result() {
        runsOnceAndFailsWith(new TransientDataAccessResourceException("commit", labelled(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_a_cause_has_the_unknown_commit_label_below_one_with_the_transient_label() {
        MongoException outer = new MongoException("outer", labelled(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
        outer.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
        runsOnceAndFailsWith(new DataIntegrityViolationException("commit", outer));
    }

    @Test
    void the_default_executor_runs_the_action_once_when_the_cause_chain_is_a_cycle_without_a_label() {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second", first);
        first.initCause(second);
        runsOnceAndFailsWith(first);
    }

    @Test
    void the_default_executor_runs_the_action_again_when_the_label_is_a_hundred_causes_down() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw deeplyNested(labelled(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    // The callback fails the test if it runs a second time, so a retry that should not happen fails on this
    // assertion straight away instead of running the default two minute budget out.
    private static void runsOnceAndFailsWith(RuntimeException failure) {
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> executor.inTransaction(() -> {
            if (attempts.incrementAndGet() > 1) {
                throw new AssertionError("the action ran again after " + failure);
            }
            throw failure;
        })).isSameAs(failure);
        assertThat(attempts).hasValue(1);
    }

    private static RuntimeException deeplyNested(Throwable root) {
        Throwable cause = root;
        for (int depth = 0; depth < 100; depth++) {
            cause = new RuntimeException("wrapper " + depth, cause);
        }
        return (RuntimeException) cause;
    }

    private static MongoException labelled(String label) {
        MongoException exception = new MongoException("labelled");
        exception.addLabel(label);
        return exception;
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
