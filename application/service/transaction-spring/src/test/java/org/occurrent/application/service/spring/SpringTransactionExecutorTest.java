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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.ExecutorScope;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringTransactionExecutorTest {

    // A failed test can leave a resource or a synchronization on the thread, which would make a later test join it
    @AfterEach
    void clearTheThread() {
        TransactionSynchronizationManager.unbindResourceIfPossible(BindingTransactionManager.RESOURCE_KEY);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
    }

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

    @Test
    void the_default_executor_runs_the_action_a_third_time_when_the_commit_fails_twice_with_the_transient_transaction_error_label() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager(noSuchTransaction(), noSuchTransaction()));
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            attempts.incrementAndGet();
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(3);
    }

    @Test
    void the_count_of_failed_commits_belongs_to_one_call_and_a_later_call_starts_from_zero() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager(noSuchTransaction(), noSuchTransaction());
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionManager);
        AtomicInteger attempts = new AtomicInteger();

        // When
        executor.inTransaction(() -> attempts.incrementAndGet());
        transactionManager.failNextCommits(noSuchTransaction(), noSuchTransaction());
        executor.inTransaction(() -> attempts.incrementAndGet());

        // Then
        assertThat(attempts).hasValue(6);
    }

    @Test
    void the_default_executor_does_not_count_a_transaction_exception_that_the_action_throws_itself() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager());
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 3) {
                throw noSuchTransaction();
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_stops_after_three_failed_commits_and_says_which_error_failed_the_commit() {
        // Given
        TransactionSystemException lastCommitFailure = noSuchTransaction();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager(noSuchTransaction(), noSuchTransaction(), lastCommitFailure, noSuchTransaction()));
        AtomicInteger attempts = new AtomicInteger();

        // When
        assertThatThrownBy(() -> executor.inTransaction(() -> attempts.incrementAndGet())).isInstanceOf(TransactionSystemException.class)
                .hasMessage("The commit failed with NoSuchTransaction (code 251) after 3 failed commits, so it was not retried again. "
                        + "MongoDB no longer had the transaction when the commit arrived. That follows a failover, "
                        + "a transaction that outlived transactionLifetimeLimitSeconds, or a write error such as a duplicate key "
                        + "that the transaction body caught and swallowed.")
                .hasCause(lastCommitFailure);

        // Then
        assertThat(attempts).hasValue(3);
    }

    @Test
    void the_default_executor_names_only_the_error_when_a_failed_commit_is_not_no_such_transaction() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager(writeConflict(), writeConflict(), writeConflict()));

        // When
        assertThatThrownBy(() -> executor.inTransaction(() -> "unused"))
                // Then
                .hasMessage("The commit failed with WriteConflict (code 112) after 3 failed commits, so it was not retried again.");
    }

    @Test
    void the_default_executor_counts_failed_commits_and_not_errors_thrown_from_the_action() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager(noSuchTransaction(), noSuchTransaction()));
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() <= 5) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(8);
    }

    @Test
    void the_default_executor_does_not_count_a_commit_that_fails_without_the_transient_label() {
        // Given
        TransactionSystemException unlabelled = new TransactionSystemException("commit failed", new MongoException("no label"));
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new StubTransactionManager(unlabelled));
        AtomicInteger attempts = new AtomicInteger();

        // When
        assertThatThrownBy(() -> executor.inTransaction(() -> attempts.incrementAndGet())).isSameAs(unlabelled);

        // Then
        assertThat(attempts).hasValue(1);
    }

    @Test
    void a_retry_passed_to_the_constructor_is_used_as_given_and_has_no_cap_on_failed_commits() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(
                new StubTransactionManager(noSuchTransaction(), noSuchTransaction(), noSuchTransaction(), noSuchTransaction(), noSuchTransaction()),
                RetryStrategy.fixed(1).maxAttempts(10).retryIf(throwable -> throwable instanceof TransactionSystemException));
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            attempts.incrementAndGet();
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(6);
    }

    @Test
    void the_action_is_told_that_the_executor_started_the_synchronization_under_propagation_supports_with_no_caller_scope() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(supports(new StubTransactionManager()));

        // When
        ExecutorScope scope = executor.inTransactionTellingIfOpened(it -> it);

        // Then
        assertThat(scope).isEqualTo(new ExecutorScope(false, true));
    }

    @Test
    void the_action_is_told_that_the_executor_started_no_synchronization_under_propagation_supports_in_a_callers_supports_scope() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(supports(transactionManager));

        // When
        ExecutorScope scope = supports(transactionManager).execute(status -> executor.inTransactionTellingIfOpened(it -> it));

        // Then
        assertThat(scope).isEqualTo(new ExecutorScope(false, false));
    }

    @Test
    void the_default_executor_runs_the_action_again_after_a_transient_conflict_under_propagation_supports_with_no_caller_scope() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(supports(new StubTransactionManager()));
        AtomicInteger attempts = new AtomicInteger();

        // When
        String result = executor.inTransaction(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(2);
    }

    @Test
    void the_default_executor_runs_the_action_once_and_gives_a_transient_conflict_to_the_caller_in_a_callers_supports_scope() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(supports(transactionManager));
        AtomicInteger attempts = new AtomicInteger();
        TransientDataAccessResourceException conflict = new TransientDataAccessResourceException("conflict");

        // When
        Throwable thrown = catchThrowable(() -> supports(transactionManager).execute(status -> executor.inTransaction(() -> {
            if (attempts.incrementAndGet() > 1) {
                throw new AssertionError("the action ran again after " + conflict);
            }
            throw conflict;
        })));

        // Then
        assertThat(thrown).isSameAs(conflict);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void the_default_executor_runs_the_action_again_after_a_transient_conflict_under_propagation_required_in_a_callers_supports_scope() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionManager);
        AtomicInteger attempts = new AtomicInteger();

        // When
        Throwable thrown = catchThrowable(() -> supports(transactionManager).execute(status -> executor.inTransaction(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        })));

        // Then
        assertThat(thrown).as("[failure reaching the caller]").isNull();
        assertThat(attempts).hasValue(2);
    }

    @Test
    void an_executor_whose_retry_policy_isnt_a_retry_runs_the_action_once_in_a_callers_supports_scope() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(supports(transactionManager), new RunsUpToThreeTimes());
        AtomicInteger attempts = new AtomicInteger();
        TransientDataAccessResourceException conflict = new TransientDataAccessResourceException("conflict");

        // When
        Throwable thrown = catchThrowable(() -> supports(transactionManager).execute(status -> executor.inTransaction(() -> {
            attempts.incrementAndGet();
            throw conflict;
        })));

        // Then
        assertThat(attempts).hasValue(1);
        assertThat(thrown).isSameAs(conflict);
    }

    @Test
    void an_executor_runs_the_action_once_when_it_joins_a_transaction_that_a_caller_opened_without_synchronization() {
        // Given
        BindingTransactionManager callersTransactionManager = new BindingTransactionManager();
        callersTransactionManager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new BindingTransactionManager(), RetryStrategy.fixed(1).maxAttempts(3));
        AtomicInteger attempts = new AtomicInteger();
        TransientDataAccessResourceException conflict = new TransientDataAccessResourceException("conflict");

        // When
        Throwable thrown = catchThrowable(() -> new TransactionTemplate(callersTransactionManager).execute(status -> executor.inTransaction(() -> {
            attempts.incrementAndGet();
            throw conflict;
        })));

        // Then
        assertThat(attempts).hasValue(1);
        assertThat(thrown).isSameAs(conflict);
    }

    @Test
    void the_default_executor_runs_the_action_again_on_a_transaction_manager_whose_status_isnt_springs_default_one() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new SynchronizingTransactionManager());
        AtomicInteger attempts = new AtomicInteger();
        List<ExecutorScope> scopes = new ArrayList<>();

        // When
        Throwable thrown = catchThrowable(() -> executor.inTransactionTellingIfOpened(scope -> {
            scopes.add(scope);
            if (attempts.incrementAndGet() == 1) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        }));

        // Then
        assertThat(thrown).as("[failure reaching the caller]").isNull();
        assertThat(attempts).hasValue(2);
        assertThat(scopes).allMatch(ExecutorScope::startedSynchronization);
    }

    @Test
    void the_default_executor_runs_the_action_after_its_transaction_failed_to_start_under_propagation_required_in_a_callers_supports_scope() {
        // Given
        StubTransactionManager transactionManager = new StubTransactionManager();
        transactionManager.failNextBegin(new TransientDataAccessResourceException("transaction didn't start"));
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionManager);
        AtomicInteger attempts = new AtomicInteger();

        // When
        Throwable thrown = catchThrowable(() -> supports(transactionManager).execute(status -> executor.inTransaction(() -> {
            attempts.incrementAndGet();
            return "done";
        })));

        // Then
        assertThat(thrown).as("[failure reaching the caller]").isNull();
        assertThat(attempts).hasValue(1);
    }

    @Test
    void an_executor_runs_the_action_once_when_it_joins_a_session_bound_in_a_callers_supports_scope() {
        // Given
        BindingTransactionManager transactionManager = new BindingTransactionManager();
        SpringTransactionExecutor executor = new SpringTransactionExecutor(transactionManager, RetryStrategy.fixed(1).maxAttempts(3));
        AtomicInteger attempts = new AtomicInteger();
        TransientDataAccessResourceException conflict = new TransientDataAccessResourceException("conflict");

        // When
        Throwable thrown = catchThrowable(() -> supports(transactionManager).execute(status -> {
            // As a read through a template on SessionSynchronization.ALWAYS binds a session
            TransactionSynchronizationManager.bindResource(BindingTransactionManager.RESOURCE_KEY, new Object());
            try {
                return executor.inTransaction(() -> {
                    attempts.incrementAndGet();
                    throw conflict;
                });
            } finally {
                TransactionSynchronizationManager.unbindResourceIfPossible(BindingTransactionManager.RESOURCE_KEY);
            }
        }));

        // Then
        assertThat(attempts).hasValue(1);
        assertThat(thrown).isSameAs(conflict);
    }

    @Test
    void an_executor_whose_retry_policy_isnt_a_retry_runs_the_action_again_outside_a_callers_scope() {
        // Given
        SpringTransactionExecutor executor = new SpringTransactionExecutor(new TransactionTemplate(new StubTransactionManager()), new RunsUpToThreeTimes());
        AtomicInteger attempts = new AtomicInteger();

        // When
        Throwable thrown = catchThrowable(() -> executor.inTransaction(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        }));

        // Then
        assertThat(thrown).as("[failure reaching the caller]").isNull();
        assertThat(attempts).hasValue(3);
    }

    private static TransactionTemplate supports(PlatformTransactionManager transactionManager) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_SUPPORTS);
        return transactionTemplate;
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

    private static TransactionSystemException noSuchTransaction() {
        return commitFailure(251, "NoSuchTransaction");
    }

    private static TransactionSystemException writeConflict() {
        return commitFailure(112, "WriteConflict");
    }

    // What the real MongoTransactionManager throws when the server refuses the commit.
    private static TransactionSystemException commitFailure(int code, String codeName) {
        MongoCommandException failure = new MongoCommandException(new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName))
                .append("errmsg", new BsonString("commit refused")), new ServerAddress());
        failure.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
        return new TransactionSystemException("Could not commit Mongo transaction", failure);
    }

    private static MongoException labelled(String label) {
        MongoException exception = new MongoException("labelled");
        exception.addLabel(label);
        return exception;
    }

    // A transaction manager with no real resource to manage: doBegin/doCommit/doRollback are no-ops, and
    // doGetTransaction hands out a fresh marker object per call so AbstractPlatformTransactionManager treats every
    // call as a request for a new transaction rather than joining an existing one. The commit fails with each
    // exception it was given, one per commit, and then succeeds.
    private static final class StubTransactionManager extends AbstractPlatformTransactionManager {

        private final Deque<TransactionSystemException> commitFailures = new ArrayDeque<>();
        private final Deque<RuntimeException> beginFailures = new ArrayDeque<>();

        StubTransactionManager(TransactionSystemException... commitFailures) {
            failNextCommits(commitFailures);
        }

        void failNextBegin(RuntimeException beginFailure) {
            beginFailures.add(beginFailure);
        }

        void failNextCommits(TransactionSystemException... commitFailures) {
            this.commitFailures.addAll(List.of(commitFailures));
        }

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            RuntimeException failure = beginFailures.poll();
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
            TransactionSystemException failure = commitFailures.poll();
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }

    // A retry policy of the application's own, which this executor can't give a retry predicate
    private static final class RunsUpToThreeTimes implements RetryStrategy {
        @Override
        public <T extends @Nullable Object> T execute(Supplier<T> supplier) {
            for (int run = 1; ; run++) {
                try {
                    return supplier.get();
                } catch (RuntimeException e) {
                    if (run == 3) {
                        throw e;
                    }
                }
            }
        }
    }

    // Binds its transaction as a resource and joins one already bound, also when the manager that bound it doesn't
    // synchronize, as MongoTransactionManager does with a session
    private static final class BindingTransactionManager extends AbstractPlatformTransactionManager {
        private static final Object RESOURCE_KEY = BindingTransactionManager.class;

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected boolean isExistingTransaction(Object transaction) {
            return TransactionSynchronizationManager.hasResource(RESOURCE_KEY);
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            TransactionSynchronizationManager.bindResource(RESOURCE_KEY, transaction);
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }

        @Override
        protected void doSetRollbackOnly(DefaultTransactionStatus status) {
        }

        @Override
        protected void doCleanupAfterCompletion(Object transaction) {
            TransactionSynchronizationManager.unbindResourceIfPossible(RESOURCE_KEY);
        }
    }

    // Starts a synchronization when none is active and returns a status that isn't a DefaultTransactionStatus, as
    // ChainedTransactionManager does
    private static final class SynchronizingTransactionManager implements PlatformTransactionManager {
        @Override
        public TransactionStatus getTransaction(@Nullable TransactionDefinition definition) {
            boolean newSynchronization = !TransactionSynchronizationManager.isSynchronizationActive();
            if (newSynchronization) {
                TransactionSynchronizationManager.initSynchronization();
            }
            return new SynchronizingStatus(newSynchronization);
        }

        @Override
        public void commit(TransactionStatus status) {
            complete(status);
        }

        @Override
        public void rollback(TransactionStatus status) {
            complete(status);
        }

        private static void complete(TransactionStatus status) {
            if (((SynchronizingStatus) status).newSynchronization) {
                TransactionSynchronizationManager.clear();
            }
        }

        private static final class SynchronizingStatus extends SimpleTransactionStatus {
            private final boolean newSynchronization;

            SynchronizingStatus(boolean newSynchronization) {
                super(true);
                this.newSynchronization = newSynchronization;
            }
        }
    }
}
