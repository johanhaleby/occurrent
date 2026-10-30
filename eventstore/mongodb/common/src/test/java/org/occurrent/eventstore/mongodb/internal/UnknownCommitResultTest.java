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

package org.occurrent.eventstore.mongodb.internal;

import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.WriteConcernError;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class UnknownCommitResultTest {

    private static final int MAX_TIME_MS_EXPIRED = 50;
    private static final int WRITE_CONCERN_FAILED = 64;

    @Test
    void a_mongo_exception_with_the_unknown_commit_result_label_is_retryable() {
        assertThat(UnknownCommitResult.isRetryable(labelled(new MongoSocketReadException("lost the reply", new ServerAddress())))).isTrue();
    }

    @Test
    void a_mongo_exception_without_the_label_is_not_retryable() {
        assertThat(UnknownCommitResult.isRetryable(new MongoSocketReadException("lost the reply", new ServerAddress()))).isFalse();
    }

    @Test
    void a_label_on_the_cause_of_a_wrapping_exception_is_found() {
        assertThat(UnknownCommitResult.isRetryable(new IllegalStateException("wrapped", labelled(new MongoException("unknown"))))).isTrue();
    }

    @Test
    void only_the_first_mongo_exception_in_the_cause_chain_decides() {
        MongoException labelledCause = labelled(new MongoException("unknown"));
        MongoException unlabelledOuter = new MongoException("outer", labelledCause);

        assertThat(UnknownCommitResult.isRetryable(unlabelledOuter)).isFalse();
    }

    @Test
    void a_failure_that_is_not_a_mongo_exception_is_not_retryable() {
        assertThat(UnknownCommitResult.isRetryable(new IllegalStateException("not mongo"))).isFalse();
    }

    @Test
    void a_max_time_ms_expired_error_is_not_retryable_even_with_the_label() {
        assertThat(UnknownCommitResult.isRetryable(labelled(new MongoExecutionTimeoutException(MAX_TIME_MS_EXPIRED, "max time")))).isFalse();
    }

    @Test
    void an_operation_timeout_is_not_retryable_even_with_the_label() {
        assertThat(UnknownCommitResult.isRetryable(labelled(new MongoOperationTimeoutException("timeout")))).isFalse();
    }

    @Test
    void a_write_concern_error_with_the_max_time_ms_expired_code_is_retryable_when_it_has_the_label() {
        MongoWriteConcernException failure = new MongoWriteConcernException(
                new WriteConcernError(MAX_TIME_MS_EXPIRED, "MaxTimeMSExpired", "max time", new BsonDocument()), WriteConcernResult.unacknowledged(), new ServerAddress());
        failure.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);

        assertThat(UnknownCommitResult.isRetryable(failure)).isTrue();
    }

    @Test
    void a_write_concern_error_with_another_code_is_retryable_when_it_has_the_label() {
        MongoWriteConcernException failure = new MongoWriteConcernException(
                new WriteConcernError(WRITE_CONCERN_FAILED, "WriteConcernFailed", "unknown", new BsonDocument()), WriteConcernResult.unacknowledged(), new ServerAddress());
        failure.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);

        assertThat(UnknownCommitResult.isRetryable(failure)).isTrue();
    }

    private static <T extends MongoException> T labelled(T exception) {
        exception.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        return exception;
    }
}
