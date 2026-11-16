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

package org.occurrent.subscription.mongodb.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.subscription.mongodb.internal.CatchupMarkerForgetWrites.JOURNALING_REQUIRED_MESSAGE;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupMarkerForgetWritesTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    @Test
    void a_write_the_server_accepts_runs_once_and_throws_nothing() {
        AtomicInteger runs = new AtomicInteger();

        CatchupMarkerForgetWrites.write(runs::incrementAndGet);

        assertThat(runs).hasValue(1);
    }

    @Test
    void a_refusal_for_lack_of_journaling_fails_as_an_illegal_state_exception_with_the_journaling_message_and_the_refusal_as_cause() {
        MongoCommandException refusal = commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE);
        AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> CatchupMarkerForgetWrites.write(() -> {
            runs.incrementAndGet();
            throw refusal;
        })).isExactlyInstanceOf(IllegalStateException.class).hasMessage(JOURNALING_REQUIRED_MESSAGE).hasCause(refusal);
        assertThat(runs).as("runs, the write is never sent again without journal").hasValue(1);
    }

    @Test
    void a_refusal_wrapped_as_the_cause_of_another_exception_also_fails_as_an_illegal_state_exception_with_the_wrapper_as_cause() {
        RuntimeException translated = new RuntimeException("translated", commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE));

        assertThatThrownBy(() -> CatchupMarkerForgetWrites.write(() -> {
            throw translated;
        })).isExactlyInstanceOf(IllegalStateException.class).hasMessage(JOURNALING_REQUIRED_MESSAGE).hasCause(translated);
    }

    @Test
    void error_code_2_with_another_message_is_rethrown_as_the_same_instance() {
        MongoCommandException failure = commandFailure(2, "unrelated bad value");

        assertThatThrownBy(() -> CatchupMarkerForgetWrites.write(() -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void the_journal_message_with_another_error_code_is_rethrown_as_the_same_instance() {
        MongoCommandException failure = commandFailure(50, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE);

        assertThatThrownBy(() -> CatchupMarkerForgetWrites.write(() -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void any_other_exception_is_rethrown_as_the_same_instance() {
        IllegalArgumentException failure = new IllegalArgumentException("boom");

        assertThatThrownBy(() -> CatchupMarkerForgetWrites.write(() -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void the_refusal_for_lack_of_journaling_is_not_retryable() {
        assertThat(CatchupMarkerForgetWrites.isRetryable(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE))).isFalse();
    }

    @Test
    void the_refusal_wrapped_as_a_cause_is_not_retryable() {
        assertThat(CatchupMarkerForgetWrites.isRetryable(new RuntimeException("translated", commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)))).isFalse();
    }

    @Test
    void error_code_2_with_another_message_is_retryable() {
        assertThat(CatchupMarkerForgetWrites.isRetryable(commandFailure(2, "unrelated bad value"))).isTrue();
    }

    @Test
    void the_journal_message_with_another_error_code_is_retryable() {
        assertThat(CatchupMarkerForgetWrites.isRetryable(commandFailure(50, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE))).isTrue();
    }

    @Test
    void any_other_exception_is_retryable() {
        assertThat(CatchupMarkerForgetWrites.isRetryable(new IllegalStateException("boom"))).isTrue();
    }

    private static MongoCommandException commandFailure(int code, String message) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString(message));
        return new MongoCommandException(response, ADDRESS);
    }
}
