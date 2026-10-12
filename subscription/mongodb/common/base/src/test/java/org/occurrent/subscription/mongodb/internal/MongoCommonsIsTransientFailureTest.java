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
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoCommonsIsTransientFailureTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    @Test
    void a_socket_exception_is_transient() {
        assertThat(MongoCommons.isTransientFailure(new MongoSocketException("connection reset", ADDRESS))).isTrue();
    }

    @Test
    void every_subclass_of_the_socket_exception_is_transient() {
        IOException cause = new IOException("connection reset");

        assertThat(MongoCommons.isTransientFailure(new MongoSocketReadException("read", ADDRESS, cause))).isTrue();
        assertThat(MongoCommons.isTransientFailure(new MongoSocketWriteException("write", ADDRESS, cause))).isTrue();
        assertThat(MongoCommons.isTransientFailure(new MongoSocketOpenException("open", ADDRESS, cause))).isTrue();
        assertThat(MongoCommons.isTransientFailure(new MongoSocketClosedException("closed", ADDRESS))).isTrue();
    }

    @Test
    void a_timeout_is_transient() {
        assertThat(MongoCommons.isTransientFailure(new MongoTimeoutException("Timed out while waiting for a server"))).isTrue();
    }

    @Test
    void a_primary_that_stepped_down_is_transient_whatever_its_error_code() {
        assertThat(MongoCommons.isTransientFailure(new MongoNotPrimaryException(commandFailure(2), ADDRESS))).isTrue();
    }

    @Test
    void a_node_that_is_recovering_is_transient_whatever_its_error_code() {
        assertThat(MongoCommons.isTransientFailure(new MongoNodeIsRecoveringException(commandFailure(2), ADDRESS))).isTrue();
    }

    @Test
    void a_cleared_connection_pool_is_transient() {
        ServerId serverId = new ServerId(new ClusterId(), ADDRESS);

        assertThat(MongoCommons.isTransientFailure(new MongoConnectionPoolClearedException(serverId, null))).isTrue();
    }

    @Test
    void a_command_failure_with_a_retryable_error_code_is_transient() {
        for (int code : new int[]{6, 7, 89, 91, 134, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436}) {
            assertThat(MongoCommons.isTransientFailure(new MongoCommandException(commandFailure(code), ADDRESS)))
                    .as("error code %d", code)
                    .isTrue();
        }
    }

    @Test
    void a_transient_exception_is_transient_when_it_is_the_cause_of_a_wrapper() {
        MongoSocketReadException socketFailure = new MongoSocketReadException("read", ADDRESS, new IOException("connection reset"));
        RuntimeException wrapper = new IllegalStateException("outer", new RuntimeException("middle", socketFailure));

        assertThat(MongoCommons.isTransientFailure(wrapper)).isTrue();
    }

    @Test
    void a_command_failure_with_a_retryable_error_code_is_transient_when_it_is_the_cause_of_a_wrapper() {
        RuntimeException wrapper = new RuntimeException("translated", new MongoCommandException(commandFailure(11600), ADDRESS));

        assertThat(MongoCommons.isTransientFailure(wrapper)).isTrue();
    }

    @Test
    void a_command_failure_with_a_permanent_error_code_is_not_transient() {
        for (int code : new int[]{2, 13, MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE}) {
            assertThat(MongoCommons.isTransientFailure(new MongoCommandException(commandFailure(code), ADDRESS)))
                    .as("error code %d", code)
                    .isFalse();
        }
    }

    @Test
    void a_permanent_command_failure_is_not_transient_when_it_is_the_cause_of_a_wrapper() {
        RuntimeException wrapper = new RuntimeException("translated", new MongoCommandException(commandFailure(13), ADDRESS));

        assertThat(MongoCommons.isTransientFailure(wrapper)).isFalse();
    }

    @Test
    void a_plain_runtime_exception_is_not_transient() {
        assertThat(MongoCommons.isTransientFailure(new RuntimeException("boom"))).isFalse();
        assertThat(MongoCommons.isTransientFailure(new RuntimeException("boom", new IllegalArgumentException("root")))).isFalse();
    }

    private static BsonDocument commandFailure(int code) {
        return new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString("command failed with code " + code));
    }
}
