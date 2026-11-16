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
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupMarkerGenerationReadsTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    private final CatchupMarkerGenerationReads reads = new CatchupMarkerGenerationReads();
    private final AtomicInteger linearizableCalls = new AtomicInteger();
    private final AtomicInteger majorityCalls = new AtomicInteger();

    @Test
    void a_generation_document_is_read_with_linearizable_and_majority_is_never_asked() {
        long generation = reads.read(counting(linearizableCalls, new Document("generation", 7L)), counting(majorityCalls, new Document("generation", 99L)));

        assertThat(generation).isEqualTo(7);
        assertThat(linearizableCalls).hasValue(1);
        assertThat(majorityCalls).hasValue(0);
    }

    @Test
    void a_missing_generation_document_reads_as_zero() {
        long generation = reads.read(counting(linearizableCalls, null), counting(majorityCalls, null));

        assertThat(generation).isZero();
    }

    @Test
    void error_123_is_answered_from_majority_on_the_same_call() {
        long generation = reads.read(failing(linearizableCalls, commandFailure(123)), counting(majorityCalls, new Document("generation", 5L)));

        assertThat(generation).isEqualTo(5);
        assertThat(linearizableCalls).hasValue(1);
        assertThat(majorityCalls).hasValue(1);
    }

    @Test
    void after_error_123_a_later_call_skips_linearizable_entirely() {
        reads.read(failing(linearizableCalls, commandFailure(123)), counting(majorityCalls, new Document("generation", 5L)));

        long generation = reads.read(failing(linearizableCalls, commandFailure(123)), counting(majorityCalls, new Document("generation", 6L)));

        assertThat(generation).isEqualTo(6);
        assertThat(linearizableCalls).hasValue(1);
        assertThat(majorityCalls).hasValue(2);
    }

    @Test
    void error_123_wrapped_as_the_cause_of_another_exception_also_falls_back() {
        RuntimeException translated = new RuntimeException("translated", commandFailure(123));

        long generation = reads.read(failing(linearizableCalls, translated), counting(majorityCalls, new Document("generation", 5L)));

        assertThat(generation).isEqualTo(5);
        assertThat(majorityCalls).hasValue(1);
    }

    @Test
    void another_command_error_is_rethrown_as_the_same_instance_and_the_next_call_still_tries_linearizable() {
        MongoCommandException failure = commandFailure(50);

        assertThatThrownBy(() -> reads.read(failing(linearizableCalls, failure), counting(majorityCalls, null))).isSameAs(failure);
        long generation = reads.read(counting(linearizableCalls, new Document("generation", 3L)), counting(majorityCalls, null));

        assertThat(generation).isEqualTo(3);
        assertThat(linearizableCalls).hasValue(2);
        assertThat(majorityCalls).hasValue(0);
    }

    @Test
    void a_plain_runtime_exception_is_rethrown_as_the_same_instance_and_the_next_call_still_tries_linearizable() {
        IllegalStateException failure = new IllegalStateException("boom");

        assertThatThrownBy(() -> reads.read(failing(linearizableCalls, failure), counting(majorityCalls, null))).isSameAs(failure);
        long generation = reads.read(counting(linearizableCalls, new Document("generation", 3L)), counting(majorityCalls, null));

        assertThat(generation).isEqualTo(3);
        assertThat(linearizableCalls).hasValue(2);
        assertThat(majorityCalls).hasValue(0);
    }

    private static Supplier<Document> counting(AtomicInteger calls, Document document) {
        return () -> {
            calls.incrementAndGet();
            return document;
        };
    }

    private static Supplier<Document> failing(AtomicInteger calls, RuntimeException failure) {
        return () -> {
            calls.incrementAndGet();
            throw failure;
        };
    }

    private static MongoCommandException commandFailure(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString("failure " + code));
        return new MongoCommandException(response, ADDRESS);
    }
}
