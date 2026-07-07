/*
 * Copyright 2020 Johan Haleby
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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a duplicate-key error on the unique streamid+streamversion index is classified as a retryable
 * partition stream-version collision, while a duplicate-key error on the unique id+source index stays a genuine
 * duplicate CloudEvent. This is the classification the DCB insert paths rely on to retry a partition collision instead
 * of surfacing a misleading duplicate CloudEvent failure.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoExceptionTranslatorTest {

    private static final int DUPLICATE_KEY_CODE = 11000;

    @Test
    void classifies_duplicate_key_on_stream_version_index_as_retryable() {
        MongoBulkWriteException e = duplicateKeyBulkWriteException(
                "E11000 duplicate key error collection: test.events index: streamid_1_streamversion_1 dup key: { streamid: \"dcb:partition:\", streamversion: 1 }");

        assertThat(MongoExceptionTranslator.isDuplicateKeyErrorOnStreamVersionIndex(e)).isTrue();
    }

    @Test
    void does_not_classify_duplicate_key_on_id_source_index_as_retryable() {
        MongoBulkWriteException e = duplicateKeyBulkWriteException(
                "E11000 duplicate key error collection: test.events index: id_1_source_1 dup key: { id: \"some-id\", source: \"urn:test\" }");

        assertThat(MongoExceptionTranslator.isDuplicateKeyErrorOnStreamVersionIndex(e)).isFalse();
    }

    @Test
    void translates_duplicate_key_on_id_source_index_to_duplicate_cloud_event_exception() {
        MongoBulkWriteException e = duplicateKeyBulkWriteException(
                "E11000 duplicate key error collection: test.events index: id_1_source_1 dup key: { id: \"some-id\", source: \"urn:test\" }");

        RuntimeException translated = MongoExceptionTranslator.translateException(
                new WriteContext("some-stream", 0, WriteCondition.anyStreamVersion()), e);

        assertThat(translated).isInstanceOf(DuplicateCloudEventException.class);
    }

    private static MongoBulkWriteException duplicateKeyBulkWriteException(String message) {
        BulkWriteError error = new BulkWriteError(DUPLICATE_KEY_CODE, message, new org.bson.BsonDocument(), 0);
        BulkWriteResult writeResult = BulkWriteResult.acknowledged(0, 0, 0, 0, List.of(), List.of());
        // Empty error labels so the exception is NOT a transient transaction error, matching a post-commit E11000.
        return new MongoBulkWriteException(writeResult, List.of(error), null, new ServerAddress("localhost", 27017), Set.of());
    }
}
