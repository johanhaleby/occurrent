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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;

import java.net.URI;

/**
 * Translates a {@link MongoBulkWriteException} to a {@link DuplicateCloudEventException}.
 */
public class MongoExceptionTranslator {

    /**
     * Translates a {@link MongoBulkWriteException} to a {@link DuplicateCloudEventException}.
     *
     * @param e The {@code MongoBulkWriteException} to translate
     * @return The resulting {@code DuplicateCloudEventException}
     */
    public static RuntimeException translateException(WriteContext ctx, MongoException e) {
        final RuntimeException runtimeException;
        if (e instanceof MongoBulkWriteException) {
            MongoBulkWriteException mongoBulkWriteException = (MongoBulkWriteException) e;
            runtimeException = mongoBulkWriteException.getWriteErrors().stream()
                    .filter(bulkWriteError -> ErrorCategory.fromErrorCode(bulkWriteError.getCode()) == ErrorCategory.DUPLICATE_KEY)
                    .map(bulkWriteError -> translateToDuplicateCloudEventException(mongoBulkWriteException, bulkWriteError))
                    .findFirst()
                    .map(RuntimeException.class::cast)
                    .orElse(e);
        } else if (e instanceof MongoCommandException && e.getCode() == 112) {
            // See https://github.com/johanhaleby/occurrent/issues/85
            // We increase version by 1 since this error only happens when two or more clients write to the same stream at the same time
            // while also have read the same previous event stream version. This means that one of these write "have won" and the
            // version has increased by at least one.
            long eventStreamVersion = ctx.eventStreamVersion + 1;
            runtimeException = new WriteConditionNotFulfilledException(ctx.eventStreamId, eventStreamVersion, ctx.writeCondition,
                    String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), ctx.writeCondition.toString(), eventStreamVersion));
        } else {
            runtimeException = e;
        }
        return runtimeException;
    }

    private static DuplicateCloudEventException translateToDuplicateCloudEventException(MongoBulkWriteException e, BulkWriteError bulk) {
        final DuplicateCloudEventException translatedException;
        String errorMessage = bulk.getMessage();
        if (errorMessage.contains("{ id: \"") && errorMessage.contains(", source: \"")) {
            int idKeyStartIndex = errorMessage.indexOf("{ id: \"");
            int idValueStartIndex = idKeyStartIndex + "{ id: \"".length();
            int idValueEndIndex = errorMessage.indexOf("\"", idValueStartIndex);
            String id = errorMessage.substring(idValueStartIndex, idValueEndIndex);

            int sourceKeyStartIndex = errorMessage.indexOf(", source: \"", idValueEndIndex);
            int sourceValueStartIndex = sourceKeyStartIndex + ", source: \"".length();
            int sourceValueEndIndex = errorMessage.indexOf("\" }", sourceValueStartIndex);
            String source = errorMessage.substring(sourceValueStartIndex, sourceValueEndIndex);

            translatedException = new DuplicateCloudEventException(id, URI.create(source), e.getMessage().trim(), e);
        } else {
            translatedException = new DuplicateCloudEventException(null, null, e.getMessage().trim(), e);
        }
        return translatedException;
    }

    public static class WriteContext {
        public final String eventStreamId;
        public final long eventStreamVersion;
        public final WriteCondition writeCondition;

        public WriteContext(String eventStreamId, long eventStreamVersion, WriteCondition writeCondition) {
            this.eventStreamId = eventStreamId;
            this.eventStreamVersion = eventStreamVersion;
            this.writeCondition = writeCondition;
        }
    }
}