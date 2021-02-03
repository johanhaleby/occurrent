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
import com.mongodb.WriteError;
import org.occurrent.eventstore.api.DuplicateCloudEventException;

import java.net.URI;

/**
 * Translates a {@link MongoBulkWriteException} to a {@link DuplicateCloudEventException}.
 */
public class MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator {

    /**
     * Translates a {@link MongoBulkWriteException} to a {@link DuplicateCloudEventException}.
     *
     * @param e The {@code MongoBulkWriteException} to translate
     * @return The resulting {@code DuplicateCloudEventException}
     */
    public static DuplicateCloudEventException translateToDuplicateCloudEventException(MongoBulkWriteException e) {
        return e.getWriteErrors().stream()
                .filter(error -> error.getCode() == 11000)
                .map(WriteError::getMessage)
                .filter(errorMessage -> errorMessage.contains("{ id: \"") && errorMessage.contains(", source: \""))
                .map(errorMessage -> {
                    int idKeyStartIndex = errorMessage.indexOf("{ id: \"");
                    int idValueStartIndex = idKeyStartIndex + "{ id: \"".length();
                    int idValueEndIndex = errorMessage.indexOf("\"", idValueStartIndex);
                    String id = errorMessage.substring(idValueStartIndex, idValueEndIndex);

                    int sourceKeyStartIndex = errorMessage.indexOf(", source: \"", idValueEndIndex);
                    int sourceValueStartIndex = sourceKeyStartIndex + ", source: \"".length();
                    int sourceValueEndIndex = errorMessage.indexOf("\" }", sourceValueStartIndex);
                    String source = errorMessage.substring(sourceValueStartIndex, sourceValueEndIndex);

                    return new DuplicateCloudEventException(id, URI.create(source), e.getMessage().trim(), e);
                })
                .findFirst()
                .orElse(new DuplicateCloudEventException(null, null, e.getMessage().trim(), e));
    }
}