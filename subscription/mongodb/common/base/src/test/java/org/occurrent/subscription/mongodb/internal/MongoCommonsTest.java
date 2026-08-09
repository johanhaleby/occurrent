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

import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoCommonsTest {

    @Test
    void extract_write_version_returns_empty_when_document_is_null() {
        OptionalLong result = MongoCommons.extractWriteVersion(null);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_empty_when_key_is_absent() {
        Document document = new Document("_id", "subscription-1");

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_empty_when_value_is_null() {
        Document document = new Document("_id", "subscription-1")
                .append(MongoCommons.WRITE_VERSION, null);

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_the_value_when_present() {
        Document document = new Document("_id", "subscription-1")
                .append(MongoCommons.WRITE_VERSION, 42L);

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).hasValue(42L);
    }
}
