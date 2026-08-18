/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppendIdTest {

    @Test
    void from_reads_the_append_id_extension_metadata_has() {
        UUID id = UUID.randomUUID();
        EventMetadata metadata = new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, id.toString()));

        assertThat(AppendId.from(metadata)).contains(AppendId.of(id));
    }

    @Test
    void from_returns_empty_when_metadata_has_no_append_id_extension() {
        EventMetadata metadata = new EventMetadata(Map.of());

        assertThat(AppendId.from(metadata)).isEmpty();
    }
}
