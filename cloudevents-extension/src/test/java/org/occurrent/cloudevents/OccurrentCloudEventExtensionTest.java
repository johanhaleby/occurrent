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

package org.occurrent.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.OffsetDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class OccurrentCloudEventExtensionTest {

    private static CloudEvent baseEvent() {
        return new CloudEventBuilder()
                .withId("id")
                .withTime(OffsetDateTime.now())
                .withSource(URI.create("urn:test"))
                .withType("type")
                .withData("text/plain", "hello".getBytes(UTF_8))
                .build();
    }

    @Test
    void preserveAppendId_copies_the_original_append_id_onto_the_updated_event() {
        CloudEvent original = new CloudEventBuilder(baseEvent())
                .withExtension(OccurrentCloudEventExtension.APPEND_ID, "7c2f6b8e-3f9d-4b7a-9c9d-9f2e9c7b8e3f")
                .build();
        CloudEvent updated = baseEvent();

        CloudEvent result = OccurrentCloudEventExtension.preserveAppendId(original, updated);

        assertThat(OccurrentCloudEventExtension.getAppendId(result)).isEqualTo("7c2f6b8e-3f9d-4b7a-9c9d-9f2e9c7b8e3f");
    }

    @Test
    void preserveAppendId_strips_an_append_id_the_updated_event_picked_up_when_the_original_had_none() {
        CloudEvent original = baseEvent();
        CloudEvent updated = new CloudEventBuilder(baseEvent())
                .withExtension(OccurrentCloudEventExtension.APPEND_ID, "7c2f6b8e-3f9d-4b7a-9c9d-9f2e9c7b8e3f")
                .build();

        CloudEvent result = OccurrentCloudEventExtension.preserveAppendId(original, updated);

        assertThat(OccurrentCloudEventExtension.getAppendId(result)).isNull();
    }
}
