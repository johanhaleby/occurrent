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

package org.occurrent.cloudevents;


import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.ZonedDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;

@ExtendWith(SoftAssertionsExtension.class)
class OccurrentExtensionRemoverTest {

    @Test
    void removes_all_occurrent_extensions(SoftAssertions softly) {
        // Given
        CloudEvent originalCloudEvent = new CloudEventBuilder()
                .withId("id")
                .withTime(ZonedDateTime.now())
                .withDataSchema(URI.create("urn:schema"))
                .withSource(URI.create("urn:test"))
                .withSubject("subject")
                .withType("type")
                .withData("text/plain", "hello".getBytes(UTF_8))
                .build();

        CloudEvent occurrentCloudEvent = new CloudEventBuilder(originalCloudEvent)
                .withExtension(new OccurrentCloudEventExtension("streamId", 1))
                .build();

        // When
        CloudEvent removedExtensionsFromOccurrentCloudEvent = OccurrentExtensionRemover.removeOccurrentExtensions(occurrentCloudEvent);

        // Then

        // Unfortunately "assertThat(removedExtensionsFromOccurrentCloudEvent).isEqualTo(originalCloudEvent);" doesn't work, see https://github.com/cloudevents/sdk-java/issues/215
        OccurrentCloudEventExtension.KEYS.forEach(occurrentExtensionKey -> {
            softly.assertThat(removedExtensionsFromOccurrentCloudEvent.getExtension(occurrentExtensionKey)).isNull();
        });
    }
}