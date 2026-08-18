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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CloudEventSinkTest {

    @Test
    void the_default_batch_publish_publishes_each_event_one_at_a_time_in_order() {
        FakeCloudEventSink sink = new FakeCloudEventSink();
        CloudEvent first = event("first");
        CloudEvent second = event("second");

        sink.publish(List.of(first, second));

        assertThat(sink.published()).containsExactly(first, second);
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("SomethingHappened")
                .build();
    }
}
