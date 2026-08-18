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

import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DomainEventSinkTest {

    @Test
    void the_default_batch_publish_publishes_each_domain_event_one_at_a_time_in_order() {
        FakeDomainEventSink<String> sink = new FakeDomainEventSink<>();

        sink.publish(List.of("first", "second"));

        assertThat(sink.published()).extracting(FakeDomainEventSink.Published::domainEvent).containsExactly("first", "second");
    }

    @Test
    void publishing_without_metadata_arrives_with_empty_metadata() {
        FakeDomainEventSink<String> sink = new FakeDomainEventSink<>();

        sink.publish("first");

        assertThat(sink.published()).hasSize(1);
        assertThat(sink.published().get(0).metadata()).isEqualTo(EventMetadata.empty());
    }
}
