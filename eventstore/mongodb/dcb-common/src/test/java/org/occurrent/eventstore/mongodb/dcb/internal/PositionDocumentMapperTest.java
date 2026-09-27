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

package org.occurrent.eventstore.mongodb.dcb.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.math.BigDecimal;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionDocumentMapperTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("a")
            .withSource(URI.create("urn:test"))
            .withType("Defined")
            .build();

    @Test
    void a_decimal_position_above_two_to_the_53_reads_back_as_the_same_whole_number() {
        // requireRepairedEvents accepts this value, so a read must not turn it into its neighbour
        CloudEvent read = PositionDocumentMapper.reattachPosition(EVENT, new Decimal128(new BigDecimal("9007199254740993")));

        assertThat(OccurrentCloudEventExtension.getPosition(read)).isEqualTo(9007199254740993L);
    }

    @Test
    void one_below_the_largest_long_stored_as_a_decimal_reads_back_as_itself() {
        // Through a double this rounds up to 2^63, which a cast to long then clamps to the largest long
        CloudEvent read = PositionDocumentMapper.reattachPosition(EVENT, new Decimal128(Long.MAX_VALUE - 1));

        assertThat(OccurrentCloudEventExtension.getPosition(read)).isEqualTo(Long.MAX_VALUE - 1);
    }
}
