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

package org.occurrent.dsl.snapshot.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("a marker version written by a default Jackson 2 object mapper")
class MarkerVersionJacksonTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void reads_back_equal_when_absent() {
        assertThat(roundTripFailureOf(MarkerVersion.absent())).isNull();
        assertThat(roundTrip(MarkerVersion.absent())).isEqualTo(MarkerVersion.absent());
    }

    @Test
    void reads_back_equal_when_present() {
        MarkerVersion present = new MarkerVersion("epoch-1", 3, 1);

        assertThat(roundTripFailureOf(present)).isNull();
        assertThat(roundTrip(present)).isEqualTo(present);
    }

    private String roundTripFailureOf(MarkerVersion version) {
        try {
            objectMapper.readValue(objectMapper.writeValueAsString(version), MarkerVersion.class);
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private MarkerVersion roundTrip(MarkerVersion version) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(version), MarkerVersion.class);
        } catch (Exception e) {
            return null;
        }
    }
}
