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

package org.occurrent.dsl.saga.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lives beside the Mongo tests and not in {@code saga-dsl-common}, since a {@code JsonCloudEventData} needs
 * {@code cloudevents-json-jackson}, which only this module has on its test classpath.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SkippedInputDataTest {

    private static final byte[] BYTES = "{\"a\":1}".getBytes(UTF_8);

    private static final class CustomData implements CloudEventData {
        @Override
        public byte[] toBytes() {
            return BYTES.clone();
        }
    }

    @Test
    void JsonCloudEventData_held_as_BytesCloudEventData_differs_and_the_reason_names_both_classes() {
        CloudEventData delivered = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.classDifference(delivered, BytesCloudEventData.wrap(BYTES)))
                .contains(JsonCloudEventData.class.getName())
                .contains(BytesCloudEventData.class.getName());
    }

    @Test
    void JsonCloudEventData_held_as_JsonCloudEventData_does_not_differ() {
        CloudEventData delivered = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.classDifference(delivered, JsonCloudEventData.wrap(new ObjectMapper().createObjectNode()))).isNull();
    }

    @Test
    void BytesCloudEventData_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.classDifference(BytesCloudEventData.wrap(BYTES), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.classDifference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void data_of_a_custom_class_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.classDifference(new CustomData(), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_PojoCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.classDifference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), PojoCloudEventData.wrap("x", s -> BYTES.clone()))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_JsonCloudEventData_differs() {
        CloudEventData held = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.classDifference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), held)).isNotNull();
    }
}
