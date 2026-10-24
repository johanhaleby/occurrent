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

import org.bson.BsonTimestamp;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class OperationTimeLiveStartTest {

    private static final BsonTimestamp T0 = new BsonTimestamp(1_700_000_000, 7);

    @Test
    void a_formatted_operation_time_parses_back_to_the_same_timestamp() {
        String liveStart = OperationTimeLiveStart.format(T0);

        assertThat(OperationTimeLiveStart.parse(liveStart)).isEqualTo(T0);
        assertThat(OperationTimeLiveStart.tryParse(liveStart)).contains(T0);
    }

    @Test
    void the_live_start_after_a_read_is_one_increment_after_the_operation_time_of_the_read() {
        String liveStart = OperationTimeLiveStart.liveStartAfterRead(T0);

        assertThat(OperationTimeLiveStart.parse(liveStart)).isEqualTo(new BsonTimestamp(T0.getTime(), T0.getInc() + 1));
    }

    @Test
    void the_live_start_after_a_read_is_the_same_string_as_formatting_one_increment_after_it() {
        assertThat(OperationTimeLiveStart.liveStartAfterRead(T0)).isEqualTo(OperationTimeLiveStart.formatOneIncrementAfter(T0));
    }

    @Test
    void one_increment_after_keeps_the_seconds_and_adds_one_to_the_ordinal() {
        BsonTimestamp after = OperationTimeLiveStart.oneIncrementAfter(T0);

        assertThat(after.getTime()).isEqualTo(T0.getTime());
        assertThat(after.getInc()).isEqualTo(T0.getInc() + 1);
        assertThat(after.compareTo(T0)).isPositive();
    }

    @Test
    void a_read_that_reported_no_operation_time_gives_no_live_start() {
        assertThatThrownBy(() -> OperationTimeLiveStart.liveStartAfterRead(null))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("MongoDB reported no operation time");
    }

    @Test
    void a_resume_token_is_not_an_operation_time() {
        assertThat(OperationTimeLiveStart.tryParse("{\"resumeToken\": {\"_data\": \"8265\"}}")).isEmpty();
    }

    @Test
    void text_that_is_not_json_is_not_an_operation_time() {
        String notJson = "operationTime but not json";

        assertThatCode(() -> OperationTimeLiveStart.tryParse(notJson))
                .as("tryParse must not throw for text that is not JSON")
                .doesNotThrowAnyException();
        assertThat(OperationTimeLiveStart.tryParse(notJson)).isEmpty();
    }

    @Test
    void a_document_whose_operation_time_is_not_a_timestamp_is_not_an_operation_time() {
        String notATimestamp = "{\"operationTime\": \"yesterday\"}";

        assertThatCode(() -> OperationTimeLiveStart.tryParse(notATimestamp))
                .as("tryParse must not throw for an operationTime that is not a timestamp")
                .doesNotThrowAnyException();
        assertThat(OperationTimeLiveStart.tryParse(notATimestamp)).isEmpty();
    }
}
