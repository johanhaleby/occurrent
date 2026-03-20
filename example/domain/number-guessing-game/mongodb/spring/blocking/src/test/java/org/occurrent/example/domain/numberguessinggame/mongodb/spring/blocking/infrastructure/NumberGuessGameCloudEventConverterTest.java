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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

class NumberGuessGameCloudEventConverterTest {

    @Test
    void preserves_event_shape_and_truncates_timestamp_to_millis() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        NumberGuessGameCloudEventConverter converter = new NumberGuessGameCloudEventConverter(objectMapper, URI.create("urn:number-guessing-game"));

        UUID eventId = UUID.fromString("11111111-2222-3333-4444-555555555555");
        UUID gameId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        UUID startedBy = UUID.fromString("99999999-8888-7777-6666-555555555555");
        LocalDateTime timestamp = LocalDateTime.of(2024, 2, 3, 4, 5, 6, 987_654_321);
        NumberGuessingGameWasStarted event = new NumberGuessingGameWasStarted(eventId, gameId, timestamp, startedBy, 42, 3);
        NumberGuessingGameWasStarted expectedRoundTrippedEvent = new NumberGuessingGameWasStarted(eventId, gameId, timestamp.withNano(987_000_000), startedBy, 42, 3);

        CloudEvent cloudEvent = converter.toCloudEvent(event);
        Map<String, Object> data = objectMapper.readValue(cloudEvent.getData().toBytes(), new TypeReference<>() {});

        assertThat(cloudEvent.getId()).isEqualTo(eventId.toString());
        assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:number-guessing-game"));
        assertThat(cloudEvent.getType()).isEqualTo(NumberGuessingGameWasStarted.class.getSimpleName());
        assertThat(cloudEvent.getSubject()).isEqualTo(gameId.toString());
        assertThat(cloudEvent.getTime()).isEqualTo(timestamp.atOffset(UTC).withNano(987_000_000));
        assertThat(cloudEvent.getDataContentType()).isEqualTo("application/json");
        assertThat(data).containsEntry("startedBy", startedBy.toString());
        assertThat(data).containsEntry("secretNumberToGuess", 42);
        assertThat(data).containsEntry("maxNumberOfGuesses", 3);
        assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(expectedRoundTrippedEvent);
    }
}
