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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class Serialization {

    private final ObjectMapper objectMapper;
    private final URI source;

    public Serialization(ObjectMapper objectMapper, URI cloudEventSource) {
        this.objectMapper = objectMapper;
        this.source = cloudEventSource;
    }

    public GameEvent deserialize(CloudEvent c) {
        UUID eventId = UUID.fromString(c.getId());
        UUID gameId = UUID.fromString(requireNonNull(c.getSubject()));
        LocalDateTime time = requireNonNull(c.getTime()).toLocalDateTime();

        GameEvent event;
        if (Objects.equals(c.getType(), GuessingAttemptsExhausted.class.getSimpleName())) {
            event = new GuessingAttemptsExhausted(eventId, gameId, time);
        } else if (Objects.equals(c.getType(), NumberGuessingGameEnded.class.getSimpleName())) {
            event = new NumberGuessingGameEnded(eventId, gameId, time);
        } else if (Objects.equals(c.getType(), PlayerGuessedANumberThatWasTooBig.class.getSimpleName())) {
            Map<String, Object> data = deserializeData(c);
            UUID playerId = UUID.fromString((String) data.get("playerId"));
            int guessedNumber = (int) data.get("guessedNumber");
            event = new PlayerGuessedANumberThatWasTooBig(eventId, gameId, time, playerId, guessedNumber);
        } else if (Objects.equals(c.getType(), PlayerGuessedANumberThatWasTooSmall.class.getSimpleName())) {
            Map<String, Object> data = deserializeData(c);
            UUID playerId = UUID.fromString((String) data.get("playerId"));
            int guessedNumber = (int) data.get("guessedNumber");
            event = new PlayerGuessedANumberThatWasTooSmall(eventId, gameId, time, playerId, guessedNumber);
        } else if (Objects.equals(c.getType(), PlayerGuessedTheRightNumber.class.getSimpleName())) {
            Map<String, Object> data = deserializeData(c);
            UUID playerId = UUID.fromString((String) data.get("playerId"));
            int guessedNumber = (int) data.get("guessedNumber");
            event = new PlayerGuessedTheRightNumber(eventId, gameId, time, playerId, guessedNumber);
        } else if (Objects.equals(c.getType(), NumberGuessingGameWasStarted.class.getSimpleName())) {
            Map<String, Object> data = deserializeData(c);
            UUID startedBy = UUID.fromString((String) data.get("startedBy"));
            int secretNumberToGuess = (int) data.get("secretNumberToGuess");
            int maxNumberOfGuesses = (int) data.get("maxNumberOfGuesses");
            event = new NumberGuessingGameWasStarted(eventId, gameId, time, startedBy, secretNumberToGuess, maxNumberOfGuesses);
        } else {
            throw new IllegalStateException("Unrecognized event: " + c.getType());
        }

        return event;
    }

    private Map<String, Object> deserializeData(CloudEvent c) {
        try {
            return objectMapper.readValue(c.getData(), new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CloudEvent serialize(GameEvent e) {
        return CloudEventBuilder.v1()
                .withId(e.eventId().toString())
                .withSource(source)
                .withSubject(e.gameId().toString())
                .withType(e.getClass().getSimpleName())
                .withTime(e.timestamp().atOffset(UTC).truncatedTo(ChronoUnit.MILLIS))
                .withData(toBytes(e))
                .build();
    }

    private byte[] toBytes(GameEvent event) {
        final Map<String, Object> eventAsMap;
        if (event instanceof GuessingAttemptsExhausted || event instanceof NumberGuessingGameEnded) {
            eventAsMap = null;
        } else if (event instanceof NumberGuessingGameWasStarted) {
            NumberGuessingGameWasStarted e = (NumberGuessingGameWasStarted) event;
            eventAsMap = new HashMap<String, Object>() {{
                put("startedBy", e.startedBy().toString());
                put("secretNumberToGuess", e.secretNumberToGuess());
                put("maxNumberOfGuesses", e.maxNumberOfGuesses());
            }};
        } else if (event instanceof PlayerGuessedANumberThatWasTooBig) {
            PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) event;
            eventAsMap = new HashMap<String, Object>() {{
                put("playerId", e.playerId().toString());
                put("guessedNumber", e.guessedNumber());
            }};
        } else if (event instanceof PlayerGuessedANumberThatWasTooSmall) {
            PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) event;
            eventAsMap = new HashMap<String, Object>() {{
                put("playerId", e.playerId().toString());
                put("guessedNumber", e.guessedNumber());
            }};
        } else if (event instanceof PlayerGuessedTheRightNumber) {
            PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) event;
            eventAsMap = new HashMap<String, Object>() {{
                put("playerId", e.playerId().toString());
                put("guessedNumber", e.guessedNumber());
            }};
        } else {
            throw new IllegalArgumentException("Unrecognized event: " + event.getClass().getName());
        }

        if (eventAsMap == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(eventAsMap);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }
}