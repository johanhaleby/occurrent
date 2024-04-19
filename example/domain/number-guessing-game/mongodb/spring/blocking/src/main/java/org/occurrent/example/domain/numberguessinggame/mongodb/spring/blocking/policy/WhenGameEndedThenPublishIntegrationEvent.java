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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.policy;

import org.occurrent.annotation.Subscription;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.policy.NumberGuessingGameCompleted.GuessedNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.filter.Filter.streamVersion;
import static org.occurrent.filter.Filter.subject;

@Component
class WhenGameEndedThenPublishIntegrationEvent {
    private static final Logger log = LoggerFactory.getLogger(WhenGameEndedThenPublishIntegrationEvent.class);

    private final DomainEventQueries<GameEvent> domainEventQueries;
    private final RabbitTemplate rabbitTemplate;
    private final TopicExchange numberGuessingGameTopic;

    WhenGameEndedThenPublishIntegrationEvent(DomainEventQueries<GameEvent> domainEventQueries, RabbitTemplate rabbitTemplate, TopicExchange numberGuessingGameTopic) {
        this.domainEventQueries = domainEventQueries;
        this.rabbitTemplate = rabbitTemplate;
        this.numberGuessingGameTopic = numberGuessingGameTopic;
    }

    @Subscription(id = "WhenGameEndedThenPublishIntegrationEvent")
    void publishIntegrationEventWhenGameEnded(EventMetadata metadata, NumberGuessingGameEnded numberGuessingGameEnded) {
        String gameId = numberGuessingGameEnded.gameId().toString();
        long streamVersion = metadata.getStreamVersion();
        NumberGuessingGameCompleted numberGuessingGameCompleted = domainEventQueries.query(subject(eq(gameId)).and(streamVersion(lte(streamVersion))))
                .collect(NumberGuessingGameCompleted::new, (integrationEvent, gameEvent) -> {
                    if (gameEvent instanceof NumberGuessingGameWasStarted e) {
                        integrationEvent.setGameId(gameEvent.gameId().toString());
                        integrationEvent.setSecretNumberToGuess(e.secretNumberToGuess());
                        integrationEvent.setMaxNumberOfGuesses(e.maxNumberOfGuesses());
                        integrationEvent.setStartedAt(toDate(e.timestamp()));
                    } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooSmall e) {
                        integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                    } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooBig e) {
                        integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                    } else if (gameEvent instanceof PlayerGuessedTheRightNumber e) {
                        integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                        integrationEvent.setRightNumberWasGuessed(true);
                    } else if (gameEvent instanceof GuessingAttemptsExhausted) {
                        integrationEvent.setRightNumberWasGuessed(false);
                    } else if (gameEvent instanceof NumberGuessingGameEnded) {
                        integrationEvent.setEndedAt(toDate(gameEvent.timestamp()));
                    }
                }, (i1, i2) -> {
                });

        log.info("Publishing integration event {} to {}", NumberGuessingGameCompleted.class.getSimpleName(), numberGuessingGameTopic.getName());
        log.debug(numberGuessingGameCompleted.toString());

        rabbitTemplate.convertAndSend(numberGuessingGameTopic.getName(), "number-guessing-game.completed", numberGuessingGameCompleted);
    }

    private static Date toDate(LocalDateTime ldt) {
        if (ldt == null) {
            return null;
        }
        return Date.from(ldt.toInstant(UTC));
    }
}