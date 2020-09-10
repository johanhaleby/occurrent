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

import io.cloudevents.CloudEvent;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionWithPositionPersistenceInMongoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.filter.Filter.*;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;

@Component
class WhenGameEndedThenPublishIntegrationEvent {
    private static final Logger log = LoggerFactory.getLogger(WhenGameEndedThenPublishIntegrationEvent.class);

    private final EventStoreQueries eventStoreQueries;
    private final Serialization serialization;
    private final SpringBlockingSubscriptionWithPositionPersistenceInMongoDB streamer;
    private final RabbitTemplate rabbitTemplate;
    private final TopicExchange numberGuessingGameTopic;

    WhenGameEndedThenPublishIntegrationEvent(EventStoreQueries eventStoreQueries, Serialization serialization,
                                             SpringBlockingSubscriptionWithPositionPersistenceInMongoDB streamer,
                                             RabbitTemplate rabbitTemplate, TopicExchange numberGuessingGameTopic) {
        this.eventStoreQueries = eventStoreQueries;
        this.serialization = serialization;
        this.streamer = streamer;
        this.rabbitTemplate = rabbitTemplate;
        this.numberGuessingGameTopic = numberGuessingGameTopic;
    }

    @PostConstruct
    void subscribeToSubscription() {
        streamer.subscribe(WhenGameEndedThenPublishIntegrationEvent.class.getSimpleName(), filter(type(NumberGuessingGameEnded.class.getSimpleName())), this::publishIntegrationEventWhenGameEnded
                // We're only interested in events of type NumberGuessingGameEnded since then we know that
                // we should publish the integration event
        )
                .waitUntilStarted(Duration.ofSeconds(4));
    }


    private void publishIntegrationEventWhenGameEnded(CloudEvent cloudEvent) {
        String gameId = cloudEvent.getSubject();
        NumberGuessingGameCompleted numberGuessingGameCompleted = eventStoreQueries.query(subject(eq(gameId)).and(streamVersion(lte(OccurrentExtensionGetter.getStreamVersion(cloudEvent)))))
                .map(serialization::deserialize)
                .collect(NumberGuessingGameCompleted::new, (integrationEvent, gameEvent) -> {
                    if (gameEvent instanceof NumberGuessingGameWasStarted) {
                        integrationEvent.setGameId(gameEvent.gameId().toString());
                        NumberGuessingGameWasStarted e = (NumberGuessingGameWasStarted) gameEvent;
                        integrationEvent.setSecretNumberToGuess(e.secretNumberToGuess());
                        integrationEvent.setMaxNumberOfGuesses(e.maxNumberOfGuesses());
                        integrationEvent.setStartedAt(toDate(e.timestamp()));
                    } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooSmall) {
                        PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) gameEvent;
                        integrationEvent.addGuess(new NumberGuessingGameCompleted.GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                    } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooBig) {
                        PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) gameEvent;
                        integrationEvent.addGuess(new NumberGuessingGameCompleted.GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                    } else if (gameEvent instanceof PlayerGuessedTheRightNumber) {
                        PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) gameEvent;
                        integrationEvent.addGuess(new NumberGuessingGameCompleted.GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
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