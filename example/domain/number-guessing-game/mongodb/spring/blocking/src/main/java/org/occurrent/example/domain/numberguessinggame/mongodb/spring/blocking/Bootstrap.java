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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.GameEvent;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.NumberGuessGameCloudEventConverter;
import org.occurrent.springboot.mongo.blocking.EnableOccurrent;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.retry.annotation.EnableRetry;

import java.net.URI;

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableRetry
@EnableMongoRepositories
@EnableRabbit
@EnableOccurrent
public class Bootstrap {
    private static final String NUMBER_GUESSING_GAME_TOPIC = "number-guessing-game";

    public static void main(String[] args) {
        SpringApplication.run(Bootstrap.class, args);
    }

    @Bean
    public CloudEventConverter<GameEvent> cloudEventConverter(ObjectMapper objectMapper) {
        return new NumberGuessGameCloudEventConverter(objectMapper, URI.create("urn:occurrent:domain:numberguessinggame"));
    }

    @Bean
    public MessageConverter amqpMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    TopicExchange numberGuessingGameTopic() {
        return new TopicExchange(NUMBER_GUESSING_GAME_TOPIC);
    }
}