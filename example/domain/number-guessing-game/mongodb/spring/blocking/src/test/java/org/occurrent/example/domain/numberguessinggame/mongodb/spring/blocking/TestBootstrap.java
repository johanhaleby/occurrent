/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.RabbitMQContainer;

@TestConfiguration(proxyBeanMethods = false)
public class TestBootstrap {

    @Bean
    @ServiceConnection(name = "rabbitmq")
    public RabbitMQContainer rabbitMQContainer() {
        return new RabbitMQContainer("rabbitmq:3.12.12");
    }


    @Bean
    @ServiceConnection(name = "mongodb")
    public static MongoDBContainer mongoDBContainer() {
        return new MongoDBContainer("mongo:7.0.9");
    }

    public static void main(String[] args) {
        SpringApplication.from(Bootstrap::main)
                .with(TestBootstrap.class)
                .run(args);
    }
}
