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

package org.occurrent.example.domain.courseenrollment

import org.springframework.boot.fromApplication
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.boot.with
import org.springframework.context.annotation.Bean
import org.testcontainers.mongodb.MongoDBContainer

/**
 * Runs the application locally with a MongoDB replica set started by Testcontainers, so you can click through the web
 * page without setting up a database. A replica set is required because DCB appends use multi-document transactions.
 *
 * Start [main] from the IDE, or run {@code mvn -Pexamples-module -pl :example-domain-course-enrollment spring-boot:test-run}.
 */
@TestConfiguration(proxyBeanMethods = false)
class TestBootstrap {

    @Bean
    @ServiceConnection
    fun mongoDbContainer(): MongoDBContainer = MongoDBContainer("mongo:4.2.8").withReplicaSet()
}

fun main(args: Array<String>) {
    fromApplication<Bootstrap>().with(TestBootstrap::class).run(*args)
}
