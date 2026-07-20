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

package org.occurrent.springboot.mongo.blocking;

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.springboot.mongo.blocking.SagaAnnotationMongoTest.CancelOrder;
import org.occurrent.springboot.mongo.blocking.SagaAnnotationMongoTest.OrderEvent;
import org.occurrent.springboot.mongo.blocking.SagaAnnotationMongoTest.OrderPlaced;
import org.occurrent.springboot.mongo.blocking.SagaAnnotationMongoTest.RecordingDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the opt-out: with {@code occurrent.saga.competing-consumer.enabled=false} the timer poller is ungated (runs on
 * every instance) so no {@code saga-timer:} lease is taken, while the saga still fires its timeout. Reuses the application
 * and domain from {@link SagaAnnotationMongoTest}. Docker-based.
 */
@DisplayName("Saga timer poller with competing-consumer gating disabled")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SagaAnnotationMongoTest.SagaApplication.class,
        properties = {
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-annotation-test",
                "occurrent.saga.timer-poll-interval=150ms",
                "occurrent.saga.competing-consumer.enabled=false"
        }
)
@Import(SagaAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(90)
class SagaCompetingConsumerDisabledMongoTest {

    @Autowired
    private ApplicationService<OrderEvent> applicationService;
    @Autowired
    private RecordingDispatcher recordingDispatcher;
    @Autowired
    private MongoOperations mongoOperations;

    @Test
    void fires_a_timeout_without_taking_a_saga_timer_lease() {
        applicationService.execute("order-disabled", events -> List.of(new OrderPlaced("order-disabled")));

        // The poller still fires the payment timeout, it just runs on every instance without coordination.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingDispatcher.issued).contains(new CancelOrder("order-disabled")));
        // With gating off the poller never registers a lease, so no saga-timer: document exists.
        assertThat(mongoOperations.getCollection("competing-consumer-locks").countDocuments(new Document("_id", "saga-timer:order-fulfillment"))).isZero();
    }
}
