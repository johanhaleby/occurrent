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

package org.occurrent.example.broker.rabbitmq;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs both {@link RabbitMqCloudEventLevelBootstrap} and {@link RabbitMqDomainEventLevelBootstrap} exactly as
 * {@code main(String[])} would, against this module's real Testcontainers MongoDB and RabbitMQ rather than the
 * localhost ones an operator supplies, since it exists to catch a broken {@code start(...)} wiring in CI, not to
 * verify infrastructure. Both bootstraps use fixed database, queue and checkpoint names by design, so an operator
 * gets stable names to point tooling at, rather than the scratch names {@link AbstractBrokerExampleTest} generates
 * per method to keep the other tests isolated. A local rerun against reused containers therefore leaves both
 * bootstraps' state behind for the next run, which is harmless here since every order id is still fresh, and does
 * not happen in CI, where each run gets fresh containers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RabbitMqBrokerExampleBootstrapSmokeTest extends AbstractBrokerExampleTest {

    @Test
    void the_cloud_event_level_bootstrap_starts_and_completes_one_order() throws Exception {
        try (RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(20));
            assertThat(view.status()).isEqualTo("SHIPPED");
        }
    }

    @Test
    void the_domain_event_level_bootstrap_starts_and_completes_one_order() throws Exception {
        try (RabbitMqDomainEventLevelBootstrap app = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(20));
            assertThat(view.status()).isEqualTo("SHIPPED");
        }
    }

    /**
     * Proves the in-memory catch-up marker and the in-memory read model actually stay paired, the round-4 review's
     * catch. A stray durable marker beside an in-memory read model would have this order caught up once, by the
     * first {@code start(...)}, then silently skipped by the second, since the marker would already claim the
     * replay done. Both {@code start(...)} calls share this test's real Mongo, so the order is genuinely still
     * there for the second one to replay.
     */
    @Test
    void a_second_cloud_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped() throws Exception {
        OrderStatusProjection.OrderStatusView placedByFirstBoot;
        try (RabbitMqCloudEventLevelBootstrap firstBoot = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            placedByFirstBoot = firstBoot.placeAndShipOneOrder(Duration.ofSeconds(20));
        }

        try (RabbitMqCloudEventLevelBootstrap secondBoot = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView replayed = secondBoot.orderStatusViews().get(placedByFirstBoot.orderId());
            assertThat(replayed).isNotNull();
            assertThat(replayed.status()).isEqualTo("SHIPPED");
        }
    }

    /**
     * The domain-level twin of {@link #a_second_cloud_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped()}.
     */
    @Test
    void a_second_domain_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped() throws Exception {
        OrderStatusProjection.OrderStatusView placedByFirstBoot;
        try (RabbitMqDomainEventLevelBootstrap firstBoot = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            placedByFirstBoot = firstBoot.placeAndShipOneOrder(Duration.ofSeconds(20));
        }

        try (RabbitMqDomainEventLevelBootstrap secondBoot = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView replayed = secondBoot.orderStatusViews().get(placedByFirstBoot.orderId());
            assertThat(replayed).isNotNull();
            assertThat(replayed.status()).isEqualTo("SHIPPED");
        }
    }
}
