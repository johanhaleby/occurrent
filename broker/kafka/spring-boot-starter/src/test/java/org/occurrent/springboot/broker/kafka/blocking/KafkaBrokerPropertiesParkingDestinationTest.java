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

package org.occurrent.springboot.broker.kafka.blocking;

import org.junit.jupiter.api.Test;
import org.occurrent.broker.kafka.blocking.KafkaDestination;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A blank {@code topic} used to reach {@code parkingDestination(...)} as a configured destination, so
 * {@code PARK} built successfully and the invalid topic was only discovered once the first failed delivery tried
 * to park there. {@link KafkaBrokerProperties.ParkingDestination#toDestination()} normalizes blank to absent so
 * the underlying builder's own missing-parking-destination refusal fires at construction time instead.
 */
class KafkaBrokerPropertiesParkingDestinationTest {

    @Test
    void a_blank_topic_is_treated_as_absent() {
        KafkaBrokerProperties.ParkingDestination parkingDestination = new KafkaBrokerProperties.ParkingDestination();
        parkingDestination.setTopic("   ");

        assertThat(parkingDestination.toDestination()).isEmpty();
    }

    @Test
    void a_null_topic_is_treated_as_absent() {
        KafkaBrokerProperties.ParkingDestination parkingDestination = new KafkaBrokerProperties.ParkingDestination();

        assertThat(parkingDestination.toDestination()).isEmpty();
    }

    @Test
    void a_configured_topic_is_returned() {
        KafkaBrokerProperties.ParkingDestination parkingDestination = new KafkaBrokerProperties.ParkingDestination();
        parkingDestination.setTopic("parking-topic");

        assertThat(parkingDestination.toDestination()).contains(KafkaDestination.of("parking-topic"));
    }
}
