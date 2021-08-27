/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.example.domain.rps.model

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.dsl.subscription.blocking.subscriptions
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.retry.RetryStrategy
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.util.concurrent.atomic.AtomicInteger


class ViewDemo {

    @Test
    fun `simple inmemory view`() {
        // Given
        val subscriptionModel = InMemorySubscriptionModel(RetryStrategy.none())
        val inMemoryEventStore = InMemoryEventStore(subscriptionModel)
        val cloudEventConverter = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConverter)

        val numberOfStartedGames = AtomicInteger()
        subscriptions(subscriptionModel, cloudEventConverter, { c -> c.qualifiedName!! }) {
            subscribe<GameCreated> {
                numberOfStartedGames.incrementAndGet()
            }
        }

        // When
        val gameId1 = GameId.random()
        val gameId2 = GameId.random()
        applicationService.execute(gameId1.value) { events: Sequence<GameEvent> ->
            handle(events, CreateGame(gameId1, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))
        }

        applicationService.execute(gameId2.value) { events: Sequence<GameEvent> ->
            handle(events, CreateGame(gameId2, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)))
        }

        // Then
        await untilAsserted {
            assertThat(numberOfStartedGames).hasValue(2)
        }
    }
}