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

package org.occurrent.example.domain.uno.es.spring.blocking

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.example.domain.uno.*
import org.occurrent.subscription.api.blocking.BlockingSubscription
import org.occurrent.subscription.api.blocking.Subscription
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.RedisOperations
import org.springframework.retry.RetryOperations


@Configuration
class ReportProgressWhenGameIsPlayed(
    private val subscriptions: BlockingSubscription, val cloudEventConverter: CloudEventConverter<Event>,
    private val redis: RedisOperations<String, String>, private val retryOperations: RetryOperations
) {
    private val log = loggerFor<ReportProgressWhenGameIsPlayed>()

    @Bean
    fun reportGameProgressDuringGamePlaySubscription(): Subscription {
        return subscriptions.subscribe("reportGameProgressDuringGamePlay",
            cloudEventConverter::toDomainEvent andThen { e: Event ->
                val turnCountForGame = if (e is CardPlayed) {
                    incrementAndGetTurnCountForGame(e.gameId, e.eventId)
                } else {
                    getTurnCountForGame(e.gameId)
                }
                ProgressTracker.trackProgress(log::info, e, turnCountForGame)
            })
    }

    // Note that we don't need a Redis transaction here since the method is both idempotent and order independent.
    // Order independence is actually not required either since the subscription guarantees the ordering of events already.
    private fun incrementAndGetTurnCountForGame(gameId: GameId, eventId: EventId): Int = retryOperations.execute<Int, Throwable> {
        redis.opsForSet().add(gameId.toString(), eventId.toString())
        getTurnCountForGame(gameId)
    }

    private fun getTurnCountForGame(gameId: GameId): Int = (redis.opsForSet().size(gameId.toString()) ?: 0L).toInt()
}