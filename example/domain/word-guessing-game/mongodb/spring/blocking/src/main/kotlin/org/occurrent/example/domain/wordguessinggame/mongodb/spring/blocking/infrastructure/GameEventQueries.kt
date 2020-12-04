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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.eventstore.api.blocking.queryForSequence
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.filter.Filter
import org.springframework.stereotype.Component


/**
 * Simple wrapper around [EventStoreQueries] that provides an interface that deals with game events instead of cloud events
 */
@Component
class GameEventQueries(private val eventStoreQueries: EventStoreQueries, private val cloudEventConverter: CloudEventConverter<GameEvent>) {

    @Suppress("UNCHECKED_CAST")
    fun <T : GameEvent> query(filter: Filter): Sequence<T> =
        eventStoreQueries.queryForSequence(filter)
            .map(cloudEventConverter::toDomainEvent)
            .map { it as T }

    fun <T : GameEvent> queryOne(filter: Filter): T = query<T>(filter).first()
}