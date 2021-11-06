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

package org.occurrent.application.converter.typemapper

import com.fasterxml.jackson.databind.ObjectMapper
import io.cloudevents.core.v1.CloudEventBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.net.URI
import java.util.*


class CustomTypeMapperTest {

    @Test
    fun `custom type mapper works`() {
        // Given
        val objectMapper = ObjectMapper()
        val expectedEvent = NameDefined("eventId", Date(), "Name")
        val customTypeMapper = CustomTypeMapper()


        // When
        val cloudEvent = CloudEventBuilder()
            .withId(UUID.randomUUID().toString())
            .withType(customTypeMapper[NameDefined::class])
            .withSource(URI.create("uri:mysource"))
            .withData(objectMapper.writeValueAsBytes(expectedEvent))
            .build()

        val actualEvent = objectMapper.readValue(cloudEvent.data!!.toBytes(), customTypeMapper.getDomainEventType(cloudEvent.type))

        // Then
        assertThat(actualEvent).isEqualTo(expectedEvent)
    }


    class CustomTypeMapper : CloudEventTypeMapper<DomainEvent> {
        override fun getCloudEventType(type: Class<out DomainEvent>): String = type.simpleName

        @Suppress("UNCHECKED_CAST")
        override fun <E : DomainEvent> getDomainEventType(cloudEventType: String): Class<E> = when (cloudEventType) {
            NameDefined::class.simpleName -> NameDefined::class
            NameWasChanged::class.simpleName -> NameDefined::class
            else -> throw IllegalStateException("Event type $cloudEventType is unknown")
        }.java as Class<E>
    }
}