/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.application.converter.jackson


import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import java.net.URI

@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class JacksonCloudEventConverterKotlinTest {

    sealed interface BaseEvent;

    data class MyEvent(var someField: String? = null) : BaseEvent

    class MyCloudEventTypeMapper : CloudEventTypeMapper<BaseEvent> {
        override fun getCloudEventType(type: Class<out BaseEvent>): String {
            return "MyEventType"
        }

        @Suppress("UNCHECKED_CAST")
        override fun <E : BaseEvent> getDomainEventType(cloudEventType: String): Class<E> {
            return MyEvent::class.java as Class<E>
        }
    }

    /**
     * Makes sure that [issue 119](https://github.com/johanhaleby/occurrent/issues/119) is resolved.
     */
    @Test
    fun `issue 119 is resolved`() {
        // Given
        val objectMapper = ObjectMapper()
        val cloudEventConverter: JacksonCloudEventConverter<BaseEvent> =
            JacksonCloudEventConverter.Builder<BaseEvent>(objectMapper, URI.create("urn:myevents"))
                .typeMapper(MyCloudEventTypeMapper())
                .build()

        // When
        val data = cloudEventConverter.toCloudEvent(MyEvent("123"))

        // Then
        assertThat(cloudEventConverter.toDomainEvent(data)).isEqualTo(MyEvent("123"))
    }
}