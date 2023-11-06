/*
 *
 *  Copyright 2023 Johan Haleby
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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import java.net.URI
import java.time.OffsetDateTime


typealias CloudEventId = String
typealias Subject = String
typealias ContentType = String

/**
 * Create a cloud event converter from Kotlin.
 *
 * See [cloud event documentation](https://occurrent.org/documentation#cloudevents) for info on what the cloud event attributes mean.<br><br>
 *
 * @param objectMapper The jackson object mapper to use
 * @param cloudEventSource The source of the cloud event
 * @param idMapper Extract the id to use from the event (default is a random UUID)
 * @param typeMapper Extract the type from the event (uses the fully-qualified name of the domain event class as cloud event type by default. You should definitely change this in production!)
 * @param subjectMapper Extract the subject from the event (defaults to `null`)
 * @param timeMapper Extract the time from the event (uses `OffsetDateTime.now(UTC)` by default)
 */
fun <E> jacksonCloudEventConverter(
    objectMapper: ObjectMapper, cloudEventSource: URI, idMapper: (E) -> CloudEventId = { e -> JacksonCloudEventConverter.defaultIdMapperFunction<E>().apply(e) },
    typeMapper: CloudEventTypeMapper<E> = JacksonCloudEventConverter.defaultTypeMapper(),
    timeMapper: (E) -> OffsetDateTime = { e -> JacksonCloudEventConverter.defaultTimeMapperFunction<E>().apply(e) },
    subjectMapper: (E) -> Subject? = { e -> JacksonCloudEventConverter.defaultSubjectMapperFunction<E>().apply(e) },
    contentType: ContentType = JacksonCloudEventConverter.DEFAULT_CONTENT_TYPE
): JacksonCloudEventConverter<E> {
    return JacksonCloudEventConverter.Builder<E>(objectMapper, cloudEventSource)
        .contentType(contentType)
        .idMapper(idMapper)
        .typeMapper(typeMapper)
        .subjectMapper(subjectMapper)
        .timeMapper(timeMapper)
        .build()
}