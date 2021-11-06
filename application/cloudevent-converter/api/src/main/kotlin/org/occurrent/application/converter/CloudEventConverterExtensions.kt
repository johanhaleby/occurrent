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

package org.occurrent.application.converter

import io.cloudevents.CloudEvent
import kotlin.reflect.KClass

typealias CloudEventType = String

/**
 * Get the cloud event type for a specific domain event type
 */
operator fun <T : Any> CloudEventConverter<T>.get(type: KClass<out T>): CloudEventType = getCloudEventType(type.java)


/**
 * Get the cloud event type for a specific domain event type
 */
operator fun <T : Any> CloudEventConverter<T>.get(type: Class<out T>): CloudEventType = getCloudEventType(type)

/**
 * Convert the domain event into a cloud event
 */
operator fun <T : Any> CloudEventConverter<in T>.get(domainEvent: T): CloudEvent = toCloudEvent(domainEvent)

/**
 * Convert the cloud event into a domain event
 */
@Suppress("UNCHECKED_CAST")
operator fun <T : Any> CloudEventConverter<in T>.get(cloudEvent: CloudEvent): T = toDomainEvent(cloudEvent) as T