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

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables the Kafka broker auto-configuration, the same {@code @Import}-based activation {@code EnableOccurrent}
 * uses for the MongoDB starter. Imports through {@link OccurrentKafkaBrokerImportSelector} rather than the
 * configuration class directly, so the domain sink's {@code @ConditionalOnBean(CloudEventConverter.class)} is
 * evaluated after every regular configuration class in the context, the application's own
 * {@code CloudEventConverter} bean included, is registered.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(OccurrentKafkaBrokerImportSelector.class)
public @interface EnableOccurrentKafkaBroker {
}
