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

/**
 * An end-to-end broker example. A stored event is forwarded to RabbitMQ by {@code CloudEventForwarder}, consumed
 * back by a bridge, and delivered to a {@code @Projection(source = PUSH)}, at the CloudEvent level and at the
 * domain level.
 */
@NullMarked
package org.occurrent.example.broker.rabbitmq;

import org.jspecify.annotations.NullMarked;
