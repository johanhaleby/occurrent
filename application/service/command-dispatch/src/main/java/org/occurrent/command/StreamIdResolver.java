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

package org.occurrent.command;

/**
 * Derives the target stream id from a command, the routing key a {@link CommandDispatcher} uses to pick the stream a
 * command's resulting events are appended to. It is the stream-side counterpart of the DCB {@code TagGenerator} (which
 * derives a tag-based boundary from an event); a stream write is keyed by a single stream id rather than a set of tags.
 * <p>
 * This is a {@code String}-returning {@link java.util.function.Function} with a name, so a plain lambda or method
 * reference such as {@code OrderCommand::orderId} is a valid resolver, and an annotation-driven implementation can be
 * injected as a bean by this type.
 *
 * @param <C> the command type
 */
@FunctionalInterface
public interface StreamIdResolver<C> {

    /**
     * The id of the stream that {@code command}'s resulting events are appended to.
     *
     * @param command the command to route
     * @return the target stream id, never {@code null} or blank
     */
    String streamId(C command);
}
