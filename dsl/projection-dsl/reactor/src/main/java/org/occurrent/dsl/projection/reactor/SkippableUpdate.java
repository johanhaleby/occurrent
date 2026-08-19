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

package org.occurrent.dsl.projection.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.cloudevents.EventMetadata;
import reactor.core.publisher.Mono;

/**
 * A reactive update that can legitimately do nothing for an event, {@link CoalescingMaterializedUpdate} when its id
 * mapper resolves an event to no key. {@link RecordingReactiveUpdate} checks for this so it never records an append
 * id for an event the delegate silently skipped, package-private because only a delegate this wrapper ships
 * alongside can report it honestly.
 */
@NullMarked
interface SkippableUpdate<E> {

    /** As the plain update {@code BiFunction}, resolving to whether the event actually changed state. */
    Mono<Boolean> applyReportingWhetherApplied(EventMetadata metadata, E event);
}
