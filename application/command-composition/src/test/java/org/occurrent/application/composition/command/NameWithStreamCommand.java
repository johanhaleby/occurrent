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

package org.occurrent.application.composition.command;

import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.time.TimeConversion;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

class NameWithStreamCommand {

    static Stream<DomainEvent> defineName(Stream<DomainEvent> __, String eventId, LocalDateTime time, String userId, String name) {
        return Stream.of(new NameDefined(eventId, TimeConversion.toDate(time), userId, name));
    }


    static Stream<DomainEvent> changeName(Stream<DomainEvent> events, String eventId, LocalDateTime time, String userId, String newName) {
        Predicate<DomainEvent> isInstanceOfNameDefined = NameDefined.class::isInstance;
        Predicate<DomainEvent> isInstanceOfNameWasChanged = NameWasChanged.class::isInstance;

        String currentName = events
                .filter(isInstanceOfNameDefined.or(isInstanceOfNameWasChanged))
                .reduce("", (__, e) -> e.name(), (name1, name2) -> name2);

        if (Objects.equals(currentName, "John Doe")) {
            throw new IllegalArgumentException("Cannot change name from John Doe since this is the ultimate name");
        } else if (currentName.isEmpty()) {
            throw new IllegalArgumentException("Cannot change name since it is currently undefined");
        }
        return Stream.of(new NameWasChanged(eventId, TimeConversion.toDate(time), userId, newName));
    }
}