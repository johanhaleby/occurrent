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

package org.occurrent.domain;

import org.occurrent.time.TimeConversion;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class Name {

    public static List<DomainEvent> defineName(String eventId, LocalDateTime time, String name) {
        return Collections.singletonList(new NameDefined(eventId, TimeConversion.toDate(time), name));
    }

    public static List<DomainEvent> changeName(List<DomainEvent> events, String eventId, LocalDateTime time, String newName) {
        Predicate<DomainEvent> isInstanceOfNameDefined = NameDefined.class::isInstance;
        Predicate<DomainEvent> isInstanceOfNameWasChanged = NameWasChanged.class::isInstance;

        String currentName = events.stream()
                .filter(isInstanceOfNameDefined.or(isInstanceOfNameWasChanged))
                .reduce("", (__, e) -> e instanceof NameDefined ? e.getName() : e.getName(), (name1, name2) -> name2);

        if (Objects.equals(currentName, "John Doe")) {
            throw new IllegalArgumentException("Cannot change name from John Doe since this is the ultimate name");
        } else if (currentName.isEmpty()) {
            throw new IllegalArgumentException("Cannot change name this it is currently undefined");
        }
        return Collections.singletonList(new NameWasChanged(eventId, TimeConversion.toDate(time), newName));
    }
}