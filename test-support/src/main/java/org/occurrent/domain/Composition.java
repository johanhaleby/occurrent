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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class Composition {
    public static List<DomainEvent> chain(List<DomainEvent> initialDomainEvents, Function<List<DomainEvent>, List<DomainEvent>> f) {
        List<DomainEvent> newDomainEvents = f.apply(initialDomainEvents);

        ArrayList<DomainEvent> composedEvents = new ArrayList<>(initialDomainEvents);
        composedEvents.addAll(newDomainEvents);
        return Collections.unmodifiableList(composedEvents);
    }
}
