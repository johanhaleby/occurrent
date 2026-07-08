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

package org.occurrent.example.domain.appointmentscheduling.model;

import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.event.SlotDefined;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.DefineSlot;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

/**
 * Defines a bookable slot once.
 */
public final class Slot {
    private Slot() {
    }

    public enum State {NOT_DEFINED, DEFINED}

    public static final Decider<DefineSlot, State, DomainEvent> DECIDER = Decider.create(
            State.NOT_DEFINED,
            (command, state) -> {
                if (state == State.DEFINED) {
                    throw new IllegalStateException("Slot " + command.slotId() + " is already defined");
                }
                return List.of(new SlotDefined(UUID.randomUUID(), OffsetDateTime.now(UTC), command.slotId(), command.startTime()));
            },
            (state, event) -> event instanceof SlotDefined ? State.DEFINED : state);

    public static DcbCriteria criteria(DefineSlot command) {
        return DcbCriteria.type(SlotDefined.class.getSimpleName()).tags(Tags.slot(command.slotId()));
    }
}
