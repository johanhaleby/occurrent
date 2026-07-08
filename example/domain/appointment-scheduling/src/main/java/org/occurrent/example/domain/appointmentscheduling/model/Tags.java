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

import org.occurrent.eventstore.api.dcb.Tag;

import java.util.UUID;

/**
 * The DCB tags for the three entities. The keys match the {@code @DcbTag} keys on the events, so a criterion
 * built here selects the events the annotation tag generator produced.
 */
public final class Tags {
    private Tags() {
    }

    public static Tag clinician(UUID clinicianId) {
        return Tag.of("clinician", clinicianId.toString());
    }

    public static Tag patient(UUID patientId) {
        return Tag.of("patient", patientId.toString());
    }

    public static Tag slot(UUID slotId) {
        return Tag.of("slot", slotId.toString());
    }
}
