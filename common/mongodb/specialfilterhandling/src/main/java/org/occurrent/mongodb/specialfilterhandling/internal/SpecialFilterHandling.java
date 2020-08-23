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

package org.occurrent.mongodb.specialfilterhandling.internal;

import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.sql.Date;
import java.time.ZonedDateTime;

import static org.occurrent.filter.Filter.TIME;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.mongodb.timerepresentation.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Some filters need to be treated specially, for example they may be dependent on the EventStore configuration.
 */
public class SpecialFilterHandling {

    @SuppressWarnings("unchecked")
    public static Condition<?> resolveSpecialCases(TimeRepresentation timeRepresentation, SingleConditionFilter scf) {
        if (TIME.equals(scf.fieldName)) {
            Condition<ZonedDateTime> zdfCondition = (Condition<ZonedDateTime>) scf.condition;
            if (timeRepresentation == RFC_3339_STRING) {
                return zdfCondition.map(RFC_3339_DATE_TIME_FORMATTER::format);
            } else {
                return zdfCondition.map(zdf -> Date.from(zdf.toInstant()));
            }
        }
        return scf.condition;
    }
}