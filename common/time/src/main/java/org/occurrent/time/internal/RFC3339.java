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

package org.occurrent.time.internal;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Utilities for RFC3339 date/time conversions.
 */
public class RFC3339 {

    public static final DateTimeFormatter RFC_3339_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendOffset("+HH:MM", "Z")
            .optionalEnd()
            .toFormatter();
}
