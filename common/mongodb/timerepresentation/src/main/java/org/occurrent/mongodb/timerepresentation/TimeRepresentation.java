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

package org.occurrent.mongodb.timerepresentation;


import java.time.OffsetDateTime;
import java.util.Date;

/**
 * An enum describing different ways of representing (persisting) the "time" field of the cloud event in the database.
 * <br>
 * <br>
 * <b>!!IMPORTANT!!</b><br>
 * Changing this setting for an EventStore implementation may lead to unexpected behavior so choose the setting carefully!
 */
public enum TimeRepresentation {
    /**
     * Persist the {@code CloudEvent} "time" field as an RFC 3339 string. This string is able to represent both
     * nanoseconds and a timezone so this is recommended for apps that need to store this information or
     * if you are uncertain of whether this is required in the future.
     * <br>
     * <br>
     * <p>Note that the downside of using this is that you cannot do range queries on the "time" field.
     * If you need nanosecond precision and/or timezone information <i>and</i> need to time queries
     * then it's recommended to add a custom field (extension) to your {@code CloudEvent}'s
     * where you convert the {@link OffsetDateTime} to a {@link Date}. Then index this field and
     * query it using the Mongo Client API (or a derivative such as MongoTemplate in Spring).
     * </p>
     */
    RFC_3339_STRING,

    /**
     * Persist the {@code CloudEvent} "time" field as a MongoDB [Date](https://docs.mongodb.com/manual/reference/method/Date/#behavior).
     * Note the behavior of a Date in MongoDB:
     *
     * <pre>
     * Internally, Date objects are stored as a signed 64-bit integer representing the number of milliseconds since the Unix epoch (Jan 1, 1970).
     * </pre>
     * <p>
     * This is <i>really</i> important because if you choose the represent "time" as a Date then you will loose nanosecond precision
     * <i>and</i> timezone information. But for many application this is fine. For example if you're already representing date/time in your
     * application with a {@link Date} you can safely use this option. If you don't need to do queries on the time field of the
     * {@code CloudEvent} then it's recommended to use {@link TimeRepresentation#RFC_3339_STRING}.
     *
     * <br><br>
     * The benefit of using this approach is that you can do range queries etc on the "time" field on the cloud event. This can be
     * really useful for certain types on analytics or projections (such as show the 10 latest number of started games) without
     * writing any custom code. Note that if you choose to go with {@link TimeRepresentation#RFC_3339_STRING}
     * you can of course writing these kinds of queries yourself directly towards MongoDB given that you've stored a "Date" in an
     * extension field of the {@code CloudEvent}.
     */
    DATE
}
