/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.mongodb.internal;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Interal representation of a stream version before and after a write.
 * Don't depend on this class since it's supposed to be internal!
 */
public class StreamVersionDiff {
    public final long oldStreamVersion;
    public final long newStreamVersion;

    public StreamVersionDiff(long oldStreamVersion, long newStreamVersion) {
        this.oldStreamVersion = oldStreamVersion;
        this.newStreamVersion = newStreamVersion;
    }

    public static StreamVersionDiff of(long oldStreamVersion, long newStreamVersion) {
        return new StreamVersionDiff(oldStreamVersion, newStreamVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamVersionDiff)) return false;
        StreamVersionDiff that = (StreamVersionDiff) o;
        return oldStreamVersion == that.oldStreamVersion && newStreamVersion == that.newStreamVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldStreamVersion, newStreamVersion);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StreamVersionDiff.class.getSimpleName() + "[", "]")
                .add("oldStreamVersion=" + oldStreamVersion)
                .add("newStreamVersion=" + newStreamVersion)
                .toString();
    }
}
