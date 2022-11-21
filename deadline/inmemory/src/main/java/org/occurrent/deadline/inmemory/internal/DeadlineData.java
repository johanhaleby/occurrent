/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.deadline.inmemory.internal;

import org.occurrent.deadline.api.blocking.Deadline;

import java.util.Objects;
import java.util.StringJoiner;

public class DeadlineData {
    public final String id;
    public final String category;
    public final Deadline deadline;
    public final Object data;

    public DeadlineData(String id, String category, Deadline deadline, Object data) {
        this.id = id;
        this.category = category;
        this.deadline = deadline;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeadlineData)) return false;
        DeadlineData that = (DeadlineData) o;
        return Objects.equals(id, that.id) && Objects.equals(category, that.category) && Objects.equals(deadline, that.deadline) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, category, deadline, data);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DeadlineData.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("category='" + category + "'")
                .add("deadline=" + deadline)
                .add("data=" + data)
                .toString();
    }
}
