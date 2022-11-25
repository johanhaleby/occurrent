/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.command;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.StringJoiner;

public class ChangeName implements Command {

    private final String id;
    private final LocalDateTime time;
    private final String newName;


    public ChangeName(String id, LocalDateTime time, String newName) {
        this.id = id;
        this.time = time;
        this.newName = newName;
    }

    @Override
    public String getId() {
        return id;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public String getNewName() {
        return newName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChangeName)) return false;
        ChangeName that = (ChangeName) o;
        return Objects.equals(id, that.id) && Objects.equals(time, that.time) && Objects.equals(newName, that.newName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, time, newName);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ChangeName.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("time=" + time)
                .add("name='" + newName + "'")
                .toString();
    }
}