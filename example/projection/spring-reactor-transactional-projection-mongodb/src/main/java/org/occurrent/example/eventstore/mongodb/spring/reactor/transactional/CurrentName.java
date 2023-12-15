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

package org.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;
import java.util.StringJoiner;

@Document(collection = "current-name-projection")
@TypeAlias("CurrentName")
public class CurrentName {
    @Id
    private String userId;
    private String name;

    @SuppressWarnings("unused")
    CurrentName() {
    }

    CurrentName(String userId) {
        this(userId, null);
    }

    public CurrentName(String userId, String name) {
        this.userId = userId;
        this.name = name;
    }

    public String getUserId() {
        return userId;
    }

    void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    CurrentName changeName(String name) {
        setName(name);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrentName that)) return false;
        return Objects.equals(userId, that.userId) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, name);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CurrentName.class.getSimpleName() + "[", "]")
                .add("userId='" + userId + "'")
                .add("name='" + name + "'")
                .toString();
    }
}