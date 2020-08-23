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

package org.occurrent.example.eventstore.mongodb.spring.projections.adhoc;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Component
public class MostNumberOfWorkouts {

    private final MongoOperations mongo;

    public MostNumberOfWorkouts(MongoOperations mongo) {
        this.mongo = mongo;
    }

    public PersonWithMostNumberOfWorkouts personWithMostNumberOfWorkouts() {
        Aggregation aggregation = newAggregation(
                sortByCount("data.completedBy"),
                limit(1),
                // Spring is a bit strange here, since "data.completedBy" is named "_id"
                // and using "aliases" such as project("count").and("data.completedBy").as("name")
                // has no effect, see https://stackoverflow.com/questions/62479752/spring-data-mongodb-aggregation-rename-id-in-projection
                project("count", "data.completedBy"));
        return mongo.aggregate(aggregation, "events", PersonWithMostNumberOfWorkouts.class).getUniqueMappedResult();
    }

    public static class PersonWithMostNumberOfWorkouts {
        @Id
        private String name;
        private int count;

        public String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        void setCount(int count) {
            this.count = count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PersonWithMostNumberOfWorkouts)) return false;
            PersonWithMostNumberOfWorkouts that = (PersonWithMostNumberOfWorkouts) o;
            return count == that.count &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, count);
        }

        @Override
        public String toString() {
            return "PersonWithMostNumberOfWorkouts{" +
                    "name='" + name + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}