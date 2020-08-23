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

package org.occurrent.eventstore.mongodb.spring.reactor;

import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the <a href="https://projectreactor.io/">project reactor</a> Spring java driver for MongoDB EventStore
 */
public class EventStoreConfig {
    public final String eventStoreCollectionName;
    public final TransactionalOperator transactionalOperator;
    public final TimeRepresentation timeRepresentation;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionalOperator    The transactional operator responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionalOperator transactionalOperator, TimeRepresentation timeRepresentation) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionalOperator, TransactionalOperator.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionalOperator = transactionalOperator;
        this.timeRepresentation = timeRepresentation;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) &&
                Objects.equals(transactionalOperator, that.transactionalOperator) &&
                timeRepresentation == that.timeRepresentation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionalOperator, timeRepresentation);
    }

    @Override
    public String toString() {
        return "EventStoreConfig{" +
                "eventStoreCollectionName='" + eventStoreCollectionName + '\'' +
                ", transactionTemplate=" + transactionalOperator +
                ", timeRepresentation=" + timeRepresentation +
                '}';
    }

    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionalOperator transactionalOperator;
        private TimeRepresentation timeRepresentation;

        /**
         * @param eventStoreCollectionName The collection in which the events are persisted
         * @return A same {@code Builder instance}
         */
        public Builder eventStoreCollectionName(String eventStoreCollectionName) {
            this.eventStoreCollectionName = eventStoreCollectionName;
            return this;
        }

        /**
         * @param transactionalOperator The transactional operator responsible to starting MongoDB transactions
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(TransactionalOperator transactionalOperator) {
            this.transactionalOperator = transactionalOperator;
            return this;
        }

        /**
         * @param reactiveTransactionManager Create a {@link TransactionalOperator} from the supplied {@code reactiveTransactionManager}
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(ReactiveTransactionManager reactiveTransactionManager) {
            this.transactionalOperator = TransactionalOperator.create(reactiveTransactionManager);
            return this;
        }

        /**
         * @param timeRepresentation How time should be represented in the database
         * @return A same {@code Builder instance}
         */
        public Builder timeRepresentation(TimeRepresentation timeRepresentation) {
            this.timeRepresentation = timeRepresentation;
            return this;
        }


        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionalOperator, timeRepresentation);
        }
    }
}