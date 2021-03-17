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

package org.occurrent.subscription.mongodb;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.subscription.SubscriptionPosition;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * A {@link SubscriptionPosition} implementation for MongoDB that provides the operation time
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoOperationTimeSubscriptionPosition implements SubscriptionPosition {
    public final BsonTimestamp operationTime;

    public MongoOperationTimeSubscriptionPosition(BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    public BsonValue getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoOperationTimeSubscriptionPosition)) return false;
        MongoOperationTimeSubscriptionPosition that = (MongoOperationTimeSubscriptionPosition) o;
        return Objects.equals(operationTime, that.operationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationTime);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MongoOperationTimeSubscriptionPosition.class.getSimpleName() + "[", "]")
                .add("operationTime=" + operationTime)
                .toString();
    }

    @Override
    public String asString() {
        return new Document("operationTime", operationTime).toJson();
    }
}