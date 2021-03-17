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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.subscription.SubscriptionPosition;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * A {@link SubscriptionPosition} implementation for MongoDB that provides a resumeToken
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoResumeTokenSubscriptionPosition implements SubscriptionPosition {
    public final BsonDocument resumeToken;

    public MongoResumeTokenSubscriptionPosition(BsonDocument resumeToken) {
        this.resumeToken = resumeToken;
    }

    public BsonValue getResumeToken() {
        return resumeToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoResumeTokenSubscriptionPosition)) return false;
        MongoResumeTokenSubscriptionPosition that = (MongoResumeTokenSubscriptionPosition) o;
        return Objects.equals(resumeToken, that.resumeToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resumeToken);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MongoResumeTokenSubscriptionPosition.class.getSimpleName() + "[", "]")
                .add("resumeToken=" + resumeToken)
                .toString();
    }

    @Override
    public String asString() {
        return new Document("resumeToken", resumeToken).toJson();
    }
}