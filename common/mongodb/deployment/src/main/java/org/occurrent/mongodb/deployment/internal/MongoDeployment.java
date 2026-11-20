/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.mongodb.deployment.internal;

import com.mongodb.MongoCommandException;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.function.Function;

/**
 * What a MongoDB deployment answers to {@code hello}.
 */
@NullMarked
public enum MongoDeployment {
    /**
     * A replica set.
     */
    REPLICA_SET,
    /**
     * A mongos, or a load balancer in front of one.
     */
    MONGOS,
    /**
     * Anything else, such as a standalone server.
     */
    OTHER;

    private static final int COMMAND_NOT_FOUND = 59;

    /**
     * The deployment a store runs on, to be asked once when the store is created. A mongos answers {@code msg} isdbgrid,
     * also through a load balancer, and a replica set member answers a {@code setName}. Every failure except a server
     * that doesn't know {@code hello} propagates.
     *
     * @param runCommand runs a command against the store's database
     */
    public static MongoDeployment detect(Function<Document, Document> runCommand) {
        Document answer = Objects.requireNonNull(helloOrIsMaster(runCommand), "The server sent no answer to hello or isMaster");
        if ("isdbgrid".equals(answer.get("msg"))) {
            return MONGOS;
        }
        return answer.get("setName") != null ? REPLICA_SET : OTHER;
    }

    // An old server, such as MongoDB 4.2.8, doesn't know hello. The Spring templates wrap the driver's exception, so the
    // causes are walked.
    private static Document helloOrIsMaster(Function<Document, Document> runCommand) {
        try {
            return runCommand.apply(new Document("hello", 1));
        } catch (RuntimeException e) {
            Throwable cause = e;
            for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
                if (cause instanceof MongoCommandException commandException && commandException.getErrorCode() == COMMAND_NOT_FOUND) {
                    return runCommand.apply(new Document("isMaster", 1));
                }
            }
            throw e;
        }
    }
}
