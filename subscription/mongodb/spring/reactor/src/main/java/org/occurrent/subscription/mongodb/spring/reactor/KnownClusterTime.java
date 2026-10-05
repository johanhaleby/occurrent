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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.reactivestreams.client.MongoCluster;
import org.bson.BsonTimestamp;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoClusterCapable;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The newest cluster time the MongoDB driver has seen on the client behind a {@link ReactiveMongoOperations}. The
 * driver keeps it only in its internal {@code com.mongodb.internal.connection.ClusterClock}, so this finds that clock
 * once, by reflection, and then reads it without any I/O. When the clock can't be found, every read answers
 * {@code null}.
 */
@NullMarked
final class KnownClusterTime {
    private static final Logger log = LoggerFactory.getLogger(KnownClusterTime.class);

    // Names in the driver's internals, which a driver upgrade can change without a compile error
    private static final String MONGO_CLIENT_IMPL = "com.mongodb.reactivestreams.client.internal.MongoClientImpl";
    private static final String CLUSTER = "com.mongodb.internal.connection.Cluster";
    private static final String CLUSTER_CLOCK = "com.mongodb.internal.connection.ClusterClock";

    private static final KnownClusterTime UNREADABLE = new KnownClusterTime(null, null);

    private final @Nullable Object clock;
    private final @Nullable Method getClusterTime;

    private KnownClusterTime(@Nullable Object clock, @Nullable Method getClusterTime) {
        this.clock = clock;
        this.getClusterTime = getClusterTime;
    }

    /**
     * Finds the clock of the client behind {@code mongo}. Logs a warning and returns a {@code KnownClusterTime}
     * whose reads all answer {@code null} when it can't.
     */
    static KnownClusterTime of(ReactiveMongoOperations mongo) {
        try {
            if (!(mongo instanceof ReactiveMongoTemplate template)
                    || !(template.getMongoDatabaseFactory() instanceof ReactiveMongoClusterCapable clusterCapable)) {
                warnCannotRead(mongo.getClass().getName() + " is not a " + ReactiveMongoTemplate.class.getName() + " whose database factory gives access to the MongoClient", null);
                return UNREADABLE;
            }
            MongoCluster mongoCluster = clusterCapable.getMongoCluster();
            ClassLoader driverClassLoader = MongoCluster.class.getClassLoader();
            Class<?> mongoClientImpl = Class.forName(MONGO_CLIENT_IMPL, false, driverClassLoader);
            if (!mongoClientImpl.isInstance(mongoCluster)) {
                warnCannotRead(mongoCluster.getClass().getName() + " is not the driver's " + MONGO_CLIENT_IMPL, null);
                return UNREADABLE;
            }
            Method getCluster = mongoClientImpl.getDeclaredMethod("getCluster");
            getCluster.setAccessible(true);
            Object cluster = getCluster.invoke(mongoCluster);
            Object clock = Class.forName(CLUSTER, false, driverClassLoader).getMethod("getClock").invoke(cluster);
            Method getClusterTime = Class.forName(CLUSTER_CLOCK, false, driverClassLoader).getMethod("getClusterTime");
            KnownClusterTime knownClusterTime = new KnownClusterTime(clock, getClusterTime);
            // Read once, so a clock that answers something other than a BsonTimestamp is found here
            knownClusterTime.readOrThrow();
            return knownClusterTime;
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            warnCannotRead("looking up the driver's internal clock failed", e);
            return UNREADABLE;
        }
    }

    boolean isReadable() {
        return clock != null;
    }

    /**
     * @return The newest cluster time the client has seen, or {@code null} when it has seen none or the clock can't be read
     */
    @Nullable BsonTimestamp read() {
        if (clock == null) {
            return null;
        }
        try {
            return readOrThrow();
        } catch (ReflectiveOperationException | RuntimeException e) {
            // The driver throws when the calling thread is interrupted while it waits for the clock's lock
            log.debug("Couldn't read the cluster time the MongoDB driver knows", e);
            return null;
        }
    }

    private @Nullable BsonTimestamp readOrThrow() throws IllegalAccessException, InvocationTargetException {
        if (getClusterTime == null) {
            return null;
        }
        return (BsonTimestamp) getClusterTime.invoke(clock);
    }

    private static void warnCannotRead(String reason, @Nullable Throwable throwable) {
        String message = "Can't read the cluster time the MongoDB driver knows, because " + reason + ". A subscription that starts at the present then starts from the server's clock alone, "
                + "so it can skip events written right after subscribe(..) returns when that clock is stepped forward.";
        if (throwable == null) {
            log.warn(message);
        } else {
            log.warn(message, throwable);
        }
    }
}
