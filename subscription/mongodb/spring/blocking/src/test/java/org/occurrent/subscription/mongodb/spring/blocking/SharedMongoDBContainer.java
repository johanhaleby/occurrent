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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * One container for both conformance test classes in this module, rather than one each.
 * <p>
 * The module already had a class managing its own container, and adding two more tripled how many this module starts and
 * stops in one run. That is what a container-launch failure here looks like: the first class to lose the race reports
 * {@code ContainerLaunchException} and every class after it reports "should be started first". Sharing one keeps the
 * count at two.
 * <p>
 * The native driver's module deliberately does not do this. Its pre-existing classes build their container differently,
 * and a holder started from a static initializer there raced them into the same failure this one prevents.
 */
final class SharedMongoDBContainer {

    private static final MongoDBContainer CONTAINER = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    static {
        CONTAINER.start();
    }

    private SharedMongoDBContainer() {
    }

    static String replicaSetUrl(String databaseName) {
        return CONTAINER.getReplicaSetUrl(databaseName);
    }
}
