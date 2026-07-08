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

package org.occurrent.example.domain.appointmentscheduling;

import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Runs the example locally against a throwaway MongoDB replica set started with Testcontainers, so no local
 * MongoDB is required. Run this {@code main} and open http://localhost:7000.
 */
public final class LocalLauncher {
    private LocalLauncher() {
    }

    public static void main(String[] args) {
        MongoDBContainer mongo = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version", "8.0")).withReplicaSet();
        mongo.start();
        Bootstrap application = Bootstrap.bootstrap(mongo.getReplicaSetUrl(), 7000);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            application.shutdown();
            mongo.stop();
        }));
        System.out.println("Appointment scheduling is running on http://localhost:7000");
    }
}
