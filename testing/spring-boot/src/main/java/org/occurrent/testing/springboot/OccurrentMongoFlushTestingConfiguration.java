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

package org.occurrent.testing.springboot;

import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * Wires an {@link OccurrentTestStateClearer} bean over {@link OccurrentMongoFlush#everyCollectionIn(com.mongodb.client.MongoDatabase)}
 * against the context's {@link MongoTemplate}, so {@link OccurrentTestingConfiguration} and
 * {@link OccurrentReactorTestingConfiguration} can apply it with {@code clearingStateWith(..)} without either of them
 * referring to MongoDB.
 * <p>
 * {@link OccurrentTestingImportSelector} imports this class only when {@link EnableOccurrentTesting#clearState()} is
 * {@code true} and both {@code occurrent-testing-mongodb} and {@code MongoTemplate} are on the classpath, so a
 * non-Mongo application never loads it and this is the only file in the module allowed to mention MongoDB types.
 *
 * @see EnableOccurrentTesting
 */
@Configuration(proxyBeanMethods = false)
class OccurrentMongoFlushTestingConfiguration {

    /**
     * Empties every collection in the template's database before each test, index-preserving rather than dropping,
     * for the reasons {@link OccurrentMongoFlush} documents.
     *
     * @param mongoTemplate the context's {@code MongoTemplate} bean
     * @return the clearer {@code OccurrentTestingConfiguration} and {@code OccurrentReactorTestingConfiguration} apply
     */
    @Bean
    OccurrentTestStateClearer occurrentTestStateClearer(MongoTemplate mongoTemplate) {
        OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(mongoTemplate.getDb());
        return flush::run;
    }
}
