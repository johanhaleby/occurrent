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
package org.occurrent.springboot.mongo.blocking;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Imports {@link OccurrentMongoHealthConfiguration} once every regular configuration class is processed.
 * {@link EnableOccurrent} imports {@link OccurrentMongoAutoConfiguration} with a plain {@code @Import}, and Spring
 * registers the beans of an imported class before the {@code @Bean} methods of the class that imports it. A
 * {@code @ConditionalOnMissingBean} in a class imported that way doesn't see an application bean declared next to
 * {@code @EnableOccurrent}, so the starter's health contributor and the application's replacement would both register
 * and fail startup. Spring Boot's {@code @EnableAutoConfiguration} defers its classes the same way.
 * <p>
 * Selected by name, so the class is read as metadata and its {@code @ConditionalOnClass} skips it without
 * {@code spring-boot-health} on the classpath.
 */
class OccurrentMongoHealthImportSelector implements DeferredImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{"org.occurrent.springboot.mongo.blocking.OccurrentMongoHealthConfiguration"};
    }
}
