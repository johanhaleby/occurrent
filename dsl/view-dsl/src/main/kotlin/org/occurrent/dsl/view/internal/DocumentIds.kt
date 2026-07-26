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

package org.occurrent.dsl.view.internal

import org.springframework.data.mongodb.core.MongoOperations

/**
 * Fails when the stored document's id does not match the id resolved for the event.
 */
fun <S : Any> requireMatchingDocumentId(mongoOperations: MongoOperations, stateType: Class<S>, state: S, resolvedId: Any) {
    val entity = mongoOperations.converter.mappingContext.getPersistentEntity(stateType) ?: return
    val documentId = entity.getIdentifierAccessor(state).identifier
    if (documentId != resolvedId) {
        throw IllegalStateException("the stored document's @Id is " + documentId + " but the id resolved for this event "
                + "is " + resolvedId + ", so reads and writes would use different documents and the read model would "
                + "never accumulate. Make the fold set the document's @Id to the same value the id resolves to.")
    }
}
