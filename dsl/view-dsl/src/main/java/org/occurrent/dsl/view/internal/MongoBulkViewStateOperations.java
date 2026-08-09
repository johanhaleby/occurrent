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

package org.occurrent.dsl.view.internal;

import com.mongodb.bulk.BulkWriteResult;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared bulk-read and bulk-write behaviour for the Mongo-backed {@code ViewStateRepository} implementations the
 * library ships (the {@code MongoOperations}-based repository in {@code SpringMongoViewExtensions.kt} and the
 * default projection store in {@code MongoProjectionStoreProvider}). Both build a repository around a raw
 * {@link MongoOperations} and a Mongo-mapped state type, so the {@code findAllById}/{@code saveAll} overrides that
 * turn a batched replay into one round trip are identical for both, down to id-type coercion, optimistic-locking and
 * duplicate-key exception translation.
 */
public final class MongoBulkViewStateOperations {

    private MongoBulkViewStateOperations() {
    }

    /**
     * Reads every id in {@code ids} with a single {@code _id in (..)} query, in the same shape the looping
     * {@code ViewStateRepository.findAllById} default produces: a {@link LinkedHashMap} in {@code ids} iteration
     * order, with an id that has no stored state simply absent from the returned map.
     */
    @SuppressWarnings("unchecked")
    public static <ID, S> Map<ID, S> findAllById(MongoOperations mongoOperations, Class<S> stateType, Collection<ID> ids) {
        Map<ID, S> result = new LinkedHashMap<>();
        if (ids.isEmpty()) {
            return result;
        }

        MongoPersistentEntity<S> entity = (MongoPersistentEntity<S>) mongoOperations.getConverter().getMappingContext().getRequiredPersistentEntity(stateType);
        MongoPersistentProperty idProperty = entity.getRequiredIdProperty();
        ConversionService conversionService = mongoOperations.getConverter().getConversionService();

        Map<ID, Object> documentIdByInputId = new LinkedHashMap<>();
        List<Object> documentIds = new ArrayList<>(ids.size());
        for (ID id : ids) {
            Object documentId = toDocumentId(id, idProperty.getType(), conversionService);
            documentIdByInputId.put(id, documentId);
            documentIds.add(documentId);
        }

        Query query = Query.query(Criteria.where(idProperty.getName()).in(documentIds));
        List<S> found = mongoOperations.find(query, stateType);

        Map<Object, S> foundByDocumentId = new HashMap<>();
        for (S state : found) {
            Object documentId = entity.getIdentifierAccessor(state).getIdentifier();
            foundByDocumentId.put(documentId, state);
        }

        for (ID id : ids) {
            S state = foundByDocumentId.get(documentIdByInputId.get(id));
            if (state != null) {
                result.put(id, state);
            }
        }
        return result;
    }

    /**
     * Writes {@code states} with as few round trips as {@code @Version}-aware optimistic locking allows: one bulk
     * write for entries that can be blindly upserted (new documents, and updates to a state type with no
     * {@code @Version}), and, when the state type carries {@code @Version}, a second bulk write of id-and-version
     * matched replacements for entries that are updates. The two are separate so a lost optimistic-locking race in
     * the second batch can be detected from the aggregate matched count and turned into an
     * {@link OptimisticLockingFailureException}, exactly as a single {@code MongoOperations.save} does for a
     * versioned entity.
     * <p>
     * Not atomic across entries, the same as the looping {@code ViewStateRepository.saveAll} default: a batch that
     * fails partway leaves some entries durable and some not.
     */
    @SuppressWarnings("unchecked")
    public static <S> void saveAll(MongoOperations mongoOperations, Class<S> stateType, List<S> states) {
        if (states.isEmpty()) {
            return;
        }

        MongoPersistentEntity<S> entity = (MongoPersistentEntity<S>) mongoOperations.getConverter().getMappingContext().getRequiredPersistentEntity(stateType);
        MongoPersistentProperty idProperty = entity.getRequiredIdProperty();
        ConversionService conversionService = mongoOperations.getConverter().getConversionService();

        BulkOperations plainOps = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, stateType);
        boolean hasPlainOps = false;
        BulkOperations versionedOps = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, stateType);
        int versionedOpsCount = 0;

        if (entity.hasVersionProperty()) {
            MongoPersistentProperty versionProperty = entity.getRequiredVersionProperty();
            for (S state : states) {
                if (entity.isNew(state)) {
                    // MongoOperations.insert initializes an unset @Version to 0 (1 if the field is a primitive)
                    // before writing, so a subsequent versioned update has something to match against. Raw
                    // BulkOperations.insert skips that entity lifecycle step, so it is replicated here.
                    ConvertingPropertyAccessor<S> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(state), conversionService);
                    accessor.setProperty(versionProperty, versionProperty.getType().isPrimitive() ? 1 : 0);
                    plainOps.insert(accessor.getBean());
                    hasPlainOps = true;
                } else {
                    ConvertingPropertyAccessor<S> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(state), conversionService);
                    Number currentVersion = accessor.getProperty(versionProperty, Number.class);
                    long nextVersion = currentVersion == null ? 0L : currentVersion.longValue() + 1;
                    accessor.setProperty(versionProperty, nextVersion);
                    S toSave = accessor.getBean();

                    Object documentId = entity.getIdentifierAccessor(state).getIdentifier();
                    Query query = Query.query(Criteria.where(idProperty.getName()).is(documentId).and(versionProperty.getName()).is(currentVersion));
                    versionedOps.replaceOne(query, toSave);
                    versionedOpsCount++;
                }
            }
        } else {
            for (S state : states) {
                if (entity.isNew(state)) {
                    plainOps.insert(state);
                } else {
                    Object documentId = entity.getIdentifierAccessor(state).getIdentifier();
                    Query query = Query.query(Criteria.where(idProperty.getName()).is(documentId));
                    plainOps.replaceOne(query, state, FindAndReplaceOptions.empty().upsert());
                }
                hasPlainOps = true;
            }
        }

        if (hasPlainOps) {
            execute(plainOps);
        }
        if (versionedOpsCount > 0) {
            BulkWriteResult result = execute(versionedOps);
            if (result.getMatchedCount() < versionedOpsCount) {
                throw new OptimisticLockingFailureException(
                        "Cannot save one or more entities of type " + stateType.getName() + " in this batch; has at least one been modified meanwhile");
            }
        }
    }

    private static Object toDocumentId(Object id, Class<?> idPropertyType, ConversionService conversionService) {
        if (idPropertyType.isInstance(id)) {
            return id;
        }
        if (conversionService.canConvert(id.getClass(), idPropertyType)) {
            Object converted = conversionService.convert(id, idPropertyType);
            if (converted != null) {
                return converted;
            }
        }
        return id;
    }

    // DefaultBulkOperations#execute wraps a MongoBulkWriteException in a BulkOperationException without running it
    // through MongoExceptionTranslator first, so a duplicate-key write error surfaces as BulkOperationException
    // instead of DuplicateKeyException here, unlike every other write path in this library. Unwrap and retranslate
    // so a caller sees the same DuplicateKeyException a single MongoOperations.save(..) would have thrown.
    private static BulkWriteResult execute(BulkOperations bulkOperations) {
        try {
            return bulkOperations.execute();
        } catch (BulkOperationException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                DataAccessException translated = MongoExceptionTranslator.DEFAULT_EXCEPTION_TRANSLATOR.translateExceptionIfPossible(cause);
                if (translated instanceof DuplicateKeyException duplicateKeyException) {
                    throw duplicateKeyException;
                }
            }
            throw e;
        }
    }
}
