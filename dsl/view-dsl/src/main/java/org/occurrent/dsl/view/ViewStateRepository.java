/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.view;


import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An interface that finds and saves the view state. If you're using Kotlin, see the <code>org.occurrent.dsl.view.fetch</code>
 * function in the <code>ViewStateRepositoryExtensions.kt</code> file to avoid the <code>Optional</code> returned by <code>findById</code>.
 *
 * @param <S>  The state to store
 * @param <ID> The id that uniquely identifies the state
 */
public interface ViewStateRepository<S extends @Nullable Object, ID> {
    Optional<@NonNull S> findById(@NonNull ID id);

    void save(@NonNull ID id, @NonNull S state);

    default S findByIdOrElse(@NonNull ID id, View<S, ?> view) {
        return findByIdOrElse(id, view.initialState());
    }

    default S findByIdOrElse(@NonNull ID id, S initialState) {
        return findById(id).orElse(initialState);
    }

    /**
     * Read the state for every id in {@code ids}, in the same shape
     * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0076-batch-command-dispatch-seam.md">ADR 76</a>
     * settled for {@code CommandDispatcher.dispatchAll}: a seam an implementation may exploit, not a guarantee the
     * framework provides. The default loops {@link #findById(Object)} one id at a time, so behaviour is unchanged for
     * every existing repository, including one built from two lambdas through {@link #create(Function, BiConsumer)}.
     * An id absent from {@code ids} is not present as a key of the returned map, the same as an empty
     * {@link #findById(Object)} result. An implementation backed by a store that supports an {@code _id in (..)} query
     * can override this to answer with one round trip instead of {@code ids.size()}.
     *
     * @param ids The ids to read state for.
     * @return The state found for each id, absent ids omitted.
     */
    default Map<@NonNull ID, @NonNull S> findAllById(Collection<@NonNull ID> ids) {
        Map<ID, S> result = new LinkedHashMap<>();
        for (ID id : ids) {
            findById(id).ifPresent(state -> result.put(id, state));
        }
        return result;
    }

    /**
     * Save the state for every entry in {@code states}, in the same shape
     * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0076-batch-command-dispatch-seam.md">ADR 76</a>
     * settled for {@code CommandDispatcher.dispatchAll}: a seam an implementation may exploit, not a guarantee the
     * framework provides. The default loops {@link #save(Object, Object)} one id at a time, so behaviour is unchanged
     * for every existing repository. Takes a {@link Map} rather than the {@link Iterable} Spring Data's
     * {@code saveAll} takes, because this repository is keyed externally and the association between an id and its
     * state would otherwise be lost.
     * <p>
     * <strong>This is not atomic across ids, and this method does not promise that it is.</strong> An override that
     * writes several ids in one store round trip and fails partway leaves some ids durable and some not, exactly as
     * the looping default does when an individual {@link #save(Object, Object)} call fails partway through the loop.
     * An implementation that can make the write atomic should, but nothing in this type enforces it.
     * <p>
     * <strong>An override trades per-id optimistic-locking retry for fewer round trips.</strong> A caller that retries
     * a lost update by re-reading the winner's state and reapplying the change recovers correctly when the write is
     * {@link #save(Object, Object)} one id at a time, because a failed save leaves that id's state unchanged. It does
     * not work across an overridden {@code saveAll} that writes several ids in one round trip and reports no per-id
     * outcome: a retry after such a failure would re-read ids it already wrote and reapply their changes a second
     * time. An implementation that overrides this method for fewer round trips should make the write atomic if it can,
     * rather than leave retry silently unsafe.
     *
     * @param states The state to save, keyed by id.
     */
    default void saveAll(Map<@NonNull ID, @NonNull S> states) {
        states.forEach(this::save);
    }

    static <S extends @Nullable Object, ID> ViewStateRepository<S, ID> create(Function<@NonNull ID, @Nullable S> findById, BiConsumer<@NonNull ID, @NonNull S> save) {
        return new ViewStateRepository<>() {
            @Override
            public Optional<S> findById(@NonNull ID id) {
                return Optional.ofNullable(findById.apply(id));
            }

            @Override
            public void save(@NonNull ID id, @NonNull S state) {
                save.accept(id, state);
            }
        };
    }
}
