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

package org.occurrent.application.service.dcb;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Derives DCB {@link Tag tags} from domain events before they are stored as CloudEvents.
 * <p>
 * Tags describe the Dynamic Consistency Boundary that future reads and append
 * conditions can match. The application service stores them in the {@code dcbtags}
 * CloudEvent extension.
 */
@FunctionalInterface
@NullMarked
public interface TagGenerator<E> {

    /**
     * Returns the DCB {@link Tag tags} that should be attached to {@code event}.
     *
     * @param event the domain event about to be written
     * @return the tags that make the event visible to relevant DCB queries
     */
    Set<Tag> tags(E event);

    /**
     * Composes several taggers into one whose result, for a given event, is the set-union of the
     * tags returned by each of {@code taggers}, preserving the order in which the taggers were supplied.
     *
     * @param taggers the taggers to combine
     * @param <E>     the event type
     * @return a tagger producing the union of tags from {@code taggers}
     */
    static <E> TagGenerator<E> compose(List<? extends TagGenerator<? super E>> taggers) {
        Objects.requireNonNull(taggers, "taggers cannot be null");
        List<? extends TagGenerator<? super E>> copy = List.copyOf(taggers);
        return event -> {
            Set<Tag> union = new LinkedHashSet<>();
            for (TagGenerator<? super E> tagger : copy) {
                union.addAll(tagger.tags(event));
            }
            return union;
        };
    }

    /**
     * Composes two or more taggers into one whose result, for a given event, is the set-union of the tags returned by
     * each, preserving the order in which they were supplied. Requiring two arguments keeps the varargs form from being
     * called with a pointless single tagger. Use {@link #compose(List)} when the count is dynamic.
     *
     * @param first  the first tagger to combine
     * @param second the second tagger to combine
     * @param rest   any further taggers to combine
     * @param <E>    the event type
     * @return a tagger producing the union of tags from the supplied taggers
     */
    @SafeVarargs
    static <E> TagGenerator<E> compose(TagGenerator<? super E> first, TagGenerator<? super E> second, TagGenerator<? super E>... rest) {
        Objects.requireNonNull(first, "first cannot be null");
        Objects.requireNonNull(second, "second cannot be null");
        Objects.requireNonNull(rest, "rest cannot be null");
        List<TagGenerator<? super E>> copy = new ArrayList<>();
        copy.add(first);
        copy.add(second);
        Collections.addAll(copy, rest);
        return event -> {
            Set<Tag> union = new LinkedHashSet<>();
            for (TagGenerator<? super E> tagger : copy) {
                union.addAll(tagger.tags(event));
            }
            return union;
        };
    }
}
