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

package org.occurrent.dsl.dcb;

import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.dsl.decider.CompositeState;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A self-describing, composable DCB (Dynamic Consistency Boundary) decision model.
 * <p>
 * A plain {@link Decider} only knows how to decide and evolve. To run it against a DCB event store, a caller must also
 * know which events to read before deciding (the {@link DcbCriteria} read boundary for the incoming command) and which
 * tags to stamp on the events it writes (via a {@link TagGenerator}). {@code DcbDecider} couples those three pieces
 * together so a feature can describe its own read boundary and write tags right next to its decision logic, instead of
 * that knowledge living separately in whatever application service wires the decider to the store.
 * <p>
 * Like {@link Decider}, a {@code DcbDecider} can be widened with {@link #adapt} to a broader command/event type and
 * combined with {@link #compose} into a single decider over several features. Composing preserves the DCB contract:
 * the composed criteria is the union ({@link DcbCriteria#anyOf}) of the boundaries of the children that recognize the
 * command, and the composed tags is the union of tags contributed by whichever child recognizes the event.
 *
 * @param decider  the decision logic: what to do for a command given the current state, and how to fold events into state
 * @param criteria returns the DCB read boundary for a command, or {@code null} if this decider does not apply to that
 *                 command
 * @param tags     returns the DCB tags for an event this decider emits, or an empty set for an event it does not
 *                 recognize
 * @param <C>      the command type
 * @param <S>      the state type
 * @param <E>      the event type
 */
public record DcbDecider<C, S extends @Nullable Object, E>(
        Decider<C, S, E> decider,
        Function<C, @Nullable DcbCriteria> criteria,
        TagGenerator<E> tags
) {

    public DcbDecider {
        Objects.requireNonNull(decider, "decider cannot be null");
        Objects.requireNonNull(criteria, "criteria cannot be null");
        Objects.requireNonNull(tags, "tags cannot be null");
    }

    /**
     * Creates a {@code DcbDecider} from its three components. Equivalent to calling the canonical constructor, provided
     * as a static factory for a more fluent call site.
     */
    public static <C, S extends @Nullable Object, E> DcbDecider<C, S, E> from(Decider<C, S, E> decider, Function<C, @Nullable DcbCriteria> criteria, TagGenerator<E> tags) {
        return new DcbDecider<>(decider, criteria, tags);
    }

    /**
     * Widen a {@code DcbDecider} so it can be used where a decider over broader command and event types is expected,
     * mirroring {@link Decider#adapt(Decider, Class, Class)}. The wrapped decider is widened the same way. The
     * {@code criteria} function is widened to return {@code null} for commands that are not {@code commandType}, and the
     * {@code tags} generator is widened to return an empty set for events that are not {@code eventType}, so a
     * composed criteria/tags computed over the broader types still correctly skips this decider for anything foreign to
     * it.
     *
     * @param d           the feature DcbDecider to widen
     * @param commandType the command type the decider understands
     * @param eventType   the event type the decider understands
     */
    public static <C, S extends @Nullable Object, E, SubC extends C, SubE extends E> DcbDecider<C, S, E> adapt(DcbDecider<SubC, S, SubE> d, Class<SubC> commandType, Class<SubE> eventType) {
        Objects.requireNonNull(d, "DcbDecider cannot be null");
        Objects.requireNonNull(commandType, "commandType cannot be null");
        Objects.requireNonNull(eventType, "eventType cannot be null");
        Decider<C, S, E> widenedDecider = Decider.adapt(d.decider(), commandType, eventType);
        Function<C, @Nullable DcbCriteria> widenedCriteria = c -> commandType.isInstance(c) ? d.criteria().apply(commandType.cast(c)) : null;
        TagGenerator<E> widenedTags = e -> eventType.isInstance(e) ? d.tags().tags(eventType.cast(e)) : Set.of();
        return new DcbDecider<>(widenedDecider, widenedCriteria, widenedTags);
    }

    /**
     * Combine several DcbDeciders that already share the same command and event types into one, mirroring
     * {@link Decider#compose(Decider[])}. Use {@link #adapt} first to bring each feature decider onto the common
     * types.
     * <p>
     * The combined {@code criteria} reads the union of the boundaries of the children that recognize the command: each
     * child is asked for its {@link DcbCriteria}, children that return {@code null} (they do not recognize the command)
     * are skipped, and the remaining boundaries are OR-ed together with {@link DcbCriteria#anyOf}. If no child
     * recognizes the command, the combined criteria is {@code null}. This matches {@link Decider#compose}, where a
     * command is offered to every child decider and adapted children silently ignore commands that are not their own,
     * so the resulting state only depends on the children that actually recognize it.
     * <p>
     * The combined {@code tags} is {@link TagGenerator#compose} over the children's tag generators, so an event is
     * tagged by whichever child recognizes it.
     */
    @SafeVarargs
    public static <C, E> DcbDecider<C, CompositeState, E> compose(DcbDecider<C, ?, E>... deciders) {
        return compose(List.of(deciders));
    }

    /**
     * Like {@link #compose(DcbDecider[])} but takes the deciders as a list, for when the number is not known at compile
     * time.
     */
    public static <C, E> DcbDecider<C, CompositeState, E> compose(List<? extends DcbDecider<C, ?, E>> deciders) {
        Objects.requireNonNull(deciders, "deciders cannot be null");
        if (deciders.isEmpty()) {
            throw new IllegalArgumentException("Cannot compose an empty list of deciders");
        }

        List<Decider<C, ?, E>> childDeciders = new ArrayList<>();
        for (DcbDecider<C, ?, E> d : deciders) {
            Objects.requireNonNull(d, "deciders cannot contain null");
            childDeciders.add(d.decider());
        }
        Decider<C, CompositeState, E> composedDecider = Decider.compose(childDeciders);

        Function<C, @Nullable DcbCriteria> composedCriteria = command -> {
            List<DcbCriteria> parts = deciders.stream()
                    .map(d -> d.criteria().apply(command))
                    .filter(Objects::nonNull)
                    .toList();
            return parts.isEmpty() ? null : DcbCriteria.anyOf(parts);
        };

        TagGenerator<E> composedTags = TagGenerator.compose(deciders.stream().map(DcbDecider::tags).toList());

        return new DcbDecider<>(composedDecider, composedCriteria, composedTags);
    }
}
