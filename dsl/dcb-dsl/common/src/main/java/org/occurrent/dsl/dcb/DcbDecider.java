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

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@link Decider} paired with the two things DCB execution needs from it: the {@link DcbCriteria} read boundary for a
 * command, and a {@link TagGenerator} for the events it writes. This keeps a feature's read boundary and write tags
 * next to its decision logic rather than in whatever application service wires it to the store.
 * <p>
 * Like {@link Decider}, it widens with {@link #adapt} and combines with {@link #compose}. Composing unions the
 * children's criteria ({@link DcbCriteria#anyOf}) over the commands they recognize, and unions the tags from whichever
 * child recognizes each event.
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
     * <p>
     * {@code criteria} and {@code decider} are expected to agree on which commands they recognize: whenever
     * {@code decider} produces events for a command, {@code criteria} should return a non-null boundary for that same
     * command. This is not enforced here. Building both from a shared command type check, as {@link #adapt} does,
     * keeps them in sync automatically; a hand-built pair that disagrees can under-scope the DCB append condition
     * without either the decider or the criteria function raising an error.
     */
    public static <C, S extends @Nullable Object, E> DcbDecider<C, S, E> from(Decider<C, S, E> decider, Function<C, @Nullable DcbCriteria> criteria, TagGenerator<E> tags) {
        return new DcbDecider<>(decider, criteria, tags);
    }

    /**
     * Builds a {@code DcbDecider} directly from decision parts plus its DCB {@code criteria} and {@code tags}, without
     * naming an intermediate {@link Decider}. Never terminal, see {@link #create(Object, BiFunction, BiFunction, Function, TagGenerator, Predicate)}
     * to also supply an {@code isTerminal} predicate.
     */
    public static <C, S extends @Nullable Object, E> DcbDecider<C, S, E> create(
            S initialState, BiFunction<C, S, List<E>> decide, BiFunction<S, E, S> evolve,
            Function<C, @Nullable DcbCriteria> criteria, TagGenerator<E> tags) {
        return from(Decider.create(initialState, decide, evolve), criteria, tags);
    }

    /**
     * Like {@link #create(Object, BiFunction, BiFunction, Function, TagGenerator)} but also supplies an
     * {@code isTerminal} predicate for the built decider.
     */
    public static <C, S extends @Nullable Object, E> DcbDecider<C, S, E> create(
            S initialState, BiFunction<C, S, List<E>> decide, BiFunction<S, E, S> evolve,
            Function<C, @Nullable DcbCriteria> criteria, TagGenerator<E> tags, Predicate<S> isTerminal) {
        return from(Decider.create(initialState, decide, evolve, isTerminal), criteria, tags);
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
     * {@link Decider#compose(Decider, Decider, Decider[])}. Use {@link #adapt} first to bring each feature decider onto
     * the common types. Requires at least two deciders; use {@link #compose(List)} when the count is only known at
     * runtime. It enforces the same two-decider minimum.
     * <p>
     * The combined {@code criteria} reads the union of the boundaries of the children that recognize the command: each
     * child is asked for its {@link DcbCriteria}, children that return {@code null} (they do not recognize the command)
     * are skipped, and the remaining boundaries are OR-ed together with {@link DcbCriteria#anyOf}. If no child
     * recognizes the command, the combined criteria is {@code null}. This matches {@link Decider#compose}, where a
     * command is offered to every child decider and adapted children silently ignore commands that are not their own,
     * so the resulting state only depends on the children that actually recognize it.
     * <p>
     * <b>{@code MatchAll} collapse:</b> {@link DcbCriteria#anyOf} collapses to {@link DcbCriteria.MatchAll} as soon as
     * any one of the boundaries being combined is itself {@code MatchAll}. So if any child that recognizes a command
     * reads the whole store, the composed criteria for that command silently becomes whole-store too, downgrading every
     * other child's scoped optimistic lock for that command to a whole-store lock. This is rarely what you want:
     * before composing, check whether a child's {@code criteria} function ever returns {@code MatchAll} and, if so,
     * whether that is intentional for the composed decider.
     * <p>
     * The combined {@code tags} is {@link TagGenerator#compose} over the children's tag generators, so an event is
     * tagged by whichever child recognizes it.
     * <p>
     * {@code criteria} and {@code decider} on each child are expected to agree on which commands they recognize, see
     * {@link #from}. {@code compose} does not enforce that agreement.
     */
    @SafeVarargs
    public static <C, E> DcbDecider<C, CompositeState, E> compose(DcbDecider<C, ?, E> first, DcbDecider<C, ?, E> second, DcbDecider<C, ?, E>... rest) {
        Objects.requireNonNull(first, "first cannot be null");
        Objects.requireNonNull(second, "second cannot be null");
        Objects.requireNonNull(rest, "rest cannot be null");
        List<DcbDecider<C, ?, E>> deciders = new ArrayList<>();
        deciders.add(first);
        deciders.add(second);
        Collections.addAll(deciders, rest);
        return compose(deciders);
    }

    /**
     * Like {@link #compose(DcbDecider, DcbDecider, DcbDecider[])} but takes the deciders as a list, for when the
     * number is not known at compile time. Requires at least two deciders.
     */
    public static <C, E> DcbDecider<C, CompositeState, E> compose(List<? extends DcbDecider<C, ?, E>> deciders) {
        Objects.requireNonNull(deciders, "deciders cannot be null");
        if (deciders.size() < 2) {
            throw new IllegalArgumentException("compose requires at least two deciders, got " + deciders.size());
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

    /**
     * Resolve the DCB read boundary for a single {@code command}. Throws {@link IllegalArgumentException} if this
     * decider does not recognize the command (its {@link #criteria} returns {@code null}), since then there is no
     * boundary to read and no decision to make.
     */
    public DcbCriteria criteriaFor(C command) {
        DcbCriteria boundary = criteria.apply(command);
        if (boundary == null) {
            throw new IllegalArgumentException("The decider does not recognize command " + command + ", so there is no boundary to read and no decision to make");
        }
        return boundary;
    }

    /**
     * Resolve the single DCB read boundary shared by all of {@code commands}. Requires at least one command, and
     * requires every command to resolve to the same boundary, since the events they produce are appended atomically
     * under one append condition. Throws {@link IllegalArgumentException} otherwise.
     */
    public DcbCriteria criteriaFor(List<C> commands) {
        if (commands.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one command");
        }
        DcbCriteria first = criteriaFor(commands.getFirst());
        for (int i = 1; i < commands.size(); i++) {
            DcbCriteria boundary = criteriaFor(commands.get(i));
            if (!boundary.equals(first)) {
                throw new IllegalArgumentException("All commands in a single execute must resolve to the same DcbCriteria boundary, they are appended atomically under one condition");
            }
        }
        return first;
    }
}
