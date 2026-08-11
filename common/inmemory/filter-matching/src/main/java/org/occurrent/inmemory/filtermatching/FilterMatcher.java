/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.EventStoreCloudEventExtensions;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.All;
import org.occurrent.filter.Filter.CapabilityFilter;
import org.occurrent.filter.Filter.CompositionFilter;
import org.occurrent.filter.Filter.CompositionOperator;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.filtermatching.DataFieldReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Check if a cloud event matching a given filter
 */
@NullMarked
public class FilterMatcher {

    public static boolean matchesFilter(CloudEvent cloudEvent, Filter filter) {
        return matchesFilter(cloudEvent, filter, DataFieldReader.refusing());
    }

    public static boolean matchesFilter(CloudEvent cloudEvent, Filter filter, DataFieldReader dataFieldReader) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }

        return switch (filter) {
            case All ignored -> true;
            case SingleConditionFilter scf -> ConditionMatcher.matchesCondition(cloudEvent, scf.fieldName(), scf.condition(), dataFieldReader);
            case CapabilityFilter cpf -> matchesCapabilityFilter(cloudEvent, cpf);
            case CompositionFilter cf -> switch (cf.operator()) {
                case AND -> matchesAndFilter(cloudEvent, cf, dataFieldReader);
                case OR -> cf.filters().stream().anyMatch(f -> matchesFilter(cloudEvent, f, dataFieldReader));
            };
        };
    }

    /**
     * An {@code AND} composition built by chaining {@link Filter#and(Filter, Filter...)} nests left-deep rather than
     * holding every operand in one flat list ({@code a.and(b).and(c)} is {@code AND(AND(a,b),c)}, not
     * {@code AND(a,b,c)}), so a filter with several data-field leaves ANDed together is several nested
     * {@link CompositionFilter}s, one leaf apart. This flattens through all of them into one operand list and walks
     * it strictly left to right, stopping at the first mismatch the same way {@code allMatch} does, so a cheap
     * metadata operand ahead of a data leaf still short-circuits the whole AND before any payload is touched. That
     * matters beyond speed: {@link DataFieldReader#refusing()} throws on a read, and the store-query path (unlike a
     * subscription, which refuses an unreadable filter before the first event) reaches {@code matchesFilter} with
     * that reader and no earlier gate, so a data leaf that short-circuiting would never have reached must not be
     * read.
     * <p>
     * Only a contiguous run of two or more data-field leaves, the ones the left-to-right walk actually reaches
     * together, is resolved with one {@link DataFieldReader#readAll} call, so the payload behind a byte-backed event
     * is still parsed once for that run instead of once per leaf. A batched leaf is then evaluated straight from
     * that result, skipping {@link DataFieldReader#read} and the field-name branch in {@link ConditionMatcher}
     * entirely, since a {@code DataFieldReader} round trip for a value already in hand costs more than it saves,
     * which is what keeps a Map-backed event (already cheap to read, the production MongoDB path) from paying for a
     * batch it did not need. A run ends at the first non-data operand or the end of the operand list. A lone data
     * leaf, or one separated from its neighbours by a non-data operand, reads its own path the way it always did,
     * since batching one path with nothing to share a parse with would only add a {@code Map} allocation around the
     * read it was always going to be.
     */
    private static boolean matchesAndFilter(CloudEvent cloudEvent, CompositionFilter cf, DataFieldReader dataFieldReader) {
        List<Filter> operands = new ArrayList<>();
        flattenAnd(cf, operands);

        int size = operands.size();
        int i = 0;
        while (i < size) {
            Filter operand = operands.get(i);
            String path = dataPath(operand);
            if (path == null) {
                if (!matchesFilter(cloudEvent, operand, dataFieldReader)) {
                    return false;
                }
                i++;
                continue;
            }

            int runStart = i;
            List<String> runPaths = new ArrayList<>();
            for (; i < size; i++) {
                String candidatePath = dataPath(operands.get(i));
                if (candidatePath == null) {
                    break;
                }
                runPaths.add(candidatePath);
            }

            Map<String, Object> resolved = runPaths.size() < 2 ? null : dataFieldReader.readAll(cloudEvent, runPaths);
            for (int j = runStart; j < i; j++) {
                SingleConditionFilter scf = (SingleConditionFilter) operands.get(j);
                boolean matches = resolved == null
                        ? ConditionMatcher.matchesCondition(cloudEvent, scf.fieldName(), scf.condition(), dataFieldReader)
                        : ConditionMatcher.matchesCondition(resolved.getOrDefault(runPaths.get(j - runStart), ConditionMatcher.ABSENT), scf.condition());
                if (!matches) {
                    return false;
                }
            }
        }
        return true;
    }

    // The path a data-field leaf reads, without the leading "data.", or null for anything else, an attribute or
    // extension leaf, a capability filter, or a nested composition. Used both to decide whether an operand joins
    // the batch run being collected and, once it does, as the key readAll's result is looked up by.
    private static String dataPath(Filter operand) {
        if (operand instanceof SingleConditionFilter scf && scf.fieldName().startsWith(Filter.DATA + ".")) {
            return scf.fieldName().substring((Filter.DATA + ".").length());
        }
        return null;
    }

    private static void flattenAnd(Filter filter, List<Filter> operands) {
        if (filter instanceof CompositionFilter cf && cf.operator() == CompositionOperator.AND) {
            for (Filter operand : cf.filters()) {
                flattenAnd(operand, operands);
            }
        } else {
            operands.add(filter);
        }
    }

    /**
     * A predicate that checks everything in {@code filter} except a condition on a field inside an event's {@code data}
     * payload, which it treats as already satisfied.
     * <p>
     * For a caller re-checking a filter against an event a store has already matched, where re-reading the payload is
     * not possible without a {@link DataFieldReader} and not necessary either, because the store applied the real
     * condition to have delivered the event. An attribute or extension is still checked, so a store that honors no
     * filter at all is still held to the part that can be checked here.
     * <p>
     * A predicate rather than a rewritten {@link Filter}, so the widened filter cannot escape and reach a store query,
     * where it would match more than the filter that was written. The widening also happens once, here, rather than per
     * event.
     */
    public static Predicate<CloudEvent> matcherIgnoringPayloadConditions(Filter filter) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }
        Filter withoutPayloadConditions = PayloadConditions.assumingPayloadConditionsMatch(filter);
        return cloudEvent -> matchesFilter(cloudEvent, withoutPayloadConditions);
    }

    /**
     * Whether {@code filter} contains a condition on a field inside an event's {@code data} payload, anywhere in its
     * tree. A caller that owns no {@link DataFieldReader} can check this before registering a subscription, so a
     * filter it can never honor is refused at subscribe time rather than on the first event that would have needed
     * the reader.
     */
    public static boolean referencesPayloadCondition(Filter filter) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }
        return PayloadConditions.hasPayloadCondition(filter);
    }

    private static boolean matchesCapabilityFilter(CloudEvent cloudEvent, CapabilityFilter cpf) {
        // A DCB append always stamps the dcbtags extension on the live CloudEvent; a stream event never carries it.
        boolean isDcbEvent = cloudEvent.getExtension(EventStoreCloudEventExtensions.DCB_TAGS) != null;
        // Exhaustive switch so a new EventStoreCapability constant forces a compile error here rather than being
        // silently treated as a stream event.
        boolean shouldBeDcbEvent = switch (cpf.capability()) {
            case DCB -> true;
            case STREAM -> false;
        };
        return isDcbEvent == shouldBeDcbEvent;
    }
}
