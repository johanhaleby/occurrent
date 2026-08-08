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

package org.occurrent.filtermatching.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import org.jspecify.annotations.Nullable;
import org.occurrent.filtermatching.DataFieldReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link DataFieldReader} that parses the event payload as JSON using Jackson.
 * <p>
 * Only a payload the event itself claims is JSON gets parsed. That follows the same rule MongoDB uses. A missing
 * {@code datacontenttype} means JSON per the CloudEvents spec, and a content type naming {@code json} (for example
 * {@code application/json} or a {@code +json} suffix) also means JSON. Anything else, for example {@code text/plain}, is
 * left alone even if the bytes happen to look like JSON, so a plain-text payload that happens to contain braces never
 * matches a data filter here when it would not match on MongoDB either.
 * <p>
 * A payload that fails to parse, or whose root is not a JSON object, answers {@link Optional#empty()} rather than
 * throwing, because one malformed event must not break a query over the whole store.
 */
public class JacksonDataFieldReader implements DataFieldReader {

    private final ObjectMapper objectMapper;

    /**
     * Create a reader backed by a plain, default-configured {@link ObjectMapper}.
     */
    public JacksonDataFieldReader() {
        this(new ObjectMapper());
    }

    /**
     * Create a reader backed by a caller-supplied {@link ObjectMapper}, so it can share configuration (modules,
     * naming strategy, and so on) with the rest of the application.
     *
     * @param objectMapper the mapper to parse payloads with
     */
    public JacksonDataFieldReader(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, ObjectMapper.class.getSimpleName() + " cannot be null");
    }

    @Override
    public Optional<Object> read(CloudEvent cloudEvent, String path) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Objects.requireNonNull(path, "path cannot be null");

        if (!isJson(cloudEvent.getDataContentType())) {
            return Optional.empty();
        }

        CloudEventData data = cloudEvent.getData();
        if (data == null) {
            return Optional.empty();
        }

        String[] segments = path.split("\\.");

        if (data instanceof PojoCloudEventData<?> pojoData && pojoData.getValue() instanceof Map<?, ?> map) {
            // The store (currently MongoDB's DocumentCloudEventReader) already decoded the payload into a Map, so
            // this walks it directly rather than calling toBytes(), which would serialise that Map back to JSON
            // text only to have the streaming path below parse the text right back into the same shape of data.
            return resolveFromMap(map, segments, 0);
        }

        byte[] bytes = data.toBytes();
        if (bytes == null) {
            return Optional.empty();
        }
        return readByStreaming(bytes, segments);
    }

    /**
     * Resolves every path against the payload in one parse instead of one per path, which is what makes this
     * override worth having, since {@link #read(CloudEvent, String)} reparses byte-backed JSON from the start for
     * each path it is called with, so a composed filter with several leaves on the same event paid for the payload
     * once per leaf before this method existed.
     */
    @Override
    public Map<String, Object> readAll(CloudEvent cloudEvent, Collection<String> paths) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Objects.requireNonNull(paths, "paths cannot be null");

        if (paths.isEmpty()) {
            return Map.of();
        }

        if (!isJson(cloudEvent.getDataContentType())) {
            return Map.of();
        }

        CloudEventData data = cloudEvent.getData();
        if (data == null) {
            return Map.of();
        }

        if (data instanceof PojoCloudEventData<?> pojoData && pojoData.getValue() instanceof Map<?, ?> map) {
            // Already decoded, same as read()'s Map-backed branch, and each path is a handful of cheap map lookups,
            // so there is no parse to share across paths and nothing to gain from a combined pass here.
            Map<String, Object> result = new LinkedHashMap<>();
            for (String path : paths) {
                resolveFromMap(map, path.split("\\."), 0).ifPresent(value -> result.put(path, value));
            }
            return result;
        }

        byte[] bytes = data.toBytes();
        if (bytes == null) {
            return Map.of();
        }
        return readAllByStreaming(bytes, paths);
    }

    /**
     * One parse of the payload that resolves every requested path, grouping paths by the field name they need next
     * at each object level instead of restarting from byte 0 per path. A path whose remaining paths all still need
     * to look inside the current node keeps streaming (an object's fields are scanned once and grouped, an array's
     * elements are visited once and every continuing path is tried against each). A path that ends exactly at the
     * current node, alongside at least one path that needs to look inside it, forces a single materialisation of
     * that node (the same {@link #resolve} would do for a lone terminal path), after which the still-continuing
     * paths are resolved from the materialised value the same way {@link #resolveFromMap} resolves a Map-backed
     * payload, since there is nothing left to stream once the value has been read into memory.
     */
    private Map<String, Object> readAllByStreaming(byte[] bytes, Collection<String> paths) {
        List<PathQuery> queries = new ArrayList<>(paths.size());
        for (String path : paths) {
            queries.add(new PathQuery(path, path.split("\\.")));
        }

        Map<String, Object> resolved = new LinkedHashMap<>();
        try (JsonParser parser = objectMapper.getFactory().createParser(bytes)) {
            if (parser.nextToken() == JsonToken.START_OBJECT) {
                resolveMulti(parser, queries, 0, resolved, true);
            }
        } catch (IOException e) {
            // Malformed JSON, or bytes that are not JSON at all, the same as readByStreaming(). A path already
            // resolved before the parser reached the bad bytes stays resolved, since a fresh read() call for that
            // same path would reach it too, before ever reaching the same malformed position; a path that had not
            // yet been reached stays absent, which is what a fresh read() call for it would also answer, since it
            // would hit the same malformed bytes on its own way to that path.
        }

        // Preserve the caller's path order rather than the order paths happened to resolve in.
        Map<String, Object> ordered = new LinkedHashMap<>();
        for (PathQuery query : queries) {
            if (resolved.containsKey(query.originalPath())) {
                ordered.put(query.originalPath(), resolved.get(query.originalPath()));
            }
        }
        return ordered;
    }

    private record PathQuery(String originalPath, String[] segments) {
    }

    // Mirrors resolve()'s rules (terminal path answers the node's own value, an array traverses element by element
    // without consuming a path segment, an object needs its next segment's field), but for many paths sharing one
    // parser instead of one path owning it alone, so a shared prefix like "person.city" and "person.age" walks
    // "person" once rather than twice.
    private void resolveMulti(JsonParser parser, List<PathQuery> queries, int segmentIndex, Map<String, Object> results, boolean topLevel) throws IOException {
        List<PathQuery> terminal = new ArrayList<>();
        List<PathQuery> continuing = new ArrayList<>();
        for (PathQuery query : queries) {
            if (segmentIndex == query.segments().length) {
                terminal.add(query);
            } else {
                continuing.add(query);
            }
        }

        if (continuing.isEmpty()) {
            // Every active query wants this node's value exactly as it is, so one materialisation covers all of them.
            Object value = objectMapper.readValue(parser, Object.class);
            if (value != null) {
                for (PathQuery query : terminal) {
                    results.put(query.originalPath(), value);
                }
            }
            return;
        }

        if (!terminal.isEmpty()) {
            // A mix of paths ending here and paths that need to look inside, so materialise once and answer both
            // kinds from that single in-memory value instead of parsing it twice.
            Object value = objectMapper.readValue(parser, Object.class);
            if (value != null) {
                for (PathQuery query : terminal) {
                    results.put(query.originalPath(), value);
                }
            }
            for (PathQuery query : continuing) {
                resolveFromMap(value, query.segments(), segmentIndex).ifPresent(v -> results.put(query.originalPath(), v));
            }
            return;
        }

        // Every active query continues past this node, so it is worth staying in streaming mode rather than
        // materialising something most of the paths will only look a piece of.
        if (parser.currentToken() == JsonToken.START_ARRAY) {
            Map<String, List<Object>> matchedByPath = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                Map<String, Object> elementResults = new LinkedHashMap<>();
                resolveMulti(parser, continuing, segmentIndex, elementResults, false);
                for (Map.Entry<String, Object> entry : elementResults.entrySet()) {
                    matchedByPath.computeIfAbsent(entry.getKey(), key -> new ArrayList<>()).add(entry.getValue());
                }
            }
            for (PathQuery query : continuing) {
                List<Object> matched = matchedByPath.get(query.originalPath());
                if (matched != null && !matched.isEmpty()) {
                    results.put(query.originalPath(), matched);
                }
            }
            return;
        }

        if (parser.currentToken() != JsonToken.START_OBJECT) {
            // An opaque value (a bare scalar) with a path continuing past it has nothing to resolve, the same as
            // resolve() answering empty for the same shape.
            return;
        }

        Map<String, List<PathQuery>> bySegment = new LinkedHashMap<>();
        for (PathQuery query : continuing) {
            bySegment.computeIfAbsent(query.segments()[segmentIndex], key -> new ArrayList<>()).add(query);
        }

        // At the top level, once every requested field has been found there is nothing left for a caller to resume
        // from afterwards (the parser is discarded right after this call returns), so scanning the rest of a large
        // trailing field the way EARLY-position payloads have is pure waste. Anywhere else, the object sits inside
        // something that keeps scanning siblings after this call returns (an enclosing object's own field loop, or
        // an array element loop), so every field has to be visited to leave the parser positioned at this object's
        // own closing token.
        int remaining = bySegment.size();
        while (!(topLevel && remaining == 0) && parser.nextToken() == JsonToken.FIELD_NAME) {
            String fieldName = parser.currentName();
            parser.nextToken();
            List<PathQuery> matched = bySegment.remove(fieldName);
            if (matched == null) {
                parser.skipChildren();
            } else {
                resolveMulti(parser, matched, segmentIndex + 1, results, false);
                remaining--;
            }
        }
    }

    /**
     * Streams the payload instead of materialising it into a tree first. A data filter reads one field, so most of
     * the document is irrelevant, and {@link JsonParser#skipChildren()} lets a sibling field's nested object or
     * array pass by without ever being turned into Java objects.
     */
    private Optional<Object> readByStreaming(byte[] bytes, String[] segments) {
        try (JsonParser parser = objectMapper.getFactory().createParser(bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                // MongoDB never stores a document whose top level is anything but an object, so there is nothing to
                // compare a bare array, string or number root against; resolve() below traverses an array it meets
                // partway through a path (an object field whose value is an array), but the payload root itself is
                // not that, it is the whole thing being queried, so it is opaque here the same way it is on Mongo.
                return Optional.empty();
            }
            return resolve(parser, segments, 0);
        } catch (IOException e) {
            // Malformed JSON, or bytes that are not JSON at all. A single bad payload must not fail a query.
            return Optional.empty();
        }
    }

    // Same rules as resolve() below, an array traverses element by element without consuming a path segment, a
    // path continuing past a value with no fields of its own answers empty, but walking a Map/List that already
    // exists rather than a JsonParser's token stream, since there is nothing left to parse.
    private static Optional<Object> resolveFromMap(@Nullable Object current, String[] segments, int segmentIndex) {
        if (segmentIndex == segments.length) {
            return Optional.ofNullable(current);
        }
        if (current instanceof List<?> list) {
            List<Object> matched = new ArrayList<>();
            for (Object element : list) {
                resolveFromMap(element, segments, segmentIndex).ifPresent(matched::add);
            }
            return matched.isEmpty() ? Optional.empty() : Optional.of(matched);
        }
        if (!(current instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        return resolveFromMap(map.get(segments[segmentIndex]), segments, segmentIndex + 1);
    }

    // A dotted path is resolved one segment at a time, the way MongoDB resolves it, with the parser positioned on
    // the current value's token. An array is stepped into element by element, the same "any element" reading
    // FilterMatcher's anyElementMatches gives the result, so items.sku against [{"sku":"a"},{"sku":"b"}] reaches
    // into both rather than stopping at the array; the segment index does not advance for that step, since the
    // array itself named no field. Anything else that is not an object when a field remains to read (a bare number,
    // a bare string, ...) is an opaque value with no field to step into, which covers a non-object root on the
    // first segment and a path that continues past a value with no fields of its own on a later one; MongoDB stops
    // the same way, but only for that case, not for the array case above it.
    private Optional<Object> resolve(JsonParser parser, String[] segments, int segmentIndex) throws IOException {
        if (segmentIndex == segments.length) {
            return Optional.ofNullable(objectMapper.readValue(parser, Object.class));
        }
        if (parser.currentToken() == JsonToken.START_ARRAY) {
            List<Object> matched = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                resolve(parser, segments, segmentIndex).ifPresent(matched::add);
            }
            return matched.isEmpty() ? Optional.empty() : Optional.of(matched);
        }
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            return Optional.empty();
        }
        if (!advanceToField(parser, segments[segmentIndex])) {
            return Optional.empty();
        }
        return resolve(parser, segments, segmentIndex + 1);
    }

    /**
     * Scans the object the parser is positioned inside of for a field named {@code fieldName}, leaving the parser at
     * its value token and returning {@code true} if found. On duplicate field names within one object, which valid
     * JSON should not contain, the first occurrence wins here rather than the last one a full tree parse would keep.
     */
    private static boolean advanceToField(JsonParser parser, String fieldName) throws IOException {
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String currentField = parser.currentName();
            parser.nextToken();
            if (currentField.equals(fieldName)) {
                return true;
            }
            parser.skipChildren();
        }
        return false;
    }

    private static boolean isJson(@Nullable String dataContentType) {
        if (dataContentType == null) {
            // No content-type means application/json per the CloudEvents spec.
            return true;
        }
        String lowerCase = dataContentType.toLowerCase();
        return lowerCase.contains("/json") || lowerCase.contains("+json");
    }
}
