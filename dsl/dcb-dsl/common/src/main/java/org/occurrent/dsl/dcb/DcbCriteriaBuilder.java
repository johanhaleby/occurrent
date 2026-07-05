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

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Builds {@link DcbCriteria} from domain event {@link Class classes} rather than raw CloudEvent type strings.
 * <p>
 * A {@link DcbCriterion} matches on the CloudEvent type string produced at write time by the configured
 * {@link CloudEventTypeMapper} (or {@link CloudEventConverter}). This builder resolves each supplied
 * {@code Class} to that type string through {@link CloudEventTypeGetter#getCloudEventType(Class)}, so the
 * criteria match the same string the events were written with. It never uses {@link Class#getName()} or the
 * simple name, which would not match a mapper that maps types to custom strings.
 * <p>
 * The tag- and combinator-oriented factories ({@link #tags(Tag, Tag...)}, {@link #tagsAnyOf(Tag, Tag...)},
 * {@link #all()}, {@link #anyOf(DcbCriteria, DcbCriteria...)}) are thin passthroughs to {@link DcbCriteria} so a
 * caller can build an entire criteria from a single object.
 *
 * @param <E> the base domain event type
 */
@NullMarked
public final class DcbCriteriaBuilder<E> {

    private final CloudEventTypeGetter<E> typeGetter;

    /**
     * Creates a builder backed by a {@link CloudEventTypeMapper}.
     */
    public DcbCriteriaBuilder(CloudEventTypeMapper<E> typeMapper) {
        this((CloudEventTypeGetter<E>) requireNonNull(typeMapper, CloudEventTypeMapper.class.getSimpleName() + " cannot be null"));
    }

    /**
     * Creates a builder backed by a {@link CloudEventConverter}. The converter's
     * {@link CloudEventConverter#getCloudEventType(Class)} resolves the CloudEvent type string, matching the string the
     * events were written with.
     */
    public DcbCriteriaBuilder(CloudEventConverter<E> cloudEventConverter) {
        this(requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null")::getCloudEventType);
    }

    private DcbCriteriaBuilder(CloudEventTypeGetter<E> typeGetter) {
        this.typeGetter = requireNonNull(typeGetter, CloudEventTypeGetter.class.getSimpleName() + " cannot be null");
    }

    /**
     * Creates a criterion matching events whose CloudEvent type is the type string mapped from {@code type}.
     */
    public DcbCriterion type(Class<? extends E> type) {
        requireNonNull(type, "Type cannot be null");
        return DcbCriteria.type(typeGetter.getCloudEventType(type));
    }

    /**
     * Creates a criterion matching events whose CloudEvent type is any of the type strings mapped from the supplied
     * classes (any-of).
     */
    @SafeVarargs
    public final DcbCriterion types(Class<? extends E> first, Class<? extends E>... rest) {
        requireNonNull(first, "First type cannot be null");
        requireNonNull(rest, "Additional types cannot be null");
        List<String> types = new ArrayList<>();
        types.add(typeGetter.getCloudEventType(first));
        for (Class<? extends E> type : rest) {
            types.add(typeGetter.getCloudEventType(requireNonNull(type, "Type cannot be null")));
        }
        return DcbCriteria.types(types);
    }

    /**
     * Creates a criterion matching events containing all the supplied DCB tags (all-of). Passthrough to
     * {@link DcbCriteria#tags(Tag, Tag...)}.
     */
    public DcbCriterion tags(Tag first, Tag... rest) {
        return DcbCriteria.tags(first, rest);
    }

    /**
     * Creates a criteria matching events that carry any one of the supplied DCB tags. Passthrough to
     * {@link DcbCriteria#tagsAnyOf(Tag, Tag...)}.
     */
    public DcbCriteria tagsAnyOf(Tag first, Tag... rest) {
        return DcbCriteria.tagsAnyOf(first, rest);
    }

    /**
     * Creates a criteria that matches every DCB event. Passthrough to {@link DcbCriteria#all()}.
     */
    public DcbCriteria all() {
        return DcbCriteria.all();
    }

    /**
     * Creates a criteria matching an event when it matches any of the supplied alternatives. Passthrough to
     * {@link DcbCriteria#anyOf(DcbCriteria, DcbCriteria...)}.
     */
    public DcbCriteria anyOf(DcbCriteria first, DcbCriteria... rest) {
        return DcbCriteria.anyOf(first, rest);
    }
}
