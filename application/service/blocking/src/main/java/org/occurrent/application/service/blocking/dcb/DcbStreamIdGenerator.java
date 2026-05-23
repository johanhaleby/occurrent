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

package org.occurrent.application.service.blocking.dcb;

import java.util.Set;

/**
 * Chooses the Occurrent stream id used to store events written through the DCB API.
 * <p>
 * Dynamic Consistency Boundary rules are expressed by DCB tags and append conditions,
 * not by this stream id. The generated id is only a storage placement choice for the
 * CloudEvents that are still persisted in Occurrent stream storage.
 */
@FunctionalInterface
public interface DcbStreamIdGenerator {

    /**
     * Generates a storage stream id for events written under the supplied DCB boundary tags.
     *
     * @param boundaryTags the tags that participate in the current consistency boundary
     * @return the Occurrent stream id to use for storage
     */
    String generateStreamId(Set<String> boundaryTags);
}
