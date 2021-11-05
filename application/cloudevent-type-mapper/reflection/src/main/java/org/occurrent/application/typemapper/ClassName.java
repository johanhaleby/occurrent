/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.application.typemapper;

/**
 * The different class name options available for the {@link ReflectionCloudEventTypeMapper}.
 */
public enum ClassName {
    /**
     * Use the simple name of a class to represent the cloud event type
     */
    SIMPLE,
    /**
     * Use the (fully) qualified name of a class to represent the cloud event type.
     * This is not recommended in production systems.
     */
    QUALIFIED
}
