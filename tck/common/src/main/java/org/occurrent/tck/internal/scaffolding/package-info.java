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

/**
 * Scaffolding internal to the stack of changes that fixes issues 1230 and 1297. It is not part of the TCK's API, no
 * implementation outside this repository may depend on it, and it never ships in a release.
 * <p>
 * The late-commit suites state guarantees that the MongoDB stores do not give yet. Until the fix lands, those stores
 * run the suites with {@link org.occurrent.tck.internal.scaffolding.ExpectedGuaranteeViolation}, which turns the
 * violation into a pass and turns a guarantee that starts holding into a failure, so a fix that lands cannot leave its
 * entry behind. Every guarantee check in those suites throws {@link org.occurrent.tck.internal.scaffolding.GuaranteeViolated}
 * for the extension to recognise, and every other failure (a hold that never engaged, a fixture that cannot hold at all)
 * fails as usual.
 * <p>
 * The fix layer that clears the last use of {@code ExpectedGuaranteeViolation} deletes this whole package. The build
 * fails a release that still contains it, see {@code ScaffoldingNeverShipsInAReleaseTest}.
 */
package org.occurrent.tck.internal.scaffolding;
