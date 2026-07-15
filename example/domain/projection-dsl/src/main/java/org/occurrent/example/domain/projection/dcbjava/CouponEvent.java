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

package org.occurrent.example.domain.projection.dcbjava;

/**
 * Coupon events. Top-level (not nested) so the reflection CloudEvent type mapper resolves each from its simple name.
 */
public sealed interface CouponEvent permits CouponIssued, CouponRedeemed {
    String code();
}

record CouponIssued(String code) implements CouponEvent {
}

record CouponRedeemed(String code, String orderId) implements CouponEvent {
}
