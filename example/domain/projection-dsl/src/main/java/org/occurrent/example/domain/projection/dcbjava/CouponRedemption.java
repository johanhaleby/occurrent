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

import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;

/**
 * A single-instance DCB read model built with the Java handler builder: "has this coupon been redeemed?". The read
 * boundary is one tag, the coupon code, so the projection reads only the events that ever mentioned that coupon. The
 * same descriptor answers the question either eventually (subscription-fed) or strongly (query-folded on demand).
 */
public final class CouponRedemption {

    private CouponRedemption() {
    }

    public static DcbProjection<Boolean, CouponEvent, String> isCouponRedeemedProjection(String code) {
        Projection<Boolean, CouponEvent, String> projection = Projection.<Boolean, CouponEvent, String>builder(false)
                .id(event -> code)
                .on(CouponRedeemed.class, (redeemed, event) -> true)
                .build();
        return new DcbProjection<>(projection, DcbCriteria.tags(Tag.of("coupon", code)));
    }
}
