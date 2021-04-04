/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package cool.baymax.aws.taxi.data.analytics.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.WGS84Point;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
/** @author Baymax */
public class GeoUtils {
	private static final BoundingBox NYC = new BoundingBox(new WGS84Point(41.23, -74.78),
			new WGS84Point(40.53, -71.78));

	private static final Logger LOG = LoggerFactory.getLogger(GeoUtils.class);

	public static boolean validCoordinates(Double lat, Double lng) {
		try {
			if (lat == null || lng == null) {
				return false;
			}
			WGS84Point pickup = new WGS84Point(lat, lng);
			return NYC.contains(pickup);
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	public static boolean hasValidCoordinates(TripRecord trip) {
		return validCoordinates(trip.pickUpLat, trip.pickUpLng);
	}
}
